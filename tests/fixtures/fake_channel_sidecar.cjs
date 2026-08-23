'use strict';

const fs = require('node:fs');
const { spawn } = require('node:child_process');

const VERSION = 1;
const mode = process.argv[2] || 'lifecycle';
const marker = process.argv[3];
let buffer = Buffer.alloc(0);
let configured = false;
let secondRun = false;
const seen = {
  positive: false,
  backpressure: false,
  durableFailure: false,
  unknown: false,
};
const ackOrder = [];

const countedModes = new Set([
  'protocol-descendant',
  'crash-descendant',
  'eof',
  'clean-exit',
  'duplicate-active',
  'connect-crash',
  'configure-failed',
]);
let run = 1;
if (marker && countedModes.has(mode)) {
  const counter = `${marker}.runs`;
  const previous = fs.existsSync(counter) ? Number(fs.readFileSync(counter, 'utf8')) : 0;
  run = previous + 1;
  fs.writeFileSync(counter, String(run));
}

function write(value, done) {
  process.stdout.write(`${JSON.stringify(value)}\n`, done);
}

function state(value, attempt) {
  const frame = { v: VERSION, type: 'state', id: `state-${value}-${attempt || 1}`, state: value };
  if (attempt) frame.attempt = attempt;
  write(frame);
}

function spawnHeartbeatDescendant() {
  const heartbeat = `${marker}.heartbeat-1`;
  const script = [
    "'use strict';",
    "const fs = require('node:fs');",
    'const path = process.argv[1];',
    "fs.appendFileSync(path, 'x');",
    "setInterval(() => fs.appendFileSync(path, 'x'), 20);",
  ].join('');
  const descendant = spawn(process.execPath, ['-e', script, heartbeat], {
    stdio: 'ignore',
  });
  fs.writeFileSync(`${marker}.pid-1`, String(descendant.pid));
  return heartbeat;
}

const descendantModes = new Set([
  'startup-descendant',
  'timeout-descendant',
  'configure-failed',
  'protocol-descendant',
  'crash-descendant',
  'shutdown-descendant',
  'drop-descendant',
]);
const heartbeat = marker && descendantModes.has(mode) && run === 1
  ? spawnHeartbeatDescendant()
  : undefined;

function afterHeartbeatReady(callback) {
  if (!heartbeat || fs.existsSync(heartbeat)) {
    callback();
    return;
  }
  setTimeout(() => afterHeartbeatReady(callback), 5);
}

function maybeCrash() {
  if (secondRun || !Object.values(seen).every(Boolean)) return;
  fs.writeFileSync(marker, JSON.stringify(seen));
  setTimeout(() => process.exit(42), 20);
}

function firstRunEvents() {
  write({ v: VERSION, type: 'mystery', id: 'mystery-1' });
  write({ v: VERSION, type: 'event', id: 'event-a', payload: { ordinal: 'a' } });
  setTimeout(() => {
    write({ v: VERSION, type: 'event', id: 'event-b', payload: { ordinal: 'b' } });
    write({ v: VERSION, type: 'event', id: 'event-c', payload: { ordinal: 'c' } });
  }, 25);
}

function connect(frame) {
  configured = true;
  write({ v: VERSION, type: 'response', id: frame.id, ok: true });
  state('connecting', 1);
  state('connected');
}

function configure(frame) {
  if (mode === 'silence' || mode === 'timeout-descendant') {
    fs.writeFileSync(`${marker}.configured`, 'seen');
    return;
  }
  if (mode === 'configure-failed') {
    configured = true;
    write({ v: VERSION, type: 'response', id: frame.id, ok: true });
    state('connecting', 1);
    state('failed', 1);
    setTimeout(() => process.exit(19), 100);
    return;
  }

  secondRun = Boolean(marker && fs.existsSync(marker));
  connect(frame);

  if (run > 1 && new Set([
    'protocol-descendant',
    'crash-descendant',
    'eof',
    'clean-exit',
  ]).has(mode)) {
    fs.writeFileSync(`${marker}.second`, 'connected');
    return;
  }
  if (run > 1 && mode === 'duplicate-active') {
    write({
      v: VERSION,
      type: 'event',
      id: 'event-duplicate',
      payload: { ordinal: 'after-restart' },
    });
    return;
  }
  if (secondRun && mode === 'lifecycle') {
    fs.writeFileSync(`${marker}.second`, 'connected');
    return;
  }
  if (mode === 'handler-timeout') {
    write({ v: VERSION, type: 'event', id: 'event-timeout', payload: { ordinal: 'timeout' } });
  } else if (mode === 'lifecycle') {
    firstRunEvents();
  } else if (mode === 'protocol-descendant') {
    setTimeout(() => process.stdout.write('{not-json}\n'), 75);
  } else if (mode === 'crash-descendant' || mode === 'connect-crash') {
    setTimeout(() => process.exit(42), 50);
  } else if (mode === 'eof') {
    // POSIX can close the protocol fd while the wrapper remains alive. The
    // Windows test uses `clean-exit` because closing CRT fd 1 there does not
    // close Node's libuv pipe.
    setTimeout(() => fs.closeSync(1), 75);
  } else if (mode === 'clean-exit') {
    setTimeout(() => process.exit(0), 75);
  } else if (mode === 'stderr-oversize') {
    process.stderr.write(Buffer.alloc(512 * 1024, 0x78), () => {
      process.stderr.write('\nsmall-record\n');
      write({ v: VERSION, type: 'event', id: 'event-stderr', payload: { ordinal: 'stderr' } });
    });
  } else if (mode === 'duplicate-active') {
    write({ v: VERSION, type: 'event', id: 'event-duplicate', payload: { ordinal: 'first' } });
    setTimeout(() => {
      write({ v: VERSION, type: 'event', id: 'event-duplicate', payload: { ordinal: 'second' } });
    }, 100);
  } else if (mode === 'reverse-acks') {
    write({ v: VERSION, type: 'event', id: 'event-slow', payload: { ordinal: 'slow' } });
    write({ v: VERSION, type: 'event', id: 'event-fast', payload: { ordinal: 'fast' } });
  }
}

function handle(frame) {
  if (frame.type === 'configure') {
    configure(frame);
    return;
  }
  if (!configured) process.exit(3);
  if (frame.type === 'error' && frame.id === 'mystery-1' && frame.code === 'unknown_message') {
    seen.unknown = true;
    maybeCrash();
    return;
  }
  if (frame.type === 'event_ack') {
    if (mode === 'duplicate-active' && run > 1
        && frame.id === 'event-duplicate' && frame.ok) {
      fs.writeFileSync(`${marker}.second`, 'correlation-released');
      return;
    }
    if (mode === 'stderr-oversize' && frame.id === 'event-stderr' && frame.ok) {
      fs.writeFileSync(marker, 'acked-after-oversized-stderr');
      return;
    }
    if (mode === 'reverse-acks' && frame.ok) {
      ackOrder.push(frame.id);
      if (ackOrder.length === 2) fs.writeFileSync(marker, JSON.stringify(ackOrder));
      return;
    }
    if (frame.ok) {
      seen.positive = true;
      state('backoff', 7);
    } else if (frame.error === 'backpressure') {
      seen.backpressure = true;
      state('backoff', 11);
    } else if (frame.error === 'durable_intake_failed') {
      seen.durableFailure = true;
      state('connecting', 9);
    } else if (frame.error === 'durable_intake_timeout' && mode === 'handler-timeout') {
      fs.writeFileSync(marker, 'durable_intake_timeout');
    }
    maybeCrash();
    return;
  }
  if (frame.type === 'shutdown') {
    if (mode === 'shutdown-descendant') {
      fs.writeFileSync(`${marker}.shutdown-requested`, 'observed');
      return;
    }
    if (marker) fs.writeFileSync(`${marker}.shutdown`, 'clean');
    write({ v: VERSION, type: 'response', id: frame.id, ok: true }, () => process.exit(0));
  }
}

process.stdin.on('data', (chunk) => {
  buffer = Buffer.concat([buffer, chunk]);
  while (true) {
    const newline = buffer.indexOf(0x0a);
    if (newline < 0) break;
    const line = buffer.subarray(0, newline);
    buffer = buffer.subarray(newline + 1);
    handle(JSON.parse(line.toString('utf8')));
  }
});

function hello(version = VERSION) {
  write({
    v: version,
    type: 'hello',
    id: 'hello-1',
    protocol: 'lark-channel',
    capabilities: ['connection_state', 'durable_event_ack', 'inbound_events', 'graceful_shutdown'],
    max_frame_bytes: 1024 * 1024,
  });
}

afterHeartbeatReady(() => {
  if (mode === 'oversize-hello') {
    process.stdout.write(`${'x'.repeat(1024 * 1024 + 1)}\n`);
  } else if (mode === 'bad-version' || mode === 'startup-descendant') {
    hello(2);
  } else {
    hello();
  }
});
