'use strict';

const fs = require('node:fs');

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

function write(value, done) {
  process.stdout.write(`${JSON.stringify(value)}\n`, done);
}

function state(value, attempt) {
  const frame = { v: VERSION, type: 'state', id: `state-${value}-${attempt || 1}`, state: value };
  if (attempt) frame.attempt = attempt;
  write(frame);
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

function handle(frame) {
  if (frame.type === 'configure') {
    if (mode === 'silence') return;
    configured = true;
    secondRun = Boolean(marker && fs.existsSync(marker));
    write({ v: VERSION, type: 'response', id: frame.id, ok: true });
    state('connecting', 1);
    state('connected');
    if (secondRun) {
      fs.writeFileSync(`${marker}.second`, 'connected');
    } else if (mode === 'handler-timeout') {
      write({ v: VERSION, type: 'event', id: 'event-timeout', payload: { ordinal: 'timeout' } });
    } else {
      firstRunEvents();
    }
    return;
  }
  if (!configured) process.exit(3);
  if (frame.type === 'error' && frame.id === 'mystery-1' && frame.code === 'unknown_message') {
    seen.unknown = true;
    maybeCrash();
    return;
  }
  if (frame.type === 'event_ack') {
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

if (mode === 'oversize-hello') {
  process.stdout.write(`${'x'.repeat(1024 * 1024 + 1)}\n`);
} else if (mode === 'bad-version') {
  write({
    v: 2,
    type: 'hello',
    id: 'hello-1',
    protocol: 'lark-channel',
    capabilities: ['connection_state', 'durable_event_ack', 'inbound_events', 'graceful_shutdown'],
    max_frame_bytes: 1024 * 1024,
  });
} else {
  write({
    v: VERSION,
    type: 'hello',
    id: 'hello-1',
    protocol: 'lark-channel',
    capabilities: ['connection_state', 'durable_event_ack', 'inbound_events', 'graceful_shutdown'],
    max_frame_bytes: 1024 * 1024,
  });
}
