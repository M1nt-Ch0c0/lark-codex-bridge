'use strict';

// Stdout is protocol-only. Do not add console.log calls to this process.
const lark = require('@larksuiteoapi/node-sdk');
const { createDurableEventDispatcher } = require('./dispatcher.cjs');

const VERSION = 1;
const PROTOCOL = 'lark-channel';
const HARD_MAX_FRAME_BYTES = 1024 * 1024;
const HARD_MAX_IN_FLIGHT = 64;
const HARD_MAX_OUTBOUND_QUEUE = 128;
const CAPABILITIES = [
  'connection_state',
  'durable_event_ack',
  'inbound_events',
  'graceful_shutdown',
];

let configuredMaxFrameBytes = HARD_MAX_FRAME_BYTES;
let configuredMaxInFlight = HARD_MAX_IN_FLIGHT;
let ackTimeoutMs = 60_000;
let configured = false;
let protocolReady = false;
let shuttingDown = false;
let wsClient;
let sequence = 0;
let inputBuffer = Buffer.alloc(0);
let inputChain = Promise.resolve();
let terminalExitScheduled = false;
const pending = new Map();
const outbound = [];
let writing = false;

function safeStderr(kind) {
  // Never forward SDK messages/errors: they may contain payloads or secrets.
  process.stderr.write(`[channel-sidecar] ${kind}\n`);
}

const safeLogger = {
  error: () => safeStderr('sdk_error'),
  warn: () => safeStderr('sdk_warning'),
  info: () => {},
  debug: () => {},
  trace: () => {},
};

function nextId(prefix) {
  sequence = (sequence + 1) % Number.MAX_SAFE_INTEGER;
  return `${prefix}-${process.pid}-${sequence}`;
}

function validId(value) {
  return typeof value === 'string'
    && value.length > 0
    && value.length <= 128
    && /^[A-Za-z0-9_.-]+$/.test(value);
}

function exactKeys(value, required, optional = []) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const allowed = new Set([...required, ...optional]);
  const keys = Object.keys(value);
  return required.every((key) => Object.hasOwn(value, key))
    && keys.every((key) => allowed.has(key));
}

function pumpOutbound() {
  if (writing || outbound.length === 0) return;
  writing = true;
  const frame = outbound.shift();
  process.stdout.write(frame, () => {
    writing = false;
    pumpOutbound();
  });
}

function writeFrame(value) {
  let encoded;
  try {
    encoded = `${JSON.stringify(value)}\n`;
  } catch (_) {
    return false;
  }
  if (Buffer.byteLength(encoded) - 1 > configuredMaxFrameBytes) return false;
  if (outbound.length >= HARD_MAX_OUTBOUND_QUEUE) return false;
  outbound.push(encoded);
  pumpOutbound();
  return true;
}

function sendState(state, attempt, delayMs, fatal) {
  // The Rust bootstrap expects its correlated configure response before any
  // asynchronous SDK state callback. JavaScript cannot interleave callbacks
  // while configure() flips this flag and publishes the initial snapshot.
  if (!protocolReady) return;
  const frame = {
    v: VERSION,
    type: 'state',
    id: nextId('state'),
    state,
  };
  if (Number.isSafeInteger(attempt) && attempt > 0) frame.attempt = attempt;
  if (Number.isSafeInteger(delayMs) && delayMs >= 0) frame.delay_ms = delayMs;
  if (fatal === true) frame.fatal = true;
  if (!writeFrame(frame)) safeStderr('state_frame_dropped');
}

// The SDK only gives up on a bootstrap code it considers non-retryable, or
// when reconnect attempts are exhausted. Mirror the native transport's
// fail-closed classification: an explicit bootstrap rejection is permanent
// (revoked credentials, disabled app, connection limit), while system busy
// and exhausted reconnects stay restartable. Only the numeric code is read;
// server-provided messages never leave this process.
function isFatalSdkError(error) {
  const message = error && typeof error.message === 'string' ? error.message : '';
  const match = /pullConnectConfig failed: code=(\d+)/.exec(message);
  return match !== null && Number(match[1]) !== 1;
}

function exitAfterProtocolFlush(code) {
  if (terminalExitScheduled) return;
  terminalExitScheduled = true;
  const deadline = Date.now() + 1_000;
  const finish = () => {
    if ((!writing && outbound.length === 0) || Date.now() >= deadline) {
      process.exit(code);
    }
    setTimeout(finish, 5);
  };
  finish();
}

function failPending(reason) {
  for (const [id, waiter] of pending) {
    clearTimeout(waiter.timer);
    pending.delete(id);
    waiter.reject(new Error(reason));
  }
}

function forwardEvent(payload) {
  if (shuttingDown) return Promise.reject(new Error('sidecar is shutting down'));
  if (pending.size >= configuredMaxInFlight) {
    return Promise.reject(new Error('sidecar event capacity exhausted'));
  }
  const id = nextId('event');
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error('durable ack timeout'));
    }, ackTimeoutMs);
    pending.set(id, { resolve, reject, timer });
    if (!writeFrame({ v: VERSION, type: 'event', id, payload })) {
      clearTimeout(timer);
      pending.delete(id);
      reject(new Error('sidecar output capacity exhausted'));
    }
  });
}

async function configure(frame) {
  if (configured || !exactKeys(frame, [
    'v', 'type', 'id', 'app_id', 'app_secret', 'tenant',
    'max_frame_bytes', 'max_in_flight', 'ack_timeout_ms',
  ])) {
    throw new Error('invalid configure frame');
  }
  if (frame.v !== VERSION || frame.type !== 'configure' || !validId(frame.id)) {
    throw new Error('incompatible configure frame');
  }
  if (typeof frame.app_id !== 'string' || !/^cli_[0-9a-fA-F]{16}$/.test(frame.app_id)
      || typeof frame.app_secret !== 'string' || frame.app_secret.length === 0
      || !['feishu', 'lark'].includes(frame.tenant)) {
    throw new Error('invalid provider configuration');
  }
  if (!Number.isSafeInteger(frame.max_frame_bytes)
      || frame.max_frame_bytes <= 0
      || frame.max_frame_bytes > HARD_MAX_FRAME_BYTES
      || !Number.isSafeInteger(frame.max_in_flight)
      || frame.max_in_flight <= 0
      || frame.max_in_flight > HARD_MAX_IN_FLIGHT
      || !Number.isSafeInteger(frame.ack_timeout_ms)
      || frame.ack_timeout_ms <= 0
      || frame.ack_timeout_ms > 5 * 60_000) {
    throw new Error('invalid provider bounds');
  }

  configuredMaxFrameBytes = frame.max_frame_bytes;
  configuredMaxInFlight = frame.max_in_flight;
  ackTimeoutMs = frame.ack_timeout_ms;

  // Keep the original envelope intact so Rust remains the sole normalizer.
  const dispatcher = createDurableEventDispatcher(lark, forwardEvent, safeLogger);

  const domain = frame.tenant === 'feishu' ? lark.Domain.Feishu : lark.Domain.Lark;
  wsClient = new lark.WSClient({
    appId: frame.app_id,
    appSecret: frame.app_secret,
    domain,
    logger: safeLogger,
    loggerLevel: lark.LoggerLevel.error,
    autoReconnect: true,
    handshakeTimeoutMs: 15_000,
    wsConfig: { pingTimeout: 10 },
    onReady: () => sendState('connected'),
    onReconnecting: () => {
      const status = wsClient.getConnectionStatus();
      sendState('reconnecting', Math.max(1, status.reconnectAttempts || 1));
    },
    onReconnected: () => sendState('connected'),
    onError: (error) => {
      sendState('failed', undefined, undefined, isFatalSdkError(error));
      exitAfterProtocolFlush(1);
    },
  });

  configured = true;
  await wsClient.start({ eventDispatcher: dispatcher });
  if (!writeFrame({ v: VERSION, type: 'response', id: frame.id, ok: true })) {
    throw new Error('configuration response capacity exhausted');
  }
  protocolReady = true;
  const status = wsClient.getConnectionStatus();
  sendState(status.state === 'idle' ? 'connecting' : status.state,
    Math.max(1, status.reconnectAttempts || 1));
}

function handleAck(frame) {
  if (!exactKeys(frame, ['v', 'type', 'id', 'ok'], ['data', 'error'])
      || frame.v !== VERSION || frame.type !== 'event_ack' || !validId(frame.id)
      || typeof frame.ok !== 'boolean'
      || (frame.ok && Object.hasOwn(frame, 'error'))
      || (!frame.ok && (typeof frame.error !== 'string'
        || frame.error.length === 0 || frame.error.length > 64
        || Object.hasOwn(frame, 'data')))) {
    throw new Error('invalid event ack');
  }
  const waiter = pending.get(frame.id);
  if (!waiter) throw new Error('unknown event correlation id');
  clearTimeout(waiter.timer);
  pending.delete(frame.id);
  if (frame.ok) waiter.resolve(frame.data);
  else waiter.reject(new Error('rust durable intake rejected the event'));
}

function shutdown(frame) {
  if (!exactKeys(frame, ['v', 'type', 'id'])
      || frame.v !== VERSION || frame.type !== 'shutdown' || !validId(frame.id)) {
    throw new Error('invalid shutdown frame');
  }
  shuttingDown = true;
  failPending('sidecar shutdown');
  if (wsClient) wsClient.close({ force: false });
  writeFrame({ v: VERSION, type: 'response', id: frame.id, ok: true });
  sendState('stopped');
  const finish = () => {
    if (!writing && outbound.length === 0) process.exit(0);
    setTimeout(finish, 5);
  };
  finish();
}

async function handleFrame(frame) {
  if (!frame || typeof frame !== 'object' || Array.isArray(frame)) {
    throw new Error('frame must be an object');
  }
  if (frame.v !== VERSION || typeof frame.type !== 'string'
      || frame.type.length === 0 || frame.type.length > 64 || !validId(frame.id)) {
    throw new Error('invalid frame header');
  }
  if (frame.type === 'configure') return configure(frame);
  if (!configured) throw new Error('sidecar is not configured');
  if (frame.type === 'event_ack') return handleAck(frame);
  if (frame.type === 'shutdown') return shutdown(frame);
  if (frame.type === 'error') {
    throw new Error('rust rejected a protocol message');
  }
  if (!writeFrame({ v: VERSION, type: 'error', id: frame.id, code: 'unknown_message' })) {
    throw new Error('unknown-message response capacity exhausted');
  }
  return undefined;
}

function consumeInput(chunk) {
  inputBuffer = Buffer.concat([inputBuffer, chunk]);
  while (true) {
    const newline = inputBuffer.indexOf(0x0a);
    if (newline < 0) break;
    if (newline > configuredMaxFrameBytes) throw new Error('input frame too large');
    const line = inputBuffer.subarray(0, newline);
    inputBuffer = inputBuffer.subarray(newline + 1);
    if (line.length === 0) throw new Error('empty input frame');
    let frame;
    try {
      frame = JSON.parse(line.toString('utf8'));
    } catch (_) {
      throw new Error('invalid input JSON');
    }
    inputChain = inputChain.then(() => handleFrame(frame));
    inputChain.catch(() => {
      safeStderr('protocol_failure');
      process.exit(2);
    });
  }
  if (inputBuffer.length > configuredMaxFrameBytes) throw new Error('input frame too large');
}

process.on('uncaughtException', () => {
  safeStderr('uncaught_exception');
  process.exit(2);
});
process.on('unhandledRejection', () => {
  safeStderr('unhandled_rejection');
  process.exit(2);
});
process.stdin.on('data', consumeInput);
process.stdin.on('end', () => {
  failPending('rust input closed');
  if (wsClient) wsClient.close({ force: true });
  process.exit(0);
});

writeFrame({
  v: VERSION,
  type: 'hello',
  id: 'hello-1',
  protocol: PROTOCOL,
  capabilities: CAPABILITIES,
  max_frame_bytes: HARD_MAX_FRAME_BYTES,
});
