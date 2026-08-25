'use strict';

// Offline guard for the exact upstream receipt assumption in SDK 1.72.0.
// It calls the compiled low-level WSClient method with a fake reassembled
// event, so no network or credentials are involved.
const assert = require('node:assert/strict');
const lark = require('@larksuiteoapi/node-sdk');
const { createDurableEventDispatcher } = require('./dispatcher.cjs');

const silentLogger = {
  error: () => {},
  warn: () => {},
  info: () => {},
  debug: () => {},
  trace: () => {},
};

function deferred() {
  let resolve;
  const promise = new Promise((done) => { resolve = done; });
  return { promise, resolve };
}

function eventFrame() {
  return {
    headers: [
      { key: 'type', value: 'event' },
      { key: 'message_id', value: 'contract-message' },
      { key: 'sum', value: '1' },
      { key: 'seq', value: '0' },
      { key: 'trace_id', value: 'contract-trace' },
    ],
    payload: new Uint8Array(),
  };
}

function receiptCode(frame) {
  return JSON.parse(new TextDecoder('utf-8').decode(frame.payload)).code;
}

async function main() {
  const rawEnvelope = {
    schema: '2.0',
    header: { event_type: 'im.message.receive_v1' },
    event: { message: { message_id: 'om_contract' } },
  };
  const entered = deferred();
  const release = deferred();
  let fail = false;
  const dispatcher = createDurableEventDispatcher(lark, async (actual) => {
    assert.strictEqual(actual, rawEnvelope, 'the adapter must preserve the raw envelope');
    entered.resolve();
    await release.promise;
    if (fail) throw new Error('static contract failure');
    return undefined;
  }, silentLogger);
  const client = new lark.WSClient({
    appId: 'cli_0123456789abcdef',
    appSecret: 'offline-contract-secret',
    logger: silentLogger,
    loggerLevel: lark.LoggerLevel.error,
  });
  client.dataCache.mergeData = () => rawEnvelope;
  const receipts = [];
  client.sendMessage = (frame) => receipts.push(frame);

  const success = client.handleEventData(eventFrame());
  await entered.promise;
  await new Promise((done) => setImmediate(done));
  assert.equal(receipts.length, 0, 'WSClient must await EventDispatcher.invoke');
  release.resolve();
  await success;
  assert.equal(receipts.length, 1);
  assert.equal(receiptCode(receipts[0]), 200);

  fail = true;
  await client.handleEventData(eventFrame());
  assert.equal(receipts.length, 2);
  assert.equal(receiptCode(receipts[1]), 500,
    'a rejected Rust decision must produce an upstream failure receipt');
  client.dataCache.destroy();
}

main().catch(() => {
  process.stderr.write('[channel-sidecar-contract] failed\n');
  process.exit(1);
});
