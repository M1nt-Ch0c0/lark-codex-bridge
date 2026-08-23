'use strict';

// Keep the SDK-specific override in a tiny testable module. WSClient invokes
// this public EventDispatcher entrypoint with the reassembled raw envelope.
function createDurableEventDispatcher(lark, forwardEvent, logger) {
  return new (class DurableEventDispatcher extends lark.EventDispatcher {
    async invoke(rawEnvelope, params) {
      const eventType = rawEnvelope && rawEnvelope.schema
        ? rawEnvelope.header && rawEnvelope.header.event_type
        : rawEnvelope && rawEnvelope.event && rawEnvelope.event.type;
      if (eventType === 'im.message.receive_v1') {
        return forwardEvent(rawEnvelope);
      }
      return super.invoke(rawEnvelope, params);
    }
  })({
    logger,
    loggerLevel: lark.LoggerLevel.error,
  });
}

module.exports = { createDurableEventDispatcher };
