"use strict";

const {
  HELLO_CAPABILITIES,
  SidecarError,
  validateErrorObject,
  validateMethod,
  validateParams,
} = require("../wire.cjs");

const ADAPTER_CAPABILITIES = HELLO_CAPABILITIES;

class UnsupportedMethodError extends SidecarError {
  constructor() {
    super("unsupported_method", "RPC method is not promoted by this adapter");
    this.name = "UnsupportedMethodError";
  }
}

function projectorFor(projectors, method) {
  const projector = projectors[method];
  if (typeof projector !== "function") {
    throw new UnsupportedMethodError();
  }
  return projector;
}

// Version modules own every promoted method and every version-specific
// projection. This helper only validates envelopes and dispatches to those
// explicit maps; there is deliberately no identity or catch-all path.
function createAdapter(options) {
  const requestProjectors = Object.freeze({ ...options.requestProjectors });
  const responseProjectors = Object.freeze({ ...options.responseProjectors });
  const notificationProjectors = Object.freeze({ ...options.notificationProjectors });
  const serverRequestProjectors = Object.freeze({ ...options.serverRequestProjectors });
  const serverResponseProjectors = Object.freeze({ ...options.serverResponseProjectors });
  const localNotificationProjectors = Object.freeze({
    ...options.localNotificationProjectors,
  });

  return Object.freeze({
    upstreamVersion: options.upstreamVersion,
    adapterVersion: options.adapterVersion,
    capabilities: [...ADAPTER_CAPABILITIES],
    requestMethods: Object.freeze(Object.keys(requestProjectors).sort()),
    notificationMethods: Object.freeze(Object.keys(notificationProjectors).sort()),
    serverRequestMethods: Object.freeze(Object.keys(serverRequestProjectors).sort()),
    localNotificationMethods: Object.freeze(Object.keys(localNotificationProjectors).sort()),

    toUpstreamRequest(method, params) {
      validateMethod(method);
      validateParams(params);
      return {
        method,
        params: projectorFor(requestProjectors, method)(params),
      };
    },

    fromUpstreamResponse(method, result) {
      validateMethod(method);
      if (result === undefined) {
        throw new SidecarError(
          "adapter_contract",
          "upstream response is outside the stable domain contract",
        );
      }
      return projectorFor(responseProjectors, method)(result);
    },

    toUpstreamNotification(method, params) {
      validateMethod(method);
      validateParams(params);
      return {
        method,
        params: projectorFor(localNotificationProjectors, method)(params),
      };
    },

    fromUpstreamNotification(method, params) {
      validateMethod(method);
      const projector = notificationProjectors[method];
      if (typeof projector !== "function") {
        // Unreviewed upstream notifications are filtered inside the sidecar.
        // Neither their provider payload nor their method name crosses Rust's
        // stable-domain boundary.
        return null;
      }
      return { method, params: projector(params) };
    },

    fromUpstreamServerRequest(method, params) {
      validateMethod(method);
      const projector = serverRequestProjectors[method];
      if (typeof projector !== "function") {
        return null;
      }
      const projected = projector(params);
      return projected === null ? null : { method, params: projected };
    },

    toUpstreamServerResponse(method, result, error) {
      const projector = projectorFor(serverResponseProjectors, method);
      if (error !== undefined) {
        validateErrorObject(error);
        return {
          error: {
            code: error.code,
            message: "bridge rejected server request",
          },
        };
      }
      return { result: projector(result) };
    },
  });
}

module.exports = {
  ADAPTER_CAPABILITIES,
  UnsupportedMethodError,
  createAdapter,
};
