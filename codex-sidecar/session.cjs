"use strict";

const crypto = require("node:crypto");
const { once } = require("node:events");

const { UnsupportedMethodError } = require("./adapters/common.cjs");
const { PriorityWriteQueue } = require("./priority-write-queue.cjs");
const {
  SidecarError,
  boundedLines,
  classifyRpcFrame,
  correlationKey,
  parseJsonLine,
} = require("./wire.cjs");
const { drainStderr, terminateChild } = require("./upstream.cjs");

const LOCAL_REQUEST_TIMEOUT_MS = 30_000;
// The bridge handler path may spend 15s fetching from Lark, 30s in ffmpeg, and
// 60s in ASR. Keep a separate reverse-request envelope with
// queue/scheduling margin instead of applying the ordinary upstream RPC
// deadline to an approval that is still being reviewed.
const SERVER_REQUEST_TIMEOUT_MS = 180_000;
const CONTROL_REQUEST_METHODS = new Set(["turn/interrupt"]);
const CONTROL_NOTIFICATION_METHODS = new Set([
  "error",
  "turn/completed",
  "serverRequest/resolved",
  "thread/status/changed",
  "thread/queue/changed",
  "thread/settings/updated",
]);

function boundedWait(promise, milliseconds, code) {
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      const timer = setTimeout(
        () => reject(new SidecarError(code, "bounded shutdown operation timed out")),
        milliseconds,
      );
      timer.unref();
    }),
  ]);
}

class RetiredCorrelations {
  constructor(maximum) {
    this.maximum = Math.max(16, maximum);
    this.values = new Map();
  }

  has(key) {
    return this.values.has(key);
  }

  add(key) {
    this.values.delete(key);
    this.values.set(key, true);
    while (this.values.size > this.maximum) {
      this.values.delete(this.values.keys().next().value);
    }
  }
}

class RetiredMappings {
  constructor(maximum) {
    this.maximum = Math.max(16, maximum);
    this.values = new Map();
  }

  set(key, value) {
    this.values.delete(key);
    this.values.set(key, value);
    while (this.values.size > this.maximum) {
      this.values.delete(this.values.keys().next().value);
    }
  }

  take(key) {
    const value = this.values.get(key);
    this.values.delete(key);
    return value;
  }
}

function rpcError(id, code, message) {
  return { id, error: { code, message } };
}

function responsePriority(method) {
  return CONTROL_REQUEST_METHODS.has(method) ? "control" : "normal";
}

class ProtocolSession {
  constructor(options) {
    this.configuration = options.configuration;
    this.adapter = options.adapter;
    this.child = options.child;
    this.localInput = options.localInput;
    this.localLines = options.localLines;
    this.localOutput = options.localOutput;
    this.nonce = crypto.randomBytes(8).toString("hex");
    this.nextUpstreamId = 1;
    this.nextServerId = 1;
    this.pendingRequestsByUpstream = new Map();
    this.activeLocalRequestIds = new Set();
    this.pendingServerByLocal = new Map();
    this.activeUpstreamServerIds = new Set();
    const retiredBound = options.configuration.maxPending * 4;
    this.retiredLocalRequestIds = new RetiredCorrelations(retiredBound);
    this.retiredUpstreamResponseIds = new RetiredCorrelations(retiredBound);
    this.retiredLocalServerIds = new RetiredCorrelations(retiredBound);
    this.retiredUpstreamServerIds = new RetiredCorrelations(retiredBound);
    this.expiredLocalServerIds = new RetiredMappings(retiredBound);
    this.serverResolutionIds = new RetiredMappings(retiredBound);
    this.retiredServerResolutionIds = new RetiredCorrelations(retiredBound);
    this.localRequestTimeoutMs = options.localRequestTimeoutMs ?? LOCAL_REQUEST_TIMEOUT_MS;
    this.serverRequestTimeoutMs =
      options.serverRequestTimeoutMs ?? SERVER_REQUEST_TIMEOUT_MS;
    if (
      !Number.isSafeInteger(this.localRequestTimeoutMs) ||
      this.localRequestTimeoutMs <= 0 ||
      !Number.isSafeInteger(this.serverRequestTimeoutMs) ||
      this.serverRequestTimeoutMs <= 0
    ) {
      throw new SidecarError("invalid_configuration", "protocol timeout is invalid");
    }
    this.stopping = false;
    this.stopPromise = new Promise((resolve) => {
      this.resolveStop = resolve;
    });

    const queueOptions = {
      maxFrameBytes: options.configuration.maxFrameBytes,
      maxFrames: options.configuration.maxWriteQueueFrames,
      maxBytes: options.configuration.maxWriteQueueBytes,
      onError: (error) => this.fail(error),
    };
    this.toLocal = new PriorityWriteQueue(options.localOutput, queueOptions);
    this.toUpstream = new PriorityWriteQueue(options.child.stdin, queueOptions);
  }

  fail(error) {
    if (!this.stopping) {
      this.stopping = true;
      const safe =
        error instanceof SidecarError
          ? error
          : new SidecarError("session_failed", "protocol session failed");
      this.resolveStop({ kind: "fatal", error: safe });
    }
  }

  requestShutdown() {
    if (!this.stopping) {
      this.stopping = true;
      this.resolveStop({ kind: "shutdown" });
    }
  }

  async run() {
    const stderr = drainStderr(this.child.stderr);
    const local = this.#consumeLocal().then(
      (outcome) => outcome,
      (error) => ({ kind: "fatal", error }),
    );
    const upstream = this.#consumeUpstream().then(
      () => ({
        kind: "fatal",
        error: new SidecarError("upstream_stdout_eof", "Codex app-server stdout closed"),
      }),
      (error) => ({ kind: "fatal", error }),
    );
    const childExit = once(this.child, "exit").then(
      ([code, signal]) => ({
        kind: "fatal",
        error: new SidecarError(
          "upstream_exited",
          code === 0 && signal === null
            ? "Codex app-server exited unexpectedly"
            : "Codex app-server failed",
        ),
      }),
      () => ({
        kind: "fatal",
        error: new SidecarError("upstream_failed", "Codex app-server failed"),
      }),
    );

    let outcome = await Promise.race([local, upstream, childExit, this.stopPromise]);
    if (outcome.kind === "local_eof" || outcome.kind === "shutdown") {
      this.stopping = true;
      outcome = { kind: "graceful" };
      try {
        await boundedWait(
          this.toUpstream.end(),
          this.configuration.shutdownGraceMs,
          "upstream_shutdown_timeout",
        );
      } catch {
        // Cleanup below remains bounded.
      }
    }

    this.#clearAllTimeouts();
    if (outcome.kind === "fatal") {
      this.toUpstream.abort(outcome.error);
    }
    await terminateChild(this.child, this.configuration.shutdownGraceMs);
    if (outcome.kind === "graceful") {
      try {
        await boundedWait(
          this.toLocal.waitIdle(),
          this.configuration.shutdownGraceMs,
          "local_shutdown_timeout",
        );
      } catch {
        outcome = {
          kind: "fatal",
          error: new SidecarError("local_write_failed", "local protocol output failed"),
        };
      }
    }
    if (outcome.kind === "fatal") {
      this.toLocal.abort(outcome.error);
    }
    if (!this.localInput.destroyed) {
      this.localInput.destroy();
    }
    await boundedWait(stderr, this.configuration.shutdownGraceMs, "stderr_shutdown_timeout").catch(
      () => {},
    );
    return outcome;
  }

  async #consumeLocal() {
    for await (const line of this.localLines) {
      const message = classifyRpcFrame(parseJsonLine(line));
      const outcome = await this.#handleLocal(message);
      if (outcome !== null) {
        return outcome;
      }
    }
    return { kind: "local_eof" };
  }

  async #consumeUpstream() {
    for await (const line of boundedLines(
      this.child.stdout,
      () => this.configuration.maxFrameBytes,
    )) {
      const message = classifyRpcFrame(parseJsonLine(line));
      await this.#handleUpstream(message);
    }
  }

  async #handleLocal(message) {
    switch (message.kind) {
      case "request":
        if (message.method === "sidecar/shutdown") {
          if (
            message.params !== undefined &&
            message.params !== null &&
            Object.keys(message.params).length !== 0
          ) {
            throw new SidecarError(
              "invalid_params",
              "sidecar shutdown params must be absent, null, or empty",
            );
          }
          const localKey = correlationKey(message.id);
          if (
            this.activeLocalRequestIds.has(localKey) ||
            this.retiredLocalRequestIds.has(localKey) ||
            this.pendingServerByLocal.has(localKey) ||
            this.retiredLocalServerIds.has(localKey)
          ) {
            throw new SidecarError(
              "correlation_reuse",
              "sidecar shutdown reused a correlation",
            );
          }
          this.retiredLocalRequestIds.add(localKey);
          await this.toLocal.enqueue({ id: message.id, result: {} }, "control");
          return { kind: "shutdown" };
        }
        this.#forwardLocalRequest(message);
        return null;
      case "notification":
        this.#forwardLocalNotification(message);
        return null;
      case "response":
      case "error":
        this.#forwardLocalServerResponse(message);
        return null;
      default:
        throw new SidecarError("invalid_rpc", "unknown local RPC frame");
    }
  }

  #forwardLocalRequest(message) {
    const localKey = correlationKey(message.id);
    if (
      this.activeLocalRequestIds.has(localKey) ||
      this.retiredLocalRequestIds.has(localKey) ||
      this.pendingServerByLocal.has(localKey) ||
      this.retiredLocalServerIds.has(localKey)
    ) {
      throw new SidecarError("correlation_reuse", "local request reused a correlation");
    }
    let adapted;
    try {
      adapted = this.adapter.toUpstreamRequest(message.method, message.params);
    } catch (error) {
      if (error instanceof UnsupportedMethodError) {
        this.toLocal.enqueue(
          rpcError(message.id, -32601, "method is not promoted by the active adapter"),
          "control",
        );
        this.retiredLocalRequestIds.add(localKey);
        return;
      }
      throw error;
    }
    if (this.#totalPending() >= this.configuration.maxPending) {
      this.toLocal.enqueue(
        rpcError(message.id, -32020, "sidecar pending-request capacity is exhausted"),
        "control",
      );
      this.retiredLocalRequestIds.add(localKey);
      return;
    }

    const upstreamId = `bridge:${this.nonce}:${this.nextUpstreamId}`;
    this.nextUpstreamId += 1;
    const upstreamKey = correlationKey(upstreamId);
    const pending = {
      localId: message.id,
      localKey,
      upstreamKey,
      method: message.method,
      timeout: null,
    };
    pending.timeout = setTimeout(() => {
      this.fail(new SidecarError("request_timeout", "upstream request did not complete"));
    }, this.localRequestTimeoutMs);
    this.pendingRequestsByUpstream.set(upstreamKey, pending);
    this.activeLocalRequestIds.add(localKey);
    try {
      this.toUpstream.enqueue(
        { id: upstreamId, method: adapted.method, params: adapted.params },
        responsePriority(message.method),
      );
    } catch (error) {
      clearTimeout(pending.timeout);
      this.pendingRequestsByUpstream.delete(upstreamKey);
      this.activeLocalRequestIds.delete(localKey);
      if (error instanceof SidecarError && error.code === "write_queue_full") {
        this.toLocal.enqueue(
          rpcError(message.id, -32020, "sidecar write capacity is exhausted"),
          "control",
        );
        this.retiredLocalRequestIds.add(localKey);
        return;
      }
      throw error;
    }
  }

  #forwardLocalNotification(message) {
    const adapted = this.adapter.toUpstreamNotification(message.method, message.params);
    this.toUpstream.enqueue(
      { method: adapted.method, params: adapted.params },
      CONTROL_NOTIFICATION_METHODS.has(message.method) ? "control" : "normal",
    );
  }

  #forwardLocalServerResponse(message) {
    const localKey = correlationKey(message.id);
    const pending = this.pendingServerByLocal.get(localKey);
    if (pending === undefined) {
      if (this.expiredLocalServerIds.take(localKey) !== undefined) {
        // The timeout response has already completed this upstream request.
        // Consume exactly one racing Rust response; another response for the
        // same retired correlation remains a fail-closed protocol violation.
        return;
      }
      throw new SidecarError(
        this.retiredLocalServerIds.has(localKey) ? "late_response" : "unknown_correlation",
        "local response does not match an active server request",
      );
    }
    clearTimeout(pending.timeout);
    this.pendingServerByLocal.delete(localKey);
    this.activeUpstreamServerIds.delete(pending.upstreamKey);
    this.retiredLocalServerIds.add(localKey);
    this.retiredUpstreamServerIds.add(pending.upstreamKey);
    this.serverResolutionIds.set(pending.upstreamKey, pending.localId);
    const adapted = this.adapter.toUpstreamServerResponse(
      pending.method,
      message.kind === "response" ? message.result : undefined,
      message.kind === "error" ? message.error : undefined,
    );
    if (Object.hasOwn(adapted, "error")) {
      this.toUpstream.enqueue({ id: pending.upstreamId, error: adapted.error }, "control");
    } else {
      this.toUpstream.enqueue({ id: pending.upstreamId, result: adapted.result }, "control");
    }
  }

  async #handleUpstream(message) {
    switch (message.kind) {
      case "response":
      case "error":
        this.#completeLocalRequest(message);
        break;
      case "notification":
        this.#forwardUpstreamNotification(message);
        break;
      case "request":
        this.#forwardUpstreamServerRequest(message);
        break;
      default:
        throw new SidecarError("invalid_rpc", "unknown upstream RPC frame");
    }
  }

  #completeLocalRequest(message) {
    const upstreamKey = correlationKey(message.id);
    const pending = this.pendingRequestsByUpstream.get(upstreamKey);
    if (pending === undefined) {
      throw new SidecarError(
        this.retiredUpstreamResponseIds.has(upstreamKey)
          ? "late_response"
          : "unknown_correlation",
        "upstream response does not match an active request",
      );
    }
    clearTimeout(pending.timeout);
    this.pendingRequestsByUpstream.delete(upstreamKey);
    this.activeLocalRequestIds.delete(pending.localKey);
    this.retiredUpstreamResponseIds.add(upstreamKey);
    this.retiredLocalRequestIds.add(pending.localKey);
    const priority = responsePriority(pending.method);
    if (message.kind === "error") {
      this.toLocal.enqueue(
        rpcError(pending.localId, message.error.code, "upstream request failed"),
        priority,
      );
      return;
    }
    const result = this.adapter.fromUpstreamResponse(pending.method, message.result);
    this.toLocal.enqueue({ id: pending.localId, result }, priority);
  }

  #forwardUpstreamNotification(message) {
    let params = message.params;
    if (message.method === "serverRequest/resolved") {
      if (
        params === undefined ||
        params === null ||
        typeof params !== "object" ||
        Array.isArray(params) ||
        !Object.hasOwn(params, "requestId")
      ) {
        throw new SidecarError(
          "adapter_contract",
          "server-request resolution is outside the stable domain contract",
        );
      }
      const upstreamKey = correlationKey(params.requestId);
      let localId = this.serverResolutionIds.take(upstreamKey);
      if (localId === undefined) {
        const active = [...this.pendingServerByLocal.values()].find(
          (pending) => pending.upstreamKey === upstreamKey,
        );
        if (active !== undefined) {
          clearTimeout(active.timeout);
          this.pendingServerByLocal.delete(active.localKey);
          this.activeUpstreamServerIds.delete(active.upstreamKey);
          this.retiredLocalServerIds.add(active.localKey);
          this.retiredUpstreamServerIds.add(active.upstreamKey);
          localId = active.localId;
        }
      }
      if (localId === undefined) {
        throw new SidecarError(
          this.retiredServerResolutionIds.has(upstreamKey)
            ? "late_response"
            : "unknown_correlation",
          "server-request resolution does not match a reviewed correlation",
        );
      }
      this.retiredServerResolutionIds.add(upstreamKey);
      params = { ...params, requestId: localId };
    }
    const adapted = this.adapter.fromUpstreamNotification(message.method, params);
    if (adapted === null) {
      return;
    }
    const frame = { method: adapted.method };
    if (adapted.params !== undefined) {
      frame.params = adapted.params;
    }
    this.toLocal.enqueue(
      frame,
      CONTROL_NOTIFICATION_METHODS.has(adapted.method) ? "control" : "normal",
    );
  }

  #forwardUpstreamServerRequest(message) {
    const upstreamKey = correlationKey(message.id);
    if (
      this.activeUpstreamServerIds.has(upstreamKey) ||
      this.retiredUpstreamServerIds.has(upstreamKey)
    ) {
      throw new SidecarError("correlation_reuse", "upstream reused a server-request correlation");
    }
    const adapted = this.adapter.fromUpstreamServerRequest(message.method, message.params);
    if (adapted === null) {
      this.toUpstream.enqueue(
        rpcError(message.id, -32601, "server request is not promoted by the active adapter"),
        "control",
      );
      this.retiredUpstreamServerIds.add(upstreamKey);
      return;
    }
    if (this.#totalPending() >= this.configuration.maxPending) {
      this.toUpstream.enqueue(
        rpcError(message.id, -32021, "server-request capacity is exhausted"),
        "control",
      );
      this.retiredUpstreamServerIds.add(upstreamKey);
      return;
    }

    const localId = `server:${this.nonce}:${this.nextServerId}`;
    this.nextServerId += 1;
    const localKey = correlationKey(localId);
    const pending = {
      localId,
      localKey,
      upstreamId: message.id,
      upstreamKey,
      method: adapted.method,
      timeout: null,
    };
    pending.timeout = setTimeout(() => {
      this.#expireServerRequest(localKey);
    }, this.serverRequestTimeoutMs);
    this.pendingServerByLocal.set(localKey, pending);
    this.activeUpstreamServerIds.add(upstreamKey);
    this.toLocal.enqueue(
      { id: localId, method: adapted.method, params: adapted.params },
      "control",
    );
  }

  #expireServerRequest(localKey) {
    const pending = this.pendingServerByLocal.get(localKey);
    if (pending === undefined || this.stopping) {
      return;
    }
    this.pendingServerByLocal.delete(localKey);
    this.activeUpstreamServerIds.delete(pending.upstreamKey);
    this.retiredLocalServerIds.add(localKey);
    this.retiredUpstreamServerIds.add(pending.upstreamKey);
    this.expiredLocalServerIds.set(localKey, true);
    // Codex may acknowledge the request-specific timeout with the ordinary
    // resolution notification. Retain the same ID mapping Rust originally
    // observed so that notification remains correlated and content-free.
    this.serverResolutionIds.set(pending.upstreamKey, pending.localId);
    try {
      this.toUpstream.enqueue(
        rpcError(pending.upstreamId, -32022, "bridge server request timed out"),
        "control",
      );
    } catch (error) {
      this.fail(error);
    }
  }

  #clearAllTimeouts() {
    for (const pending of this.pendingRequestsByUpstream.values()) {
      clearTimeout(pending.timeout);
    }
    for (const pending of this.pendingServerByLocal.values()) {
      clearTimeout(pending.timeout);
    }
    this.pendingRequestsByUpstream.clear();
    this.pendingServerByLocal.clear();
  }

  #totalPending() {
    return this.pendingRequestsByUpstream.size + this.pendingServerByLocal.size;
  }
}

module.exports = {
  CONTROL_NOTIFICATION_METHODS,
  CONTROL_REQUEST_METHODS,
  LOCAL_REQUEST_TIMEOUT_MS,
  ProtocolSession,
  RetiredCorrelations,
  RetiredMappings,
  SERVER_REQUEST_TIMEOUT_MS,
};
