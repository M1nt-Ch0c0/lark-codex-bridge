"use strict";

const { SidecarError, encodeFrame } = require("./wire.cjs");

const CONTROL_BURST = 8;

class PriorityWriteQueue {
  constructor(stream, options) {
    this.stream = stream;
    this.maxFrameBytes = options.maxFrameBytes;
    this.maxFrames = options.maxFrames;
    this.maxBytes = options.maxBytes;
    this.control = [];
    this.normal = [];
    this.queuedFrames = 0;
    this.queuedBytes = 0;
    this.controlBurst = 0;
    this.draining = false;
    this.inFlight = null;
    this.ending = false;
    this.endPromise = null;
    this.closed = false;
    this.failure = null;
    this.idleWaiters = [];
    this.errorHandler = options.onError ?? (() => {});
    // Writable streams emit `error` even when the write callback already
    // received that error. Own the event so raw OS/provider errors cannot
    // become an uncaught exception or escape to stderr.
    this.stream.on("error", () => this.#failWrite());
  }

  enqueue(value, priority = "normal") {
    if (
      this.ending ||
      this.closed ||
      this.failure !== null ||
      this.stream.destroyed ||
      !this.stream.writable
    ) {
      throw new SidecarError("write_closed", "protocol write stream is closed");
    }
    if (priority !== "control" && priority !== "normal") {
      throw new SidecarError("invalid_priority", "protocol write priority is invalid");
    }
    const bytes = encodeFrame(value, this.maxFrameBytes);
    if (
      this.queuedFrames >= this.maxFrames ||
      bytes.length > this.maxBytes - this.queuedBytes
    ) {
      throw new SidecarError("write_queue_full", "protocol write queue is full");
    }

    let resolve;
    let reject;
    const completion = new Promise((onResolve, onReject) => {
      resolve = onResolve;
      reject = onReject;
    });
    // Callers that intentionally fire-and-forget should not produce an
    // unhandled-rejection warning; failures still reach errorHandler.
    completion.catch(() => {});
    const item = { bytes, resolve, reject, settled: false };
    if (priority === "control") {
      this.control.push(item);
    } else {
      this.normal.push(item);
    }
    this.queuedFrames += 1;
    this.queuedBytes += bytes.length;
    this.#drain();
    return completion;
  }

  async waitIdle() {
    if (this.failure !== null) {
      throw this.failure;
    }
    if (!this.draining && this.queuedFrames === 0) {
      return;
    }
    await new Promise((resolve, reject) => this.idleWaiters.push({ resolve, reject }));
  }

  end() {
    if (this.endPromise !== null) {
      return this.endPromise;
    }
    if (this.closed) {
      return Promise.resolve();
    }
    this.ending = true;
    this.endPromise = this.#finishEnd();
    return this.endPromise;
  }

  async #finishEnd() {
    await this.waitIdle();
    this.closed = true;
    if (this.stream.destroyed || !this.stream.writable) {
      return;
    }
    await new Promise((resolve, reject) =>
      this.stream.end((error) => {
        if (error) {
          this.#failWrite();
          reject(this.failure);
        } else {
          resolve();
        }
      }),
    );
  }

  abort(error = new SidecarError("write_aborted", "protocol write queue was aborted")) {
    if (this.failure === null) {
      this.failure = error;
    }
    this.closed = true;
    const pending = [...this.control.splice(0), ...this.normal.splice(0)];
    if (this.inFlight !== null) {
      pending.unshift(this.inFlight);
    }
    for (const item of pending) {
      this.#settleItem(item, this.failure);
    }
    this.#rejectIdle(this.failure);
  }

  #next() {
    if (this.control.length > 0 && (this.controlBurst < CONTROL_BURST || this.normal.length === 0)) {
      this.controlBurst += 1;
      return this.control.shift();
    }
    if (this.normal.length > 0) {
      this.controlBurst = 0;
      return this.normal.shift();
    }
    if (this.control.length > 0) {
      this.controlBurst = 1;
      return this.control.shift();
    }
    return null;
  }

  #drain() {
    if (this.draining || this.failure !== null || this.closed) {
      return;
    }
    const item = this.#next();
    if (item === null) {
      this.#settleIdle(null);
      return;
    }
    this.draining = true;
    this.inFlight = item;
    try {
      this.stream.write(item.bytes, (error) => {
        this.draining = false;
        if (error) {
          this.#failWrite();
          this.inFlight = null;
          return;
        }
        this.inFlight = null;
        if (item.settled) {
          this.#settleIdle(this.failure);
          return;
        }
        this.#settleItem(item, null);
        this.#drain();
      });
    } catch {
      this.draining = false;
      this.#failWrite();
      this.inFlight = null;
    }
  }

  #failWrite() {
    if (this.failure !== null) {
      return;
    }
    const safeError = new SidecarError("write_failed", "protocol write failed");
    this.abort(safeError);
    this.errorHandler(safeError);
  }

  #settleItem(item, error) {
    if (item.settled) {
      return;
    }
    item.settled = true;
    this.queuedFrames -= 1;
    this.queuedBytes -= item.bytes.length;
    if (error === null) {
      item.resolve();
    } else {
      item.reject(error);
    }
  }

  #rejectIdle(error) {
    const waiters = this.idleWaiters.splice(0);
    for (const waiter of waiters) {
      waiter.reject(error);
    }
  }

  #settleIdle(error) {
    if (this.draining || this.queuedFrames !== 0) {
      return;
    }
    const waiters = this.idleWaiters.splice(0);
    for (const waiter of waiters) {
      if (error === null) {
        waiter.resolve();
      } else {
        waiter.reject(error);
      }
    }
  }
}

module.exports = { CONTROL_BURST, PriorityWriteQueue };
