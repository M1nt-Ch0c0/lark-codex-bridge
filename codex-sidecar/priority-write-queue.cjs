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
    this.finalization = null;
    this.finalized = false;
    this.closed = false;
    this.failure = null;
    this.idleWaiters = [];
    this.capacityWaiters = [];
    this.errorHandler = options.onError ?? (() => {});
    // Writable streams emit `error` even when the write callback already
    // received that error. Own the event so raw OS/provider errors cannot
    // become an uncaught exception or escape to stderr.
    this.streamErrorListener = () => this.#failWrite();
    this.streamFinishListener = () => {
      if (this.ending && this.closed && this.failure === null) {
        this.#settleFinalization(null);
      } else if (this.failure === null) {
        this.#failWrite();
      }
    };
    this.streamCloseListener = () => {
      if (
        this.failure === null &&
        !this.finalized &&
        !this.stream.writableFinished
      ) {
        this.#failWrite();
      }
      this.#detachStreamListeners();
    };
    this.stream.on("error", this.streamErrorListener);
    this.stream.on("finish", this.streamFinishListener);
    this.stream.on("close", this.streamCloseListener);
  }

  enqueue(value, priority = "normal") {
    const bytes = this.#prepare(value, priority);
    if (!this.#hasCapacity(bytes)) {
      throw new SidecarError("write_queue_full", "protocol write queue is full");
    }
    return this.#admit(bytes, priority);
  }

  async enqueueWithBackpressure(value, priority = "normal") {
    const bytes = this.#prepare(value, priority);
    if (bytes.length > this.maxBytes) {
      throw new SidecarError("write_queue_full", "protocol write queue is full");
    }
    if (this.capacityWaiters.length === 0 && this.#hasCapacity(bytes)) {
      return { completion: this.#admit(bytes, priority) };
    }
    // The protocol consumer awaits each admission before reading another
    // frame. Retaining at most one encoded overflow frame makes that invariant
    // explicit and keeps backpressure memory bounded if another producer is
    // introduced accidentally.
    if (this.capacityWaiters.length !== 0) {
      throw new SidecarError("write_queue_full", "protocol write queue is full");
    }
    return await new Promise((resolve, reject) => {
      this.capacityWaiters.push({ bytes, priority, resolve, reject });
      this.#promoteCapacityWaiters();
    });
  }

  #prepare(value, priority) {
    this.#assertAccepting();
    if (priority !== "control" && priority !== "normal") {
      throw new SidecarError("invalid_priority", "protocol write priority is invalid");
    }
    return encodeFrame(value, this.maxFrameBytes);
  }

  #assertAccepting() {
    if (
      this.ending ||
      this.closed ||
      this.failure !== null ||
      this.stream.destroyed ||
      !this.stream.writable
    ) {
      throw new SidecarError("write_closed", "protocol write stream is closed");
    }
  }

  #hasCapacity(bytes) {
    return !(
      this.queuedFrames >= this.maxFrames ||
      bytes.length > this.maxBytes - this.queuedBytes
    );
  }

  #admit(bytes, priority, drain = true) {
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
    if (drain) {
      this.#drain();
    }
    return completion;
  }

  async waitIdle() {
    if (this.failure !== null) {
      throw this.failure;
    }
    if (
      !this.draining &&
      this.queuedFrames === 0 &&
      this.capacityWaiters.length === 0
    ) {
      return;
    }
    await new Promise((resolve, reject) => this.idleWaiters.push({ resolve, reject }));
  }

  release() {
    if (this.failure !== null) {
      throw this.failure;
    }
    if (
      this.ending ||
      this.closed ||
      this.draining ||
      this.queuedFrames !== 0 ||
      this.capacityWaiters.length !== 0
    ) {
      throw new SidecarError("write_busy", "protocol write queue is not idle");
    }
    this.closed = true;
    this.#detachStreamListeners();
  }

  end() {
    if (this.endPromise !== null) {
      return this.endPromise;
    }
    if (this.failure !== null) {
      return Promise.reject(this.failure);
    }
    if (this.closed) {
      return Promise.resolve();
    }
    this.ending = true;
    this.#rejectCapacity(
      new SidecarError("write_closed", "protocol write stream is closed"),
    );
    this.endPromise = this.#finishEnd();
    return this.endPromise;
  }

  async #finishEnd() {
    await this.waitIdle();
    if (this.failure !== null) {
      throw this.failure;
    }
    this.closed = true;
    if (this.stream.destroyed || !this.stream.writable) {
      return;
    }
    await new Promise((resolve, reject) => {
      this.finalization = { resolve, reject, settled: false };
      try {
        this.stream.end((error) => {
          if (error) {
            this.#failWrite();
          } else {
            this.#settleFinalization(this.failure);
          }
        });
      } catch {
        this.#failWrite();
      }
    });
    if (this.failure !== null) {
      throw this.failure;
    }
  }

  abort(error = new SidecarError("write_aborted", "protocol write queue was aborted")) {
    if (this.failure === null) {
      this.failure = error;
    }
    this.closed = true;
    this.#settleFinalization(this.failure);
    this.#rejectCapacity(this.failure);
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
    this.#promoteCapacityWaiters();
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

  #settleFinalization(error) {
    if (error === null) {
      this.finalized = true;
    }
    const finalization = this.finalization;
    if (finalization === null || finalization.settled) {
      return;
    }
    finalization.settled = true;
    this.finalization = null;
    if (error === null) {
      finalization.resolve();
    } else {
      finalization.reject(error);
    }
  }

  #detachStreamListeners() {
    this.stream.removeListener("error", this.streamErrorListener);
    this.stream.removeListener("finish", this.streamFinishListener);
    this.stream.removeListener("close", this.streamCloseListener);
  }

  #promoteCapacityWaiters() {
    const firstControl = this.capacityWaiters.findIndex(
      (waiter) => waiter.priority === "control",
    );
    const firstNormal = this.capacityWaiters.findIndex(
      (waiter) => waiter.priority === "normal",
    );
    const index =
      firstControl !== -1 &&
      (this.controlBurst < CONTROL_BURST || firstNormal === -1)
        ? firstControl
        : firstNormal;
    if (index === -1 || !this.#hasCapacity(this.capacityWaiters[index].bytes)) {
      return;
    }
    const [waiter] = this.capacityWaiters.splice(index, 1);
    const completion = this.#admit(waiter.bytes, waiter.priority, false);
    waiter.resolve({ completion });
    this.#drain();
  }

  #rejectCapacity(error) {
    for (const waiter of this.capacityWaiters.splice(0)) {
      waiter.reject(error);
    }
  }

  #settleIdle(error) {
    if (
      this.draining ||
      this.queuedFrames !== 0 ||
      this.capacityWaiters.length !== 0
    ) {
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
