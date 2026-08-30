#!/usr/bin/env node
"use strict";

const { PriorityWriteQueue } = require("./priority-write-queue.cjs");
const { ProtocolSession } = require("./session.cjs");
const {
  HARD_MAX_FRAME_BYTES,
  SidecarError,
  boundedLines,
  configureError,
  configureResponse,
  helloFrame,
  parseJsonLine,
  validateConfigureFrame,
  validateCorrelationId,
} = require("./wire.cjs");
const { probeVersion, startAppServer, terminateChild } = require("./upstream.cjs");

function safeCode(error) {
  if (error instanceof SidecarError && /^[a-z0-9_]+$/u.test(error.code)) {
    return error.code;
  }
  return "sidecar_failed";
}

async function bootstrap(input, output, signal) {
  let frameMaximum = HARD_MAX_FRAME_BYTES;
  const lines = boundedLines(input, () => frameMaximum)[Symbol.asyncIterator]();
  const bootstrapWrites = new PriorityWriteQueue(output, {
    maxFrameBytes: HARD_MAX_FRAME_BYTES,
    maxFrames: 8,
    maxBytes: HARD_MAX_FRAME_BYTES,
    onError: () => {},
  });
  const cancellation = new SidecarError(
    "shutdown_requested",
    "sidecar shutdown was requested",
  );
  const onAbort = () => {
    bootstrapWrites.abort(cancellation);
    if (!input.destroyed) {
      input.destroy();
    }
  };
  signal?.addEventListener("abort", onAbort, { once: true });
  if (signal?.aborted) {
    onAbort();
  }

  let configuration;
  let configureId = null;
  let child;
  try {
    await bootstrapWrites.enqueue(helloFrame(), "control");
    try {
      const first = await lines.next();
      if (first.done) {
        throw new SidecarError("configure_eof", "local input closed before configuration");
      }
      const decoded = parseJsonLine(first.value);
      if (decoded && Object.hasOwn(decoded, "id")) {
        try {
          configureId = validateCorrelationId(decoded.id, "configure correlation");
        } catch {
          configureId = null;
        }
      }
      configuration = validateConfigureFrame(decoded);
      configureId = configuration.id;
      frameMaximum = configuration.maxFrameBytes;
    } catch (error) {
      const code = safeCode(error);
      if (configureId !== null) {
        try {
          await bootstrapWrites.enqueue(configureError(configureId, code), "control");
        } catch {
          // The static process exit classification remains authoritative.
        }
      }
      throw error;
    }

    let probed;
    try {
      probed = await probeVersion(configuration, signal);
      child = await startAppServer(configuration, probed, signal);
    } catch (error) {
      try {
        await bootstrapWrites.enqueue(
          configureError(configuration.id, safeCode(error)),
          "control",
        );
      } catch {
        // Exit remains fail closed if the peer cannot receive diagnostics.
      }
      throw error;
    }

    await bootstrapWrites.enqueue(
      configureResponse(configuration.id, {
        upstreamVersion: probed.version,
        adapterVersion: probed.adapter.adapterVersion,
        capabilities: [...probed.adapter.capabilities],
      }),
      "control",
    );
    await bootstrapWrites.waitIdle();
    bootstrapWrites.release();
    return { child, configuration, adapter: probed.adapter, lines };
  } catch (error) {
    if (child !== undefined) {
      await terminateChild(child, configuration?.shutdownGraceMs ?? 100);
    }
    throw error;
  } finally {
    signal?.removeEventListener("abort", onAbort);
  }
}

async function main(options = {}) {
  const input = options.input ?? process.stdin;
  const output = options.output ?? process.stdout;
  let session = null;
  let pendingSignal = false;
  const bootstrapAbort = new AbortController();
  const onSignal = () => {
    pendingSignal = true;
    if (session !== null) {
      session.requestShutdown();
    } else {
      bootstrapAbort.abort();
    }
  };
  process.once("SIGINT", onSignal);
  process.once("SIGTERM", onSignal);

  try {
    const ready = await bootstrap(input, output, bootstrapAbort.signal);
    session = new ProtocolSession({
      configuration: ready.configuration,
      adapter: ready.adapter,
      child: ready.child,
      localInput: input,
      localLines: ready.lines,
      localOutput: output,
    });
    if (pendingSignal) {
      session.requestShutdown();
    }
    const outcome = await session.run();
    if (outcome.kind === "fatal") {
      process.stderr.write(`codex_sidecar_failure code=${safeCode(outcome.error)}\n`);
      return 1;
    }
    return 0;
  } catch (error) {
    if (pendingSignal) {
      return 0;
    }
    process.stderr.write(`codex_sidecar_failure code=${safeCode(error)}\n`);
    return 1;
  } finally {
    process.removeListener("SIGINT", onSignal);
    process.removeListener("SIGTERM", onSignal);
    if (!input.destroyed) {
      input.destroy();
    }
  }
}

if (require.main === module) {
  main().then(
    (code) => {
      process.exitCode = code;
    },
    () => {
      process.stderr.write("codex_sidecar_failure code=sidecar_failed\n");
      process.exitCode = 1;
    },
  );
}

module.exports = { bootstrap, main, safeCode };
