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

async function bootstrap(input, output) {
  let frameMaximum = HARD_MAX_FRAME_BYTES;
  const lines = boundedLines(input, () => frameMaximum)[Symbol.asyncIterator]();
  const bootstrapWrites = new PriorityWriteQueue(output, {
    maxFrameBytes: HARD_MAX_FRAME_BYTES,
    maxFrames: 8,
    maxBytes: HARD_MAX_FRAME_BYTES,
    onError: () => {},
  });
  await bootstrapWrites.enqueue(helloFrame(), "control");

  let configuration;
  let configureId = null;
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
  let child;
  try {
    probed = await probeVersion(configuration);
    child = await startAppServer(configuration, probed);
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

  try {
    await bootstrapWrites.enqueue(
      configureResponse(configuration.id, {
        upstreamVersion: probed.version,
        adapterVersion: probed.adapter.adapterVersion,
        capabilities: [...probed.adapter.capabilities],
      }),
      "control",
    );
    await bootstrapWrites.waitIdle();
  } catch (error) {
    await terminateChild(child, configuration.shutdownGraceMs);
    throw error;
  }

  return { child, configuration, adapter: probed.adapter, lines };
}

async function main(options = {}) {
  const input = options.input ?? process.stdin;
  const output = options.output ?? process.stdout;
  let session = null;
  let pendingSignal = false;
  const onSignal = () => {
    pendingSignal = true;
    if (session !== null) {
      session.requestShutdown();
    }
  };
  process.once("SIGINT", onSignal);
  process.once("SIGTERM", onSignal);

  try {
    const ready = await bootstrap(input, output);
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
