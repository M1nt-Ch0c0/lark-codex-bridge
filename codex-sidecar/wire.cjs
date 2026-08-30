"use strict";

const { TextDecoder } = require("node:util");

const PROTOCOL = "codex-sidecar-wire";
const VERSION = 1;
const HARD_MAX_FRAME_BYTES = 33_554_432;
// Rust can hold 320 normal-priority plus 64 reserved high-priority outbound
// requests while independently retaining 64 upstream reverse requests. Node
// counts both directions in one correlation table, so the negotiated cap is
// the full 320 + 64 + 64 protocol envelope.
const DEFAULT_MAX_PENDING = 448;
const HARD_MAX_PENDING = 448;
const DEFAULT_MAX_WRITE_QUEUE_FRAMES = 384;
const HARD_MAX_WRITE_QUEUE_FRAMES = 2_048;
const DEFAULT_MAX_WRITE_QUEUE_BYTES = 67_108_864;
const DEFAULT_SHUTDOWN_GRACE_MS = 5_000;
const MAX_CORRELATION_BYTES = 128;
const MAX_METHOD_BYTES = 256;
const MAX_CODEX_ARGUMENTS = 8;
const MAX_CODEX_ARGUMENT_BYTES = 1_024;
// Keep the local sidecar preflight exactly aligned with Rust's JSONL parser so
// neither half admits a payload the other half rejects after negotiation.
const MAX_JSON_NESTING = 128;
const MAX_JSON_STRUCTURAL_TOKENS = 65_536;
const MAX_FRAME_CHUNKS = 4_096;

const HELLO_CAPABILITIES = Object.freeze([
  "bounded-ndjson",
  "correlated-requests",
  "correlated-server-requests",
  "epoch-on-restart",
  "no-mutation-replay",
  "priority-control-lane",
  "stable-domain-jsonrpc",
]);

class SidecarError extends Error {
  constructor(code, message) {
    super(message);
    this.name = "SidecarError";
    this.code = code;
  }
}

function helloFrame() {
  return {
    protocol: PROTOCOL,
    v: VERSION,
    type: "hello",
    maxFrameBytes: HARD_MAX_FRAME_BYTES,
    capabilities: [...HELLO_CAPABILITIES],
  };
}

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function byteLength(value) {
  return Buffer.byteLength(value, "utf8");
}

function validateCorrelationId(value, label = "correlation") {
  const validInteger = Number.isSafeInteger(value);
  const validString =
    typeof value === "string" &&
    value.length > 0 &&
    byteLength(value) <= MAX_CORRELATION_BYTES &&
    /^[A-Za-z0-9_.:-]+$/u.test(value);
  if (!validInteger && !validString) {
    throw new SidecarError("invalid_correlation", `${label} is invalid`);
  }
  return value;
}

function correlationKey(value) {
  validateCorrelationId(value);
  return `${typeof value === "number" ? "n" : "s"}:${String(value)}`;
}

function validateMethod(value) {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    byteLength(value) > MAX_METHOD_BYTES ||
    !/^[A-Za-z0-9_./:-]+$/u.test(value)
  ) {
    throw new SidecarError("invalid_method", "RPC method is invalid");
  }
  return value;
}

function validateParams(value) {
  if (value !== undefined && value !== null && !isPlainObject(value)) {
    throw new SidecarError("invalid_params", "RPC params must be an object or null");
  }
  return value;
}

function validateErrorObject(value) {
  if (
    !isPlainObject(value) ||
    !Number.isSafeInteger(value.code) ||
    typeof value.message !== "string"
  ) {
    throw new SidecarError("invalid_error", "RPC error object is invalid");
  }
  return value;
}

function classifyRpcFrame(value) {
  if (!isPlainObject(value)) {
    throw new SidecarError("invalid_rpc", "RPC frame must be an object");
  }
  const hasId = Object.hasOwn(value, "id");
  const hasMethod = Object.hasOwn(value, "method");
  const hasResult = Object.hasOwn(value, "result");
  const hasError = Object.hasOwn(value, "error");
  const hasParams = Object.hasOwn(value, "params");

  if (hasMethod) {
    if (hasResult || hasError) {
      throw new SidecarError("invalid_rpc", "RPC method cannot include result or error");
    }
    const method = validateMethod(value.method);
    const params = hasParams ? validateParams(value.params) : undefined;
    if (hasId) {
      return { kind: "request", id: validateCorrelationId(value.id), method, params };
    }
    return { kind: "notification", method, params };
  }

  if (hasParams || !hasId || hasResult === hasError) {
    throw new SidecarError("invalid_rpc", "RPC response envelope is invalid");
  }
  const id = validateCorrelationId(value.id);
  if (hasError) {
    return { kind: "error", id, error: validateErrorObject(value.error) };
  }
  return { kind: "response", id, result: value.result };
}

function preflightJson(bytes) {
  let nesting = 0;
  let structural = 0;
  let inString = false;
  let escaped = false;
  for (const byte of bytes) {
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (byte === 0x5c) {
        escaped = true;
      } else if (byte === 0x22) {
        inString = false;
      }
      continue;
    }
    if (byte === 0x22) {
      inString = true;
    } else if (byte === 0x7b || byte === 0x5b) {
      nesting += 1;
      structural += 1;
      if (nesting > MAX_JSON_NESTING) {
        throw new SidecarError("json_nesting", "JSON nesting limit exceeded");
      }
    } else if (byte === 0x7d || byte === 0x5d) {
      nesting = Math.max(0, nesting - 1);
    } else if (byte === 0x2c || byte === 0x3a) {
      structural += 1;
    }
    if (structural > MAX_JSON_STRUCTURAL_TOKENS) {
      throw new SidecarError("json_structure", "JSON structural-token limit exceeded");
    }
  }
}

function parseJsonLine(bytes) {
  preflightJson(bytes);
  let text;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    throw new SidecarError("invalid_utf8", "protocol frame is not valid UTF-8");
  }
  try {
    return JSON.parse(text);
  } catch {
    throw new SidecarError("invalid_json", "protocol frame is not valid JSON");
  }
}

async function* boundedLines(readable, getMaximum) {
  let pieces = [];
  let length = 0;
  for await (const rawChunk of readable) {
    const chunk = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk);
    let start = 0;
    for (let index = 0; index < chunk.length; index += 1) {
      if (chunk[index] !== 0x0a) {
        continue;
      }
      let piece = chunk.subarray(start, index);
      if (length === 0 && pieces.length === 0 && piece.length > 0 && piece.at(-1) === 0x0d) {
        piece = piece.subarray(0, -1);
      }
      const nextLength = length + piece.length;
      if (nextLength > getMaximum()) {
        throw new SidecarError("frame_too_large", "protocol frame exceeds the byte limit");
      }
      pieces.push(piece);
      if (pieces.length > MAX_FRAME_CHUNKS) {
        throw new SidecarError("frame_fragmented", "protocol frame has too many fragments");
      }
      let line = Buffer.concat(pieces, nextLength);
      if (line.length > 0 && line.at(-1) === 0x0d) {
        line = line.subarray(0, -1);
      }
      if (line.length === 0) {
        throw new SidecarError("empty_frame", "protocol frame is empty");
      }
      yield line;
      pieces = [];
      length = 0;
      start = index + 1;
    }
    if (start < chunk.length) {
      const remainder = chunk.subarray(start);
      length += remainder.length;
      if (length > getMaximum()) {
        throw new SidecarError("frame_too_large", "protocol frame exceeds the byte limit");
      }
      pieces.push(remainder);
      if (pieces.length > MAX_FRAME_CHUNKS) {
        throw new SidecarError("frame_fragmented", "protocol frame has too many fragments");
      }
    }
  }
  if (length > 0) {
    throw new SidecarError(
      "unterminated_frame",
      "protocol input ended before the NDJSON record delimiter",
    );
  }
}

function boundedInteger(value, fallback, minimum, maximum, label) {
  const selected = value === undefined ? fallback : value;
  if (!Number.isSafeInteger(selected) || selected < minimum || selected > maximum) {
    throw new SidecarError("invalid_configuration", `${label} is outside its bound`);
  }
  return selected;
}

function validateConfigureFrame(value) {
  const allowedKeys = new Set([
    "v",
    "type",
    "id",
    "codexBinary",
    "codexHome",
    "codexArguments",
    "maxFrameBytes",
    "maxPending",
  ]);
  const requiredKeys = [
    "v",
    "type",
    "id",
    "codexBinary",
    "codexHome",
    "maxFrameBytes",
    "maxPending",
  ];
  if (
    !isPlainObject(value) ||
    value.v !== VERSION ||
    value.type !== "configure"
  ) {
    throw new SidecarError("invalid_configuration", "configure frame header is incompatible");
  }
  if (Object.keys(value).some((key) => !allowedKeys.has(key))) {
    throw new SidecarError("invalid_configuration", "configure frame has an unknown field");
  }
  if (requiredKeys.some((key) => !Object.hasOwn(value, key))) {
    throw new SidecarError("invalid_configuration", "configure frame is missing a required field");
  }
  const id = validateCorrelationId(value.id, "configure correlation");
  if (value.codexBinary !== null && typeof value.codexBinary !== "string") {
    throw new SidecarError("invalid_configuration", "codexBinary must be a string or null");
  }
  if (
    typeof value.codexBinary === "string" &&
    (value.codexBinary.length === 0 || byteLength(value.codexBinary) > 4_096)
  ) {
    throw new SidecarError("invalid_configuration", "codexBinary is invalid");
  }
  if (value.codexHome !== null && typeof value.codexHome !== "string") {
    throw new SidecarError("invalid_configuration", "codexHome must be a string or null");
  }
  if (
    typeof value.codexHome === "string" &&
    (value.codexHome.length === 0 || byteLength(value.codexHome) > 4_096)
  ) {
    throw new SidecarError("invalid_configuration", "codexHome is invalid");
  }
  const codexArguments = value.codexArguments === undefined ? [] : value.codexArguments;
  if (!Array.isArray(codexArguments) || codexArguments.length > MAX_CODEX_ARGUMENTS) {
    throw new SidecarError("invalid_configuration", "codexArguments exceeds its count bound");
  }
  for (const argument of codexArguments) {
    if (
      typeof argument !== "string" ||
      byteLength(argument) > MAX_CODEX_ARGUMENT_BYTES ||
      argument.includes("\0")
    ) {
      throw new SidecarError("invalid_configuration", "codexArguments contains an invalid item");
    }
  }

  const maxFrameBytes = boundedInteger(
    value.maxFrameBytes,
    HARD_MAX_FRAME_BYTES,
    4_096,
    HARD_MAX_FRAME_BYTES,
    "maxFrameBytes",
  );
  const maxPending = boundedInteger(
    value.maxPending,
    DEFAULT_MAX_PENDING,
    1,
    HARD_MAX_PENDING,
    "maxPending",
  );
  return {
    id,
    codexBinary: value.codexBinary,
    codexHome: value.codexHome,
    codexArguments: [...codexArguments],
    maxFrameBytes,
    maxPending,
    maxWriteQueueFrames: Math.min(
      HARD_MAX_WRITE_QUEUE_FRAMES,
      Math.max(DEFAULT_MAX_WRITE_QUEUE_FRAMES, maxPending + 64),
    ),
    maxWriteQueueBytes: Math.max(DEFAULT_MAX_WRITE_QUEUE_BYTES, maxFrameBytes),
    shutdownGraceMs: DEFAULT_SHUTDOWN_GRACE_MS,
  };
}

function encodeFrame(value, maximum = HARD_MAX_FRAME_BYTES) {
  let bytes;
  try {
    bytes = Buffer.from(`${JSON.stringify(value)}\n`, "utf8");
  } catch {
    throw new SidecarError("encode_failed", "protocol frame could not be encoded");
  }
  if (bytes.length - 1 > maximum) {
    throw new SidecarError("frame_too_large", "encoded protocol frame exceeds the byte limit");
  }
  return bytes;
}

function configureResponse(id, data) {
  return { v: VERSION, type: "response", id, ok: true, data };
}

function configureError(id, code) {
  return { v: VERSION, type: "response", id, ok: false, error: code };
}

module.exports = {
  DEFAULT_MAX_PENDING,
  DEFAULT_SHUTDOWN_GRACE_MS,
  HARD_MAX_FRAME_BYTES,
  HELLO_CAPABILITIES,
  MAX_JSON_NESTING,
  MAX_JSON_STRUCTURAL_TOKENS,
  PROTOCOL,
  SidecarError,
  VERSION,
  boundedLines,
  classifyRpcFrame,
  configureError,
  configureResponse,
  correlationKey,
  encodeFrame,
  helloFrame,
  isPlainObject,
  parseJsonLine,
  validateConfigureFrame,
  validateCorrelationId,
  validateErrorObject,
  validateMethod,
  validateParams,
};
