#!/usr/bin/env node
"use strict";

process.stdout.write(
  `${JSON.stringify({
    protocol: "incompatible-sidecar-wire",
    v: 1,
    type: "hello",
    maxFrameBytes: 33_554_432,
    capabilities: [],
  })}\n`,
);
