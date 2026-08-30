"use strict";

const v0149 = require("./0.149.0.cjs");
const v0151 = require("./0.151.0.cjs");

const ADAPTERS = new Map([
  [v0149.upstreamVersion, v0149],
  [v0151.upstreamVersion, v0151],
]);

function adapterForVersion(version) {
  return ADAPTERS.get(version) ?? null;
}

module.exports = {
  SUPPORTED_UPSTREAM_VERSIONS: Object.freeze([...ADAPTERS.keys()]),
  adapterForVersion,
};
