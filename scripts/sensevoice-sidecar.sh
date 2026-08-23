#!/bin/sh
# Local ASR sidecar for lark-codex-bridge.
# The bridge invokes:  $command [args...] <wav-or-decoded-pcm>
# The bridge parses sherpa's bounded JSON/text stdout. `exec` is intentional:
# the recognizer becomes the supervised process, so cancellation cannot leave
# it running as a descendant of this wrapper.
set -eu

wav=
for arg in "$@"; do
  wav=$arg
done

if [ -z "${wav:-}" ] || [ ! -f "$wav" ]; then
  echo "sensevoice-sidecar: missing audio file argument" >&2
  exit 2
fi

bin=${SENSEVOICE_BIN:?SENSEVOICE_BIN must point to sherpa-onnx-offline}
model=${SENSEVOICE_MODEL:?SENSEVOICE_MODEL must point to model.int8.onnx}
tokens=${SENSEVOICE_TOKENS:?SENSEVOICE_TOKENS must point to tokens.txt}

exec "$bin" \
  --sense-voice-model="$model" \
  --tokens="$tokens" \
  --num-threads="${SENSEVOICE_THREADS:-2}" \
  --debug=false \
  "$wav"
