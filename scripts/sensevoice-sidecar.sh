#!/bin/sh
# Local ASR sidecar for lark-codex-bridge.
# The bridge invokes:  $command [args...] <wav-or-decoded-pcm>
# This wrapper prints ONLY the SenseVoice transcript on stdout.
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

output=$("$bin" \
  --sense-voice-model="$model" \
  --tokens="$tokens" \
  --num-threads="${SENSEVOICE_THREADS:-2}" \
  --debug=false \
  "$wav" 2>/dev/null) || {
  echo "sensevoice-sidecar: sherpa-onnx-offline failed" >&2
  exit 2
}

printf '%s\n' "$output" | python3 -c '
import json, sys
text = ""
for line in sys.stdin:
    line = line.strip()
    if line.startswith("{") and "\"text\"" in line:
        try:
            text = json.loads(line).get("text") or ""
        except json.JSONDecodeError:
            continue
        break
text = (text or "").strip()
if not text:
    sys.exit(3)
print(text)
'
