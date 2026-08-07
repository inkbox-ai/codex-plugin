#!/usr/bin/env bash
set -euo pipefail

attempts="${CODEX_INSTALL_ATTEMPTS:-4}"

for attempt in $(seq 1 "$attempts"); do
  if npm install -g @openai/codex@alpha; then
    codex --version
    exit 0
  fi
  if [ "$attempt" -eq "$attempts" ]; then
    break
  fi
  delay=$((attempt * 15))
  echo "Codex install attempt $attempt failed; retrying in ${delay}s"
  sleep "$delay"
done

echo "Codex installation failed after $attempts attempts" >&2
exit 1
