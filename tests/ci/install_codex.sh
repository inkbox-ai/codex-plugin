#!/usr/bin/env bash
set -euo pipefail

attempts="${CODEX_INSTALL_ATTEMPTS:-4}"
platform="$(node -p '`${process.platform}-${process.arch}`')"

for attempt in $(seq 1 "$attempts"); do
  # Resolve once so the launcher and native executable always share a version.
  # Installing the native package explicitly makes a missing artifact fatal.
  if version="$(npm view @openai/codex@alpha version)" &&
    npm install -g --include=optional "@openai/codex@$version" \
      "@openai/codex-$platform@npm:@openai/codex@$version-$platform" &&
    codex --version; then
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
