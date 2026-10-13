#!/usr/bin/env bash
set -euo pipefail

attempts="${CODEX_INSTALL_ATTEMPTS:-12}"
budget="${CODEX_INSTALL_TIMEOUT_SECONDS:-360}"
if ! [[ "$attempts" =~ ^[1-9][0-9]*$ && "$budget" =~ ^[1-9][0-9]*$ ]]; then
  echo "Codex install attempts and timeout must be positive integers" >&2
  exit 2
fi
deadline=$((SECONDS + budget))
platform="$(node -p '`${process.platform}-${process.arch}`')"

within_budget() {
  local remaining=$((deadline - SECONDS))
  [ "$remaining" -gt 0 ] || return 124
  timeout --kill-after=5 "$remaining" "$@"
}

registry() {
  within_budget npm "$@" --prefer-online --fetch-retries=0 --fetch-timeout=20000
}

install_current_alpha() {
  local version native_version installed_version
  version="$(registry view @openai/codex@alpha version)" || return 1
  if ! [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+-alpha\.[0-9]+$ ]]; then
    echo "Codex alpha metadata did not identify an alpha release" >&2
    return 1
  fi
  # The alpha tag can advance before its native artifact is published.
  native_version="$(registry view "@openai/codex@$version-$platform" version)" || return 1
  if [ "$native_version" != "$version-$platform" ]; then
    echo "Codex native artifact is not ready for $version ($platform)" >&2
    return 1
  fi
  registry install -g --include=optional "@openai/codex@$version" \
    "@openai/codex-$platform@npm:@openai/codex@$version-$platform" || return 1
  installed_version="$(within_budget codex --version)" || return 1
  if [ "$installed_version" != "codex-cli $version" ]; then
    echo "Codex executable version does not match requested $version" >&2
    return 1
  fi
  echo "$installed_version"
}

for ((attempt = 1; attempt <= attempts; attempt++)); do
  if install_current_alpha; then
    exit 0
  fi
  remaining=$((deadline - SECONDS))
  if [ "$attempt" -eq "$attempts" ] || [ "$remaining" -le 0 ]; then
    break
  fi
  delay=$((attempt * 15))
  [ "$delay" -le 60 ] || delay=60
  [ "$delay" -le "$remaining" ] || delay="$remaining"
  echo "Codex install attempt $attempt failed; retrying in ${delay}s"
  sleep "$delay"
done

echo "Codex installation failed after $attempt attempts (budget ${budget}s)" >&2
exit 1
