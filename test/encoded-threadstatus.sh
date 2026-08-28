#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
jolt_bin=${JOLT_BIN:-jolt}
jolt_runner=${JOLT_RUNNER:-}
scratch=$(mktemp -d /tmp/jolt-chdb-encoded-threadstatus.XXXXXX)
transcript=$scratch/typescript

cleanup() {
  rm -rf -- "$scratch"
}
trap cleanup EXIT INT TERM

: "${JOLT_CHDB_LIB:?set JOLT_CHDB_LIB to the qualified libchdb.so}"

cd "$repo_dir"
if [[ -n "$jolt_runner" ]]; then
  jolt_command="$jolt_runner $jolt_bin"
else
  jolt_command=$jolt_bin
fi
script -qefc \
  "$jolt_command -A:test -m jdbc.chdb-encoded-threadstatus-probe" \
  "$transcript" >/dev/null

if grep -Fq 'ThreadStatus: current_thread contains invalid address' "$transcript"; then
  sed -n '1,240p' "$transcript" >&2
  exit 1
fi
if ! grep -Fq 'PASS: bounded encoded query ThreadStatus probe' "$transcript"; then
  sed -n '1,240p' "$transcript" >&2
  exit 1
fi

printf 'PASS: no ThreadStatus diagnostic in bounded encoded query lifecycle\n'
