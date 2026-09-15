#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

if (( $# == 0 )); then
  jolt_command=(jolt)
else
  jolt_command=("$@")
fi

run_child() {
  local mode=$1
  local output
  output=$("${jolt_command[@]}" -M:native-process-test "$mode")
  printf '%s\n' "$output"
  grep -Fq "PASS" <<<"$output"
}

# Each invocation is a separate OS process. The first proves that logical last
# close does not destroy the anchored in-memory engine; the second proves that
# process exit is terminal and that a fresh process gets fresh :memory: state.
run_child within
run_child fresh
