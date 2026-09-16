#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
fixture_root="$repo_root/test/fixtures/native-process-typed-exit"

if (( $# == 0 )); then
  jolt_command=(jolt)
else
  jolt_command=("$@")
fi

run_root=$(mktemp -d "${TMPDIR:-/tmp}/jolt-chdb-typed-exit.XXXXXX")
completion=false
cleanup() {
  local status=$?
  if [[ "$completion" == true && "$status" == 0 &&
        "$run_root" == "${TMPDIR:-/tmp}"/jolt-chdb-typed-exit.* &&
        -d "$run_root" && ! -L "$run_root" ]]; then
    rm -rf -- "$run_root"
  fi
}
trap cleanup EXIT
trap 'completion=false; exit 129' HUP
trap 'completion=false; exit 130' INT
trap 'completion=false; exit 143' TERM

set +e
output=$(cd "$fixture_root" && timeout --signal=TERM --kill-after=5s 120s "${jolt_command[@]}" -M:run "$run_root" 2>&1)
status=$?
set -e
printf '%s\n' "$output"

if [[ $status -ne 0 ]]; then
  echo "typed process-exit child failed with status $status" >&2
  exit "$status"
fi

grep -Fq "PASS typed export readback checkpoint explicit close" <<<"$output"
grep -Fq "PASS typed process-exit anchor closed before host teardown" <<<"$output"
completion=true
