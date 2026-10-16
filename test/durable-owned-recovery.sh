#!/usr/bin/env bash
set -euo pipefail
writer_runner=${1:?supply the qualified Jolt executable}
reader_runner=${2:?supply the stock Jolt executable}
task_dir=$(mktemp -d /tmp/chdb-owned-recovery.XXXXXXXX)
cleanup() {
  rm -rf -- "$task_dir"
}
trap cleanup EXIT
"$writer_runner" -Sdeps '{:paths ["src" "resources" "test"]}' -M -m jdbc.chdb-owned-recovery-test writer "$task_dir"
"$reader_runner" -Sdeps '{:paths ["src" "resources" "test"]}' -M -m jdbc.chdb-owned-recovery-test reader "$task_dir"
