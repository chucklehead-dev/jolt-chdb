#!/usr/bin/env bash
# Actual immutable launcher/SDK entry point, injected failure before native/cloud.
set -euo pipefail
test "$#" -eq 1
task_source=$(realpath "$1")
task_repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
task_root=$(mktemp -d /tmp/confirmation-entrypoint-failure.XXXXXXXX)
cleanup() {
  if [[ "$task_root" =~ ^/tmp/confirmation-entrypoint-failure\.[A-Za-z0-9]+$ ]]; then
    rm -rf -- "$task_root"
  fi
}
trap cleanup EXIT
export SDK_STORAGE_ARM=s3 SDK_WAL_HEAD_ARM=baseline
export CHDB_BENCH_METADATA_READER=stock
export TASK_ORIGINAL_TYPED_DRIVER="$task_source/bench/typed-storage-window.clj"
export TASK_TYPED_BUDGET_DRIVER="$task_repo/scripts/fixtures/confirmation/failure_driver.clj"
export TEST_CONFIRMATION_DRIVER="$task_repo/scripts/typed_storage_confirmation_driver.clj"
task_status=0
timeout --kill-after=5s 90s bash "$task_source/bench/run-typed-storage-window.sh" \
  window "$task_root" 1 256 true wide16 > "$task_root/process.log" 2>&1 || task_status=$?
test "$task_status" = 1
grep -Fq ':qualification-failed :phase window :label :unknown' "$task_root/process.log"
grep -Fq ':error-type :jdbc.chdb.durable.s3/permission' "$task_root/process.log"
echo 'actual benchmark reached the injected pre-connection permission failure'
# A bare System/exit in the original entry point bypasses this diagnostic and
# MUST fail this control, rather than being accepted as the expected failure.
test -f "$task_root/driver-failure.edn"
grep -Fq ':stage :driver' "$task_root/driver-failure.edn"
grep -Fq ':scope-returned false' "$task_root/driver-failure.edn"
grep -Fq ':type :jdbc.chdb.durable.s3/permission' "$task_root/driver-failure.edn"
if grep -Fq 'PRIVATE-ENTRYPOINT-CANARY' "$task_root/driver-failure.edn"; then exit 1; fi
test ! -e "$task_root/confirmation-selection.edn"
test ! -e "$task_root/writer.edn"
echo 'actual benchmark failure propagates into a closed diagnostic and remains nonzero'
