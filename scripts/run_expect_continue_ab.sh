#!/usr/bin/env bash
# Hosted-only, fixed candidate comparison. No credentials or process logs emitted.
set -euo pipefail
test "$#" -eq 2
task_candidate=$(realpath "$1")
task_receipts=$(realpath "$2")
test "$(git -C "$task_candidate" rev-parse HEAD)" = 405a9c4ac6f0efbc8db925be1221cce28b8067c4
git -C "$task_candidate" diff --quiet HEAD
test "${SDK_STORAGE_ARM:?}" = s3
test "${TASK_ORIGINAL_TYPED_DRIVER:?}" = "$task_candidate/bench/typed-storage-window.clj"
test -f "$task_receipts/preflight.json"
test -f "$task_receipts/local-writer.edn"
test -f "$task_receipts/local-reader.edn"
task_root=""
cleanup() {
  if [[ "$task_root" =~ ^/tmp/current-typed-expect\.[A-Za-z0-9]+$ ]]; then
    rm -rf -- "$task_root"
  fi
}
trap cleanup EXIT
for task_wait in 1000 10; do
  task_output="$task_receipts/expect-$task_wait"
  test ! -e "$task_output"
  mkdir "$task_output"
  cp "$task_receipts/preflight.json" "$task_receipts/local-writer.edn" "$task_receipts/local-reader.edn" "$task_output/"
  task_root=$(mktemp -d /tmp/current-typed-expect.XXXXXXXX)
  export QUALIFIED_STORAGE_SOURCE_PROFILE=expect-continue-ab
  export SDK_S3_EXPECT_WAIT_MS="$task_wait"
  task_driver="$task_candidate/bench/run-typed-storage-window.sh"
  timeout --kill-after=5s 1800s bash "$task_driver" window "$task_root" 10000 256 true wide16 > "$task_root/writer.log" 2>&1
  cp "$task_root/http-budget.edn" "$task_output/s3-writer-budget.edn"
  CHDB_BENCH_METADATA_READER=stock timeout --kill-after=5s 240s bash "$task_driver" reader "$task_root" 10000 256 true wide16 > "$task_root/reader.log" 2>&1
  cp "$task_root/writer.edn" "$task_output/s3-writer.edn"
  cp "$task_root/reader.edn" "$task_output/s3-reader.edn"
  cp "$task_root/http-budget.edn" "$task_output/s3-reader-budget.edn"
  cleanup
  task_root=""
done
