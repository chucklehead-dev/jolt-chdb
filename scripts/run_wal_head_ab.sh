#!/usr/bin/env bash
# Fixed same-host comparison; retain scalar receipts only, never process logs.
set -euo pipefail
test "$#" -eq 3
task_baseline=$(realpath "$1")
task_candidate=$(realpath "$2")
task_receipts=$(realpath "$3")
test "$(git -C "$task_baseline" rev-parse HEAD)" = 3fd96c4691c5db49ed4d3ef55622aaa6b9074127
test "$(git -C "$task_candidate" rev-parse HEAD)" = 1a23e211dda170e0d927728d8035d1f823e0d0be
for task_source in "$task_baseline" "$task_candidate" "${BENCH_EXPECT_EXPORTER_ROOT:?}" "${BENCH_EXPECT_JSON_ROOT:?}"; do
  git -C "$task_source" diff --quiet HEAD
  task_untracked=$(git -C "$task_source" ls-files --others -- src resources deps.edn bench test)
  test -z "$task_untracked"
done
test -f "$task_receipts/preflight.json"
for task_arm in baseline candidate; do
  test ! -e "$task_receipts/$task_arm"
  mkdir "$task_receipts/$task_arm"
  cp "$task_receipts/preflight.json" "$task_receipts/$task_arm/"
done
task_root=""
cleanup() {
  if [[ "$task_root" =~ ^/tmp/current-typed-wal-head\.[A-Za-z0-9]+$ ]]; then
    rm -rf -- "$task_root"
  fi
}
trap cleanup EXIT
# Keep the S3 windows adjacent. One live fixture at a time. The shared selected
# driver supplies distinct closed S3 object IDs; random local roots alone cannot.
for task_storage in local s3; do
  export SDK_STORAGE_ARM="$task_storage"
  for task_arm in baseline candidate; do
    export SDK_WAL_HEAD_ARM="$task_arm"
    task_output="$task_receipts/$task_arm"
    case "$task_arm" in
      baseline) task_source="$task_baseline"; unset QUALIFIED_STORAGE_SOURCE_PROFILE ;;
      candidate) task_source="$task_candidate"; export QUALIFIED_STORAGE_SOURCE_PROFILE=wal-single-head ;;
    esac
    export TASK_ORIGINAL_TYPED_DRIVER="$task_source/bench/typed-storage-window.clj"
    task_driver="$task_source/bench/run-typed-storage-window.sh"
    task_root=$(mktemp -d /tmp/current-typed-wal-head.XXXXXXXX)
    task_seconds=360
    if [[ "$task_storage" == s3 ]]; then task_seconds=1200; fi
    timeout --kill-after=5s "${task_seconds}s" bash "$task_driver" window "$task_root" 10000 256 true wide16 > "$task_root/writer.log" 2>&1
    if [[ "$task_storage" == s3 ]]; then
      cp "$task_root/http-budget.edn" "$task_output/s3-writer-budget.edn"
      cp "$task_root/curl-profile.edn" "$task_output/s3-writer-curl-profile.edn"
    fi
    CHDB_BENCH_METADATA_READER=stock timeout --kill-after=5s 240s bash "$task_driver" reader "$task_root" 10000 256 true wide16 > "$task_root/reader.log" 2>&1
    cp "$task_root/writer.edn" "$task_output/$task_storage-writer.edn"
    cp "$task_root/reader.edn" "$task_output/$task_storage-reader.edn"
    if [[ "$task_storage" == s3 ]]; then
      cp "$task_root/http-budget.edn" "$task_output/s3-reader-budget.edn"
      cp "$task_root/curl-profile.edn" "$task_output/s3-reader-curl-profile.edn"
    fi
    cleanup
    task_root=""
  done
done
