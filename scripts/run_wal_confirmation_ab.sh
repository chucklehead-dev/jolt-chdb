#!/usr/bin/env bash
# Fixed same-host comparison; retain scalar receipts only, never process logs.
set -euo pipefail
test "$#" -eq 3
task_baseline=$(realpath "$1")
task_candidate=$(realpath "$2")
task_receipts=$(realpath "$3")
test "$(git -C "$task_baseline" rev-parse HEAD)" = fd8f961364e73bac370286d5481407b23120e9cd
test "$(git -C "$task_candidate" rev-parse HEAD)" = fd8f961364e73bac370286d5481407b23120e9cd
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
  if [[ "$task_root" =~ ^/tmp/current-typed-wal-confirmation\.[A-Za-z0-9]+$ ]]; then
    rm -rf -- "$task_root"
  fi
}
trap cleanup EXIT
run_phase() {
  local task_phase="$1" task_limit="$2" task_status
  if timeout --kill-after=5s "${task_limit}s" bash "$task_driver" "$task_phase" "$task_root" 10000 256 true wide16 > "$task_root/$task_phase.log" 2>&1; then
    return 0
  else
    task_status=$?
    # Only closed loop-selected enums and the process exit code. Never copy or
    # parse logs, exception messages, URLs, object names, or credentials.
    printf '{:scope :wal-confirmation-process-failure :storage :%s :arm :%s :phase :%s :exit-code %d}\n' \
      "$task_storage" "$task_arm" "$task_phase" "$task_status" > "$task_output/process-failure.edn" || true
    if [[ -f "$task_root/driver-failure.edn" && ! -L "$task_root/driver-failure.edn" ]]; then
      cp "$task_root/driver-failure.edn" "$task_output/driver-failure.edn" || true
    fi
    return "$task_status"
  fi
}
# Keep the S3 windows adjacent. One live fixture at a time. The shared selected
# driver supplies distinct closed S3 object IDs; random local roots alone cannot.
for task_storage in local s3; do
  export SDK_STORAGE_ARM="$task_storage"
  for task_arm in baseline candidate; do
    export SDK_WAL_HEAD_ARM="$task_arm"
    task_output="$task_receipts/$task_arm"
    case "$task_arm" in
      baseline) task_source="$task_baseline"; export QUALIFIED_STORAGE_SOURCE_PROFILE=wal-checksum-confirmation ;;
      candidate) task_source="$task_candidate"; export QUALIFIED_STORAGE_SOURCE_PROFILE=wal-checksum-confirmation ;;
    esac
    export TASK_ORIGINAL_TYPED_DRIVER="$task_source/bench/typed-storage-window.clj"
    task_driver="$task_source/bench/run-typed-storage-window.sh"
    task_root=$(mktemp -d /tmp/current-typed-wal-confirmation.XXXXXXXX)
    task_seconds=360
    if [[ "$task_storage" == s3 ]]; then task_seconds=1200; fi
    run_phase window "$task_seconds"
    if [[ "$task_storage" == s3 ]]; then
      cp "$task_root/http-budget.edn" "$task_output/s3-writer-budget.edn"
      cp "$task_root/curl-profile.edn" "$task_output/s3-writer-curl-profile.edn"
      cp "$task_root/confirmation-selection.edn" "$task_output/s3-writer-confirmation.edn"
    fi
    CHDB_BENCH_METADATA_READER=stock run_phase reader 240
    cp "$task_root/writer.edn" "$task_output/$task_storage-writer.edn"
    cp "$task_root/reader.edn" "$task_output/$task_storage-reader.edn"
    if [[ "$task_storage" == s3 ]]; then
      cp "$task_root/http-budget.edn" "$task_output/s3-reader-budget.edn"
      cp "$task_root/curl-profile.edn" "$task_output/s3-reader-curl-profile.edn"
      cp "$task_root/confirmation-selection.edn" "$task_output/s3-reader-confirmation.edn"
    fi
    cleanup
    task_root=""
  done
done
