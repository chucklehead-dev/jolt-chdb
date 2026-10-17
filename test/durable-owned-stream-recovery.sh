#!/usr/bin/env bash
set -euo pipefail
task_writer=${1:?supply the qualified writer runtime}
task_reader=${2:?supply the reader runtime}
: "${JOLT_CHDB_LIB:?select the separately qualified native 26.9.0 library}"
command -v script >/dev/null

run_native() {
  local task_runner=$1 task_log=$2 task_command
  shift 2
  printf -v task_command '%q ' "$task_runner" -Srepro \
    -Sdeps '{:paths ["src" "resources" "test"]}' \
    test/jdbc/chdb_owned_stream_native_runner.clj "$@"
  script --quiet --return --flush --command "$task_command" "$task_log"
  if LC_ALL=C grep -Fq 'current_thread contains invalid address' "$task_log"; then
    printf 'Native stream lifecycle diagnostic detected\n' >&2
    return 1
  fi
  if ! LC_ALL=C grep -Fq 'Owned stream native acceptance completed' "$task_log"; then
    printf 'Native stream acceptance receipt missing\n' >&2
    return 1
  fi
}

for task_outcome in success uncertain; do
  task_store=$(mktemp -d /tmp/chdb-owned-stream-recovery.XXXXXXXX)
  printf 'outcome=%s scratch=%s\n' "$task_outcome" "$task_store"
  run_native "$task_writer" "$task_store/writer.log" option writer "$task_outcome" "$task_store"
  run_native "$task_reader" "$task_store/reader.log" option reader "$task_outcome" "$task_store"
  run_native "$task_reader" "$task_store/control.log" bad-option-read "$task_outcome" "$task_store"
  case "$task_store" in /tmp/chdb-owned-stream-recovery.*) rm -rf -- "$task_store";; *) exit 64;; esac
done
task_store=$(mktemp -d /tmp/chdb-owned-stream-recovery.XXXXXXXX)
printf 'outcome=post-native-loss scratch=%s\n' "$task_store"
run_native "$task_writer" "$task_store/writer.log" uncertainty writer "$task_store"
run_native "$task_reader" "$task_store/reader.log" uncertainty reader "$task_store"
case "$task_store" in /tmp/chdb-owned-stream-recovery.*) rm -rf -- "$task_store";; *) exit 64;; esac
printf 'Owned stream, fallback, uncertainty and ordinary recovery checks passed; successful stores removed\n'
