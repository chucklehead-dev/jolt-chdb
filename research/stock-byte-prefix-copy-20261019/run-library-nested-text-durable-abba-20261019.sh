#!/usr/bin/env bash
set -euo pipefail
task_ev=/home/chuck/ai-src/evidence
task_candidate=/home/chuck/ai-src/worktrees/exporter-library-nested-byte-text-20261019
task_reader=/home/chuck/ai-src/worktrees/exporter-live-empty-link-writers-20261018
task_out=${DURABLE_ABBA_OUTPUT:-$task_ev/library-nested-text-durable-abba-20261019}
task_manifest=${DURABLE_ABBA_MANIFEST:-$task_ev/library-nested-text-candidate-20261019.sources.sha256}
task_patch=${DURABLE_ABBA_PATCH:-$task_ev/library-nested-text-candidate-20261019.diff}
task_program=${DURABLE_ABBA_PROGRAM:-$task_ev/library-nested-text-durable-arm-20261019.clj}
read -r -a task_arms <<< "${DURABLE_ABBA_ARMS:-string bytes bytes string}"
test ! -e "$task_out"
mkdir -p "$task_out"
cd "$task_candidate"
sha256sum -c "$task_manifest"
cp "$task_patch" "$task_out/candidate.diff"
task_writer_deps='{:paths ["src" "test" "bench" "/home/chuck/ai-src/worktrees/chdb-current-typed-storage-0821-20261014/test" "/home/chuck/ai-src/worktrees/chdb-current-typed-storage-0821-20261014/bench"] :deps {io.github.chucklehead-dev/jolt-chdb {:local/root "/home/chuck/ai-src/worktrees/chdb-stock-owned-byte-adoption-20261019"}}}'
task_writer_deps=${DURABLE_ABBA_DEPS:-$task_writer_deps}
task_reader_deps='{:paths ["src" "test" "bench" "/home/chuck/ai-src/worktrees/chdb-current-typed-storage-0821-20261014/test" "/home/chuck/ai-src/worktrees/chdb-current-typed-storage-0821-20261014/bench"] :deps {org.clojure/data.json {:local/root "/home/chuck/ai-src/worktrees/data-json-qualified-8d557cc-20261017"} io.github.casselc/otel {:local/root "/home/chuck/ai-src/worktrees/otel-batch-threshold-wakeup-20261017"}}}'
export JOLT_CHDB_LIB=/home/chuck/.cache/jolt-chdb/26.9.0/linux-amd64/libchdb.so
export SDK_STORAGE_ARM=local SDK_STREAM_ARM=stream SDK_EXPORT_CALLERS=1
export CHDB_BENCH_OWNED_OUTPUT=true CHDB_BENCH_SAMPLE_COUNTERS=true
export BENCH_CLK_TCK
BENCH_CLK_TCK=$(getconf CLK_TCK)
task_index=0
for task_arm in "${task_arms[@]}"; do
  task_store=$(mktemp -d /tmp/library-nested-abba-20261019.XXXXXXXX)
  task_report=$task_out/$task_index-$task_arm
  printf 'start index=%s arm=%s store=%s\n' "$task_index" "$task_arm" "$task_store"
  cd "$task_candidate"
  /home/chuck/ai-src/tools/jolt-with-chez-10.4.1 env JOLT_NO_USER_DEPS=1 JOLT_AOT_CACHE=0 \
    /home/chuck/.cache/jolt-stock-v0.8.23-codec/jolt-v0.8.23-x86_64-linux/jolt \
    -Srepro -Sdeps "$task_writer_deps" -A:experimental-span-codec \
    "$task_program" "$task_store" "$task_arm" \
    > "$task_report.writer.log" 2>&1
  cp "$task_store/writer.edn" "$task_report.writer.edn"
  cd "$task_reader"
  /home/chuck/ai-src/tools/jolt-with-chez-10.4.1 env CHDB_BENCH_METADATA_READER=stock \
    "$task_ev/jolt-measured-1ae-20261015.sh" -Srepro -Sdeps "$task_reader_deps" \
    "$task_ev/nested-sorted-sdk-durable-reader-20261018.clj" "$task_store" \
    > "$task_report.reader.log" 2>&1
  cp "$task_store/reader.edn" "$task_report.reader.edn"
  cmp "$task_store/writer.edn" "$task_report.writer.edn"
  cmp "$task_store/reader.edn" "$task_report.reader.edn"
  bb /home/chuck/ai-src/worktrees/exporter-declared-string-evidence-20261018/research/fork-free-codec-20261019/validate-durable.clj \
    "$task_report" on > "$task_report.validation.edn"
  du -sk "$task_store" > "$task_report.retired-kib.txt"
  case "$task_store" in /tmp/library-nested-abba-20261019.????????) ;; *) exit 91;; esac
  test ! -L "$task_store"
  test "$(realpath "$task_store")" = "$task_store"
  rm -rf -- "$task_store"
  test ! -e "$task_store"
  cd "$task_candidate"
  sha256sum -c "$task_manifest" > "$task_report.source-stability.log"
  printf 'verified-and-cleaned index=%s arm=%s\n' "$task_index" "$task_arm"
  task_index=$((task_index + 1))
done
printf 'terminal0: four arms written, independently recovered, validated and cleaned\n'
