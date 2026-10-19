#!/usr/bin/env bash
set -euo pipefail
task_ev=/home/chuck/ai-src/evidence
task_out=$task_ev/stock-batch-geometry-screen-20261019
task_manifest=$task_ev/stock-batch-geometry-20261019.sources.sha256
task_exporter=/home/chuck/ai-src/worktrees/exporter-library-nested-byte-text-20261019
task_reader=/home/chuck/ai-src/worktrees/exporter-live-empty-link-writers-20261018
task_writer_deps='{:paths ["src" "test" "bench" "/home/chuck/ai-src/worktrees/chdb-current-typed-storage-0821-20261014/test" "/home/chuck/ai-src/worktrees/chdb-current-typed-storage-0821-20261014/bench"] :deps {io.github.chucklehead-dev/jolt-chdb {:local/root "/home/chuck/ai-src/worktrees/chdb-stock-output-prefix-20261019"}}}'
task_reader_deps='{:paths ["src" "test" "bench" "/home/chuck/ai-src/worktrees/chdb-current-typed-storage-0821-20261014/test" "/home/chuck/ai-src/worktrees/chdb-current-typed-storage-0821-20261014/bench"] :deps {org.clojure/data.json {:local/root "/home/chuck/ai-src/worktrees/data-json-qualified-8d557cc-20261017"} io.github.casselc/otel {:local/root "/home/chuck/ai-src/worktrees/otel-batch-threshold-wakeup-20261017"}}}'
test ! -e "$task_out"
mkdir -p "$task_out"
sha256sum -c "$task_manifest" > "$task_out/source-preflight.log"
export JOLT_CHDB_LIB=/home/chuck/.cache/jolt-chdb/26.9.0/linux-amd64/libchdb.so
export SDK_STORAGE_ARM=local SDK_STREAM_ARM=stream SDK_EXPORT_CALLERS=1
export CHDB_BENCH_OWNED_OUTPUT=true CHDB_BENCH_SAMPLE_COUNTERS=true
export BENCH_CLK_TCK
BENCH_CLK_TCK=$(getconf CLK_TCK)
for task_geometry in 5000:1 10000:1 10000:2 5000:2; do
  task_batch=${task_geometry%:*}
  task_lanes=${task_geometry#*:}
  task_store=$(mktemp -d /tmp/stock-batch-geometry-20261019.XXXXXXXX)
  task_report=$task_out/$task_batch-$task_lanes
  printf 'start batch=%s lanes=%s store=%s\n' "$task_batch" "$task_lanes" "$task_store"
  cd "$task_exporter"
  /home/chuck/ai-src/tools/jolt-with-chez-10.4.1 env JOLT_NO_USER_DEPS=1 JOLT_AOT_CACHE=0 \
    /home/chuck/.cache/jolt-stock-v0.8.23-codec/jolt-v0.8.23-x86_64-linux/jolt \
    -Srepro -Sdeps "$task_writer_deps" -A:experimental-span-codec \
    "$task_ev/stock-batch-geometry-writer-20261019.clj" "$task_store" "$task_batch" "$task_lanes" \
    > "$task_report.writer.log" 2>&1
  cp "$task_store/writer.edn" "$task_report.writer.edn"
  cd "$task_reader"
  /home/chuck/ai-src/tools/jolt-with-chez-10.4.1 env CHDB_BENCH_METADATA_READER=stock \
    "$task_ev/jolt-measured-1ae-20261015.sh" -Srepro -Sdeps "$task_reader_deps" \
    "$task_ev/stock-batch-geometry-reader-20261019.clj" "$task_store" "$task_batch" \
    > "$task_report.reader.log" 2>&1
  cp "$task_store/reader.edn" "$task_report.reader.edn"
  cmp "$task_store/writer.edn" "$task_report.writer.edn"
  cmp "$task_store/reader.edn" "$task_report.reader.edn"
  bb "$task_ev/validate-stock-batch-geometry-20261019.bb" "$task_report" "$task_batch" "$task_lanes" \
    > "$task_report.validation.edn"
  sha256sum -c "$task_manifest" > "$task_report.source-stability.log"
  du -sk "$task_store" > "$task_report.retired-kib.txt"
  case "$task_store" in /tmp/stock-batch-geometry-20261019.????????) ;; *) exit 91;; esac
  test ! -L "$task_store"
  test "$(realpath "$task_store")" = "$task_store"
  rm -rf -- "$task_store"
  test ! -e "$task_store"
  printf 'verified-and-cleaned batch=%s lanes=%s\n' "$task_batch" "$task_lanes"
done
printf 'terminal0: all geometry screens independently recovered, validated and cleaned\n'
