#!/usr/bin/env bash
# Explicit immutable source/runtime selection; no credential values are logged.
set -euo pipefail
test "$#" -eq 6
case "$1" in writer|window|reader) ;; *) exit 64 ;; esac
test "$5" = true
case "$6" in small|wide16) ;; *) exit 64 ;; esac
task_exporter=${BENCH_EXPECT_EXPORTER_ROOT:?exact exporter checkout required}
task_json=${BENCH_EXPECT_JSON_ROOT:?exact JSON checkout required}
task_jolt=${BENCH_JOLT_BIN:?qualified source Jolt required}
task_wrapper=${BENCH_JOLT_WRAPPER:?pinned Chez wrapper required}
task_checksum=${BENCH_JOLT_SHA256:?qualified runtime checksum required}
task_chdb=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
test "$(git -C "$task_exporter" rev-parse HEAD)" = 4f3bfd2bac88227710a3687b2a14fb5274689ef8
test "$(git -C "$task_json" rev-parse HEAD)" = 679878df187cd376ff91a0cba77c0031ecb02b54
git -C "$task_exporter" diff --quiet HEAD
git -C "$task_json" diff --quiet HEAD
# Benchmark-only additions may differ, but actual library code must match.
git -C "$task_chdb" diff --quiet 291e1f43a859428f737211c0e9853e1616b54097 -- src resources deps.edn
test "$(sha256sum "$task_jolt" | cut -d ' ' -f1)" = "$task_checksum"
test "$("$task_wrapper" "$task_jolt" --version)" = 'jolt v0.8.17-49-g7c57cf7e'
case "$task_exporter$task_json$task_chdb" in
  *'"'*|*'\'*|*$'\n'*|*$'\r'*) exit 64 ;;
esac
task_deps="{:paths [\"$task_chdb/bench\" \"$task_exporter/test\" \"$task_exporter/bench\"] :deps {io.github.chucklehead-dev/jolt-chdb {:local/root \"$task_chdb\"} io.github.chucklehead-dev/jolt-otel-clickhouse {:local/root \"$task_exporter\"} org.clojure/data.json {:local/root \"$task_json\"} io.github.casselc/otel {:git/url \"https://github.com/casselc/otel.git\" :git/sha \"19fc49d20b3a75906f0ccbb8b50c7e48b03e4813\" :exclusions [jolt-lang/jolt-crypto]} jolt-lang/jolt-crypto {:git/url \"https://github.com/jolt-lang/jolt-crypto.git\" :git/sha \"5effcc89a3258499a79a2a3d69edad9e7800d1bf\"} io.github.chucklehead-dev/jolt-hegel {:git/url \"https://github.com/chucklehead-dev/jolt-hegel.git\" :git/sha \"b214f769983211431c74e427f0f35553cfba7b34\"}}}"
cd "$task_chdb"
exec env JOLT_AOT_CACHE=0 "$task_wrapper" "$task_jolt" -Srepro -Sdeps "$task_deps" \
  "$task_chdb/bench/durable-stream-export-qualification.clj" "$@"
