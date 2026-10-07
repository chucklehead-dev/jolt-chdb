#!/usr/bin/env bash
# Run through a pinned Chez wrapper. This benchmark deliberately uses an exact
# external exporter benchmark, not the library's default dependency pins.
set -euo pipefail
test "$#" -eq 6
task_exporter=${BENCH_EXPECT_EXPORTER_ROOT:?exact exporter checkout required}
task_json=${BENCH_EXPECT_JSON_ROOT:?exact JSON checkout required}
task_jolt=${BENCH_JOLT_BIN:?measured standalone Jolt required}
task_wrapper=${BENCH_JOLT_WRAPPER:?pinned Chez command wrapper required}
task_chdb=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)

# These are source identities, not promises made by the exporter's transitive
# dependency declaration. The explicit overrides below must actually resolve.
test "$(git -C "$task_exporter" rev-parse HEAD)" = 715cf81509e007b3eb3cfe873c5edf311f49e278
test "$(git -C "$task_json" rev-parse HEAD)" = 3adc8c5d3a57a30d18f62bad9d1c7a5c1f6857e0
git -C "$task_exporter" diff --quiet HEAD
git -C "$task_json" diff --quiet HEAD
git -C "$task_chdb" merge-base --is-ancestor ca7fc8a6ec1d65f8efb153e8b4c1935836f9440e HEAD
git -C "$task_chdb" diff --quiet ca7fc8a6ec1d65f8efb153e8b4c1935836f9440e -- src resources deps.edn
task_version=$("$task_wrapper" "$task_jolt" --version)
case "$task_version" in
  'jolt v0.8.17-37-gab9b8580'|'jolt v0.8.17-38-gf1116c53'|'jolt v0.8.17-39-gbcb376a0') ;;
  *) printf 'Unexpected benchmark runtime version\n' >&2; exit 1 ;;
esac

# Reject paths that cannot safely be represented as these EDN string literals.
case "$task_exporter$task_json$task_chdb" in
  *'"'*|*'\'*|*$'\n'*|*$'\r'*) exit 1 ;;
esac
task_deps="{:paths [\"src\" \"bench\" \"$task_chdb/bench\"] :aliases {:storage-driver {:extra-deps {io.github.chucklehead-dev/jolt-chdb {:local/root \"$task_chdb\"} org.clojure/data.json {:local/root \"$task_json\"}}}}}"
cd "$task_exporter"
exec env JOLT_AOT_CACHE=0 BENCH_EXPECT_CHDB_ROOT="$task_chdb" \
  "$task_wrapper" "$task_jolt" -Srepro -Sdeps "$task_deps" \
  -M:native-json-string-cache:storage-driver \
  -m jdbc.chdb-exporter-storage-qualification "$@"
