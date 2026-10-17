#!/usr/bin/env bash
# Closed current-product graph around an unchanged, independently pinned fixture.
# The protected consumer authenticates the compiler artifact before this runs.
set -euo pipefail
test "$#" -eq 6
case "$1" in window|reader) ;; *) exit 64 ;; esac
test "$3" = 10000
test "$4" = 256
test "$5" = true
test "$6" = wide16
task_benchmark=${BENCH_EXPECT_BENCHMARK_ROOT:?exact benchmark checkout required}
task_exporter=${BENCH_EXPECT_EXPORTER_ROOT:?exact exporter checkout required}
task_json=${BENCH_EXPECT_JSON_ROOT:?exact JSON checkout required}
task_jolt=${BENCH_JOLT_BIN:?exact standalone runtime required}
task_wrapper=${BENCH_JOLT_WRAPPER:?pinned toolchain wrapper required}
task_driver=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)

# Reject drift before any process capable of storage access is invoked.
test "${BENCH_RUNTIME_BINARY_SHA256:?}" = 06a2ccb858c072bfcd74ce94d96eb95f008d75e0fb1f46117728602572189811
test "${QUALIFIED_RUNTIME_PROFILE:?}" = current-0821-1ae7
test "${QUALIFIED_RUNTIME_BINARY_SHA256:?}" = "$BENCH_RUNTIME_BINARY_SHA256"
test "$(sha256sum "$task_jolt" | cut -d ' ' -f1)" = "$BENCH_RUNTIME_BINARY_SHA256"
test "$(git -C "$task_benchmark" rev-parse HEAD)" = 3fd96c4691c5db49ed4d3ef55622aaa6b9074127
test "$(git -C "$task_exporter" rev-parse HEAD)" = 5be78d565be0a7a929d108be929540c1b4f19524
test "$(git -C "$task_json" rev-parse HEAD)" = 8d557cc18eb9526ed29ae5613089c3793d5973fa
for task_checkout in "$task_benchmark" "$task_exporter" "$task_json"; do
  git -C "$task_checkout" diff --quiet HEAD
done
git -C "$task_driver" merge-base --is-ancestor 3d766fa295b0e98c30f275d0a334db92e6dd998c HEAD
git -C "$task_driver" diff --quiet 3d766fa295b0e98c30f275d0a334db92e6dd998c -- src resources deps.edn
test "$(sha256sum "${JOLT_CHDB_LIB:?exact native library required}" | cut -d ' ' -f1)" = a7d22ade86e1de82b187adf572b569a6034600ce932c52b0acb8c97b82430557
case "$task_benchmark$task_exporter$task_json$task_driver" in
  *'"'*|*'\'*|*$'\n'*|*$'\r'*) exit 64 ;;
esac
test "$("$task_wrapper" "$task_jolt" --version)" = 'jolt v0.8.21-58-g1ae7e0d1'

# Only the fixture's bench/test roots are borrowed. Its historical deps.edn,
# launcher and library src/resources do not select the product implementation.
task_deps="{:paths [\"$task_benchmark/bench\" \"$task_benchmark/test\" \"$task_exporter/src\" \"$task_exporter/resources\" \"$task_exporter/test\" \"$task_exporter/bench\"] :deps {io.github.casselc/jolt-http {:git/url \"https://github.com/casselc/jolt-http.git\" :git/sha \"35d1d7f9ebdc796ee9bd4c80745298b2c8b7fdf8\"} io.github.chucklehead-dev/jolt-hegel {:git/url \"https://github.com/chucklehead-dev/jolt-hegel.git\" :git/sha \"b214f769983211431c74e427f0f35553cfba7b34\"} io.github.casselc/otel {:git/url \"https://github.com/casselc/otel.git\" :git/sha \"d93846e40b837dcecb546fb59f6cb05ca639f054\" :exclusions [jolt-lang/jolt-crypto]} jolt-lang/jolt-crypto {:git/url \"https://github.com/jolt-lang/jolt-crypto.git\" :git/sha \"5effcc89a3258499a79a2a3d69edad9e7800d1bf\"} io.github.chucklehead-dev/jolt-chdb {:local/root \"$task_driver\"} io.github.chucklehead-dev/jolt-otel-clickhouse {:local/root \"$task_exporter\"} org.clojure/data.json {:local/root \"$task_json\"}}}"
cd /tmp
exec env JOLT_AOT_CACHE=0 JOLT_NO_USER_DEPS=1 \
  "$task_wrapper" "$task_jolt" -Srepro -Sdeps "$task_deps" \
  "$task_benchmark/bench/typed-storage-window.clj" "$@"
