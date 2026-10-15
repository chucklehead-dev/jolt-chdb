#!/usr/bin/env bash
# Exact current-stack driver. The artifact consumer is responsible for proving
# its immutable producer/profile before selecting an independently pinned binary.
set -euo pipefail
test "$#" -eq 6
case "$1" in writer|window|reader) ;; *) exit 64 ;; esac
task_exporter=${BENCH_EXPECT_EXPORTER_ROOT:?exact exporter checkout required}
task_json=${BENCH_EXPECT_JSON_ROOT:?exact JSON checkout required}
task_jolt=${BENCH_JOLT_BIN:?exact standalone runtime required}
task_wrapper=${BENCH_JOLT_WRAPPER:?pinned toolchain wrapper required}
task_binary_sha=${BENCH_RUNTIME_BINARY_SHA256:?independent binary digest required}
task_chdb=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)

[[ "$task_binary_sha" =~ ^[a-f0-9]{64}$ ]]
test "$(sha256sum "$task_jolt" | cut -d ' ' -f1)" = "$task_binary_sha"
task_version=$("$task_wrapper" "$task_jolt" --version)
case "$task_version" in
  'jolt v0.8.17-219-g3250c32d')
    # Existing measured binary contains 0.8.21 ancestry; its baked label predates
    # fetching the upstream tag. This is not a stock 0.8.17 compatibility lane.
    test "$task_binary_sha" = 677615fba7cb7548d92cbdf8b000eb1854e11b9a19ec9405b492acc12ba06dda ;;
  'jolt v0.8.21-58-g1ae7e0d1')
    test "${QUALIFIED_RUNTIME_PROFILE:-}" = current-0821-1ae7
    # The protected consumer must authenticate the full immutable pair first.
    test "$task_binary_sha" = "${QUALIFIED_RUNTIME_BINARY_SHA256:?qualified pair required}" ;;
  *) echo unexpected-typed-storage-runtime >&2; exit 1 ;;
esac

test "$(git -C "$task_exporter" rev-parse HEAD)" = 4f3bfd2bac88227710a3687b2a14fb5274689ef8
test "$(git -C "$task_json" rev-parse HEAD)" = b959a5625c28cc8571e04de5ca02a2c8902b8b5e
git -C "$task_exporter" diff --quiet HEAD
git -C "$task_json" diff --quiet HEAD
git -C "$task_chdb" merge-base --is-ancestor 291e1f43a859428f737211c0e9853e1616b54097 HEAD
case "${QUALIFIED_STORAGE_SOURCE_PROFILE:-baseline}" in
  baseline)
    git -C "$task_chdb" diff --quiet 291e1f43a859428f737211c0e9853e1616b54097 -- src resources deps.edn ;;
  wal-single-head)
    # Exactly the reviewed control blob on the frozen feature graph, not a
    # mutable branch or a caller-selected source revision. All other production
    # files/dependencies retain baseline contents.
    git -C "$task_chdb" diff --quiet 85738d512025ae056f7721c6ffd89c061046d94e -- src resources deps.edn
    test "$(git -C "$task_chdb" rev-parse HEAD:src/jdbc/chdb/durable/control.clj)" = c198a3887fd8c4073763676e32c9945a58f34913 ;;
  wal-selection-head)
    # Surgical selection/publication-read change on the same measured graph.
    # Preserve owned-statement admission, every other production file and pins.
    git -C "$task_chdb" diff --quiet 6ae42f4e57aa131cf020269aea306c196585cb9e -- src resources deps.edn
    test "$(git -C "$task_chdb" rev-parse HEAD:src/jdbc/chdb/durable/control.clj)" = 180d2d018c8b153ed9fc812528e909076f6ed7f1
    test "$(git -C "$task_chdb" rev-parse HEAD:src/jdbc/chdb/durable/writer.clj)" = 16cb452d375d59418f30718d73bdfa2a2bb431bd ;;
  wal-selection-head-seal)
    # Same measured graph with the reviewed seal-error provenance correction.
    # Keep the historical profile above closed over its original source.
    git -C "$task_chdb" diff --quiet 92526362d112c2da0aa4993f09db8c25c42cba45 -- src resources deps.edn
    test "$(git -C "$task_chdb" rev-parse HEAD:src/jdbc/chdb/durable/control.clj)" = 180d2d018c8b153ed9fc812528e909076f6ed7f1
    test "$(git -C "$task_chdb" rev-parse HEAD:src/jdbc/chdb/durable/writer.clj)" = 6eac0599c0e33d4971305011355c3a9de3b1bcc1 ;;
  *) echo unexpected-typed-storage-source-profile >&2; exit 64 ;;
esac
test -f "${JOLT_CHDB_LIB:?exact native library required}"
test "$(sha256sum "$JOLT_CHDB_LIB" | cut -d ' ' -f1)" = a7d22ade86e1de82b187adf572b569a6034600ce932c52b0acb8c97b82430557

case "$task_exporter$task_json$task_chdb" in
  *'"'*|*'\'*|*$'\n'*|*$'\r'*) exit 64 ;;
esac
task_deps="{:paths [\"$task_chdb/bench\" \"$task_chdb/test\" \"$task_exporter/src\" \"$task_exporter/resources\" \"$task_exporter/test\" \"$task_exporter/bench\"] :deps {io.github.casselc/jolt-http {:git/url \"https://github.com/casselc/jolt-http.git\" :git/sha \"35d1d7f9ebdc796ee9bd4c80745298b2c8b7fdf8\"} io.github.chucklehead-dev/jolt-hegel {:git/url \"https://github.com/chucklehead-dev/jolt-hegel.git\" :git/sha \"b214f769983211431c74e427f0f35553cfba7b34\"} io.github.casselc/otel {:git/url \"https://github.com/casselc/otel.git\" :git/sha \"19fc49d20b3a75906f0ccbb8b50c7e48b03e4813\" :exclusions [jolt-lang/jolt-crypto]} jolt-lang/jolt-crypto {:git/url \"https://github.com/jolt-lang/jolt-crypto.git\" :git/sha \"5effcc89a3258499a79a2a3d69edad9e7800d1bf\"} io.github.chucklehead-dev/jolt-chdb {:local/root \"$task_chdb\"} io.github.chucklehead-dev/jolt-otel-clickhouse {:local/root \"$task_exporter\"} org.clojure/data.json {:local/root \"$task_json\"}}}"
# Deliberately outside the project: its defaults must not select another stack.
cd /tmp
exec env JOLT_AOT_CACHE=0 JOLT_NO_USER_DEPS=1 \
  "$task_wrapper" "$task_jolt" -Srepro -Sdeps "$task_deps" \
  "$task_chdb/bench/typed-storage-window.clj" "$@"
