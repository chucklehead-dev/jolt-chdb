#!/usr/bin/env bash
# Compare two exact data.json provider checkouts through fresh Jolt Durable
# readers.  This runner is intentionally separate from the Rust/Jolt oracle.
set -euo pipefail

if [[ $# -lt 8 || $# -gt 10 ]]; then
  echo "usage: $0 OUTPUT_DIR A_CHDB_CHECKOUT B_CHDB_CHECKOUT JOLT_BIN JOLT_SOURCE_SHA LIBCHDB A_DATA_JSON_SHA B_DATA_JSON_SHA [WARMUP_BATCHES MEASURED_BATCHES]" >&2
  exit 2
fi
output_dir=$(realpath -m "$1")
a_checkout=$(realpath "$2"); b_checkout=$(realpath "$3"); jolt_bin=$(realpath "$4")
jolt_source_sha=$5; libchdb=$(realpath "$6"); a_data_json_sha=$7; b_data_json_sha=$8
warmup=${9:-2}; measured=${10:-100}
repo_root=$(cd "$(dirname "$0")/.." && pwd -P)
wrapper=/home/chuck/ai-src/tools/jolt-with-chez-10.4.1
raw="$output_dir/raw"; reports="$output_dir/reports"; fixture_root="$raw/fixture-store"; raw_reports="$raw/reports"
native_dir=$(dirname "$libchdb"); native_header="$native_dir/chdb.h"; jolt_version=""
time_bin=${BENCH_TIME_BIN:-/usr/bin/time}

fail() { echo "data.json Durable recovery A/B/B/A failed: $*" >&2; exit 2; }
[[ -x "$jolt_bin" && -f "$libchdb" && -f "$native_header" ]] || fail "Jolt executable or exact native artifacts are missing"
[[ "$jolt_source_sha" =~ ^[0-9a-f]{40}$ && "$a_data_json_sha" =~ ^[0-9a-f]{40}$ && "$b_data_json_sha" =~ ^[0-9a-f]{40}$ ]] || fail "source assertions must be full lowercase Git SHAs"
[[ "$a_data_json_sha" != "$b_data_json_sha" ]] || fail "A and B must name distinct data.json source SHAs"
[[ "$warmup" == 2 && "$measured" == 100 ]] || fail "this bounded experiment fixes the current 52,224-row fixture (2 warmup + 100 measured 512-row batches)"
[[ ! -e "$output_dir" || -z $(find "$output_dir" -mindepth 1 -print -quit) ]] || fail "output directory must be absent or empty"
for checkout in "$a_checkout" "$b_checkout"; do
  [[ -d "$checkout/.git" || -f "$checkout/.git" ]] || fail "condition checkout is not a Git worktree"
  [[ -z $(git -C "$checkout" status --porcelain) ]] || fail "condition checkout is not clean"
done
[[ $(git -C "$a_checkout" diff --name-only "$(git -C "$a_checkout" rev-parse HEAD)" "$(git -C "$b_checkout" rev-parse HEAD)") == "deps.edn" ]] || fail "A/B checkouts must differ only in deps.edn"
mkdir -p "$raw" "$output_dir/provider"

# Resolve actual classpaths before fixture creation. Each condition owns its
# condition-scoped cache/gitlibs directory, and the capture script rejects duplicate/foreign
# data.json namespace roots rather than trusting deps.edn text.
for condition in A B; do
  checkout_var=${condition,,}_checkout; sha_var=${condition,,}_data_json_sha
  checkout=${!checkout_var}; provider_sha=${!sha_var}; scope="$output_dir/$condition-cache"
  mkdir -p "$scope/cache" "$scope/gitlibs"
  JOLT_CACHE_DIR="$scope/cache" JOLT_GITLIBS_DIR="$scope/gitlibs" \
    "$wrapper" python3 "$repo_root/scripts/capture-durable-data-json-provider.py" "$checkout" "$jolt_bin" "$provider_sha" "$output_dir/provider/$condition.json"
  JOLT_CACHE_DIR="$scope/cache" JOLT_GITLIBS_DIR="$scope/gitlibs" \
    "$wrapper" "$jolt_bin" -Srepro -Sdescribe > "$output_dir/provider/$condition-sdescribe.edn" || fail "Jolt -Sdescribe failed for condition $condition"
done
jolt_version=$(JOLT_CACHE_DIR="$output_dir/A-cache/cache" JOLT_GITLIBS_DIR="$output_dir/A-cache/gitlibs" "$wrapper" "$jolt_bin" --version)

# Reuse only the immutable fixture producer from the cross-binding runner;
# none of its Rust recovery measurements participate in this experiment.
python3 "$repo_root/scripts/prepare-durable-cross-binding-run.py" "$repo_root" "$raw_reports" data-json-abba 5 512 "$warmup" "$measured"
rust_target=${BENCH_RUST_TARGET_DIR:-$raw/rust-target}
CHDB_LIB_DIR="$native_dir" CHDB_INCLUDE_DIR="$native_dir" CARGO_TARGET_DIR="$rust_target" \
 BENCH_NATIVE_LIBRARY="$libchdb" BENCH_NATIVE_HEADER="$native_header" BENCH_RUSTC_VERSION="$(rustc --version)" BENCH_CARGO_VERSION="$(cargo --version)" BENCH_HARNESS_STATE_FILE="$raw_reports/harness-state.json" \
 cargo build --locked --release --manifest-path "$repo_root/bench/rust-durable-recovery-oracle/Cargo.toml"
LD_LIBRARY_PATH="$native_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" BENCH_NATIVE_LIBRARY="$libchdb" BENCH_NATIVE_HEADER="$native_header" BENCH_RUSTC_VERSION="$(rustc --version)" BENCH_CARGO_VERSION="$(cargo --version)" BENCH_HARNESS_STATE_FILE="$raw_reports/harness-state.json" \
 "$rust_target/release/jolt-chdb-rust-recovery-oracle" prepare "$fixture_root" data-json-abba "$raw_reports/run-manifest.json" "$raw_reports/fixture.json"

python3 "$repo_root/scripts/prepare-durable-data-json-recovery-abba.py" "$reports" "$raw_reports/fixture.json" "$a_checkout" "$b_checkout" "$output_dir/provider/A.json" "$output_dir/provider/B.json" "$jolt_bin" "$jolt_source_sha" "$libchdb" "$native_header" "$jolt_version" "$output_dir/provider/A-sdescribe.edn" "$output_dir/provider/B-sdescribe.edn"

# The existing reader has six Jolt schedule slots: prime plus five measured.
# We consume exactly six as outer A-prime/B-prime/A/B/B/A and normalize their
# already-validated receipts into the independent outer schedule.
verify_condition() {
  local condition=$1 checkout=$2 provider=$3 expected_head=$4 expected_tree=$5
  [[ -z $(git -C "$checkout" status --porcelain) ]] || fail "condition $condition checkout became dirty"
  [[ $(git -C "$checkout" rev-parse HEAD) == "$expected_head" && $(git -C "$checkout" rev-parse 'HEAD^{tree}') == "$expected_tree" ]] || fail "condition $condition checkout identity changed"
  python3 "$repo_root/scripts/capture-durable-data-json-provider.py" --verify "$provider" || fail "condition $condition provider identity changed"
}
condition_head_A=$(git -C "$a_checkout" rev-parse HEAD); condition_tree_A=$(git -C "$a_checkout" rev-parse 'HEAD^{tree}')
condition_head_B=$(git -C "$b_checkout" rev-parse HEAD); condition_tree_B=$(git -C "$b_checkout" rev-parse 'HEAD^{tree}')
declare -a conditions=(A B A B B A) observations=(none none 1 1 2 2) phases=(prime prime measured measured measured measured) legacy_ordinals=(1 3 4 7 8 11) legacy_phases=(prime measured measured measured measured measured) legacy_trials=(none 1 2 3 4 5)
for index in "${!conditions[@]}"; do
  condition=${conditions[$index]}; checkout_var=${condition,,}_checkout; checkout=${!checkout_var}; scope="$output_dir/$condition-cache"
  if [[ "$condition" == A ]]; then verify_condition A "$checkout" "$output_dir/provider/A.json" "$condition_head_A" "$condition_tree_A"; else verify_condition B "$checkout" "$output_dir/provider/B.json" "$condition_head_B" "$condition_tree_B"; fi
  label=$([[ ${phases[$index]} == prime ]] && echo prime || echo "${observations[$index]}")
  legacy_dir="$raw/legacy-$index"; mkdir -p "$legacy_dir"
  if [[ ${legacy_trials[$index]} == none ]]; then legacy_name=jolt-prime; else legacy_name="jolt-trial-${legacy_trials[$index]}"; fi
  raw_receipt="$legacy_dir/$legacy_name.json"; report="$reports/$condition-$label.json"; timing="$reports/$condition-$label.time"
  # Re-resolve the live classpath now, not merely the root captured before
  # fixture setup. A changed cache or dependency resolution must stop before
  # the next fresh reader can be attributed to its prepared condition.
  resolved_provider="$legacy_dir/$condition-provider.json"
  provider_sha_var=${condition,,}_data_json_sha; provider_sha=${!provider_sha_var}
  JOLT_CACHE_DIR="$scope/cache" JOLT_GITLIBS_DIR="$scope/gitlibs" \
    "$wrapper" python3 "$repo_root/scripts/capture-durable-data-json-provider.py" "$checkout" "$jolt_bin" "$provider_sha" "$resolved_provider"
  cmp -s "$output_dir/provider/$condition.json" "$resolved_provider" || fail "condition $condition resolved provider differs from prepared provider"
  python3 "$repo_root/scripts/capture-durable-data-json-provider.py" --verify "$resolved_provider" || fail "condition $condition re-resolved provider is no longer clean"
  "$time_bin" -v -o "$timing" env JOLT_CACHE_DIR="$scope/cache" JOLT_GITLIBS_DIR="$scope/gitlibs" BENCH_JOLT_BIN="$jolt_bin" BENCH_JOLT_SOURCE_SHA_ASSERTED="$jolt_source_sha" BENCH_JOLT_EXECUTABLE_REVISION="${jolt_version##*-g}" BENCH_JOLT_VERSION="$jolt_version" BENCH_JOLT_DESCRIBE="$output_dir/provider/$condition-sdescribe.edn" BENCH_NATIVE_LIBRARY="$libchdb" BENCH_NATIVE_HEADER="$native_header" BENCH_HARNESS_STATE_FILE="$raw_reports/harness-state.json" JOLT_CHDB_LIB="$libchdb" \
    bash -c 'cd "$1" && "$2" "$3" -Srepro -M:durable-cross-binding-recovery "$4" data-json-abba "$5" "$6" "$7" "$8"' bash "$checkout" "$wrapper" "$jolt_bin" "$fixture_root" "$raw_reports/fixture.json" "$raw_reports/run-manifest.json" "${legacy_ordinals[$index]}" "$raw_receipt"
  python3 "$repo_root/scripts/normalize-durable-data-json-recovery-abba.py" "$raw_receipt" "$reports/run-manifest.json" "$condition" "${observations[$index]}" "${phases[$index]}" "$index" "${legacy_ordinals[$index]}" "${legacy_phases[$index]}" "${legacy_trials[$index]}" "$reports/run-manifest.json" "$report"
done
python3 "$repo_root/scripts/summarize-durable-data-json-recovery-abba.py" "$reports" "$reports/summary.json"
echo "Data.json Durable recovery A/B/B/A completed; summary is $reports/summary.json"
