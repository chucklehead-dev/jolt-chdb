#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 || $# -gt 8 ]]; then
  echo "usage: $0 OUTPUT_DIR JOLT_BIN JOLT_SOURCE_SHA LIBCHDB [TRIALS [BATCH_SIZE [WARMUP_BATCHES [MEASURED_BATCHES]]]]" >&2
  exit 2
fi

output_dir=$1
jolt_bin=$2
jolt_source_sha=$3
libchdb=$4
trials=${5:-5}
batch_size=${6:-512}
warmup_batches=${7:-2}
measured_batches=${8:-100}
object_id=cross-binding-recovery
repo_root=$(cd "$(dirname "$0")/.." && pwd -P)
wrapper=/home/chuck/ai-src/tools/jolt-with-chez-10.4.1
manifest="$repo_root/bench/rust-durable-recovery-oracle/Cargo.toml"
rust_target=${BENCH_RUST_TARGET_DIR:-$output_dir/rust-target}
fixture_root="$output_dir/fixture-store"
report_dir="$output_dir/reports"
descriptor="$report_dir/fixture.json"
run_manifest="$report_dir/run-manifest.json"
harness_state="$report_dir/harness-state.json"

case "$trials:$batch_size:$warmup_batches:$measured_batches" in
  *[!0-9:]*|0:*|*:0:*|*:*:0:*|*:*:*:0)
    echo "trial and workload counts must be positive integers" >&2
    exit 2
    ;;
esac
if ((trials < 5)); then
  echo "at least five measured trials are required" >&2
  exit 2
fi
if [[ ! -x "$jolt_bin" || ! -f "$libchdb" ]]; then
  echo "Jolt executable and libchdb file must exist" >&2
  exit 2
fi
if [[ -e "$output_dir" ]] && find "$output_dir" -mindepth 1 -print -quit | grep -q .; then
  echo "output directory must be absent or empty" >&2
  exit 2
fi
mkdir -p "$output_dir" "$report_dir"

python3 "$repo_root/scripts/prepare-durable-cross-binding-run.py" \
  "$repo_root" "$report_dir" "$object_id" "$trials" "$batch_size" \
  "$warmup_batches" "$measured_batches"

rustc_version=$(rustc --version)
cargo_version=$(cargo --version)
jolt_version=$("$wrapper" "$jolt_bin" --version)
native_dir=$(dirname "$libchdb")
native_name=$(basename "$libchdb")
native_header="$native_dir/chdb.h"
if [[ "$native_name" != libchdb.so ]]; then
  echo "the matched Rust build expects a libchdb.so artifact" >&2
  exit 2
fi
if [[ ! -f "$native_header" ]]; then
  echo "the exact chdb.h adjacent to libchdb.so is required" >&2
  exit 2
fi

CHDB_LIB_DIR="$native_dir" \
CHDB_INCLUDE_DIR="$native_dir" \
CARGO_TARGET_DIR="$rust_target" \
BENCH_NATIVE_LIBRARY="$libchdb" \
BENCH_NATIVE_HEADER="$native_header" \
BENCH_RUSTC_VERSION="$rustc_version" \
BENCH_CARGO_VERSION="$cargo_version" \
BENCH_HARNESS_STATE_FILE="$harness_state" \
  cargo build --locked --release --manifest-path "$manifest"
rust_bin="$rust_target/release/jolt-chdb-rust-recovery-oracle"

LD_LIBRARY_PATH="$native_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
BENCH_NATIVE_LIBRARY="$libchdb" \
BENCH_NATIVE_HEADER="$native_header" \
BENCH_RUSTC_VERSION="$rustc_version" \
BENCH_CARGO_VERSION="$cargo_version" \
BENCH_HARNESS_STATE_FILE="$harness_state" \
  "$rust_bin" prepare "$fixture_root" "$object_id" "$run_manifest" "$descriptor"

run_rust() {
  local label=$1
  local output=$2
  local ordinal=$3
  /usr/bin/time -v -o "$report_dir/rust-$label.time" \
    env LD_LIBRARY_PATH="$native_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
        BENCH_NATIVE_LIBRARY="$libchdb" \
        BENCH_NATIVE_HEADER="$native_header" \
        BENCH_RUSTC_VERSION="$rustc_version" \
        BENCH_CARGO_VERSION="$cargo_version" \
        BENCH_HARNESS_STATE_FILE="$harness_state" \
        "$rust_bin" recover "$fixture_root" "$object_id" "$descriptor" \
        "$run_manifest" "$ordinal" "$output"
}

run_jolt() {
  local label=$1
  local output=$2
  local ordinal=$3
  /usr/bin/time -v -o "$report_dir/jolt-$label.time" \
    env BENCH_JOLT_BIN="$jolt_bin" \
        BENCH_JOLT_SOURCE_SHA="$jolt_source_sha" \
        BENCH_JOLT_VERSION="$jolt_version" \
        BENCH_NATIVE_LIBRARY="$libchdb" \
        BENCH_NATIVE_HEADER="$native_header" \
        BENCH_HARNESS_STATE_FILE="$harness_state" \
        JOLT_CHDB_LIB="$libchdb" \
        "$wrapper" "$jolt_bin" -M:durable-cross-binding-recovery \
        "$fixture_root" "$object_id" "$descriptor" "$run_manifest" "$ordinal" "$output"
}

# Prime both code paths outside the measured corpus. The reported condition is
# provider-cache-warm, with a fresh process, native engine and scratch per trial.
ordinal=0
run_rust prime "$report_dir/rust-prime.json" "$ordinal"
ordinal=$((ordinal + 1))
run_jolt prime "$report_dir/jolt-prime.json" "$ordinal"
ordinal=$((ordinal + 1))

for ((trial = 1; trial <= trials; trial++)); do
  if ((trial % 2 == 1)); then
    run_rust "trial-$trial" "$report_dir/rust-trial-$trial.json" "$ordinal"
    ordinal=$((ordinal + 1))
    run_jolt "trial-$trial" "$report_dir/jolt-trial-$trial.json" "$ordinal"
    ordinal=$((ordinal + 1))
  else
    run_jolt "trial-$trial" "$report_dir/jolt-trial-$trial.json" "$ordinal"
    ordinal=$((ordinal + 1))
    run_rust "trial-$trial" "$report_dir/rust-trial-$trial.json" "$ordinal"
    ordinal=$((ordinal + 1))
  fi
done

python3 "$repo_root/scripts/summarize-durable-cross-binding-recovery.py" \
  "$report_dir" "$report_dir/summary.json"
