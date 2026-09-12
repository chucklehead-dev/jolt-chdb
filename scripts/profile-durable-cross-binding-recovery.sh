#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 || $# -gt 8 ]]; then
  echo "usage: $0 OUTPUT_DIR JOLT_BIN JOLT_SOURCE_SHA_ASSERTED LIBCHDB [TRIALS [BATCH_SIZE [WARMUP_BATCHES [MEASURED_BATCHES]]]]" >&2
  exit 2
fi

output_dir=$(realpath -m "$1")
jolt_bin_input=$2
jolt_source_sha_asserted=$3
libchdb_input=$4
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
jolt_describe="$report_dir/jolt-sdescribe.edn"
jolt_cache="$output_dir/jolt-cache"
jolt_gitlibs="$output_dir/jolt-gitlibs"
time_bin=${BENCH_TIME_BIN:-/usr/bin/time}
source "$repo_root/scripts/durable-cross-binding-recovery-lib.sh"

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
if [[ ! -x "$jolt_bin_input" || ! -f "$libchdb_input" ]]; then
  echo "Jolt executable and libchdb file must exist" >&2
  exit 2
fi
jolt_bin=$(realpath "$jolt_bin_input")
libchdb=$(realpath "$libchdb_input")
if [[ ! "$jolt_source_sha_asserted" =~ ^[0-9a-f]{40}$ ]]; then
  echo "the caller-asserted Jolt source SHA must be one full lowercase Git SHA" >&2
  exit 2
fi
if [[ -e "$output_dir" ]] && find "$output_dir" -mindepth 1 -print -quit | grep -q .; then
  echo "output directory must be absent or empty" >&2
  exit 2
fi
validate_output_dir "$repo_root" "$output_dir"
mkdir -p "$output_dir" "$report_dir"
cd "$repo_root"

python3 "$repo_root/scripts/prepare-durable-cross-binding-run.py" \
  "$repo_root" "$report_dir" "$object_id" "$trials" "$batch_size" \
  "$warmup_batches" "$measured_batches"

rustc_version=$(rustc --version)
cargo_version=$(cargo --version)
jolt_version=$(env JOLT_CACHE_DIR="$jolt_cache" \
                   JOLT_GITLIBS_DIR="$jolt_gitlibs" \
                   "$wrapper" "$jolt_bin" --version)
jolt_executable_revision=${jolt_version##*-g}
if [[ ! "$jolt_executable_revision" =~ ^[0-9a-f]{8,40}$ ]] ||
   [[ "$jolt_version" != *"-g$jolt_executable_revision" ]] ||
   [[ "$jolt_source_sha_asserted" != "$jolt_executable_revision"* ]]; then
  echo "Jolt banner revision does not agree with the caller-asserted source SHA" >&2
  exit 2
fi
env JOLT_CACHE_DIR="$jolt_cache" \
    JOLT_GITLIBS_DIR="$jolt_gitlibs" \
    "$wrapper" "$jolt_bin" -Srepro -Sdescribe > "$jolt_describe"
python3 "$repo_root/scripts/summarize-durable-cross-binding-recovery.py" \
  --validate-jolt-describe "$jolt_describe" "$jolt_version" \
  "$jolt_source_sha_asserted" "$jolt_executable_revision" "$repo_root" "$jolt_gitlibs"
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

verify_harness_state
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

verify_harness_state
LD_LIBRARY_PATH="$native_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
BENCH_NATIVE_LIBRARY="$libchdb" \
BENCH_NATIVE_HEADER="$native_header" \
BENCH_RUSTC_VERSION="$rustc_version" \
BENCH_CARGO_VERSION="$cargo_version" \
BENCH_HARNESS_STATE_FILE="$harness_state" \
  "$rust_bin" prepare "$fixture_root" "$object_id" "$run_manifest" "$descriptor"

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

verify_harness_state
python3 "$repo_root/scripts/summarize-durable-cross-binding-recovery.py" \
  "$report_dir" "$report_dir/summary.json"
