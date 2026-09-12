#!/usr/bin/env bash

# Shell helpers shared by the runner and its mutation-sensitive contract tests.

validate_output_dir() {
  local repository=$1
  local output=$2
  local relative
  case "$output/" in
    "$repository/"*)
      relative=${output#"$repository/"}
      if [[ -z "$relative" ]] || ! git -C "$repository" check-ignore -q -- "$relative"; then
        echo "an output directory inside the repository must be git-ignored" >&2
        return 2
      fi
      ;;
  esac
}

verify_harness_state() {
  python3 "$repo_root/scripts/prepare-durable-cross-binding-run.py" \
    --verify-state "$repo_root" "$report_dir"
}

run_rust() {
  local label=$1
  local output=$2
  local ordinal=$3
  verify_harness_state
  "$time_bin" -v -o "$report_dir/rust-$label.time" \
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
  verify_harness_state
  "$time_bin" -v -o "$report_dir/jolt-$label.time" \
    env JOLT_CACHE_DIR="$jolt_cache" \
        JOLT_GITLIBS_DIR="$jolt_gitlibs" \
        BENCH_JOLT_BIN="$jolt_bin" \
        BENCH_JOLT_SOURCE_SHA_ASSERTED="$jolt_source_sha_asserted" \
        BENCH_JOLT_EXECUTABLE_REVISION="$jolt_executable_revision" \
        BENCH_JOLT_VERSION="$jolt_version" \
        BENCH_JOLT_DESCRIBE="$jolt_describe" \
        BENCH_NATIVE_LIBRARY="$libchdb" \
        BENCH_NATIVE_HEADER="$native_header" \
        BENCH_HARNESS_STATE_FILE="$harness_state" \
        JOLT_CHDB_LIB="$libchdb" \
        "$wrapper" "$jolt_bin" -Srepro -M:durable-cross-binding-recovery \
        "$fixture_root" "$object_id" "$descriptor" "$run_manifest" "$ordinal" "$output"
}
