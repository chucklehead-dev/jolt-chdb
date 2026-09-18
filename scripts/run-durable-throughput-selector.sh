#!/usr/bin/env bash
set -euo pipefail

# Run exactly one local Durable throughput selector in its own process and
# retain the three artifacts needed to interpret that measurement.  This is
# deliberately a local/manual tool: remote S3 runs use the redacted CI path.

selector=${1:?usage: run-durable-throughput-selector.sh SELECTOR OUTPUT-DIR}
output_dir=${2:?usage: run-durable-throughput-selector.sh SELECTOR OUTPUT-DIR}

case "$selector" in
  scale-512|scale-1000|scale-5000|scale-10000|recovery-512-10|recovery-512-25|recovery-512-50)
    ;;
  *)
    echo "unsupported isolated Durable selector" >&2
    exit 2
    ;;
esac

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

for required in JOLT_WRAPPER BENCH_JOLT_BIN BENCH_JOLT_SOURCE_SHA JOLT_CHDB_LIB; do
  if [[ -z ${!required:-} ]]; then
    echo "missing required benchmark provenance" >&2
    exit 2
  fi
done

[[ -x "$JOLT_WRAPPER" && -x "$BENCH_JOLT_BIN" && -f "$JOLT_CHDB_LIB" ]] || {
  echo "benchmark executable or native library is unavailable" >&2
  exit 2
}
[[ -x /usr/bin/time ]] || {
  echo "GNU /usr/bin/time is required for peak RSS evidence" >&2
  exit 2
}
[[ ! -e "$output_dir" ]] || {
  echo "output directory must not already exist" >&2
  exit 2
}

git -C "$repo_root" diff --quiet && git -C "$repo_root" diff --cached --quiet || {
  echo "benchmark checkout must be clean" >&2
  exit 2
}

mkdir -p "$(dirname "$output_dir")"
mkdir "$output_dir"

report="$output_dir/report.edn"
log="$output_dir/run.log"
timing="$output_dir/time-v.txt"

export BENCH_JOLT_VERSION
BENCH_JOLT_VERSION=$("$JOLT_WRAPPER" "$BENCH_JOLT_BIN" --version)
export BENCH_GIT_HEAD BENCH_GIT_PARENT BENCH_GIT_TREE BENCH_GIT_STATUS BENCH_STARTED_AT
BENCH_GIT_HEAD=$(git -C "$repo_root" rev-parse HEAD)
BENCH_GIT_PARENT=$(git -C "$repo_root" rev-parse HEAD^)
BENCH_GIT_TREE=$(git -C "$repo_root" rev-parse HEAD^{tree})
BENCH_GIT_STATUS=clean
BENCH_STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

cd "$repo_root"
set +e
LC_ALL=C /usr/bin/time -v -o "$timing" \
  "$JOLT_WRAPPER" "$BENCH_JOLT_BIN" -M:durable-throughput "$selector" "$report" \
  >"$log" 2>&1
run_status=$?
set -e

scripts/check-durable-throughput-artifacts.sh "$report" "$log" "$timing"
test "$run_status" -eq 0

echo "Durable throughput selector completed; retain $output_dir as one evidence unit"
