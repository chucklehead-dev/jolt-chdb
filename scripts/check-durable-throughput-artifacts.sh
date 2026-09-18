#!/usr/bin/env bash
set -euo pipefail

report=${1:?usage: check-durable-throughput-artifacts.sh REPORT LOG [TIME-V]}
log=${2:?usage: check-durable-throughput-artifacts.sh REPORT LOG [TIME-V]}
timing=${3:-}

fail() {
  echo "Durable throughput artifact contract failed" >&2
  exit 1
}

artifacts=("$report" "$log")
if [[ -n $timing ]]; then
  artifacts+=("$timing")
fi

for artifact in "${artifacts[@]}"; do
  [[ -f "$artifact" && ! -L "$artifact" ]] || fail
done

report_bytes=$(wc -c < "$report")
log_bytes=$(wc -c < "$log")
[[ "$report_bytes" -le 16777216 ]] || fail
[[ "$log_bytes" -le 8388608 ]] || fail

if [[ -n $timing ]]; then
  timing_bytes=$(wc -c < "$timing")
  [[ "$timing_bytes" -le 1048576 ]] || fail
  rss=$(awk -F: '/^\tMaximum resident set size \(kbytes\):/ { count++; value=$2 }
                 END { gsub(/^[[:space:]]+/, "", value); print count ":" value }' "$timing")
  exit_status=$(awk -F: '/^\tExit status:/ { count++; value=$2 }
                         END { gsub(/^[[:space:]]+/, "", value); print count ":" value }' "$timing")
  [[ $rss =~ ^1:[1-9][0-9]*$ ]] || fail
  [[ $exit_status == "1:0" ]] || fail
fi

canary_names=(
  JOLT_CHDB_S3_ENDPOINT
  JOLT_CHDB_S3_BUCKET
  JOLT_CHDB_S3_PREFIX
  JOLT_CHDB_S3_ACCESS_KEY
  JOLT_CHDB_S3_SECRET_KEY
  JOLT_CHDB_S3_SESSION_TOKEN
  BENCH_SQL_CANARY
  BENCH_PAYLOAD_CANARY
)

for name in "${canary_names[@]}"; do
  value=${!name:-}
  if [[ -n "$value" ]]; then
    if LC_ALL=C grep -Fq -- "$value" "${artifacts[@]}"; then
      fail
    fi
  fi
done

echo "Durable throughput artifacts passed bounded redaction checks"
