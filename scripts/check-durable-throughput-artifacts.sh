#!/usr/bin/env bash
set -euo pipefail

report=${1:?usage: check-durable-throughput-artifacts.sh REPORT LOG}
log=${2:?usage: check-durable-throughput-artifacts.sh REPORT LOG}

fail() {
  echo "Durable throughput artifact contract failed" >&2
  exit 1
}

for artifact in "$report" "$log"; do
  [[ -f "$artifact" && ! -L "$artifact" ]] || fail
done

report_bytes=$(wc -c < "$report")
log_bytes=$(wc -c < "$log")
[[ "$report_bytes" -le 16777216 ]] || fail
[[ "$log_bytes" -le 8388608 ]] || fail

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
    if LC_ALL=C grep -Fq -- "$value" "$report" "$log"; then
      fail
    fi
  fi
done

echo "Durable throughput artifacts passed bounded redaction checks"
