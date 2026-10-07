#!/usr/bin/env bash
# Synthetic local fixture only. No AWS calls or real credentials.
set -euo pipefail
test "$#" -eq 1
task_report_dir=$1
mkdir -p "$task_report_dir"
task_tmp=$(mktemp -d /tmp/chdb-reuse-loopback.XXXXXXXX)
task_server_pid=""
cleanup() {
  if [[ -n "$task_server_pid" ]]; then
    kill "$task_server_pid" 2>/dev/null || true
    wait "$task_server_pid" 2>/dev/null || true
  fi
  [[ "$task_tmp" == /tmp/chdb-reuse-loopback.???????? ]]
  rm -rf -- "$task_tmp"
}
trap cleanup EXIT
python3 test/support/s3_http_fixture.py "$task_tmp/port" --tcp-nodelay &
task_server_pid=$!
for _ in $(seq 1 100); do
  [[ -s "$task_tmp/port" ]] && break
  sleep 0.05
done
test -s "$task_tmp/port"
task_port=$(<"$task_tmp/port")
export JOLT_CHDB_S3_ENDPOINT="http://127.0.0.1:$task_port"
export JOLT_CHDB_S3_BUCKET=bucket
export JOLT_CHDB_S3_REGION=us-east-1
export JOLT_CHDB_S3_ACCESS_KEY=ACCESS
export JOLT_CHDB_S3_SECRET_KEY=SECRET
export JOLT_CHDB_S3_SESSION_TOKEN=SESSION
for task_mode in fresh reuse; do
  export JOLT_CHDB_S3_PREFIX="ci/jolt-chdb/synthetic-reuse-$task_mode"
  export BENCH_REUSE_CURL=false
  if [[ "$task_mode" == reuse ]]; then export BENCH_REUSE_CURL=true; fi
  task_report="$task_report_dir/s3-$task_mode.edn"
  mkdir -p "$task_tmp/$task_mode"
  bash bench/run-exporter-storage-qualification.sh writer s3 "$task_tmp/$task_mode" "$task_report" 100 1
  bash bench/run-exporter-storage-qualification.sh reader s3 "$task_tmp/$task_mode" "$task_report" 100 1
done
