#!/usr/bin/env bash
# Dummy-credential loopback composition only, never an AWS/workload benchmark.
set -euo pipefail
test "$#" -eq 3
task_jolt=$(realpath "$1")
task_baseline=$(realpath "$2")
task_json=$(realpath "$3")
task_repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
case "$task_repo$task_baseline$task_json" in
  *'"'*|*'\'*|*$'\n'*|*$'\r'*) exit 64 ;;
esac
task_identity_root=$(mktemp -d /tmp/wal-head-identity.XXXXXXXX)
task_identity_pid=""
cleanup() {
  if [[ -n "$task_identity_pid" ]]; then
    kill "$task_identity_pid" 2>/dev/null || true
    wait "$task_identity_pid" 2>/dev/null || true
  fi
  if [[ "$task_identity_root" =~ ^/tmp/wal-head-identity\.[A-Za-z0-9]+$ ]]; then
    rm -rf -- "$task_identity_root"
  fi
}
trap cleanup EXIT
python3 "$task_repo/test/support/s3_http_fixture.py" "$task_identity_root/port" &
task_identity_pid=$!
for task_attempt in $(seq 1 100); do
  if [[ -s "$task_identity_root/port" ]]; then break; fi
  sleep 0.05
done
test -s "$task_identity_root/port"
task_identity_port=$(<"$task_identity_root/port")
export SDK_STORAGE_ARM=s3 JOLT_CHDB_S3_BUCKET=bucket JOLT_CHDB_S3_PREFIX=ci/jolt-chdb/123-1/typed-window
export JOLT_CHDB_S3_ENDPOINT="http://127.0.0.1:$task_identity_port"
export JOLT_CHDB_S3_REGION=us-east-1 JOLT_CHDB_S3_ACCESS_KEY=ACCESS JOLT_CHDB_S3_SECRET_KEY=SECRET JOLT_CHDB_S3_SESSION_TOKEN=SESSION
export TASK_ORIGINAL_TYPED_DRIVER="$task_repo/scripts/fixtures/wal-head/bench/typed-storage-window.clj"
task_deps="{:paths [\"$task_repo/scripts\" \"$task_baseline/bench\"] :deps {io.github.chucklehead-dev/jolt-chdb {:local/root \"$task_baseline\"} org.clojure/data.json {:local/root \"$task_json\"} jolt-lang/jolt-crypto {:git/url \"https://github.com/jolt-lang/jolt-crypto.git\" :git/sha \"5effcc89a3258499a79a2a3d69edad9e7800d1bf\"}}}"
cd /tmp
# Both creates MUST succeed before reading either owner back. A shared remote
# object ID would collide at acquisition or fail the first owner's readback.
for task_phase in window reader; do
  for task_arm in baseline candidate; do
    export SDK_WAL_HEAD_ARM="$task_arm"
    mkdir -p "$task_identity_root/$task_arm"
    timeout --kill-after=5s 30s "$task_jolt" -Srepro -Sdeps "$task_deps" \
      "$task_repo/scripts/typed_storage_wal_head_driver.clj" \
      "$task_phase" "$task_identity_root/$task_arm" 10000 256 true wide16
  done
done
