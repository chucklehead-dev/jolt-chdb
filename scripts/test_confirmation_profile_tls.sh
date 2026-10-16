#!/usr/bin/env bash
# Fake-credential loopback composition, never cloud or an ingestion benchmark.
set -euo pipefail
umask 077
test "$#" -eq 2
task_jolt=$(realpath "$(command -v "$1")")
test -x "$task_jolt"
task_source=$(realpath "$2")
task_repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
case "$task_source$task_repo" in
  *'"'*|*'\'*|*$'\n'*|*$'\r'*) exit 64 ;;
esac
task_root=$(mktemp -d /tmp/confirmation-profile-tls.XXXXXXXX)
task_server=
cleanup() {
  if [[ -n "$task_server" ]]; then
    kill "$task_server" 2>/dev/null || true
    wait "$task_server" 2>/dev/null || true
  fi
  if [[ "$task_root" =~ ^/tmp/confirmation-profile-tls\.[A-Za-z0-9]+$ ]]; then
    rm -rf -- "$task_root"
  fi
}
trap cleanup EXIT
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=127.0.0.1 \
  -addext subjectAltName=IP:127.0.0.1 -keyout "$task_root/key.pem" \
  -out "$task_root/cert.pem" > "$task_root/cert-generation.log" 2>&1
python3 "$task_source/test/support/s3_tls_fixture.py" "$task_root/port" \
  "$task_root/cert.pem" "$task_root/key.pem" > "$task_root/server.log" 2>&1 &
task_server=$!
for _ in $(seq 1 100); do
  [[ -s "$task_root/port" ]] && break
  kill -0 "$task_server"
  sleep 0.05
done
test -s "$task_root/port"
task_port=$(<"$task_root/port")
[[ "$task_port" =~ ^[0-9]+$ ]]
export SDK_STORAGE_ARM=s3 JOLT_CHDB_S3_BUCKET=bucket JOLT_CHDB_S3_PREFIX=ci/jolt-chdb/123-1/typed-window
export JOLT_CHDB_S3_ENDPOINT="https://127.0.0.1:$task_port"
export JOLT_CHDB_S3_REGION=us-east-1 JOLT_CHDB_S3_ACCESS_KEY=ACCESS JOLT_CHDB_S3_SECRET_KEY=SECRET JOLT_CHDB_S3_SESSION_TOKEN=SESSION
export TEST_CONFIRMATION_CA="$task_root/cert.pem"
export TASK_ORIGINAL_TYPED_DRIVER="$task_repo/scripts/fixtures/confirmation/bench/typed-storage-window.clj"
task_deps="{:paths [\"$task_repo/scripts\" \"$task_source/bench\" \"$task_source/test\"] :deps {io.github.chucklehead-dev/jolt-chdb {:local/root \"$task_source\"}}}"
cd /tmp
for task_phase in window reader; do
  for task_arm in baseline candidate; do
    export SDK_WAL_HEAD_ARM="$task_arm"
    mkdir -p "$task_root/$task_arm"
    timeout --kill-after=5s 45s "$task_jolt" -Srepro -Sdeps "$task_deps" \
      "$task_repo/scripts/typed_storage_confirmation_driver.clj" "$task_phase" "$task_root/$task_arm"
  done
done
export SDK_WAL_HEAD_ARM=candidate
export JOLT_CHDB_S3_PREFIX=ci/jolt-chdb/123-2/typed-window
export TEST_CONFIRMATION_DROP_PROVENANCE=true
mkdir "$task_root/missing-provenance"
if timeout --kill-after=5s 45s "$task_jolt" -Srepro -Sdeps "$task_deps" \
  "$task_repo/scripts/typed_storage_confirmation_driver.clj" window "$task_root/missing-provenance" \
  > "$task_root/missing-provenance.log" 2>&1; then
  echo 'missing-provenance control unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq 'confirmation-proof-missing' "$task_root/missing-provenance.log"
grep -Fq ':real-publication-probe-passed-not-ingestion-throughput' "$task_root/missing-provenance.log"
grep -Fq ':stage :selection-validation' "$task_root/missing-provenance/driver-failure.edn"
grep -Fq ':scope-returned false' "$task_root/missing-provenance/driver-failure.edn"
echo 'real TLS fallback was rejected by the confirmation selection oracle'
export JOLT_CHDB_S3_PREFIX=ci/jolt-chdb/123-3/typed-window
export TEST_CONFIRMATION_DROP_PROVENANCE=false
export TEST_CONFIRMATION_DROP_PUBLICATION_WITNESS=true
mkdir "$task_root/missing-publication-witness"
if timeout --kill-after=5s 45s "$task_jolt" -Srepro -Sdeps "$task_deps" \
  "$task_repo/scripts/typed_storage_confirmation_driver.clj" window "$task_root/missing-publication-witness" \
  > "$task_root/missing-publication-witness.log" 2>&1; then
  echo 'missing-publication-witness control unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq 'confirmation-unexpected-readback' "$task_root/missing-publication-witness.log"
grep -Fq ':real-publication-probe-passed-not-ingestion-throughput' "$task_root/missing-publication-witness.log"
grep -Fq ':stage :selection-validation' "$task_root/missing-publication-witness/driver-failure.edn"
echo 'real TLS commit readback was rejected by the confirmation selection oracle'
