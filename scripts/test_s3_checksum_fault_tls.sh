#!/usr/bin/env bash
set -euo pipefail
umask 077
runner=${1:?explicit qualified Jolt wrapper required}
task_root=$(mktemp -d /tmp/checksum-fault-tls.XXXXXXXX)
task_server=
stop_server() {
  if [[ -n "$task_server" ]]; then
    kill "$task_server" 2>/dev/null || true
    wait "$task_server" 2>/dev/null || true
  fi
}
trap stop_server EXIT
printf 'fixture_root=%s\n' "$task_root"
mkdir "$task_root/invalid"
task_status=0
env AWS_REGION=PRIVATE-CONFIG-CANARY AWS_ACCESS_KEY_ID=PRIVATE-CONFIG-CANARY \
  AWS_SECRET_ACCESS_KEY=PRIVATE-CONFIG-CANARY AWS_SESSION_TOKEN=PRIVATE-CONFIG-CANARY \
  TASK_BUCKET=PRIVATE-CONFIG-CANARY TASK_CHECKSUM_PREFIX=PRIVATE-CONFIG-CANARY \
  GITHUB_RUN_ID=123 GITHUB_RUN_ATTEMPT=1 \
  "$runner" -Srepro -Sdeps '{:paths ["src" "resources" "scripts"]}' \
  scripts/qualify_s3_checksum_faults.clj "$task_root/invalid" \
  > "$task_root/invalid.log" 2>&1 || task_status=$?
test "$task_status" = 1
test -f "$task_root/invalid/driver-failure.edn"
test ! -e "$task_root/invalid/s3-writer-confirmation.edn"
if grep -Rq PRIVATE-CONFIG-CANARY "$task_root/invalid" "$task_root/invalid.log"; then exit 1; fi
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=127.0.0.1 \
  -addext subjectAltName=IP:127.0.0.1 -keyout "$task_root/key.pem" \
  -out "$task_root/cert.pem" > "$task_root/cert-generation.log" 2>&1
python3 test/support/s3_tls_fixture.py "$task_root/port" "$task_root/cert.pem" \
  "$task_root/key.pem" > "$task_root/server.log" 2>&1 &
task_server=$!
for _ in $(seq 1 100); do
  [[ -s "$task_root/port" ]] && break
  kill -0 "$task_server"
  sleep 0.05
done
test -s "$task_root/port"
task_port=$(<"$task_root/port")
[[ "$task_port" =~ ^[0-9]+$ ]]
"$runner" -Srepro -Sdeps '{:paths ["src" "resources" "scripts"]}' \
  scripts/test_s3_checksum_fault_tls.clj "https://127.0.0.1:$task_port" "$task_root/cert.pem" "$task_root"
stop_server
trap - EXIT
[[ "$task_root" =~ ^/tmp/checksum-fault-tls\.[A-Za-z0-9]+$ ]]
rm -rf -- "$task_root"
