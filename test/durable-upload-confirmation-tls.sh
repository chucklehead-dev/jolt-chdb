#!/usr/bin/env bash
set -euo pipefail
umask 077
runner=${1:?pass an explicitly qualified Jolt wrapper}
task_main=${2:-jdbc.chdb-durable-upload-confirmation-native-test}
case "$task_main" in
  jdbc.chdb-durable-upload-confirmation-native-test|jdbc.chdb-durable-confirmation-metrics-native-test|jdbc.chdb-durable-reused-confirmation-native-test) ;;
  *) echo 'unsupported TLS test entrypoint' >&2; exit 64 ;;
esac
task_root=$(mktemp -d)
task_server=
cleanup() {
  if [[ -n "$task_server" ]]; then
    kill "$task_server" 2>/dev/null || true
    wait "$task_server" 2>/dev/null || true
  fi
  # Exact private fixture directory; no persisted application data resides here.
  rm -rf -- "$task_root"
}
trap cleanup EXIT
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=127.0.0.1 \
  -addext subjectAltName=IP:127.0.0.1 -keyout "$task_root/key.pem" \
  -out "$task_root/cert.pem" > "$task_root/cert-generation.log" 2>&1
python3 test/support/s3_tls_fixture.py "$task_root/port" \
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
"$runner" -Srepro -Sdeps '{:paths ["src" "test" "bench"]}' \
  -m "$task_main" \
  "https://127.0.0.1:$task_port" "$task_root/cert.pem"
