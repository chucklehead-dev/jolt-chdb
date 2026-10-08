#!/usr/bin/env bash
set -euo pipefail
runner=${1:?supply the exact Jolt executable}
fixture_dir=$(mktemp -d /tmp/chdb-s3-native-overlap.XXXXXXXX)
server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf -- "$fixture_dir"
}
trap cleanup EXIT
python3 test/support/s3_http_fixture.py "$fixture_dir/port" &
server_pid=$!
for _ in $(seq 1 100); do
  [[ -s "$fixture_dir/port" ]] && break
  sleep 0.05
done
[[ -s "$fixture_dir/port" ]]
port=$(<"$fixture_dir/port")
"$runner" -Sdeps '{:paths ["src" "resources" "test"]}' -M -m jdbc.chdb-s3-curl-native-overlap-test writer "http://127.0.0.1:$port" "$fixture_dir"
"$runner" -Sdeps '{:paths ["src" "resources" "test"]}' -M -m jdbc.chdb-s3-curl-native-overlap-test reader "http://127.0.0.1:$port" "$fixture_dir"
