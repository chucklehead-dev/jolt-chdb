#!/usr/bin/env bash
set -euo pipefail

runner=${1:-jolt}
tmp_dir=$(mktemp -d)
port_file="$tmp_dir/port"
server_pid=""

cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf -- "$tmp_dir"
}
trap cleanup EXIT

python3 test/support/s3_http_fixture.py "$port_file" &
server_pid=$!

for _ in $(seq 1 100); do
  [[ -s "$port_file" ]] && break
  sleep 0.05
done
[[ -s "$port_file" ]]

port=$(<"$port_file")
"$runner" -M:durable-s3-curl-test "http://127.0.0.1:$port"
