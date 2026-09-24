#!/usr/bin/env bash
set -euo pipefail

# The old container gate used this exact release. Its public registry image is
# no longer anonymously pullable, so use the official release assets instead.
minio_release=RELEASE.2025-09-07T16-13-09Z
minio_amd64_sha256=7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f
minio_arm64_sha256=5c83cd2cf151717ba0243f73e1c7802ff36e272b67144bdd7f1f7d684fd6f03d

minio_release_artifact() {
  local architecture=$1 platform checksum
  case "$architecture" in
    x86_64|amd64)
      platform=amd64
      checksum=$minio_amd64_sha256
      ;;
    aarch64|arm64)
      platform=arm64
      checksum=$minio_arm64_sha256
      ;;
    *)
      printf 'Unsupported MinIO qualification architecture: %s\n' "$architecture" >&2
      return 1
      ;;
  esac
  printf 'https://github.com/minio/minio/releases/download/%s/minio.linux-%s.%s %s\n' \
    "$minio_release" "$platform" "$minio_release" "$checksum"
}

verify_minio_binary() {
  local path=$1 expected_sha256=$2
  printf '%s  %s\n' "$expected_sha256" "$path" | sha256sum --check --status
}

minio_loopback_ports() {
  python3 -c 'import socket
sockets = [socket.socket(), socket.socket()]
for sock in sockets:
    sock.bind(("127.0.0.1", 0))
print(*(sock.getsockname()[1] for sock in sockets))
for sock in sockets:
    sock.close()'
}

minio_qualification_temp_dir=''
minio_qualification_pid=''

cleanup_minio_qualification() {
  if [[ -n "$minio_qualification_pid" ]]; then
    kill "$minio_qualification_pid" 2>/dev/null || true
    wait "$minio_qualification_pid" 2>/dev/null || true
  fi
  if [[ -n "$minio_qualification_temp_dir" && -d "$minio_qualification_temp_dir" ]]; then
    rm -r -- "$minio_qualification_temp_dir"
  fi
}

run_minio_qualification() {
  local runner=${1:-jolt}
  local artifact_url expected_sha256 port console_port endpoint architecture
  trap cleanup_minio_qualification EXIT

  architecture=$(uname -m)
  read -r artifact_url expected_sha256 < <(minio_release_artifact "$architecture")
  minio_qualification_temp_dir=$(mktemp -d "${TMPDIR:-/tmp}/jchdb-minio.XXXXXXXX")
  curl -fsSL --retry 3 --output "$minio_qualification_temp_dir/minio" "$artifact_url"
  if ! verify_minio_binary "$minio_qualification_temp_dir/minio" "$expected_sha256"; then
    printf 'Pinned MinIO binary checksum mismatch\n' >&2
    return 1
  fi
  printf 'Verified MinIO %s (%s) SHA-256 %s\n' "$minio_release" "$architecture" "$expected_sha256"
  chmod 700 "$minio_qualification_temp_dir/minio"
  mkdir "$minio_qualification_temp_dir/data"

  read -r port console_port < <(minio_loopback_ports)
  endpoint="http://127.0.0.1:$port"
  MINIO_ROOT_USER=MINIOACCESS MINIO_ROOT_PASSWORD=MINIOSECRET \
    "$minio_qualification_temp_dir/minio" server \
      --address "127.0.0.1:$port" \
      --console-address "127.0.0.1:$console_port" \
      "$minio_qualification_temp_dir/data" >"$minio_qualification_temp_dir/minio.log" 2>&1 &
  minio_qualification_pid=$!

  for _ in $(seq 1 120); do
    if curl -fsS "$endpoint/minio/health/live" >/dev/null 2>&1; then
      "$runner" -M:durable-s3-minio-test "$endpoint"
      return
    fi
    if ! kill -0 "$minio_qualification_pid" 2>/dev/null; then
      printf 'Pinned MinIO exited before becoming healthy\n' >&2
      return 1
    fi
    sleep 0.25
  done
  printf 'Pinned MinIO did not become healthy within 30 seconds\n' >&2
  return 1
}

if [[ ${BASH_SOURCE[0]} == "$0" ]]; then
  run_minio_qualification "$@"
fi
