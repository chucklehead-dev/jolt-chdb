#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source "$script_dir/durable-s3-minio.sh"

release=RELEASE.2025-09-07T16-13-09Z
read -r amd64_url amd64_sha < <(minio_release_artifact x86_64)
read -r arm64_url arm64_sha < <(minio_release_artifact aarch64)
[[ "$amd64_url" == "https://github.com/minio/minio/releases/download/$release/minio.linux-amd64.$release" ]]
[[ "$arm64_url" == "https://github.com/minio/minio/releases/download/$release/minio.linux-arm64.$release" ]]
[[ "$amd64_sha" == 7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f ]]
[[ "$arm64_sha" == 5c83cd2cf151717ba0243f73e1c7802ff36e272b67144bdd7f1f7d684fd6f03d ]]
if minio_release_artifact unsupported >/dev/null 2>&1; then
  printf 'Unsupported architecture was accepted\n' >&2
  exit 1
fi

test_dir=$(mktemp -d "${TMPDIR:-/tmp}/jchdb-minio-script-test.XXXXXXXX")
trap 'rm -r -- "$test_dir"' EXIT
printf 'test fixture\n' > "$test_dir/fixture"
fixture_sha=$(sha256sum "$test_dir/fixture")
fixture_sha=${fixture_sha%% *}
verify_minio_binary "$test_dir/fixture" "$fixture_sha"
if verify_minio_binary "$test_dir/fixture" "$amd64_sha"; then
  printf 'Mismatched binary checksum was accepted\n' >&2
  exit 1
fi

read -r first_port second_port < <(minio_loopback_ports)
[[ "$first_port" =~ ^[0-9]+$ && "$second_port" =~ ^[0-9]+$ ]]
[[ "$first_port" != "$second_port" ]]
printf 'MinIO release selection, checksum guard, and loopback port checks passed\n'
