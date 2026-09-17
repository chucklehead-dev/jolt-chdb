#!/usr/bin/env bash
set -euo pipefail
if [[ $# -ne 8 && $# -ne 9 ]]; then
  echo 'usage: OUTPUT_DIR SOURCE_ROOT SOURCE_ARCHIVE CORE_WHEEL CORE_SITE JOLT_BIN LIBCHDB HEADER [wal|checkpoint]' >&2
  exit 2
fi
output=$(realpath -m "$1")
source_root=$(realpath "$2")
source_archive=$(realpath "$3")
core_wheel=$(realpath "$4")
core_site=$(realpath "$5")
jolt_bin=$(realpath "$6")
libchdb=$(realpath "$7")
header=$(realpath "$8")
kind=${9:-wal}
[[ "$kind" == wal || "$kind" == checkpoint ]] || exit 2
repo_root=$(cd "$(dirname "$0")/.." && pwd -P)
[[ ! -e "$output" ]] || { echo 'output directory must be absent' >&2; exit 2; }
for artifact in "$source_archive" "$core_wheel" "$jolt_bin" "$libchdb" "$header"; do
  test -f "$artifact"
done
test -d "$source_root"
test -d "$core_site"
test "$(sha256sum "$source_archive" | awk '{print $1}')" = 4269548e589fa34497c207e84e660785399b52edecb7294cc929be796b04b381
test "$(sha256sum "$core_wheel" | awk '{print $1}')" = b10b96f9599fab42ba51d9be80333e1819782bdc8a91b2b26979149693ba431f
test "$(sha256sum "$libchdb" | awk '{print $1}')" = 36ad4e999882821ef13cf2d0f52c93f48e6ee1b4498b35a201c23ce4b8d15bf5
test "$(sha256sum "$header" | awk '{print $1}')" = ec234db7e47589b3780bcd9a5508f5666cb44b0b56cc6f3079c8a24aa1b8d911
jolt_command=("$jolt_bin")
if [[ -n ${JOLT_WRAPPER:-} ]]; then
  wrapper=$(realpath "$JOLT_WRAPPER")
  test -x "$wrapper"
  jolt_command=("$wrapper" "$jolt_bin")
fi
cd "$repo_root"
env JOLT_CHDB_LIB="$libchdb" \
  timeout --signal=TERM --kill-after=5s 90s \
  "${jolt_command[@]}" -Srepro -M:durable-jolt-writer-fixture-test "$output" "$kind"
env PYTHONPATH="$source_root:$core_site" \
  timeout --signal=TERM --kill-after=5s 90s \
  python3 "$repo_root/scripts/read-durable-jolt-writer-fixture.py" \
  "$source_root" "$source_archive" "$core_wheel" "$libchdb" "$header" "$jolt_bin" "$output"
