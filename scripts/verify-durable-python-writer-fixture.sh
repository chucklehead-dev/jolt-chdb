#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 8 && $# -ne 9 ]]; then
  echo "usage: $0 OUTPUT_DIR SOURCE_ROOT SOURCE_ARCHIVE CORE_WHEEL CORE_SITE JOLT_BIN LIBCHDB HEADER [wal|checkpoint]" >&2
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
jolt_command=("$jolt_bin")

if [[ -n ${JOLT_WRAPPER:-} ]]; then
  wrapper=$(realpath "$JOLT_WRAPPER")
  test -x "$wrapper"
  jolt_command=("$wrapper" "$jolt_bin")
fi

for artifact in "$source_archive" "$core_wheel" "$jolt_bin" "$libchdb" "$header"; do
  test -f "$artifact"
done
test -d "$source_root"
test -d "$core_site"
if [[ -e "$output" ]] && find "$output" -mindepth 1 -print -quit | grep -q .; then
  echo "output directory must be absent or empty" >&2
  exit 2
fi
mkdir -p "$output"

env PYTHONPATH="$source_root:$core_site" \
  timeout --signal=TERM --kill-after=5s 90s \
  python3 "$repo_root/scripts/generate-durable-python-writer-fixture.py" \
  "$source_root" "$source_archive" "$core_wheel" "$libchdb" "$header" "$output" "$kind"

env JOLT_CHDB_LIB="$libchdb" \
  "${jolt_command[@]}" -Srepro -M:durable-python-writer-fixture-test \
  "$output/fixture-store" "$output/fixture.json" "$source_archive" \
  "$core_wheel" "$libchdb" "$header"

if [[ "$kind" == checkpoint ]]; then
  for label in base-only missing-base corrupt-base truncated-base missing-wal corrupt-wal; do
    control_root="$output/controls/$label"
    [[ "$label" != base-only ]] || control_root="$output/base-only-store"
    env JOLT_CHDB_LIB="$libchdb" \
      timeout --signal=TERM --kill-after=5s 90s \
      "${jolt_command[@]}" -Srepro -M:durable-python-writer-fixture-test \
      "$control_root" "$output/fixture.json" "$source_archive" \
      "$core_wheel" "$libchdb" "$header" "$label"
  done
fi
