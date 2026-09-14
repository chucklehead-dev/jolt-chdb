#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 8 ]]; then
  echo "usage: $0 OUTPUT_DIR SOURCE_ROOT SOURCE_ARCHIVE CORE_WHEEL CORE_SITE JOLT_BIN LIBCHDB HEADER" >&2
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
repo_root=$(cd "$(dirname "$0")/.." && pwd -P)
wrapper=${JOLT_WRAPPER:-/home/chuck/ai-src/tools/jolt-with-chez-10.4.1}

for artifact in "$source_archive" "$core_wheel" "$jolt_bin" "$libchdb" "$header"; do
  test -f "$artifact"
done
test -x "$wrapper"
test -d "$source_root"
test -d "$core_site"
if [[ -e "$output" ]] && find "$output" -mindepth 1 -print -quit | grep -q .; then
  echo "output directory must be absent or empty" >&2
  exit 2
fi
mkdir -p "$output"

env PYTHONPATH="$source_root:$core_site" \
  python3 "$repo_root/scripts/generate-durable-python-writer-fixture.py" \
  "$source_root" "$source_archive" "$core_wheel" "$libchdb" "$header" "$output"

env JOLT_CHDB_LIB="$libchdb" \
  "$wrapper" "$jolt_bin" -Srepro -M:durable-python-writer-fixture-test \
  "$output/fixture-store" "$output/fixture.json" "$source_archive" \
  "$core_wheel" "$libchdb" "$header"
