#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: $0 OUTPUT_DIR JOLT_BIN RC2_DIR RELEASE_DIR" >&2
  exit 2
fi

output=$(realpath -m "$1")
jolt_bin=$(realpath "$2")
rc2_dir=$(realpath "$3")
release_dir=$(realpath "$4")
repo_root=$(cd "$(dirname "$0")/.." && pwd -P)
wrapper=${JOLT_WRAPPER:-/home/chuck/ai-src/tools/jolt-with-chez-10.4.1}
matrix="$repo_root/resources/jdbc/chdb/durable_linux_compatibility.json"

test -x "$jolt_bin"
test -x "$wrapper"
test -f "$matrix"
for directory in "$rc2_dir" "$release_dir"; do
  test -f "$directory/libchdb.so"
  test -f "$directory/chdb.h"
done
if [[ -e "$output" ]] && find "$output" -mindepth 1 -print -quit | grep -q .; then
  echo "output directory must be absent or empty" >&2
  exit 2
fi
mkdir -p "$output"

python3 "$repo_root/scripts/verify-durable-header-library-matrix.py" \
  "$matrix" "$rc2_dir" "$release_dir" "$output/header-library"

run_jolt() {
  local release=$1
  shift
  local native_dir
  case "$release" in
    rc2) native_dir=$rc2_dir ;;
    release) native_dir=$release_dir ;;
    *) echo "unknown release: $release" >&2; exit 2 ;;
  esac
  env JOLT_CHDB_LIB="$native_dir/libchdb.so" \
      LD_LIBRARY_PATH="$native_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
      JOLT_GATEBOOT_BUILD_DIR="$output/gateboot" \
    "$wrapper" "$jolt_bin" -Srepro -M:durable-linux-compatibility-test "$@"
}

run_jolt rc2 produce "$matrix" "$output/rc2" rc2 \
  "$rc2_dir/libchdb.so" "$rc2_dir/chdb.h"
run_jolt release produce "$matrix" "$output/release" release \
  "$release_dir/libchdb.so" "$release_dir/chdb.h"

run_cell() {
  local producer=$1
  local reader=$2
  local reader_dir
  case "$reader" in
    rc2) reader_dir=$rc2_dir ;;
    release) reader_dir=$release_dir ;;
    *) echo "unknown reader release: $reader" >&2; exit 2 ;;
  esac
  run_jolt "$reader" read "$matrix" "$output/$producer/fixture-store" \
    "$output/$producer/fixture.json" "$producer" "$reader" \
    "$reader_dir/libchdb.so" "$reader_dir/chdb.h" \
    "$output/$producer-to-$reader.json"
}

run_cell rc2 rc2
run_cell rc2 release
run_cell release release
run_cell release rc2

python3 - "$output" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
expected = {
    "rc2-to-rc2": "accept",
    "rc2-to-release": "accept",
    "release-to-release": "accept",
    "release-to-rc2": "refuse",
}
for cell, disposition in expected.items():
    report = json.loads((root / f"{cell}.json").read_text())
    if report["cell"]["id"] != cell or report["result"]["actual"] != disposition:
        raise SystemExit(f"archive report differs for {cell}")
    if report["inventory_unchanged"] is not True:
        raise SystemExit(f"archive bytes changed for {cell}")
    controls = report["causal_controls"]
    if cell == "rc2-to-release":
        expected_controls = {"wrong-archive-identity", "unsupported-min-reader"}
        if {control["id"] for control in controls} != expected_controls:
            raise SystemExit("archive causal controls differ")
        if any(control["actual"] != "refuse" or
               control["authoritative_inventory_unchanged"] is not True
               for control in controls):
            raise SystemExit("archive causal control did not fail closed")
    elif controls:
        raise SystemExit(f"unexpected duplicate causal controls for {cell}")
print("all Durable Linux x86-64 compatibility matrix checks passed")
PY
