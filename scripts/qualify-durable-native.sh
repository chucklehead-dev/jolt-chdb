#!/usr/bin/env bash
set -euo pipefail

qualification_root=${1:?usage: scripts/qualify-durable-native.sh DIRECTORY}
release=26.7.2-rc.2
commit=30488a59b2700188ee36ecbced7713081a909f56
oracle_digest=56257403ba7563c5a6ecbe7ab4c13ca6a3a7a81a75a314ce3d54a3e108987f99

case "$(uname -s):$(uname -m)" in
  Linux:x86_64)
    asset=linux-x86_64-libchdb.tar.gz
    digest=8b6f8b95278cc223f8cae0303006aecdaf72cf975fe29c1cb14277f2165e2b85
    library=libchdb.so
    ;;
  Linux:aarch64|Linux:arm64)
    asset=linux-aarch64-libchdb.tar.gz
    digest=50c4bde57197c5947288ddab13adf9cd56910ae0efbe92317f54825db5034064
    library=libchdb.so
    ;;
  Darwin:arm64)
    asset=macos-arm64-libchdb.tar.gz
    digest=8a22ea8a18f30744d9ba1d64b915b64b4cb16827761f02ac2508df808001a56e
    library=libchdb.dylib
    ;;
  Darwin:x86_64)
    asset=macos-x86_64-libchdb.tar.gz
    digest=33ba79e7df73cb0e1755f3267c3d6ce2f8c0d6472412eec52f785150e6fa3b31
    library=libchdb.dylib
    ;;
  *)
    echo "unsupported Durable qualification platform: $(uname -s) $(uname -m)" >&2
    exit 2
    ;;
esac

mkdir -p "$qualification_root/native" "$qualification_root/oracle"
archive="$qualification_root/$asset"
release_url="https://github.com/chdb-io/chdb-core/releases/download/v$release/$asset"
if command -v sha256sum >/dev/null 2>&1; then
  digest_file() { sha256sum "$1" | awk '{print $1}'; }
else
  digest_file() { shasum -a 256 "$1" | awk '{print $1}'; }
fi

if test ! -f "$archive" || test "$(digest_file "$archive")" != "$digest"; then
  curl -fsSL --retry 2 --retry-all-errors -o "$archive" "$release_url"
fi
test "$(digest_file "$archive")" = "$digest"

tar -xzf "$archive" -C "$qualification_root/native"
library_path="$qualification_root/native/$library"
test -f "$library_path"

oracle_url="https://raw.githubusercontent.com/chdb-io/chdb-core/$commit/examples/chdbDurableAbiTest.c"
oracle_source="$qualification_root/oracle/chdbDurableAbiTest.c"
curl -fsSL --retry 2 --retry-all-errors -o "$oracle_source" "$oracle_url"
test "$(digest_file "$oracle_source")" = "$oracle_digest"
oracle_run=$(mktemp -d "$qualification_root/oracle/run.XXXXXX")

# The pinned upstream example currently produces one conservative GCC
# format-truncation warning, so warnings remain visible but are not promoted.
cc -std=c11 -Wall -Wextra \
  -I"$qualification_root/native" "$oracle_source" \
  -L"$qualification_root/native" -Wl,-rpath,"$qualification_root/native" \
  -lchdb -o "$oracle_run/chdbDurableAbiTest"
(
  cd "$oracle_run"
  ./chdbDurableAbiTest
)

JOLT_CHDB_LIB="$library_path" jolt -M:durable-native-test
