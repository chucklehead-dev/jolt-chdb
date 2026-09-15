#!/usr/bin/env bash
set -euo pipefail

qualification_root=${1:?usage: scripts/qualify-durable-native.sh DIRECTORY}
jolt_bin=${JOLT_BIN:-jolt}
release=26.7.3
commit=7d84d719da07184f6a49405a11b112f16925af72
oracle_digest=56257403ba7563c5a6ecbe7ab4c13ca6a3a7a81a75a314ce3d54a3e108987f99

if [[ "$jolt_bin" == */* ]]; then
  if [[ ! -f "$jolt_bin" || ! -x "$jolt_bin" ]]; then
    echo "JOLT_BIN must name an executable file" >&2
    exit 69
  fi
else
  if ! command -v "$jolt_bin" >/dev/null; then
    echo "JOLT_BIN command was not found" >&2
    exit 69
  fi
fi

case "$(uname -s):$(uname -m)" in
  Linux:x86_64)
    asset=linux-x86_64-libchdb.tar.gz
    digest=bc33260c32acf78eade2ac41a9115f38e00404651fa42e3bb4c419e4f011c031
    library=libchdb.so
    ;;
  Linux:aarch64|Linux:arm64)
    asset=linux-aarch64-libchdb.tar.gz
    digest=d153adad1ff39b2e3caf0417f09d8bd9edd41939c7c67a3c4978a61e73fb9227
    library=libchdb.so
    ;;
  Darwin:arm64)
    asset=macos-arm64-libchdb.tar.gz
    digest=5640e50dccf711bf3dd5551333d08e43f433edf7bd94b2289f36c2539e627762
    library=libchdb.dylib
    ;;
  Darwin:x86_64)
    asset=macos-x86_64-libchdb.tar.gz
    digest=af5ded3ed3e84c31af1cd198dcf459f11d2b6aad4f6ddeccc04b8a519b0300fc
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

JOLT_CHDB_LIB="$library_path" "$jolt_bin" -M:durable-native-test
