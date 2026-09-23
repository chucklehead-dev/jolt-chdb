#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
qualification_root=${1:?usage: scripts/qualify-durable-native.sh DIRECTORY}
jolt_bin=${JOLT_BIN:-jolt}
release=26.7.3
commit=7d84d719da07184f6a49405a11b112f16925af72
oracle_digest=56257403ba7563c5a6ecbe7ab4c13ca6a3a7a81a75a314ce3d54a3e108987f99

# Added stage labels use only pinned public asset/oracle names and phase names.
# They do not filter stderr from curl, the C compiler/oracle, or Jolt children;
# those tools remain subject to ordinary CI log handling.
stage() {
  printf 'durable-native qualification: %s\n' "$1" >&2
}

fail() {
  stage "$1"
  exit "${2:-1}"
}

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
    library=libchdb.so
    ;;
  Darwin:x86_64)
    asset=macos-x86_64-libchdb.tar.gz
    digest=af5ded3ed3e84c31af1cd198dcf459f11d2b6aad4f6ddeccc04b8a519b0300fc
    library=libchdb.so
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

fetch_verified() (
  local target=$1 expected_digest=$2 source_url=$3 label=$4
  local actual_digest temporary
  temporary=""

  # This trap belongs only to the download subshell. It removes an incomplete
  # sibling file on an interrupt without replacing the later process-fixture
  # cleanup and signal traps.
  cleanup_temporary() {
    local status=$?
    trap - EXIT HUP INT TERM
    if [[ -n "$temporary" && -f "$temporary" && ! -L "$temporary" ]]; then
      rm -f "$temporary"
    fi
    exit "$status"
  }
  trap cleanup_temporary EXIT
  trap 'exit 129' HUP
  trap 'exit 130' INT
  trap 'exit 143' TERM

  if [[ -f "$target" ]]; then
    stage "verify cached $label"
    actual_digest=$(digest_file "$target") || fail "could not verify cached $label"
    if [[ "$actual_digest" == "$expected_digest" ]]; then
      return
    fi
    stage "cached $label did not verify; refetch"
  fi

  stage "fetch $label"
  temporary=$(mktemp "$target.download.XXXXXX") || fail "could not prepare $label fetch"
  if ! curl --fail --show-error --location \
      --retry 2 --retry-all-errors --connect-timeout 20 --max-time 300 \
      --output "$temporary" "$source_url"; then
    rm -f "$temporary"
    fail "fetch failed for $label"
  fi
  stage "verify fetched $label"
  actual_digest=$(digest_file "$temporary") || {
    rm -f "$temporary"
    fail "could not verify fetched $label"
  }
  if [[ "$actual_digest" != "$expected_digest" ]]; then
    rm -f "$temporary"
    fail "fetched $label did not verify"
  fi
  mv -f "$temporary" "$target" || {
    rm -f "$temporary"
    fail "could not retain verified $label"
  }
  temporary=""
)

fetch_verified "$archive" "$digest" "$release_url" "native asset $asset"

stage "extract native asset $asset"
tar -xzf "$archive" -C "$qualification_root/native"
library_path="$qualification_root/native/$library"
[[ -f "$library_path" ]] || fail "extracted native asset $asset is incomplete"

oracle_url="https://raw.githubusercontent.com/chdb-io/chdb-core/$commit/examples/chdbDurableAbiTest.c"
oracle_source="$qualification_root/oracle/chdbDurableAbiTest.c"
fetch_verified "$oracle_source" "$oracle_digest" "$oracle_url" "upstream oracle chdbDurableAbiTest.c"
oracle_run=$(mktemp -d "$qualification_root/oracle/run.XXXXXX")

# The pinned upstream example currently produces one conservative GCC
# format-truncation warning, so warnings remain visible but are not promoted.
stage "compile upstream oracle chdbDurableAbiTest.c"
cc -std=c11 -Wall -Wextra \
  -I"$qualification_root/native" "$oracle_source" \
  -L"$qualification_root/native" -Wl,-rpath,"$qualification_root/native" \
  -lchdb -o "$oracle_run/chdbDurableAbiTest"
stage "run upstream oracle chdbDurableAbiTest.c"
(
  cd "$oracle_run"
  ./chdbDurableAbiTest
)

stage "run Jolt native process-lifecycle probe"
JOLT_CHDB_LIB="$library_path" \
  "$repo_root/scripts/check-native-process-lifecycle.sh" "$jolt_bin"
stage "run Jolt typed process-exit probe"
JOLT_CHDB_LIB="$library_path" \
  "$repo_root/scripts/check-native-typed-process-exit.sh" "$jolt_bin"

process_root=$(mktemp -d "$qualification_root/process-lifecycle.XXXXXX")
completion=false
cleanup() {
  local status=$?
  if [[ "$completion" == true && "$status" == 0 &&
        "$process_root" == "$qualification_root"/process-lifecycle.* &&
        -d "$process_root" && ! -L "$process_root" ]]; then
    rm -rf "$process_root"
  fi
}
trap cleanup EXIT
trap 'completion=false; exit 129' HUP
trap 'completion=false; exit 130' INT
trap 'completion=false; exit 143' TERM
object_root="$process_root/objects"
core_root="$process_root/core"
mkdir -p "$object_root" "$core_root"

run_phase() {
  local phase=$1
  local scratch_root="$process_root/scratch-$phase"
  mkdir -p "$scratch_root"
  stage "run Jolt Durable phase $phase"
  JOLT_CHDB_LIB="$library_path" \
  JOLT_CHDB_NATIVE_OBJECT_ROOT="$object_root" \
  JOLT_CHDB_NATIVE_CORE_ROOT="$core_root" \
  JOLT_CHDB_NATIVE_SCRATCH_ROOT="$scratch_root" \
    "$jolt_bin" -M:durable-native-test "$phase"
}

for phase in \
  core \
  object-writer object-reader \
  json-rows-writer json-rows-reader \
  wal-writer wal-reader checkpoint-writer checkpoint-reader \
  secret-mutation secret-read
do
  run_phase "$phase"
done
completion=true
