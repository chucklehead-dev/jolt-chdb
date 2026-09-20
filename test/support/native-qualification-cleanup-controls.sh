#!/usr/bin/env bash
set -euo pipefail
# Shell lifecycle only: setup/checksum providers are MOCKED, not native proof.
repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
root=$(mktemp -d /tmp/chdb-retention-controls.XXXXXXXX)
printf 'CONTROL_ROOT=%s\n' "$root"
mkdir -p "$root/repo/scripts" "$root/repo/test/fixtures/native-process-typed-exit" "$root/bin"
cp "$repo/scripts/qualify-durable-native.sh" "$repo/scripts/check-native-typed-process-exit.sh" "$repo/scripts/check-native-process-lifecycle.sh" "$root/repo/scripts/"
cat > "$root/bin/provider" <<'PROVIDER'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == -M:run ]]; then
  if [[ "$CONTROL_KIND" == typed ]]; then
    printf '%s\n' "$2" > "$LEDGER/root"
    case "$CASE" in
    nonzero) exit 7;;
    missing) echo 'PASS typed export readback checkpoint explicit close'; exit 0;;
    kill-grace)
      trap '' TERM
      printf '%s\n' "$$" > "$LEDGER/child"
      awk '{print $22}' "/proc/$$/stat" > "$LEDGER/child-start"
      exec /bin/sleep 2;;
    timeout|signal)
      printf '%s\n' "$$" > "$LEDGER/child"
      exec /bin/sleep 2;;
    esac
  fi
  echo 'PASS typed export readback checkpoint explicit close'
  echo 'PASS typed process-exit anchor closed before host teardown'
elif [[ "$1" == -M:durable-native-test ]]; then
  printf '%s\n' "${JOLT_CHDB_NATIVE_SCRATCH_ROOT%/scratch-*}" > "$LEDGER/root"
  if [[ "$2" == checkpoint-reader && "$CASE" == phase-fail ]]; then exit 7; fi
  if [[ "$2" == checkpoint-reader && "$CASE" == signal && "$CONTROL_KIND" == qualification ]]; then
    printf '%s\n' "$$" > "$LEDGER/child"
    exec /bin/sleep 2
  fi
  echo PASS
else
  echo PASS
fi
PROVIDER
cat > "$root/bin/timeout" <<'TIMEOUT'
#!/usr/bin/env bash
# Real timeout, shortened only in isolated controls; no synthetic status.
args=()
for arg in "$@"; do
  if [[ "$arg" == 120s && ( "$CASE" == timeout || "$CASE" == kill-grace ) ]]; then arg=0.2s; fi
  if [[ "$arg" == --kill-after=5s && "$CASE" == kill-grace ]]; then arg=--kill-after=0.2s; fi
  args+=("$arg")
done
exec /usr/bin/timeout "${args[@]}"
TIMEOUT
cat > "$root/bin/curl" <<'CURL'
#!/usr/bin/env bash
if [[ "$CASE" == fetch-fail ]]; then
  exit 22
fi
while (( $# )); do
  if [[ "$1" == -o || "$1" == --output ]]; then printf mocked > "$2"; exit 0; fi
  shift
done
exit 1
CURL
cat > "$root/bin/sha256sum" <<'SHA'
#!/usr/bin/env bash
# MOCK setup checksums only; tiny files are not actual upstream artifacts.
case "$1" in
  *linux-x86_64-libchdb.tar.gz*) echo "bc33260c32acf78eade2ac41a9115f38e00404651fa42e3bb4c419e4f011c031  $1";;
  *chdbDurableAbiTest.c*) echo "56257403ba7563c5a6ecbe7ab4c13ca6a3a7a81a75a314ce3d54a3e108987f99  $1";;
  *) exit 1;;
esac
SHA
cat > "$root/bin/tar" <<'TAR'
#!/usr/bin/env bash
while (( $# )); do
  if [[ "$1" == -C ]]; then touch "$2/libchdb.so"; exit 0; fi
  shift
done
exit 1
TAR
cat > "$root/bin/cc" <<'CC'
#!/usr/bin/env bash
while (( $# )); do
  if [[ "$1" == -o ]]; then
    printf '#!/bin/sh\necho PASS\n' > "$2"
    chmod +x "$2"
    exit 0
  fi
  shift
done
exit 1
CC
chmod +x "$root/bin/"*
export PATH="$root/bin:/usr/bin:/bin" JOLT_BIN="$root/bin/provider"
run_case() (
  set -euo pipefail
  local kind=$1 case_name=$2 status parent child fixture
  export CASE="$case_name" CONTROL_KIND="$kind" LEDGER="$root/$kind-$case_name" TMPDIR="$root/$kind-$case_name"
  mkdir -p "$LEDGER"
  if [[ "$kind" == typed ]]; then
    command=(bash "$root/repo/scripts/check-native-typed-process-exit.sh" "$JOLT_BIN")
  else
    command=(bash "$root/repo/scripts/qualify-durable-native.sh" "$LEDGER/qualification")
  fi
  set +e
  if [[ "$case_name" == signal ]]; then
    "${command[@]}" > "$LEDGER/output" 2>&1 & parent=$!
    for ((i=0;i<100;i++)); do [[ -f "$LEDGER/child" ]] && break; /bin/sleep 0.02; done
    if [[ ! -f "$LEDGER/child" ]]; then wait "$parent"; exit 1; fi
    child=$(< "$LEDGER/child")
    kill -0 "$child" || exit 1
    kill -TERM "$parent"
    # Leave the known child to terminate normally; parent must retain fixtures.
    wait "$parent"; status=$?
    [[ "$status" == 143 ]] || exit 1
    ! kill -0 "$child" 2>/dev/null || exit 1
  else
    "${command[@]}" > "$LEDGER/output" 2>&1; status=$?
  fi
  set -e
  if [[ "$case_name" == kill-grace ]]; then
    [[ "$status" == 137 && -s "$LEDGER/child-start" ]] || exit 1
    child=$(< "$LEDGER/child")
    ! kill -0 "$child" 2>/dev/null || exit 1
  fi
  fixture=$(< "$LEDGER/root")
  if [[ "$case_name" == success ]]; then
    [[ "$status" == 0 && ! -e "$fixture" ]] || exit 1
    if [[ "$kind" == qualification ]]; then
      grep -Fqx 'durable-native qualification: fetch native asset linux-x86_64-libchdb.tar.gz' "$LEDGER/output"
      grep -Fqx 'durable-native qualification: verify fetched native asset linux-x86_64-libchdb.tar.gz' "$LEDGER/output"
      grep -Fqx 'durable-native qualification: extract native asset linux-x86_64-libchdb.tar.gz' "$LEDGER/output"
      grep -Fqx 'durable-native qualification: compile upstream oracle chdbDurableAbiTest.c' "$LEDGER/output"
      grep -Fqx 'durable-native qualification: run upstream oracle chdbDurableAbiTest.c' "$LEDGER/output"
      grep -Fqx 'durable-native qualification: run Jolt Durable phase secret-read' "$LEDGER/output"
    fi
  else
    [[ "$status" != 0 && -d "$fixture" ]] || exit 1
  fi
  printf 'PASS kind=%s case=%s status=%s fixture-contract-confirmed\n' "$kind" "$case_name" "$status"
  printf '%s:%s\n' "$kind" "$case_name" >> "$root/pass-receipts"
)
invoke_case() {
  local status
  set +e
  run_case "$@"
  status=$?
  set -e
  if [[ "$status" != 0 ]]; then
    printf 'FAIL kind=%s case=%s status=%s (owned ledger retained)\n' "$1" "$2" "$status" >&2
    exit 1
  fi
}
for case_name in success nonzero missing timeout signal kill-grace; do invoke_case typed "$case_name"; done
for case_name in success phase-fail signal; do invoke_case qualification "$case_name"; done
fetch_failure_control() (
  set -euo pipefail
  local ledger="$root/qualification-fetch-fail" status
  mkdir -p "$ledger"
  export CASE=fetch-fail CONTROL_KIND=qualification LEDGER="$ledger" TMPDIR="$ledger"
  set +e
  bash "$root/repo/scripts/qualify-durable-native.sh" "$ledger/qualification" > "$ledger/output" 2>&1
  status=$?
  set -e
  [[ "$status" != 0 ]]
  grep -Fqx 'durable-native qualification: fetch native asset linux-x86_64-libchdb.tar.gz' "$ledger/output"
  grep -Fqx 'durable-native qualification: fetch failed for native asset linux-x86_64-libchdb.tar.gz' "$ledger/output"
  ! find "$ledger/qualification" -name '*.download.*' -print -quit | grep -q .
  printf '%s\n' qualification:fetch-fail >> "$root/pass-receipts"
  echo 'PASS qualification fetch failure names its public stage and retains no partial download'
)
fetch_failure_control
expected=$(printf '%s\n' typed:success typed:nonzero typed:missing typed:timeout typed:signal typed:kill-grace qualification:success qualification:phase-fail qualification:signal qualification:fetch-fail | sort)
actual=$(sort "$root/pass-receipts")
[[ "$actual" == "$expected" && "$(sort -u "$root/pass-receipts" | wc -l)" == 10 ]] || exit 1
echo 'PASS all shell retention controls (setup mocked; no native qualification)'
# Retain the complete owned control ledger for review; no deletion on failure.
