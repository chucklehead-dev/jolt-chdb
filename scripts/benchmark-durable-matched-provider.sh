#!/usr/bin/env bash
# Prepared manual harness; source availability is not an execution grant.
set -euo pipefail
umask 077
profile=${1:?closed matched selector required}
case "$profile" in
  matched-local-512|matched-local-1000|matched-local-5000|matched-local-10000|matched-aws-512|matched-aws-1000|matched-aws-5000|matched-aws-10000) ;;
  *) exit 2 ;;
esac
repo=$(cd "$(dirname "$0")/.." && pwd -P)
evidence_parent=${BENCH_PERSISTENT_EVIDENCE_PARENT:?existing persistent parent required}
test -d "$evidence_parent"
test "${evidence_parent:0:1}" = /
binary=${BENCH_JOLT_BIN:?authenticated private executable copy required}
test -x "$binary"
test "${binary:0:1}" = /
expected=${BENCH_JOLT_BINARY_SHA256:?independent binary digest required}
test "$(sha256sum "$binary" | cut -d' ' -f1)" = "$expected"
receipt=$(mktemp -d "$evidence_parent/durable-matched-provider.XXXXXXXX")
export BENCH_PERSISTENT_RECEIPT_ROOT="$receipt"
printf 'DURABLE_MATCHED_RECEIPT=%s\n' "$receipt"
finish() { local code=$?; trap - EXIT; printf '%s\n' "$code" > "$receipt/outer.exit"; exit "$code"; }
trap finish EXIT
sha256sum "$binary" "$repo/bench/jdbc/chdb_durable_throughput.clj" > "$receipt/source-before.sha256"
cd "$repo"
set +e
/home/chuck/ai-src/tools/jolt-with-chez-10.4.1 "$binary" -Srepro -M:durable-throughput "$profile" "$receipt/result.edn" > "$receipt/parent.stdout" 2> "$receipt/parent.stderr"
code=$?
set -e
printf '%s\n' "$code" > "$receipt/parent.exit"
test "$code" = 0
if [[ "$profile" == matched-local-512 || "$profile" == matched-aws-512 ]]; then
  grep -Eq ':encoding-inclusive-512-acceptance[[:space:]]+\{:status :passed' "$receipt/result.edn" || {
    echo "Durable encoding-inclusive 512 acceptance receipt is not passing" >&2
    exit 1
  }
fi
sha256sum -c "$receipt/source-before.sha256" > "$receipt/source-parity.stdout"
# No cleanup or environment dump: failure stores and child scratch persist.
