#!/usr/bin/env bash
# Standalone sampled gate. Does not invoke existing exhaustive model tiers.
set -euo pipefail
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
test "$(quint --version)" = 0.32.0
test "$(go version -m "$(command -v lmt)" | awk '$1 == "mod" {print $3}')" = \
  v0.0.0-20210421124901-62fe18f2f6a6
lmt formal/quint/wal-checksum-confirmation.md
model=target/formal/quint/walChecksumConfirmation.qnt
tests=target/formal/quint/walChecksumConfirmationTest.qnt
quint typecheck "$model"
quint typecheck "$tests"
for module in walChecksumConfirmationTest walChecksumUnqualifiedMutantTest \
  walChecksumBindingMutantTest walChecksumFencingMutantTest
do
  quint test "$tests" --main "$module" --match '.*Test' \
    --backend typescript --seed 0x251 --verbosity 2
done
witnesses=(selectionReached uploadReached responseReached checksumReached
  readbackReached fallbackReached existingReached ambiguousReached rejectionReached
  prepareReached commitReached conflictReached fencedReached takeoverReached
  takeoverBeforeResponseReached renewalReached)
output=$(quint run "$model" --main walChecksumConfirmation \
  --invariants evidenceIsSound commitsAreSound --witnesses "${witnesses[@]}" \
  --max-steps 24 --max-samples 1000 --seed 0x251 \
  --backend typescript --verbosity 1)
printf '%s\n' "$output"
for witness in "${witnesses[@]}"; do
  if ! grep -Eq "^$witness was witnessed in [1-9][0-9]* trace" <<< "$output"; then
    echo "unreached or missing witness: $witness" >&2
    exit 1
  fi
done
echo 'WAL checksum confirmation: deterministic controls and sampled witnesses passed'
