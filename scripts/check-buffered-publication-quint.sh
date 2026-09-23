#!/usr/bin/env bash
set -euo pipefail

model=${1:?usage: check-buffered-publication-quint.sh MODEL}

# This is a sampled fast-tier gate, not an exhaustive model check. The fixed
# seed keeps the less common close/reconciliation witnesses reproducible.
if ! corrected=$(quint run "$model" \
  --main bufferedPublicationCorrected \
  --max-steps 35 --max-samples 1000 --seed 0x970f794fc5d7b \
  --invariants nativeAppliedPrepared appliedHasRecoveryObligation \
    selectedHasExactReplay pendingRetained settlementHeadGated \
    publicationIsAdmissionPrefix settlementIsAdmissionPrefix \
    recoveredReaderHeadGated cleanupRequiresSettlement onlyCurrentOwnerCommits \
    leaseReleasedAfterJoin returnedCloseWasSettled \
  --witnesses fullQueueRejected checkpointFallbackReached \
    checkpointPreparedReached immutableUncommitted failedPublicationRetained \
    ambiguousReconciled ambiguousTakeover multiPrefixSettled closeWithWorkReached \
    prematureCleanupDenied cleanupReached \
  --backend typescript --verbosity 1 2>&1); then
  printf '%s\n' "$corrected" >&2
  echo 'buffered publication corrected run failed' >&2
  exit 1
fi
if [[ $corrected != *'[ok] No violation found'* ]] || \
   grep -Eq 'was witnessed in 0 trace\(s\)' <<<"$corrected"
then
  printf '%s\n' "$corrected" >&2
  echo 'buffered publication corrected run missed an invariant or witness' >&2
  exit 1
fi

check_mutant() {
  local module=$1 invariant=$2 steps=$3 seed=$4 output
  if output=$(quint run "$model" --main "$module" --invariant "$invariant" \
      --max-steps "$steps" --max-samples 1000 --seed "$seed" \
      --backend typescript --verbosity 1 2>&1)
  then
    printf '%s\n' "$output" >&2
    echo "mutant $module did not violate $invariant" >&2
    exit 1
  fi
  if [[ $output != *'error: Invariant violated'* ]]; then
    printf '%s\n' "$output" >&2
    echo "mutant $module failed without the expected invariant violation" >&2
    exit 1
  fi
}

check_mutant bufferedPublicationEarlySettleMutant settlementHeadGated 22 0x825c37e1e114d
check_mutant bufferedPublicationStaleCasMutant onlyCurrentOwnerCommits 22 0x970f794fc5d7b
check_mutant bufferedPublicationEarlyReaderMutant recoveredReaderHeadGated 22 0x970f794fc5d7b
check_mutant bufferedPublicationDropPendingMutant pendingRetained 22 0x970f794fc5d7b
check_mutant bufferedPublicationBroadCheckpointMutant selectedHasExactReplay 26 0xa901a8edd96e8276

echo 'Buffered publication Quint corrected witnesses and five mutants passed'
