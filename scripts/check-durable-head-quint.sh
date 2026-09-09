#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
literate_spec="$repo_root/formal/quint/durable-head-cas.md"
lifecycle_literate_spec="$repo_root/formal/quint/durable-writer-lifecycle.md"
target="$repo_root/target/formal/quint"
model="$target/durableHeadCas.qnt"
tests="$target/durableHeadCasTest.qnt"
publication_model="$target/durablePublicationAck.qnt"
publication_tests="$target/durablePublicationAckTest.qnt"
writer_model="$target/durableWriterBoundary.qnt"
writer_tests="$target/durableWriterBoundaryTest.qnt"
lifecycle_model="$target/durableWriterLifecycle.qnt"
lifecycle_tests="$target/durableWriterLifecycleTest.qnt"
required_quint_version=0.32.0
lmt_revision=62fe18f2f6a6e11c158ff2b2209e1082a4fcd59c

if ! command -v lmt >/dev/null 2>&1
then
  echo "lmt is required; install the pinned extractor with:" >&2
  echo "  go install github.com/driusan/lmt@$lmt_revision" >&2
  exit 1
fi

if ! command -v quint >/dev/null 2>&1
then
  echo "Quint $required_quint_version is required" >&2
  exit 1
fi

actual_quint_version=$(quint --version)
if [[ "$actual_quint_version" != "$required_quint_version" ]]
then
  echo "expected Quint $required_quint_version, found $actual_quint_version" >&2
  exit 1
fi

mkdir -p "$target"
(
  cd "$repo_root"
  lmt "${literate_spec#$repo_root/}"
  lmt "${lifecycle_literate_spec#$repo_root/}"
)

quint typecheck "$model"
quint typecheck "$tests"
quint typecheck "$publication_model"
quint typecheck "$publication_tests"
quint typecheck "$writer_model"
quint typecheck "$writer_tests"
quint typecheck "$lifecycle_model"
quint typecheck "$lifecycle_tests"

quint test "$tests" \
  --main durableHeadCasCorrectedTest \
  --match '.*Test' \
  --backend typescript \
  --verbosity 1

quint test "$tests" \
  --main durableHeadCasMutantTest \
  --match '.*Test' \
  --backend typescript \
  --verbosity 1

quint test "$tests" \
  --main durableHeadCasReferenceMutantTest \
  --match '.*Test' \
  --backend typescript \
  --verbosity 1

quint test "$tests" \
  --main durableHeadCasSequenceMutantTest \
  --match '.*Test' \
  --backend typescript \
  --verbosity 1

quint test "$tests" \
  --main durableHeadCasAttemptReuseMutantTest \
  --match '.*Test' \
  --backend typescript \
  --verbosity 1

quint test "$tests" \
  --main durableHeadCasCommitAttemptMutantTest \
  --match '.*Test' \
  --backend typescript \
  --verbosity 1

quint test "$tests" \
  --main durableHeadCasWholeHeadReconciliationMutantTest \
  --match '.*Test' \
  --backend typescript \
  --verbosity 1

quint test "$publication_tests" \
  --main durablePublicationAckCorrectedTest \
  --match '.*Test' \
  --backend typescript \
  --verbosity 1

quint test "$writer_tests" \
  --main durableWriterBoundaryCorrectedTest \
  --match '.*Test' \
  --backend typescript \
  --verbosity 1

quint test "$lifecycle_tests" \
  --main durableWriterLifecycleCorrectedTest \
  --match '.*Test' \
  --backend typescript \
  --verbosity 1

quint test "$lifecycle_tests" \
  --main durableWriterLifecycleMutantTest \
  --match '.*Test' \
  --backend typescript \
  --verbosity 1

quint test "$lifecycle_tests" \
  --main durableWriterLifecycleReleaseBeforeJoinMutantTest \
  --match '.*Test' \
  --backend typescript \
  --verbosity 1

quint test "$lifecycle_tests" \
  --main durableWriterLifecycleBlockingIOMutantTest \
  --match '.*Test' \
  --backend typescript \
  --verbosity 1

quint test "$writer_tests" \
  --main durableWriterBoundaryMutantTest \
  --match '.*Test' \
  --backend typescript \
  --verbosity 1

quint test "$publication_tests" \
  --main durablePublicationAckMutantTest \
  --match '.*Test' \
  --backend typescript \
  --verbosity 1

"$repo_root/scripts/generate-durable-head-itf.sh"
cmp "$target/corrected-mbt.itf.json" \
  "$repo_root/formal/quint/traces/corrected-mbt.itf.json"

sample_log="$target/corrected-sampled.log"
quint run "$model" \
  --main durableHeadCasCorrected \
  --invariants generationWithinBound sequenceWithinBound publicationAttemptIsFresh attemptIdentityIsUnique headReferenceWasPublished headReferenceIsCanonical attemptTransitionsRefineExactView exactTransitionsRefineContentView acquisitionIncrementsGeneration sequenceNeverRegresses staleWriterCannotChangeHead acknowledgedCommitIsExact failedCommitLeavesHeadUnchanged ambiguousLandedUsesOperationSpecificReconciliation releasePreservesGeneration activeOwnerHasCurrentToken \
  --witnesses acquisitionReached publicationReached confirmedCommitReached reconciledCommitReached ambiguousDropReached staleCommitRejectedReached releaseReached renewalDuringReconciliationReached postRenewalReconciliationReached \
  --max-steps 6 \
  --max-samples 10000 \
  --backend typescript \
  --verbosity 1 | tee "$sample_log"

for witness in \
  acquisitionReached publicationReached confirmedCommitReached \
  reconciledCommitReached ambiguousDropReached \
  staleCommitRejectedReached releaseReached \
  renewalDuringReconciliationReached postRenewalReconciliationReached
do
  if ! grep -Eq "^${witness} was witnessed in [1-9][0-9]* trace" "$sample_log"
  then
    echo "required witness was not reached: $witness" >&2
    exit 1
  fi
done

publication_sample_log="$target/publication-ack-sampled.log"
quint run "$publication_model" \
  --main durablePublicationAckCorrected \
  --invariant publicationAcknowledgementIsSound \
  --witnesses ambiguousLandedReached ambiguousDroppedReached \
  --max-steps 1 \
  --max-samples 1000 \
  --backend typescript \
  --verbosity 1 | tee "$publication_sample_log"

for witness in ambiguousLandedReached ambiguousDroppedReached
do
  if ! grep -Eq "^${witness} was witnessed in [1-9][0-9]* trace" \
    "$publication_sample_log"
  then
    echo "required publication witness was not reached: $witness" >&2
    exit 1
  fi
done

lifecycle_sample_log="$target/writer-lifecycle-sampled.log"
quint run "$lifecycle_model" \
  --main durableWriterLifecycleCorrected \
  --invariants heartbeatCoversCloseWork leaseCoversCloseFlush blockingIOLeavesHeartbeatIndependent releaseFollowsHeartbeatJoin noRenewAfterRelease \
  --witnesses closeReached drainRenewalReached publicationRenewalReached verificationRenewalReached commitRenewalReached \
  --max-steps 12 \
  --max-samples 3000 \
  --backend typescript \
  --verbosity 1 | tee "$lifecycle_sample_log"

for witness in closeReached drainRenewalReached publicationRenewalReached \
  verificationRenewalReached commitRenewalReached
do
  if ! grep -Eq "^${witness} was witnessed in [1-9][0-9]* trace" \
    "$lifecycle_sample_log"
  then
    echo "required writer lifecycle witness was not reached: $witness" >&2
    exit 1
  fi
done

writer_sample_log="$target/writer-boundary-sampled.log"
quint run "$writer_model" \
  --main durableWriterBoundaryCorrected \
  --invariants versionsAreOrdered checkpointFallbackIsSound failedFlushRetainsRecoveryObligation \
  --witnesses boundExecuteReached checkpointCommitReached \
  --max-steps 5 \
  --max-samples 1000 \
  --backend typescript \
  --verbosity 1 | tee "$writer_sample_log"

for witness in boundExecuteReached checkpointCommitReached
do
  if ! grep -Eq "^${witness} was witnessed in [1-9][0-9]* trace" \
    "$writer_sample_log"
  then
    echo "required writer-boundary witness was not reached: $witness" >&2
    exit 1
  fi
done

if [[ "${1:-}" != "--verify" ]]
then
  exit 0
fi

quint verify "$model" \
  --main durableHeadCasCorrected \
  --invariant staleWriterCannotChangeHead \
  --max-steps 6 \
  --backend apalache \
  --apalache-version 0.56.1 \
  --verbosity 1

quint verify "$writer_model" \
  --main durableWriterBoundaryCorrected \
  --invariant checkpointFallbackIsSound \
  --max-steps 5 \
  --backend apalache \
  --apalache-version 0.56.1 \
  --verbosity 1

quint verify "$lifecycle_model" \
  --main durableWriterLifecycleCorrected \
  --invariants heartbeatCoversCloseWork leaseCoversCloseFlush blockingIOLeavesHeartbeatIndependent releaseFollowsHeartbeatJoin noRenewAfterRelease \
  --max-steps 12 \
  --backend apalache \
  --apalache-version 0.56.1 \
  --verbosity 1

quint verify "$writer_model" \
  --main durableWriterBoundaryCorrected \
  --invariant failedFlushRetainsRecoveryObligation \
  --max-steps 5 \
  --backend apalache \
  --apalache-version 0.56.1 \
  --verbosity 1

quint verify "$model" \
  --main durableHeadCasCorrected \
  --invariant publicationAttemptIsFresh \
  --max-steps 6 \
  --backend apalache \
  --apalache-version 0.56.1 \
  --verbosity 1

quint verify "$model" \
  --main durableHeadCasCorrected \
  --invariant headReferenceIsCanonical \
  --max-steps 6 \
  --backend apalache \
  --apalache-version 0.56.1 \
  --verbosity 1

quint verify "$model" \
  --main durableHeadCasCorrected \
  --invariant attemptTransitionsRefineExactView \
  --max-steps 6 \
  --backend apalache \
  --apalache-version 0.56.1 \
  --verbosity 1

quint verify "$model" \
  --main durableHeadCasCorrected \
  --invariant exactTransitionsRefineContentView \
  --max-steps 6 \
  --backend apalache \
  --apalache-version 0.56.1 \
  --verbosity 1

quint verify "$model" \
  --main durableHeadCasCorrected \
  --invariant ambiguousLandedUsesOperationSpecificReconciliation \
  --max-steps 6 \
  --backend apalache \
  --apalache-version 0.56.1 \
  --verbosity 1

expect_violation() {
  local main=$1
  local invariant=$2
  local slug=$3
  local log="$target/$slug.log"
  local status

  set +e
  quint verify "$model" \
    --main "$main" \
    --invariant "$invariant" \
    --max-steps 6 \
    --backend apalache \
    --apalache-version 0.56.1 \
    --out-itf "$target/$slug.itf.json" \
    --verbosity 1 >"$log" 2>&1
  status=$?
  set -e

  if [[ $status -eq 0 ]] || ! grep -Eq '^\[violation\] Found an issue' "$log"
  then
    cat "$log" >&2
    echo "$slug did not produce the expected counterexample" >&2
    exit 1
  fi

  cat "$log"
}

expect_violation durableHeadCasStaleMutant \
  staleWriterCannotChangeHead stale-ownership-mutant
expect_violation durableHeadCasGenerationMutant \
  headReferenceIsCanonical reference-generation-mutant
expect_violation durableHeadCasSequenceMutant \
  headReferenceIsCanonical reference-sequence-mutant
expect_violation durableHeadCasAttemptReuseMutant \
  publicationAttemptIsFresh publication-attempt-reuse-mutant
expect_violation durableHeadCasCommitAttemptMutant \
  headReferenceWasPublished commit-attempt-mutant
expect_violation durableHeadCasWholeHeadReconciliationMutant \
  ambiguousLandedUsesOperationSpecificReconciliation whole-head-reconciliation-mutant

writer_mutant_log="$target/writer-boundary-mutant.log"
set +e
quint verify "$writer_model" \
  --main durableWriterBoundaryMutant \
  --invariant checkpointFallbackIsSound \
  --max-steps 3 \
  --backend apalache \
  --apalache-version 0.56.1 \
  --out-itf "$target/writer-boundary-mutant.itf.json" \
  --verbosity 1 >"$writer_mutant_log" 2>&1
writer_mutant_status=$?
set -e

if [[ $writer_mutant_status -eq 0 ]] || \
   ! grep -Eq '^\[violation\] Found an issue' "$writer_mutant_log"
then
  cat "$writer_mutant_log" >&2
  echo "writer-boundary mutant did not produce the expected counterexample" >&2
  exit 1
fi

cat "$writer_mutant_log"

lifecycle_mutant_log="$target/writer-lifecycle-mutant.log"
set +e
quint verify "$lifecycle_model" \
  --main durableWriterLifecycleMutant \
  --invariant heartbeatCoversCloseWork \
  --max-steps 2 \
  --backend apalache \
  --apalache-version 0.56.1 \
  --out-itf "$target/writer-lifecycle-mutant.itf.json" \
  --verbosity 1 >"$lifecycle_mutant_log" 2>&1
lifecycle_mutant_status=$?
set -e

if [[ $lifecycle_mutant_status -eq 0 ]] || \
   ! grep -Eq '^\[violation\] Found an issue' "$lifecycle_mutant_log"
then
  cat "$lifecycle_mutant_log" >&2
  echo "writer lifecycle mutant did not produce a counterexample" >&2
  exit 1
fi

cat "$lifecycle_mutant_log"

lifecycle_release_mutant_log="$target/writer-lifecycle-release-mutant.log"
set +e
quint verify "$lifecycle_model" \
  --main durableWriterLifecycleReleaseBeforeJoinMutant \
  --invariant noRenewAfterRelease \
  --max-steps 8 \
  --backend apalache \
  --apalache-version 0.56.1 \
  --out-itf "$target/writer-lifecycle-release-mutant.itf.json" \
  --verbosity 1 >"$lifecycle_release_mutant_log" 2>&1
lifecycle_release_mutant_status=$?
set -e

if [[ $lifecycle_release_mutant_status -eq 0 ]] || \
   ! grep -Eq '^\[violation\] Found an issue' \
     "$lifecycle_release_mutant_log"
then
  cat "$lifecycle_release_mutant_log" >&2
  echo "writer lifecycle release mutant did not produce a counterexample" >&2
  exit 1
fi

cat "$lifecycle_release_mutant_log"

lifecycle_blocking_io_mutant_log="$target/writer-lifecycle-blocking-io-mutant.log"
set +e
quint verify "$lifecycle_model" \
  --main durableWriterLifecycleBlockingIOMutant \
  --invariant blockingIOLeavesHeartbeatIndependent \
  --max-steps 3 \
  --backend apalache \
  --apalache-version 0.56.1 \
  --out-itf "$target/writer-lifecycle-blocking-io-mutant.itf.json" \
  --verbosity 1 >"$lifecycle_blocking_io_mutant_log" 2>&1
lifecycle_blocking_io_mutant_status=$?
set -e

if [[ $lifecycle_blocking_io_mutant_status -eq 0 ]] || \
   ! grep -Eq '^\[violation\] Found an issue' \
     "$lifecycle_blocking_io_mutant_log"
then
  cat "$lifecycle_blocking_io_mutant_log" >&2
  echo "writer lifecycle blocking-I/O mutant did not produce a counterexample" >&2
  exit 1
fi

cat "$lifecycle_blocking_io_mutant_log"

quint verify "$publication_model" \
  --main durablePublicationAckCorrected \
  --invariant publicationAcknowledgementIsSound \
  --max-steps 1 \
  --backend apalache \
  --apalache-version 0.56.1 \
  --verbosity 1

publication_mutant_log="$target/publication-ack-mutant.log"
set +e
quint verify "$publication_model" \
  --main durablePublicationAckMutant \
  --invariant publicationAcknowledgementIsSound \
  --max-steps 1 \
  --backend apalache \
  --apalache-version 0.56.1 \
  --out-itf "$target/publication-ack-mutant.itf.json" \
  --verbosity 1 >"$publication_mutant_log" 2>&1
publication_mutant_status=$?
set -e

if [[ $publication_mutant_status -eq 0 ]] || \
  ! grep -Eq '^\[violation\] Found an issue' "$publication_mutant_log"
then
  cat "$publication_mutant_log" >&2
  echo "publication acknowledgement mutant did not produce a counterexample" >&2
  exit 1
fi

cat "$publication_mutant_log"
