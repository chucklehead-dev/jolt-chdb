#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
literate_spec="$repo_root/formal/quint/durable-head-cas.md"
target="$repo_root/target/formal/quint"
model="$target/durableHeadCas.qnt"
tests="$target/durableHeadCasTest.qnt"
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
)

quint typecheck "$model"
quint typecheck "$tests"

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
  if ! rg -q "^${witness} was witnessed in [1-9][0-9]* trace" "$sample_log"
  then
    echo "required witness was not reached: $witness" >&2
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

  if [[ $status -eq 0 ]] || ! rg -q '^\[violation\] Found an issue' "$log"
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
