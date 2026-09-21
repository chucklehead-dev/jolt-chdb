#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
literate_spec="$repo_root/formal/quint/durable-file-wal-spool.md"
target="$repo_root/target/formal/quint"
model="$target/durableFileWalSpool.qnt"
tests="$target/durableFileWalSpoolTest.qnt"
required_lmt_module_version=v0.0.0-20210421124901-62fe18f2f6a6

for tool in lmt quint
do
  if ! command -v "$tool" >/dev/null 2>&1
  then
    echo "$tool is required for the Durable file-WAL spool Quint gate" >&2
    exit 1
  fi
done

if [[ "$(quint --version)" != "0.32.0" ]]
then
  echo "the Durable file-WAL spool gate requires Quint 0.32.0" >&2
  exit 1
fi

if [[ "$(go version -m "$(command -v lmt)" | awk '$1 == "mod" { print $3 }')" \
      != "$required_lmt_module_version" ]]
then
  echo "the Durable file-WAL spool gate requires the pinned lmt extractor" >&2
  exit 1
fi

mkdir -p "$target"
(
  cd "$repo_root"
  lmt "${literate_spec#$repo_root/}"
)

quint typecheck "$model"
quint typecheck "$tests"

for module in \
  durableFileWalSpoolCorrectedTest \
  durableFileWalSpoolAppendMutantTest \
  durableFileWalSpoolPublicationMutantTest \
  durableFileWalSpoolClearMutantTest \
  durableFileWalSpoolDeleteMutantTest
do
  quint test "$tests" \
    --main "$module" \
    --match '.*Test' \
    --backend typescript \
    --verbosity 1
done

quint run "$model" \
  --main durableFileWalSpoolCorrected \
  --step step \
  --invariants appendFailureRequiresCheckpoint \
    checkpointRequirementForbidsWal \
    unconfirmedPublicationRetainsSealedWork deletionOnlyAfterClear \
    closeRetainsFirstPublicationError \
  --witnesses committedDeletionReached publicationFailureRetained \
  --max-steps 6 \
  --max-samples 200 \
  --n-traces 20 \
  --seed 0x159f2 \
  --backend typescript \
  --verbosity 1

echo "Durable file-WAL spool Quint corrected trace and four mutants passed"
