#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
literate_spec="$repo_root/formal/quint/durable-writer-lifecycle.md"
target="$repo_root/target/formal/quint"
model="$target/durableWriterLifecycle.qnt"
tests="$target/durableWriterLifecycleTest.qnt"
expected="$repo_root/formal/quint/traces/renewal-loss.itf.json"
seed="${QUINT_SEED:-0x47fece}"

for tool in lmt quint jq
do
  if ! command -v "$tool" >/dev/null 2>&1
  then
    echo "$tool is required for the Durable renewal-loss Quint gate" >&2
    exit 1
  fi
done

if [[ "$(quint --version)" != "0.32.0" ]]
then
  echo "the Durable renewal-loss gate requires Quint 0.32.0" >&2
  exit 1
fi

mkdir -p "$target"
candidate_dir=$(mktemp -d "$target/renewal-loss-itf.XXXXXX")
trap 'rm -rf "$candidate_dir"' EXIT
candidate="$candidate_dir/candidate.itf.json"
normalized="$candidate_dir/normalized.itf.json"

(
  cd "$repo_root"
  lmt "${literate_spec#$repo_root/}"
)

quint typecheck "$model"
quint typecheck "$tests"

for module in \
  durableWriterRenewalLossCorrectedTest \
  durableWriterRenewalLossIgnoreExpiryMutantTest \
  durableWriterRenewalLossFenceOnFailureMutantTest \
  durableWriterRenewalLossAllowEffectsMutantTest \
  durableWriterRenewalLossDropReadMutantTest
do
  quint test "$tests" \
    --main "$module" \
    --match '.*Test' \
    --backend typescript \
    --verbosity 1
done

quint run "$model" \
  --main durableWriterRenewalLossCorrected \
  --step step \
  --invariants failedBeforeExpiryStaysWritable \
    failedRenewalThroughExpiryFences fencedWriteEffectsAreZero \
    fencedReadSurvives \
  --witnesses successfulRenewalReached failedRenewalReached \
    renewalExpiryReached publicOutcomesReached \
  --max-steps 4 \
  --max-samples 1 \
  --n-traces 1 \
  --seed "$seed" \
  --mbt \
  --out-itf "$candidate" \
  --backend typescript \
  --verbosity 1

jq -e '
  [.states[]."mbt::actionTaken"]
    == ["init", "extendLease", "failRenewalBeforeExpiry",
        "elapseThroughExpiry", "observePublicOutcomes"]
  and (.states | length) == 5
  and ([.states[-1][] | objects
        | select(has("executeEffects"))][0]
       | [.executeEffects."#bigint", .flushEffects."#bigint",
          .checkpointEffects."#bigint", .readAvailable,
          .readResult."#bigint"] == ["0", "0", "0", true, "4"])
' "$candidate" >/dev/null

jq --arg seed "$seed" '
  .vars |= map(if endswith("::renewalLoss") then "renewalLoss" else . end)
  | .states |= map(
      ([keys[] | select(endswith("::renewalLoss"))] | first) as $state_key
      | if $state_key == null then .
        else . + {renewalLoss: .[$state_key]} | del(.[$state_key])
        end
    )
  | ."#meta" = {
    format: "ITF",
    "format-description": "https://apalache-mc.org/docs/adr/015adr-trace.html",
    source: "formal/quint/durable-writer-lifecycle.md#renewal-loss-and-public-operation-fencing",
    seed: $seed,
    status: "ok"
  }
' "$candidate" >"$normalized"

cmp "$normalized" "$expected"
echo "Durable renewal-loss Quint examples and ITF projection passed"
