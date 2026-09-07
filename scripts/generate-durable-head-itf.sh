#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
literate_spec="$repo_root/formal/quint/durable-head-cas.md"
target="$repo_root/target/formal/quint"
model="$target/durableHeadCas.qnt"
trace="${1:-$target/corrected-mbt.itf.json}"
commands="${2:-$target/corrected-mbt.commands.json}"
seed="${QUINT_SEED:-0x51a7e}"

for tool in lmt quint jq
do
  if ! command -v "$tool" >/dev/null 2>&1
  then
    echo "$tool is required to generate Durable head ITF commands" >&2
    exit 1
  fi
done

mkdir -p "$target" "$(dirname -- "$trace")" "$(dirname -- "$commands")"
candidate_dir=$(mktemp -d "$target/mbt-candidates.XXXXXX")
trap 'rm -rf "$candidate_dir"' EXIT
normalized="$candidate_dir/normalized.itf.json"
(
  cd "$repo_root"
  lmt "${literate_spec#$repo_root/}"
)

quint run "$model" \
  --main durableHeadCasCorrected \
  --step legacyStep \
  --max-steps 6 \
  --max-samples 64 \
  --n-traces 64 \
  --seed "$seed" \
  --mbt \
  --out-itf "$candidate_dir/trace_{seq}.itf.json" \
  --backend typescript \
  --verbosity 0

selected=
for ((index = 0; index < 64; index++))
do
  candidate="$candidate_dir/trace_$index.itf.json"
  if jq -e '
    [.states[]."durableHeadCasCorrected::durableHeadCas::state".events[-1]?
     | select(.tag == "CommitAttempted"
              and (.value.result.tag == "Committed"
                   or .value.result.tag == "Reconciled"))]
    | length > 0
  ' "$candidate" >/dev/null
  then
    selected=$candidate
    break
  fi
done

if [[ -z "$selected" ]]
then
  echo "the MBT corpus did not reach an acknowledged commit" >&2
  exit 1
fi

cp "$selected" "$trace"

# Quint records an absolute generated source path and wall-clock metadata.
# Replace only that arbitrary ADR-015 metadata so a fixed seed is a stable,
# reviewable fixture; state values and MBT choices remain untouched.
jq --arg seed "$seed" '
  ."#meta" = {
    format: "ITF",
    "format-description": "https://apalache-mc.org/docs/adr/015adr-trace.html",
    source: "formal/quint/durable-head-cas.md",
    seed: $seed,
    status: "ok"
  }
' "$trace" >"$normalized"
mv "$normalized" "$trace"

jq -e --arg seed "$seed" -f "$repo_root/scripts/durable-head-itf-commands.jq" \
  "$trace" >"$commands"
jq -e '.source.format == "ITF" and (.operations | length == 6)' \
  "$commands" >/dev/null

echo "ITF trace: $trace"
echo "Durable commands: $commands"
echo "Replay seed: $seed"
