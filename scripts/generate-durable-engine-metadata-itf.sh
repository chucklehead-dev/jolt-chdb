#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
literate_spec="$repo_root/formal/quint/durable-head-cas.md"
target="$repo_root/target/formal/quint"
model="$target/durableEngineMetadata.qnt"
trace="${1:-$target/engine-metadata.itf.json}"
seed="${QUINT_SEED:-0xe61e45}"

for tool in lmt quint jq
do
  if ! command -v "$tool" >/dev/null 2>&1
  then
    echo "$tool is required to generate the engine-metadata ITF trace" >&2
    exit 1
  fi
done

mkdir -p "$target" "$(dirname -- "$trace")"
candidate_dir=$(mktemp -d "$target/engine-metadata-itf.XXXXXX")
trap 'rm -rf "$candidate_dir"' EXIT
candidate="$candidate_dir/trace.itf.json"
normalized="$candidate_dir/normalized.itf.json"

(
  cd "$repo_root"
  lmt "${literate_spec#$repo_root/}"
)

quint run "$model" \
  --main durableEngineMetadataCorrected \
  --step step \
  --max-steps 2 \
  --max-samples 1 \
  --n-traces 1 \
  --seed "$seed" \
  --mbt \
  --out-itf "$candidate" \
  --backend typescript \
  --verbosity 0

jq -e '
  (.states | length) == 3
  and .states[1]."mbt::actionTaken" == "takeover"
  and .states[2]."mbt::actionTaken" == "checkpoint"
' "$candidate" >/dev/null

jq --arg seed "$seed" '
  .vars |= map(if endswith("::state") then "state" else . end)
  | .states |= map(
      ([keys[] | select(endswith("::state"))] | first) as $state_key
      | if $state_key == null then .
        else . + {state: .[$state_key]} | del(.[$state_key])
        end
    )
  | ."#meta" = {
    format: "ITF",
    "format-description": "https://apalache-mc.org/docs/adr/015adr-trace.html",
    source: "formal/quint/durable-head-cas.md#engine-compatibility-metadata-boundary",
    seed: $seed,
    status: "ok"
  }
' "$candidate" >"$normalized"
mv "$normalized" "$trace"

echo "Engine metadata ITF trace: $trace"
echo "Replay seed: $seed"
