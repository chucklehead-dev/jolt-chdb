#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
literate_spec="$repo_root/formal/quint/durable-head-cas.md"
target="$repo_root/target/formal/quint"
model="$target/durableHeadCas.qnt"
coverage="$target/itf-corpus-coverage.json"
# With the pinned Quint evaluator, this seed reaches every required legacy
# action and outcome within the existing 64-trace default corpus.
seed="${QUINT_ITF_CORPUS_SEED:-0x9009}"
trace_count="${QUINT_ITF_TRACE_COUNT:-64}"
replay_mode="${1:-}"

if [[ -n "$replay_mode" && "$replay_mode" != "--with-s3" ]]
then
  echo "usage: $0 [--with-s3]" >&2
  exit 2
fi

if ! [[ "$trace_count" =~ ^[1-9][0-9]*$ ]] || ((trace_count > 256))
then
  echo "QUINT_ITF_TRACE_COUNT must be an integer from 1 through 256" >&2
  exit 1
fi

for tool in jq lmt quint
do
  if ! command -v "$tool" >/dev/null 2>&1
  then
    echo "$tool is required for the Durable ITF corpus check" >&2
    exit 1
  fi
done

mkdir -p "$target"
corpus_dir=$(mktemp -d "$target/itf-replay-corpus.XXXXXX")
trap 'rm -rf "$corpus_dir"' EXIT

(
  cd "$repo_root"
  lmt "${literate_spec#$repo_root/}"
)

quint run "$model" \
  --main durableHeadCasCorrected \
  --step legacyStep \
  --max-steps 6 \
  --max-samples "$trace_count" \
  --n-traces "$trace_count" \
  --seed "$seed" \
  --mbt \
  --out-itf "$corpus_dir/trace_{seq}.itf.json" \
  --backend typescript \
  --verbosity 0

traces=()
for ((index = 0; index < trace_count; index++))
do
  trace="$corpus_dir/trace_$index.itf.json"
  if [[ ! -s "$trace" ]]
  then
    echo "Quint did not produce expected ITF trace $index" >&2
    exit 1
  fi
  traces+=("$trace")
done

jq -s \
  --arg seed "$seed" \
  --argjson trace_count "$trace_count" \
  -f "$repo_root/scripts/durable-head-itf-coverage.jq" \
  "${traces[@]}" >"$coverage"

if ! jq -e \
  '.missing.actions == [] and .missing.outcomes == []' \
  "$coverage" >/dev/null
then
  jq . "$coverage" >&2
  echo "Durable ITF corpus did not cover every required action and outcome" >&2
  exit 1
fi

if [[ -x "$repo_root/../tools/jolt-with-chez-10.4.1" ]]
then
  jolt_wrapper="$repo_root/../tools/jolt-with-chez-10.4.1"
  jolt_command=("$jolt_wrapper" jolt)
else
  jolt_wrapper=""
  jolt_command=(jolt)
fi

"${jolt_command[@]}" -M:durable-itf-test "${traces[@]}"
if [[ "$replay_mode" == "--with-s3" ]]
then
  if [[ -n "$jolt_wrapper" ]]
  then
    "$jolt_wrapper" bash test/durable-s3-itf.sh jolt "${traces[@]}"
  else
    bash test/durable-s3-itf.sh jolt "${traces[@]}"
  fi
fi
echo "Durable Quint ITF corpus: $trace_count trace(s), seed $seed"
echo "Coverage manifest: $coverage"
jq -c '.observed' "$coverage"
