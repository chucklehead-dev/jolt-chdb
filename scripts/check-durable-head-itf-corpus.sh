#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
literate_spec="$repo_root/formal/quint/durable-head-cas.md"
target="$repo_root/target/formal/quint"
model="$target/durableHeadCas.qnt"
seed="${QUINT_ITF_CORPUS_SEED:-0xd17ab1e}"
trace_count="${QUINT_ITF_TRACE_COUNT:-64}"

if ! [[ "$trace_count" =~ ^[1-9][0-9]*$ ]] || ((trace_count > 256))
then
  echo "QUINT_ITF_TRACE_COUNT must be an integer from 1 through 256" >&2
  exit 1
fi

for tool in lmt quint
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

if [[ -x "$repo_root/../tools/jolt-with-chez-10.4.1" ]]
then
  jolt_command=("$repo_root/../tools/jolt-with-chez-10.4.1" jolt)
else
  jolt_command=(jolt)
fi

"${jolt_command[@]}" -M:durable-itf-test "${traces[@]}"
echo "Durable Quint ITF corpus: $trace_count trace(s), seed $seed"
