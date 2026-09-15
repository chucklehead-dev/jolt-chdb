#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
literate_spec="$repo_root/formal/quint/native-process-lifecycle.md"
target="$repo_root/target/formal/quint"
tests="$target/nativeProcessLifecycleTest.qnt"
trace_dir="${1:-$repo_root/formal/quint/traces}"
seed="${QUINT_SEED:-0x103a15}"

for tool in lmt quint jq
do
  if ! command -v "$tool" >/dev/null 2>&1
  then
    echo "$tool is required to generate native lifecycle ITF traces" >&2
    exit 1
  fi
done

mkdir -p "$target" "$trace_dir"
candidate_dir=$(mktemp -d "$target/native-lifecycle-itf.XXXXXX")
trap 'rm -rf "$candidate_dir"' EXIT

(
  cd "$repo_root"
  lmt "${literate_spec#$repo_root/}"
)

generate_trace() {
  local module=$1
  local steps=$2
  local output=$3
  local expected=$4
  local candidate="$candidate_dir/$output"
  local normalized="$candidate_dir/normalized-$output"

  quint run "$tests" \
    --main "$module" \
    --step traceStep \
    --max-steps "$steps" \
    --max-samples 1 \
    --n-traces 1 \
    --seed "$seed" \
    --mbt \
    --out-itf "$candidate" \
    --backend typescript \
    --verbosity 0

  jq -e --arg expected "$expected" '
    .states as $states
    | ($expected | split(",")) as $actions
    | ($actions | length) == ($states | length)
      and $states[0]."mbt::actionTaken" == "init"
      and all(range(1; $states | length);
        $states[.]."mbt::actionTaken" == "traceStep")
  ' "$candidate" >/dev/null

  jq --arg seed "$seed" --arg expected "$expected" '
    ($expected | split(",")) as $actions
    | .vars |= map(if endswith("::lifecycle") then "lifecycle" else . end)
    | .states |= (to_entries | map(
        .key as $index | .value
        | ."mbt::actionTaken" = $actions[$index]
        |
        ([keys[] | select(endswith("::lifecycle"))] | first) as $state_key
        | if $state_key == null then .
          else . + {lifecycle: .[$state_key]} | del(.[$state_key])
          end
      ))
    | ."#meta" = {
      format: "ITF",
      "format-description": "https://apalache-mc.org/docs/adr/015adr-trace.html",
      source: "formal/quint/native-process-lifecycle.md#executable-traces",
      seed: $seed,
      status: "ok"
    }
  ' "$candidate" >"$normalized"
  mv "$normalized" "$trace_dir/$output"
}

generate_trace nativeProcessLifecycleAnchorTrace 5 \
  native-process-lifecycle.itf.json \
  init,openMemory,closePublic,openMemory,closePublic,openDifferent
generate_trace nativeProcessLifecycleTerminalTrace 2 \
  native-process-terminal.itf.json \
  init,uncertainBootstrapFailure,retryAfterTerminal
generate_trace nativeProcessLifecycleOptionsTrace 3 \
  native-process-options.itf.json \
  init,openDiskWithBackups,closePublic,openDifferentOptions

echo "Native lifecycle ITF traces: $trace_dir"
echo "Replay seed: $seed"
