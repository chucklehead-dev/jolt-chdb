#!/usr/bin/env bash
# Run a bounded local-only A/B/B/A comparison using the existing scale-512
# selector. This runner deliberately adds no benchmark mode, timing boundary,
# or remote provider behavior.
set -euo pipefail
umask 077

pair_root=${1:?usage: run-durable-throughput-pair.sh PAIR-ROOT A-REPO B-REPO}
a_repo=${2:?usage: run-durable-throughput-pair.sh PAIR-ROOT A-REPO B-REPO}
b_repo=${3:?usage: run-durable-throughput-pair.sh PAIR-ROOT A-REPO B-REPO}
[[ $# == 3 ]] || { echo "unexpected paired throughput arguments" >&2; exit 2; }

[[ "$pair_root" = /* ]] || { echo "pair root must be absolute" >&2; exit 2; }
for repo in "$a_repo" "$b_repo"; do
  [[ "$repo" = /* && -d "$repo" && ! -L "$repo/.git" ]] || {
    echo "each condition must be an absolute Git checkout" >&2; exit 2;
  }
done

script_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
single_selector_name=run-durable-throughput-selector.sh
single_checker="$script_root/check-durable-throughput-artifacts.sh"
pair_checker="$script_root/check-durable-throughput-pair-artifacts.sh"
[[ -x "$single_checker" && -x "$pair_checker" ]] || {
  echo "paired throughput helper scripts are unavailable" >&2; exit 2;
}

fail() { echo "paired Durable throughput launcher failed: $*" >&2; exit 1; }
identity() {
  local repo=$1 label=$2
  [[ -z $(git -C "$repo" status --porcelain=v1 --untracked-files=all) ]] || fail "$label checkout is dirty or has untracked files"
  [[ -f "$repo/scripts/$single_selector_name" && ! -L "$repo/scripts/$single_selector_name" ]] || fail "$label selector is unavailable"
  [[ -f "$repo/bench/jdbc/chdb_durable_throughput.clj" && ! -L "$repo/bench/jdbc/chdb_durable_throughput.clj" ]] || fail "$label benchmark source is unavailable"
  [[ -f "$repo/bench/jdbc/chdb_durable_throughput_metrics.clj" && ! -L "$repo/bench/jdbc/chdb_durable_throughput_metrics.clj" ]] || fail "$label metrics benchmark source is unavailable"
  printf '%s_head=%s\n' "$label" "$(git -C "$repo" rev-parse HEAD)"
  printf '%s_parent=%s\n' "$label" "$(git -C "$repo" rev-parse HEAD^)"
  printf '%s_tree=%s\n' "$label" "$(git -C "$repo" rev-parse HEAD^{tree})"
  printf '%s_status=clean\n' "$label"
  printf '%s_selector_sha256=%s\n' "$label" "$(sha256sum "$repo/scripts/$single_selector_name" | awk '{print $1}')"
  printf '%s_benchmark_sha256=%s\n' "$label" "$(sha256sum "$repo/bench/jdbc/chdb_durable_throughput.clj" | awk '{print $1}')"
  printf '%s_metrics_sha256=%s\n' "$label" "$(sha256sum "$repo/bench/jdbc/chdb_durable_throughput_metrics.clj" | awk '{print $1}')"
}

require_matched_harness() {
  # A/B compares implementation source, not two accidentally divergent
  # workloads or selector launchers. There is deliberately no override.
  local a_selector a_benchmark a_metrics b_selector b_benchmark b_metrics
  a_selector=$(sha256sum "$a_repo/scripts/$single_selector_name" | awk '{print $1}')
  b_selector=$(sha256sum "$b_repo/scripts/$single_selector_name" | awk '{print $1}')
  a_benchmark=$(sha256sum "$a_repo/bench/jdbc/chdb_durable_throughput.clj" | awk '{print $1}')
  b_benchmark=$(sha256sum "$b_repo/bench/jdbc/chdb_durable_throughput.clj" | awk '{print $1}')
  a_metrics=$(sha256sum "$a_repo/bench/jdbc/chdb_durable_throughput_metrics.clj" | awk '{print $1}')
  b_metrics=$(sha256sum "$b_repo/bench/jdbc/chdb_durable_throughput_metrics.clj" | awk '{print $1}')
  [[ $a_selector == "$b_selector" ]] || fail "A/B selector sources differ"
  [[ $a_benchmark == "$b_benchmark" ]] || fail "A/B benchmark sources differ"
  [[ $a_metrics == "$b_metrics" ]] || fail "A/B metrics benchmark sources differ"
}

identity "$a_repo" A >/dev/null
identity "$b_repo" B >/dev/null
require_matched_harness

manifest_contents() {
  printf 'schema_version=1\nmode=local-only-durable-throughput-pair\nschedule=A1,B1,B2,A2\np50_max_ms=20.48\np99_max_ms=25.60\n'
  printf 'pair_runner_sha256=%s\n' "$(sha256sum "${BASH_SOURCE[0]}" | awk '{print $1}')"
  printf 'artifact_checker_sha256=%s\n' "$(sha256sum "$single_checker" | awk '{print $1}')"
  printf 'pair_checker_sha256=%s\n' "$(sha256sum "$pair_checker" | awk '{print $1}')"
  identity "$a_repo" A
  identity "$b_repo" B
}

[[ ! -L "$pair_root" ]] || fail "pair root cannot be a symlink"
if [[ ! -e "$pair_root" ]]; then
  mkdir -p "$pair_root/arms"
  manifest_tmp="$pair_root/.manifest.$$.tmp"
  manifest_contents > "$manifest_tmp"
  chmod 0444 "$manifest_tmp"
  mv "$manifest_tmp" "$pair_root/manifest.env"
else
  [[ -d "$pair_root" && -f "$pair_root/manifest.env" && ! -L "$pair_root/manifest.env" ]] || fail "existing pair root lacks a manifest"
  current_tmp=$(mktemp "$pair_root/.manifest-current.XXXXXX")
  manifest_contents > "$current_tmp"
  cmp -s "$pair_root/manifest.env" "$current_tmp" || { rm -f "$current_tmp"; fail "manifest no longer matches exact sources"; }
  rm -f "$current_tmp"
  mkdir -p "$pair_root/arms"
fi
manifest_sha=$(sha256sum "$pair_root/manifest.env" | awk '{print $1}')

complete_arm() {
  local arm=$1 condition=$2 arm_root=$3 output=$4 marker=$5 marker_tmp
  "$single_checker" "$output/report.edn" "$output/run.log" "$output/time-v.txt" >/dev/null
  marker_tmp="$arm_root/.complete.$$.tmp"
  {
    printf 'schema_version=1\narm=%s\ncondition=%s\nmanifest_sha256=%s\n' "$arm" "$condition" "$manifest_sha"
    printf 'report_sha256=%s\n' "$(sha256sum "$output/report.edn" | awk '{print $1}')"
    printf 'run_sha256=%s\n' "$(sha256sum "$output/run.log" | awk '{print $1}')"
    printf 'time-v_sha256=%s\n' "$(sha256sum "$output/time-v.txt" | awk '{print $1}')"
    printf 'completed_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } > "$marker_tmp"
  chmod 0444 "$marker_tmp"
  mv "$marker_tmp" "$marker"
}

run_arm() {
  local arm=$1 condition=$2 repo=$3 arm_root output marker
  arm_root="$pair_root/arms/$arm"
  output="$arm_root/selector"
  marker="$arm_root/complete.env"
  if [[ -e "$arm_root" ]]; then
    [[ -f "$marker" && ! -L "$marker" ]] || fail "$arm has partial or unvalidated evidence; retain it and use a new pair root"
    "$pair_checker" --arm "$pair_root" "$arm" >/dev/null 2>&1 || fail "$arm has invalid completed evidence; retain it and use a new pair root"
    printf 'resume: validated %s\n' "$arm"
    return
  fi
  mkdir -p "$arm_root/cache" "$arm_root/tmp"
  [[ ! -e "$output" ]] || fail "$arm output unexpectedly exists"
  (
    cd "$repo"
    export JOLT_CACHE_DIR="$arm_root/cache"
    export TMPDIR="$arm_root/tmp" TEMP="$arm_root/tmp" TMP="$arm_root/tmp"
    "$repo/scripts/$single_selector_name" scale-512 "$output"
  )
  complete_arm "$arm" "$condition" "$arm_root" "$output" "$marker"
  printf 'completed: %s\n' "$arm"
}

extract_summary() {
  local arm=$1 condition=$2 report timing
  report="$pair_root/arms/$arm/selector/report.edn"
  timing="$pair_root/arms/$arm/selector/time-v.txt"
  python3 - "$arm" "$condition" "$report" "$timing" <<'PY'
import pathlib, re, sys
arm, condition, report_path, timing_path = sys.argv[1:]
text = pathlib.Path(report_path).read_text()
# The checked-in scale selector has one encode-included summary. Require its
# bounded latency map and persisted aggregate rate rather than guessing from a
# partial progress report.
needle = ':durable-encode-included'
if text.count(needle) != 1:
    raise SystemExit('unparseable or ambiguous durable encode-included summary')
start = text.index('{', text.index(needle))
depth = 0; quoted = False; escaped = False; end = None
for index in range(start, len(text)):
    char = text[index]
    if quoted:
        if escaped: escaped = False
        elif char == '\\\\': escaped = True
        elif char == '"': quoted = False
        continue
    if char == '"': quoted = True; continue
    if char == '{': depth += 1
    elif char == '}':
        depth -= 1
        if depth == 0:
            end = index + 1; break
if end is None or depth != 0:
    raise SystemExit('unterminated durable encode-included summary')
section = text[start:end]
def one(label, source):
    values = re.findall(label + r'\s+([0-9]+(?:\.[0-9]+)?)', source)
    if len(values) != 1:
        raise SystemExit('missing or ambiguous ' + label)
    return float(values[0])
latency = re.findall(r':batch-latency-across-trials\s+\{([^{}]*)\}', section, re.S)
if len(latency) != 1:
    raise SystemExit('missing or ambiguous batch latency summary')
p50 = one(':p50-ms', latency[0]); p99 = one(':p99-ms', latency[0])
count = one(':count', latency[0])
p99_qualification = re.findall(r':p99-qualification\?\s+(true|false)', latency[0])
if len(p99_qualification) != 1:
    raise SystemExit('missing or ambiguous p99 qualification')
persisted = re.findall(r':persisted\s+\{([^{}]*)\}', section, re.S)
if len(persisted) != 1:
    raise SystemExit('missing or ambiguous persisted summary')
rate = one(':aggregate-rows-per-second', persisted[0])
rss = re.findall(r'^\tMaximum resident set size \(kbytes\):\s*([1-9][0-9]*)\s*$', pathlib.Path(timing_path).read_text(), re.M)
if len(rss) != 1:
    raise SystemExit('missing or ambiguous maximum RSS')
target = count == 500 and p99_qualification[0] == 'true' and p50 <= 20.48 and p99 <= 25.60
print('\t'.join(['arm', arm, condition, str(int(count)), p99_qualification[0], f'{p50:.6f}', f'{p99:.6f}', f'{512000.0 / p50:.2f}', f'{512000.0 / p99:.2f}', f'{rate:.2f}', rss[0], 'pass' if target else 'fail']))
PY
}

condition_summary() {
  local condition=$1 first=$2 second=$3
  python3 - "$condition" "$pair_root/arms/$first/selector/report.edn" "$pair_root/arms/$second/selector/report.edn" <<'PY'
import pathlib, re, sys
condition, first_path, second_path = sys.argv[1:]
def summary(path):
    text = pathlib.Path(path).read_text(); needle = ':durable-encode-included'
    if text.count(needle) != 1: raise SystemExit('unparseable durable encode-included summary')
    start = text.index('{', text.index(needle)); depth = 0; quoted = escaped = False; end = None
    for index in range(start, len(text)):
        char = text[index]
        if quoted:
            if escaped: escaped = False
            elif char == '\\\\': escaped = True
            elif char == '"': quoted = False
            continue
        if char == '"': quoted = True; continue
        if char == '{': depth += 1
        elif char == '}':
            depth -= 1
            if depth == 0: end = index + 1; break
    section = text[start:end] if end else ''
    latency = re.findall(r':batch-latency-across-trials\s+\{([^{}]*)\}', section, re.S)
    if len(latency) != 1: raise SystemExit('missing latency summary')
    values = latency[0]
    def number(label):
        found = re.findall(label + r'\s+([0-9]+(?:\.[0-9]+)?)', values)
        if len(found) != 1: raise SystemExit('missing ' + label)
        return float(found[0])
    qualification = re.findall(r':p99-qualification\?\s+(true|false)', values)
    if len(qualification) != 1: raise SystemExit('missing p99 qualification')
    return int(number(':count')), qualification[0] == 'true', number(':p50-ms'), number(':p99-ms')
first, second = summary(first_path), summary(second_path)
qualified = all(count == 500 and p99q for count, p99q, _, _ in (first, second))
passed = qualified and all(p50 <= 20.48 and p99 <= 25.60 for _, _, p50, p99 in (first, second))
result = 'both_independent_arms_pass' if passed else ('not-qualified' if not qualified else 'fail')
print('\t'.join(['condition', '-', condition, f'{first[0]},{second[0]}', f'{str(first[1]).lower()},{str(second[1]).lower()}', 'not-computable-from-retained-quantiles', 'not-computable-from-retained-quantiles', '-', '-', '-', '-', result]))
PY
}

run_arm A1 A "$a_repo"
run_arm B1 B "$b_repo"
run_arm B2 B "$b_repo"
run_arm A2 A "$a_repo"

if [[ -e "$pair_root/summary.tsv" || -e "$pair_root/pair-complete.env" ]]; then
  [[ -f "$pair_root/summary.tsv" && ! -L "$pair_root/summary.tsv" && -f "$pair_root/pair-complete.env" && ! -L "$pair_root/pair-complete.env" ]] || fail "final paired summary is partial; retain it and use a new pair root"
  "$pair_checker" "$pair_root" >/dev/null 2>&1 || fail "final paired summary is invalid; retain it and use a new pair root"
  cat "$pair_root/summary.tsv"
  exit 0
fi

summary_tmp="$pair_root/.summary.$$.tmp"
{
  printf 'record\tarm\tcondition\tlatency_samples\tp99_qualification\tp50_ms\tp99_ms\tp50_rows_per_second\tp99_rows_per_second\tpersisted_rows_per_second\tmax_rss_kbytes\tabsolute_target_p50_le_20.48ms_and_p99_le_25.60ms\n'
  extract_summary A1 A
  extract_summary B1 B
  extract_summary B2 B
  extract_summary A2 A
  condition_summary A A1 A2
  condition_summary B B1 B2
} > "$summary_tmp"
chmod 0444 "$summary_tmp"
mv "$summary_tmp" "$pair_root/summary.tsv"
pair_tmp="$pair_root/.pair-complete.$$.tmp"
{
  printf 'schema_version=1\nschedule=A1,B1,B2,A2\nmanifest_sha256=%s\nsummary_sha256=%s\ncompleted_at=%s\n' \
    "$manifest_sha" "$(sha256sum "$pair_root/summary.tsv" | awk '{print $1}')" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$pair_tmp"
chmod 0444 "$pair_tmp"
mv "$pair_tmp" "$pair_root/pair-complete.env"
"$pair_checker" "$pair_root" >/dev/null
cat "$pair_root/summary.tsv"
echo "Absolute target: p50 <= 20.48 ms (25,000 rows/s) and p99 <= 25.60 ms (20,000 rows/s)"
echo "Condition rows do not estimate pooled quantiles: scale reports retain qualifying counts and quantiles, not raw latency samples. They require both independent arms to pass."
echo "Paired local-only Durable throughput run completed; no remote provider was invoked"
