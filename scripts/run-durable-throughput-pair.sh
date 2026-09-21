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
  git -C "$repo" diff --quiet && git -C "$repo" diff --cached --quiet || fail "$label checkout is dirty"
  printf '%s_head=%s\n' "$label" "$(git -C "$repo" rev-parse HEAD)"
  printf '%s_parent=%s\n' "$label" "$(git -C "$repo" rev-parse HEAD^)"
  printf '%s_tree=%s\n' "$label" "$(git -C "$repo" rev-parse HEAD^{tree})"
  printf '%s_status=clean\n' "$label"
  printf '%s_selector_sha256=%s\n' "$label" "$(sha256sum "$repo/scripts/$single_selector_name" | awk '{print $1}')"
  printf '%s_benchmark_sha256=%s\n' "$label" "$(sha256sum "$repo/bench/jdbc/chdb_durable_throughput.clj" | awk '{print $1}')"
}

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
persisted = re.findall(r':persisted\s+\{([^{}]*)\}', section, re.S)
if len(persisted) != 1:
    raise SystemExit('missing or ambiguous persisted summary')
rate = one(':aggregate-rows-per-second', persisted[0])
rss = re.findall(r'^\tMaximum resident set size \(kbytes\):\s*([1-9][0-9]*)\s*$', pathlib.Path(timing_path).read_text(), re.M)
if len(rss) != 1:
    raise SystemExit('missing or ambiguous maximum RSS')
target = p50 <= 20.48 and p99 <= 25.60
print('\t'.join([arm, condition, f'{p50:.6f}', f'{p99:.6f}', f'{512000.0 / p50:.2f}', f'{512000.0 / p99:.2f}', f'{rate:.2f}', rss[0], 'pass' if target else 'fail']))
PY
}

run_arm A1 A "$a_repo"
run_arm B1 B "$b_repo"
run_arm B2 B "$b_repo"
run_arm A2 A "$a_repo"

summary_tmp="$pair_root/.summary.$$.tmp"
{
  printf 'arm\tcondition\tp50_ms\tp99_ms\tp50_rows_per_second\tp99_rows_per_second\tpersisted_rows_per_second\tmax_rss_kbytes\tabsolute_target_p50_le_20.48ms_and_p99_le_25.60ms\n'
  extract_summary A1 A
  extract_summary B1 B
  extract_summary B2 B
  extract_summary A2 A
} > "$summary_tmp"
chmod 0444 "$summary_tmp"
mv "$summary_tmp" "$pair_root/summary.tsv"
"$pair_checker" "$pair_root" >/dev/null
cat "$pair_root/summary.tsv"
echo "Absolute target: p50 <= 20.48 ms (25,000 rows/s) and p99 <= 25.60 ms (20,000 rows/s)"
echo "Paired local-only Durable throughput run completed; no remote provider was invoked"
