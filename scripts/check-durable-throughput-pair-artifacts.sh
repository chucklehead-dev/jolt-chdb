#!/usr/bin/env bash
# Validate a completed local-only A/B/B/A Durable selector receipt.
set -euo pipefail

if [[ ${1:-} == --arm ]]; then
  arm_only=true
  pair_root=${2:?usage: check-durable-throughput-pair-artifacts.sh --arm PAIR-ROOT ARM}
  requested_arm=${3:?usage: check-durable-throughput-pair-artifacts.sh --arm PAIR-ROOT ARM}
  [[ $# == 3 ]] || { echo "unexpected paired throughput checker arguments" >&2; exit 2; }
else
  arm_only=false
  pair_root=${1:?usage: check-durable-throughput-pair-artifacts.sh PAIR-ROOT}
  [[ $# == 1 ]] || { echo "unexpected paired throughput checker arguments" >&2; exit 2; }
fi
[[ "$pair_root" = /* && -d "$pair_root" && ! -L "$pair_root" ]] || {
  echo "paired Durable throughput receipt must be an absolute non-symlink directory" >&2
  exit 2
}

script_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
single_checker="$script_root/check-durable-throughput-artifacts.sh"
[[ -x "$single_checker" ]] || { echo "single-selector artifact checker is unavailable" >&2; exit 2; }

fail() { echo "paired Durable throughput artifact contract failed" >&2; exit 1; }
[[ -f "$pair_root/manifest.env" && ! -L "$pair_root/manifest.env" ]] || fail
if ! "$arm_only"; then
  [[ -f "$pair_root/summary.tsv" && ! -L "$pair_root/summary.tsv" ]] || fail
  [[ -f "$pair_root/pair-complete.env" && ! -L "$pair_root/pair-complete.env" ]] || fail
fi

manifest_sha=$(sha256sum "$pair_root/manifest.env" | awk '{print $1}')
if "$arm_only"; then
  case "$requested_arm" in A1|B1|B2|A2) arms=("$requested_arm");; *) fail;; esac
else
  arms=(A1 B1 B2 A2)
fi
for arm in "${arms[@]}"; do
  arm_root="$pair_root/arms/$arm"
  output="$arm_root/selector"
  marker="$arm_root/complete.env"
  [[ -d "$arm_root" && ! -L "$arm_root" && -d "$output" && ! -L "$output" ]] || fail
  [[ -f "$marker" && ! -L "$marker" ]] || fail
  [[ $(grep -Ec '^schema_version=1$' "$marker") == 1 ]] || fail
  [[ $(grep -Ec "^arm=$arm$" "$marker") == 1 ]] || fail
  case "$arm" in A1|A2) condition=A;; B1|B2) condition=B;; esac
  [[ $(grep -Ec "^condition=$condition$" "$marker") == 1 ]] || fail
  [[ $(grep -Ec "^manifest_sha256=$manifest_sha$" "$marker") == 1 ]] || fail
  "$single_checker" "$output/report.edn" "$output/run.log" "$output/time-v.txt" >/dev/null || fail
  for file in report.edn run.log time-v.txt; do
    expected=$(awk -F= -v key="${file%%.*}_sha256" '$1 == key { print $2 }' "$marker")
    [[ $expected =~ ^[0-9a-f]{64}$ ]] || fail
    actual=$(sha256sum "$output/$file" | awk '{print $1}')
    [[ $actual == "$expected" ]] || fail
  done
done

if ! "$arm_only"; then
  pair_marker="$pair_root/pair-complete.env"
  [[ $(grep -Ec '^schema_version=1$' "$pair_marker") == 1 ]] || fail
  [[ $(grep -Ec '^schedule=A1,B1,B2,A2$' "$pair_marker") == 1 ]] || fail
  [[ $(grep -Ec "^manifest_sha256=$manifest_sha$" "$pair_marker") == 1 ]] || fail
  summary_sha=$(sha256sum "$pair_root/summary.tsv" | awk '{print $1}')
  [[ $(grep -Ec "^summary_sha256=$summary_sha$" "$pair_marker") == 1 ]] || fail
fi

echo "Paired Durable throughput artifacts passed restart-safe validation"
