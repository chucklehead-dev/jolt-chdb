#!/usr/bin/env bash
set -euo pipefail

default_repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
repo_root=${DURABLE_MODEL_CLASSIFIER_REPO:-$default_repo}

usage() {
  echo "usage: $0 --paths [PATH ...] | --diff BASE HEAD [--merge-base]" >&2
  exit 2
}

is_zero_revision() {
  [[ $1 =~ ^0+$ ]]
}

is_exhaustive_input() {
  case "$1" in
    .github/workflows/durable-head-quint.yml | \
    formal/*.smt2 | formal/**/*.smt2 | \
    formal/quint/* | formal/quint/**/* | \
    scripts/check-durable-head-quint.sh | \
    scripts/check-durable-head-itf-corpus.sh | \
    scripts/generate-durable-head-itf.sh | \
    scripts/generate-durable-engine-metadata-itf.sh | \
    scripts/durable-head-itf-commands.jq | \
    scripts/durable-head-itf-coverage.jq | \
    scripts/classify-durable-model-paths.sh)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

emit_decision() {
  local exhaustive=$1
  local reason=$2
  local changed_count=$3
  printf 'exhaustive=%s\n' "$exhaustive"
  printf 'reason=%s\n' "$reason"
  printf 'changed_count=%s\n' "$changed_count"
}

fail_closed() {
  emit_decision true "diff-command-failed" 0
  exit 0
}

classify_paths() {
  local path
  local count=0
  local first_match=""
  for path in "$@"; do
    count=$((count + 1))
    if [[ -z $first_match ]] && is_exhaustive_input "$path"; then
      first_match=$path
    fi
  done
  if [[ -n $first_match ]]; then
    # Shell escaping keeps unusual but valid Git paths on one safe output line.
    printf -v first_match '%q' "$first_match"
    emit_decision true "model-input:$first_match" "$count"
  else
    emit_decision false "model-inputs-byte-identical" "$count"
  fi
}

[[ $# -ge 1 ]] || usage
mode=$1
shift
case "$mode" in
  --paths)
    classify_paths "$@"
    ;;
  --diff)
    [[ $# -ge 2 && $# -le 3 ]] || usage
    if [[ $# -eq 3 && ${3:-} != --merge-base ]]; then
      usage
    fi
    base=$1
    head=$2
    use_merge_base=false
    [[ ${3:-} == --merge-base ]] && use_merge_base=true
    if [[ -z $base || -z $head ]] || is_zero_revision "$base" || \
       is_zero_revision "$head"; then
      emit_decision true "missing-or-zero-diff-boundary" 0
      exit 0
    fi

    # Every fallible Git operation is checked. A bad repository, unavailable
    # object, missing merge base, or failed diff can only select the full tier.
    git -C "$repo_root" cat-file -e "$base^{commit}" 2>/dev/null || fail_closed
    git -C "$repo_root" cat-file -e "$head^{commit}" 2>/dev/null || fail_closed
    diff_base=$base
    if [[ $use_merge_base == true ]]; then
      if ! diff_base=$(git -C "$repo_root" merge-base "$base" "$head" 2>/dev/null)
      then
        fail_closed
      fi
      [[ -n $diff_base ]] || fail_closed
      git -C "$repo_root" cat-file -e "$diff_base^{commit}" 2>/dev/null || \
        fail_closed
    fi

    diff_file=$(mktemp)
    cleanup() { rm -f -- "$diff_file"; }
    trap cleanup EXIT HUP INT TERM
    # Disable rename detection so moving a model input out of its classified
    # tree reports the deleted source and cannot become a fast-only rename.
    if ! git -C "$repo_root" diff --no-renames --name-only \
         --diff-filter=ACDMRTUXB -z "$diff_base" "$head" -- > "$diff_file"
    then
      fail_closed
    fi
    paths=()
    while IFS= read -r -d '' path; do
      paths+=("$path")
    done < "$diff_file"
    classify_paths "${paths[@]}"
    ;;
  *)
    usage
    ;;
esac
