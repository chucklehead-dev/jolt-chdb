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
    .github/actions/install-jolt-aspects/action.yml | \
    .github/workflows/durable-head-quint.yml | \
    deps.edn | \
    formal/*.smt2 | formal/**/*.smt2 | \
    formal/quint/* | formal/quint/**/* | \
    scripts/check-durable-head-quint.sh | \
    scripts/check-durable-file-wal-spool-quint.sh | \
    scripts/check-buffered-publication-quint.sh | \
    scripts/check-durable-head-itf-corpus.sh | \
    scripts/generate-durable-head-itf.sh | \
    scripts/generate-durable-engine-metadata-itf.sh | \
    scripts/generate-native-process-lifecycle-itf.sh | \
    scripts/durable-head-itf-commands.jq | \
    scripts/durable-head-itf-coverage.jq | \
    scripts/classify-durable-model-paths.sh | \
    scripts/validate-durable-benchmark-aliases.py | \
    scripts/fingerprint-durable-model-inputs.sh)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

is_receipt_only_input() {
  # These inputs only define an opt-in benchmark/acceptance receipt. They do
  # not affect the literate model, generated ITF corpus, or the runtime paths
  # replayed by the fast model-linked tier.
  case "$1" in
    bench/jdbc/chdb_durable_throughput.clj | \
    bench/jdbc/chdb_durable_throughput_metrics.clj | \
    scripts/benchmark-durable-matched-provider.sh | \
    scripts/run-durable-throughput-selector.sh | \
    test/jdbc/chdb_durable_throughput_test.clj | \
    test/jdbc/chdb_durable_throughput_metrics_test.clj)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

is_fast_input() {
  # The full tier always implies the deterministic/typecheck/ITF tier.
  if is_exhaustive_input "$1"; then
    return 0
  fi
  # Receipt-only paths are deliberately checked by their own benchmark gates.
  # Test this before the broad Durable test pattern below.
  if is_receipt_only_input "$1"; then
    return 1
  fi
  case "$1" in
    src/jdbc/chdb/durable.clj | src/jdbc/chdb/durable/* | \
    test/durable-*.sh | test/fixtures/durable/* | \
    test/jdbc/chdb_durable_*.clj | test/support/*)
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
  local fast=$3
  local fast_reason=$4
  local changed_count=$5
  printf 'exhaustive=%s\n' "$exhaustive"
  printf 'reason=%s\n' "$reason"
  printf 'fast=%s\n' "$fast"
  printf 'fast_reason=%s\n' "$fast_reason"
  printf 'changed_count=%s\n' "$changed_count"
}

fail_closed() {
  emit_decision true "diff-command-failed" true "fail-closed" 0
  exit 0
}

classify_paths() {
  local path
  local count=0
  local first_exhaustive=""
  local first_fast=""
  for path in "$@"; do
    count=$((count + 1))
    if [[ -z $first_exhaustive ]] && is_exhaustive_input "$path"; then
      first_exhaustive=$path
    fi
    if [[ -z $first_fast ]] && is_fast_input "$path"; then
      first_fast=$path
    fi
  done
  if [[ -n $first_exhaustive ]]; then
    # Shell escaping keeps unusual but valid Git paths on one safe output line.
    printf -v first_exhaustive '%q' "$first_exhaustive"
    # is_fast_input delegates exhaustive inputs, so this preserves the
    # exhaustive => fast invariant independently of caller order.
    emit_decision true "model-input:$first_exhaustive" true \
      "exhaustive-input:$first_exhaustive" "$count"
  elif [[ -n $first_fast ]]; then
    printf -v first_fast '%q' "$first_fast"
    emit_decision false "model-inputs-byte-identical" true \
      "model-linked-input:$first_fast" "$count"
  else
    emit_decision false "model-inputs-byte-identical" false \
      "receipt-or-non-model-input" "$count"
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
      emit_decision true "missing-or-zero-diff-boundary" true "fail-closed" 0
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
    # Refine only edits to the listed literate sources. Unknown
    # inputs, renames/deletions and checker/workflow changes stay conservative.
    literate_only=false
    saw_literate=false
    if (( ${#paths[@]} > 0 )); then
      literate_only=true
      for path in "${paths[@]}"; do
        case "$path" in
          formal/quint/durable-head-cas.md | \
          formal/quint/durable-writer-lifecycle.md | \
          formal/quint/native-process-lifecycle.md | \
          formal/quint/durable-file-wal-spool.md) saw_literate=true ;;
          formal/quint/buffered-publication.md) saw_literate=true ;;
          README.md | CHANGELOG.md) ;;
          *) literate_only=false ;;
        esac
      done
    fi
    if [[ "$literate_only" == true && "$saw_literate" == true ]]; then
      helper="$default_repo/scripts/fingerprint-durable-model-inputs.sh"
      if base_fingerprint=$(bash "$helper" "$repo_root" "$diff_base" 2>/dev/null) && \
         head_fingerprint=$(bash "$helper" "$repo_root" "$head" 2>/dev/null) && \
         [[ "$base_fingerprint" =~ ^[a-f0-9]{64}$ && "$head_fingerprint" =~ ^[a-f0-9]{64}$ ]]; then
        if [[ "$base_fingerprint" == "$head_fingerprint" ]]; then
          # A literate source remains model-linked even when its extracted
          # bytes are unchanged, so retain the fast tangle/typecheck/ITF tier.
          emit_decision false effective-model-inputs-identical true \
            literate-model-input "${#paths[@]}"
          exit 0
        fi
      fi
      emit_decision true effective-model-inputs-changed-or-unavailable true \
        literate-model-input "${#paths[@]}"
      exit 0
    fi
    # Only a committed, structurally validated additive bench/test alias edit
    # may remove deps.edn from the exhaustive inputs for this exact diff.
    if [[ " ${paths[*]} " == *" deps.edn "* ]] && \
       python3 "$default_repo/scripts/validate-durable-benchmark-aliases.py" \
         "$repo_root" "$diff_base" "$head"; then
      filtered_paths=()
      for path in "${paths[@]}"; do
        [[ $path == deps.edn ]] || filtered_paths+=("$path")
      done
      classify_paths "${filtered_paths[@]}"
    else
      classify_paths "${paths[@]}"
    fi
    ;;
  *)
    usage
    ;;
esac
