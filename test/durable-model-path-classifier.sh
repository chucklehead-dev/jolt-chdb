#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
classifier="$repo_root/scripts/classify-durable-model-paths.sh"
failures=0
fixture_root=$(mktemp -d)
cleanup() { rm -rf -- "$fixture_root"; }
trap cleanup EXIT HUP INT TERM
cd "$repo_root"

check_output() {
  local label=$1
  local expected=$2
  local repo=$3
  shift 3
  local actual
  actual=$(DURABLE_MODEL_CLASSIFIER_REPO="$repo" "$classifier" "$@")
  if grep -Fxq "exhaustive=$expected" <<<"$actual"; then
    echo "ok $label"
  else
    echo "FAIL $label: expected exhaustive=$expected" >&2
    echo "$actual" >&2
    failures=$((failures + 1))
  fi
}

check() {
  local label=$1
  local expected=$2
  shift 2
  check_output "$label" "$expected" "$repo_root" "$@"
}

check_reason() {
  local label=$1
  local expected=$2
  local repo=$3
  shift 3
  local actual
  actual=$(DURABLE_MODEL_CLASSIFIER_REPO="$repo" "$classifier" "$@")
  if grep -Fxq "reason=$expected" <<<"$actual"; then
    echo "ok $label"
  else
    echo "FAIL $label: expected reason=$expected" >&2
    echo "$actual" >&2
    failures=$((failures + 1))
  fi
}

for path in \
  formal/quint/durable-head-cas.md \
  formal/quint/native-process-lifecycle.md \
  formal/quint/future-model.unknown \
  formal/durable-head-cas.smt2 \
  formal/quint/traces/corrected-mbt.itf.json \
  formal/quint/traces/native-process-lifecycle.itf.json \
  formal/quint/traces/native-process-terminal.itf.json \
  formal/quint/traces/native-process-options.itf.json \
  scripts/check-durable-head-quint.sh \
  scripts/check-durable-head-itf-corpus.sh \
  scripts/generate-durable-head-itf.sh \
  scripts/generate-durable-engine-metadata-itf.sh \
  scripts/generate-native-process-lifecycle-itf.sh \
  scripts/durable-head-itf-commands.jq \
  scripts/durable-head-itf-coverage.jq \
  scripts/classify-durable-model-paths.sh \
  .github/workflows/durable-head-quint.yml
do
  check "model input $path" true --paths "$path"
done

for path in \
  src/jdbc/chdb/durable/control.clj \
  src/jdbc/chdb/durable.clj \
  test/jdbc/chdb_durable_epoch_seconds_test.clj \
  test/fixtures/durable/python-live-fractional-seconds.json \
  docs/durable-trace-validation.md \
  README.md \
  deps.edn
do
  check "fast-only path $path" false --paths "$path"
done

check "mixed paths choose exhaustive" true --paths \
  docs/durable.md formal/quint/durable-writer-lifecycle.md
check "empty exact diff stays fast-only" false --diff HEAD HEAD
check "zero before SHA fails conservative" true --diff \
  0000000000000000000000000000000000000000 HEAD
check_reason "missing commit reports checked Git failure" diff-command-failed \
  "$repo_root" --diff missing-model-base HEAD

# Red control: model-looking files outside the repository's formal boundary do
# not become exhaustive merely from their extension.
check "red control does not infer models from arbitrary docs" false --paths \
  docs/example-model.qnt

fixture_repo="$fixture_root/repo"
git init -q "$fixture_repo"
git -C "$fixture_repo" config user.name "model classifier test"
git -C "$fixture_repo" config user.email "model-classifier@example.invalid"
mkdir -p "$fixture_repo/src/jdbc/chdb/durable" \
  "$fixture_repo/formal/quint" "$fixture_repo/docs"
printf 'base\n' > "$fixture_repo/README.md"
git -C "$fixture_repo" add README.md
git -C "$fixture_repo" commit -q -m base
root_commit=$(git -C "$fixture_repo" rev-parse HEAD)

printf 'fast\n' > "$fixture_repo/src/jdbc/chdb/durable/control.clj"
git -C "$fixture_repo" add src/jdbc/chdb/durable/control.clj
git -C "$fixture_repo" commit -q -m fast
fast_commit=$(git -C "$fixture_repo" rev-parse HEAD)
check_output "real Git fast-only commit" false "$fixture_repo" \
  --diff "$root_commit" "$fast_commit"

printf 'model\n' > "$fixture_repo/formal/quint/future-model.unknown"
git -C "$fixture_repo" add formal/quint/future-model.unknown
git -C "$fixture_repo" commit -q -m model
model_commit=$(git -C "$fixture_repo" rev-parse HEAD)
check_output "real Git unknown formal/quint model input" true "$fixture_repo" \
  --diff "$fast_commit" "$model_commit"

git -C "$fixture_repo" rm -q formal/quint/future-model.unknown
git -C "$fixture_repo" commit -q -m delete-model
deleted_commit=$(git -C "$fixture_repo" rev-parse HEAD)
check_output "real Git model deletion" true "$fixture_repo" \
  --diff "$model_commit" "$deleted_commit"

mkdir -p "$fixture_repo/formal/quint"
printf 'model again\n' > "$fixture_repo/formal/quint/renamed-model.qnt"
git -C "$fixture_repo" add formal/quint/renamed-model.qnt
git -C "$fixture_repo" commit -q -m model-for-rename
rename_base=$(git -C "$fixture_repo" rev-parse HEAD)
git -C "$fixture_repo" mv formal/quint/renamed-model.qnt docs/renamed-model.qnt
git -C "$fixture_repo" commit -q -m rename-model-out
rename_head=$(git -C "$fixture_repo" rev-parse HEAD)
check_output "real Git rename out retains deleted model path" true "$fixture_repo" \
  --diff "$rename_base" "$rename_head"

git -C "$fixture_repo" switch -q -c base-side "$root_commit"
mkdir -p "$fixture_repo/formal/quint"
printf 'base model\n' > "$fixture_repo/formal/quint/base-side.qnt"
git -C "$fixture_repo" add formal/quint/base-side.qnt
git -C "$fixture_repo" commit -q -m base-side-model
base_side=$(git -C "$fixture_repo" rev-parse HEAD)
git -C "$fixture_repo" switch -q -c feature-side "$root_commit"
mkdir -p "$fixture_repo/src/jdbc/chdb/durable"
printf 'feature fast\n' > "$fixture_repo/src/jdbc/chdb/durable/reader.clj"
git -C "$fixture_repo" add src/jdbc/chdb/durable/reader.clj
git -C "$fixture_repo" commit -q -m feature-fast
feature_side=$(git -C "$fixture_repo" rev-parse HEAD)
check_output "merge-base ignores model changes unique to target branch" false \
  "$fixture_repo" --diff "$base_side" "$feature_side" --merge-base
check_output "direct divergent diff remains conservative" true "$fixture_repo" \
  --diff "$base_side" "$feature_side"

empty_tree=$(git -C "$fixture_repo" mktree < /dev/null)
unrelated=$(printf 'unrelated\n' | git -C "$fixture_repo" commit-tree "$empty_tree")
check_reason "missing merge base reports checked Git failure" diff-command-failed \
  "$fixture_repo" --diff "$root_commit" "$unrelated" --merge-base
check_reason "non-repository boundary reports checked Git failure" \
  diff-command-failed "$fixture_root/not-a-repo" \
  --diff "$root_commit" "$fast_commit"

# Full real source inventory, isolated Git history and pinned extraction only.
# No exported script, Quint evaluator or solver is executed from this fixture.
effective_repo="$fixture_root/effective"
mkdir -p "$effective_repo"
git archive HEAD formal scripts .github/workflows/durable-head-quint.yml | \
  tar -x -C "$effective_repo"
git -C "$effective_repo" init -q
git -C "$effective_repo" config user.name "model classifier test"
git -C "$effective_repo" config user.email "model-classifier@example.invalid"
git -C "$effective_repo" add .
git -C "$effective_repo" commit -q -m effective-base
effective_base=$(git -C "$effective_repo" rev-parse HEAD)
printf '\nProse-only extraction control.\n' >> "$effective_repo/formal/quint/durable-writer-lifecycle.md"
git -C "$effective_repo" add .
git -C "$effective_repo" commit -q -m prose-only
prose_head=$(git -C "$effective_repo" rev-parse HEAD)
check_reason "prose-only effective inputs skip exhaustive" \
  effective-model-inputs-identical "$effective_repo" --diff "$effective_base" "$prose_head"
check_output "prose-only decision is explicitly false" false "$effective_repo" \
  --diff "$effective_base" "$prose_head"
printf 'Neutral changelog entry.\n' > "$effective_repo/CHANGELOG.md"
git -C "$effective_repo" add .
git -C "$effective_repo" commit -q -m prose-plus-changelog
prose_changelog_head=$(git -C "$effective_repo" rev-parse HEAD)
check_reason "prose plus changelog keeps effective-input skip" \
  effective-model-inputs-identical "$effective_repo" --diff "$effective_base" "$prose_changelog_head"
sed -i 's/module durableHeadCasCorrected/module durableHeadCasChanged/' \
  "$effective_repo/formal/quint/durable-head-cas.md"
git -C "$effective_repo" add .
git -C "$effective_repo" commit -q -m changed-extracted-model
model_head=$(git -C "$effective_repo" rev-parse HEAD)
check_output "changed extracted model requests exhaustive" true "$effective_repo" \
  --diff "$prose_head" "$model_head"
printf '\n```quint target/formal/quint/unknown.qnt +=\nmodule unknown {}\n```\n' \
  >> "$effective_repo/formal/quint/durable-head-cas.md"
git -C "$effective_repo" add .
git -C "$effective_repo" commit -q -m unknown-extraction-output
unknown_head=$(git -C "$effective_repo" rev-parse HEAD)
check_output "unknown output fails closed before extraction" true "$effective_repo" \
  --diff "$model_head" "$unknown_head"
check "fingerprinter changes require exhaustive" true --paths \
  scripts/fingerprint-durable-model-inputs.sh
git -C "$effective_repo" checkout -q "$prose_head"
printf 'Only prose; no lifecycle code outputs.\n' > \
  "$effective_repo/formal/quint/durable-writer-lifecycle.md"
git -C "$effective_repo" add .
git -C "$effective_repo" commit -q -m missing-extracted-inventory
missing_output_head=$(git -C "$effective_repo" rev-parse HEAD)
check_output "missing extracted output selects exhaustive" true "$effective_repo" \
  --diff "$prose_head" "$missing_output_head"
git -C "$effective_repo" checkout -q "$prose_head"
git -C "$effective_repo" rm -q formal/quint/durable-writer-lifecycle.md
git -C "$effective_repo" commit -q -m missing-literate-source
missing_source_head=$(git -C "$effective_repo" rev-parse HEAD)
check_output "missing literate source selects exhaustive" true "$effective_repo" \
  --diff "$prose_head" "$missing_source_head"
git -C "$effective_repo" checkout -q "$prose_head"
sed -i 's/--max-steps 6/--max-steps 5/g' \
  "$effective_repo/scripts/check-durable-head-quint.sh"
git -C "$effective_repo" add .
git -C "$effective_repo" commit -q -m changed-checker-bound
checker_head=$(git -C "$effective_repo" rev-parse HEAD)
check_output "changed checker bound selects exhaustive" true "$effective_repo" \
  --diff "$prose_head" "$checker_head"
git -C "$effective_repo" checkout -q "$prose_head"
printf '\ncorpus mutation\n' >> "$effective_repo/formal/quint/traces/corrected-mbt.itf.json"
git -C "$effective_repo" add .
git -C "$effective_repo" commit -q -m changed-corpus
corpus_head=$(git -C "$effective_repo" rev-parse HEAD)
check_output "changed checked corpus selects exhaustive" true "$effective_repo" \
  --diff "$prose_head" "$corpus_head"
bad_tools="$fixture_root/bad-tools"
mkdir "$bad_tools"
printf '#!/bin/sh\nprintf "tool-version-mismatch\\n"\n' > "$bad_tools/go"
chmod 755 "$bad_tools/go"
PATH="$bad_tools:$PATH" check_output "tangler metadata mismatch selects exhaustive" \
  true "$effective_repo" --diff "$effective_base" "$prose_head"
# Fault stub reaches the extraction boundary after a synthetic metadata pass.
# It is not presented as a real pinned extractor or successful extraction.
failed_extractor="$fixture_root/failed-extractor"
mkdir "$failed_extractor"
printf '#!/bin/sh\nprintf "\\tmod github.com/driusan/lmt v0.0.0-20210421124901-62fe18f2f6a6 fixture\\n"\n' > "$failed_extractor/go"
printf '#!/bin/sh\nexit 17\n' > "$failed_extractor/lmt"
chmod 755 "$failed_extractor/go" "$failed_extractor/lmt"
PATH="$failed_extractor:$PATH" check_output "extractor command failure selects exhaustive" \
  true "$effective_repo" --diff "$effective_base" "$prose_head"

if (( failures > 0 )); then
  echo "$failures classifier checks failed" >&2
  exit 1
fi
