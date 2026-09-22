#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
classifier="$repo_root/scripts/classify-durable-model-paths.sh"
check_trigger_closure() {
  # Source-only: no YAML dependency, tangler, runtime or solver is invoked.
  python3 - "$repo_root" <<'PY'
import fnmatch
import pathlib
import re
import subprocess
import sys

root = pathlib.Path(sys.argv[1])
workflow = (root / ".github/workflows/durable-head-quint.yml").read_text()
classifier = (root / "scripts/classify-durable-model-paths.sh").read_text()
fingerprinter = (root / "scripts/fingerprint-durable-model-inputs.sh").read_text()
def classifier_helpers(source):
    definitions = list(re.finditer(r"^is_exhaustive_input\(\)\s*\{", source, re.M))
    functions = list(re.finditer(r"^is_exhaustive_input\(\)\s*\{\n(.*?)^\}\s*$",
                                source, re.M | re.S))
    assert len(definitions) == len(functions) == 1, "missing/duplicate model-input function"
    function = functions[0]
    body = function[1]
    inventory = re.findall(r"scripts/[A-Za-z0-9_./-]+\.(?:sh|jq)(?=\s|\||\)|$)", body)
    assert inventory, "empty classifier model-helper input inventory"
    assert body.count("scripts/") == len(inventory), "unknown model-helper inventory syntax"
    return set(inventory), function

classifier_inventory, function = classifier_helpers(classifier)
fast_definitions = list(re.finditer(r"^is_fast_input\(\)\s*\{", classifier, re.M))
assert len(fast_definitions) == 1, "missing/duplicate fast-model classifier"
assert "is_exhaustive_input \"$1\"" in classifier, \
    "fast classifier must include every exhaustive input"
assert "is_receipt_only_input \"$1\"" in classifier, \
    "fast classifier must exclude receipt-only inputs before broad test paths"
assert "fast: ${{ steps.decision.outputs.fast }}" in workflow, \
    "workflow does not publish fast decision"
assert "outputs.fast != 'true'" in workflow, \
    "fast job lacks an explicit successful stub"
assert "test \"$decision\" != true || test \"$fast\" = true" in workflow, \
    "workflow does not enforce exhaustive implies fast"
fast_job = workflow.split("  fast-model-linked:", 1)[1].split("\n  literate-model:", 1)[0]
for step in ("actions/checkout@v4", "actions/setup-node@v4", "actions/setup-go@v6",
             "Install pinned Quint and literate tangler",
             "Install the pinned Jolt aspect compiler",
             "Replay one deterministic Quint ITF corpus through memory and S3",
             "Tangle, typecheck, test, sample, and replay generated ITF"):
    start = fast_job.index(step)
    end = fast_job.find("\n      - ", start)
    if end == -1:
        end = len(fast_job)
    assert "outputs.fast == 'true'" in fast_job[start:end], \
        f"fast=false stub can execute tool step: {step}"
fingerprint_inventory = set(re.findall(r'"(scripts/[A-Za-z0-9_./-]+\.(?:sh|jq))"',
                                      fingerprinter))
assert fingerprint_inventory, "empty fingerprint model-helper inventory"
helpers = classifier_inventory | fingerprint_inventory
# Reorder the actual function after every other definition without changing it.
reordered = classifier[:function.start()] + classifier[function.end():] + "\n" + function[0]
assert classifier_helpers(reordered)[0] == classifier_inventory, "function order changes inventory"
print("ok classifier definition reorder preserves exhaustive inventory")
for label, source in (("missing", classifier[:function.start()] + classifier[function.end():]),
                      ("empty", "is_exhaustive_input() {\n  return 1\n}\n")):
    try:
        classifier_helpers(source)
    except AssertionError:
        print(f"ok {label} classifier inventory rejected independently of fingerprinter")
    else:
        raise AssertionError(f"{label} classifier inventory accepted")

def triggers(source):
    result = {}
    event = None
    paths = False
    for line in source.splitlines():
        match = re.fullmatch(r"  (pull_request|push):", line)
        if match:
            event = match[1]
            result[event] = []
            paths = False
        elif re.match(r"  [a-z_]+:", line):
            event = None
        elif event and line == "    paths:":
            paths = True
        elif paths and line.startswith("      - "):
            result[event].append(line[8:])
        elif paths and line and not line.startswith("      "):
            paths = False
    assert set(result) == {"pull_request", "push"}, "missing automatic event"
    assert all(result.values()), "empty automatic path inventory"
    return result

def missing(paths, inputs):
    return sorted(path for path in inputs
                  if not any(fnmatch.fnmatchcase(path, pattern) for pattern in paths))

paths = triggers(workflow)
sentinel = "scripts/source_only_future_helper.sh"
assert sentinel not in fingerprint_inventory, "sentinel overlaps fingerprint inventory"
clause = "    scripts/check-durable-head-quint.sh | \\\n"
assert function[1].count(clause) == 1, "missing/duplicate sentinel case-pattern anchor"
sentinel_body = function[1].replace(clause, "    " + sentinel + " | \\\n" + clause, 1)
sentinel_source = classifier[:function.start(1)] + sentinel_body + classifier[function.end(1):]
subprocess.run(["bash", "-n"], input=sentinel_source, text=True,
               capture_output=True, timeout=5, check=True)
print("ok classifier-only sentinel is a valid future case-pattern mutation")
sentinel_inventory = classifier_helpers(sentinel_source)[0]
assert sentinel in sentinel_inventory, "classifier-only sentinel not extracted"
fast = {"deps.edn", "src/jdbc/chdb/durable.clj",
        "src/jdbc/chdb/durable/writer.clj", "test/support/durable_fixture.clj",
        "test/fixtures/durable/python-live-fractional-seconds.json",
        "test/jdbc/chdb_durable_throughput_test.clj",
        ".github/actions/install-jolt-aspects/action.yml"}
receipt_only = {
    "bench/jdbc/chdb_durable_throughput.clj",
    "bench/jdbc/chdb_durable_throughput_metrics.clj",
    "scripts/benchmark-durable-matched-provider.sh",
    "scripts/run-durable-throughput-selector.sh",
    "test/jdbc/chdb_durable_throughput_test.clj",
    "test/jdbc/chdb_durable_throughput_metrics_test.clj",
}
formal_smt = {"formal/durable-head-cas.smt2", "formal/nested/future-model.smt2"}
for event, patterns in paths.items():
    absent = missing(patterns, helpers | fast)
    assert not absent, f"{event} trigger misses declared inputs: {absent}"
    print(f"ok {event} declared model helpers and known fast paths trigger")
    absent_receipts = missing(patterns, receipt_only)
    assert not absent_receipts, f"{event} trigger misses receipt-only stub paths: {absent_receipts}"
    print(f"ok {event} all receipt-only paths trigger the explicit success stub")
    absent_smt = missing(patterns, formal_smt)
    assert not absent_smt, f"{event} trigger misses classifier SMT inputs: {absent_smt}"
    print(f"ok {event} classifier SMT inputs trigger exhaustive coverage")
    assert missing(patterns, sentinel_inventory | fingerprint_inventory) == [sentinel], \
        f"{event} classifier-only helper missing trigger is hidden"
    print(f"ok {event} classifier-only underscore helper missing trigger is rejected")
    # Causal test: independently remove the previously omitted helper from
    # each event. Component-only classifier tests cannot catch this drift.
    helper = "scripts/generate-native-process-lifecycle-itf.sh"
    mutant = [pattern for pattern in patterns if pattern != helper]
    assert missing(mutant, {helper}) == [helper], f"{event} removal is hidden"
    print(f"ok {event} missing lifecycle helper trigger is rejected")
PY
}
if [[ ${1:-} == --trigger-closure-only && $# == 1 ]]; then
  check_trigger_closure
  exit
fi
[[ $# == 0 ]] || { echo "usage: $0 [--trigger-closure-only]" >&2; exit 2; }
check_trigger_closure
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

check_fast() {
  local label=$1
  local expected=$2
  local repo=$3
  shift 3
  local actual
  actual=$(DURABLE_MODEL_CLASSIFIER_REPO="$repo" "$classifier" "$@")
  if grep -Fxq "fast=$expected" <<<"$actual"; then
    echo "ok $label"
  else
    echo "FAIL $label: expected fast=$expected" >&2
    echo "$actual" >&2
    failures=$((failures + 1))
  fi
}

for path in \
  .github/actions/install-jolt-aspects/action.yml \
  formal/quint/durable-head-cas.md \
  formal/quint/native-process-lifecycle.md \
  formal/quint/durable-persistence-observation.md \
  formal/quint/future-model.unknown \
  formal/durable-head-cas.smt2 \
  formal/nested/future-model.smt2 \
  formal/quint/traces/corrected-mbt.itf.json \
  formal/quint/traces/native-process-lifecycle.itf.json \
  formal/quint/traces/native-process-terminal.itf.json \
  formal/quint/traces/native-process-options.itf.json \
  deps.edn \
  scripts/check-durable-head-quint.sh \
  scripts/check-durable-file-wal-spool-quint.sh \
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
  check_fast "model input is fast $path" true "$repo_root" --paths "$path"
done

for path in \
  src/jdbc/chdb/durable/control.clj \
  src/jdbc/chdb/durable.clj \
  test/jdbc/chdb_durable_epoch_seconds_test.clj \
  test/fixtures/durable/python-live-fractional-seconds.json
do
  check "fast-only path $path" false --paths "$path"
  check_fast "fast-only path is model-linked $path" true "$repo_root" --paths "$path"
done

for path in \
  bench/jdbc/chdb_durable_throughput.clj \
  bench/jdbc/chdb_durable_throughput_metrics.clj \
  scripts/benchmark-durable-matched-provider.sh \
  scripts/run-durable-throughput-selector.sh \
  test/jdbc/chdb_durable_throughput_test.clj \
  test/jdbc/chdb_durable_throughput_metrics_test.clj
do
  check "receipt-only path $path" false --paths "$path"
  check_fast "receipt-only path skips model fast tier $path" false "$repo_root" --paths "$path"
done

for path in docs/durable-trace-validation.md README.md; do
  check "non-model path $path" false --paths "$path"
  check_fast "non-model path skips model fast tier $path" false "$repo_root" --paths "$path"
done

check "mixed paths choose exhaustive" true --paths \
  docs/durable.md formal/quint/durable-writer-lifecycle.md
check_fast "mixed paths retain fast tier" true "$repo_root" --paths \
  docs/durable.md formal/quint/durable-writer-lifecycle.md
check "empty exact diff stays fast-only" false --diff HEAD HEAD
check_fast "empty exact diff skips fast tier" false "$repo_root" --diff HEAD HEAD
check "zero before SHA fails conservative" true --diff \
  0000000000000000000000000000000000000000 HEAD
check_fast "zero before SHA retains fast tier" true "$repo_root" --diff \
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
check_fast "real Git runtime commit keeps fast tier" true "$fixture_repo" \
  --diff "$root_commit" "$fast_commit"

mkdir -p "$fixture_repo/test/jdbc"
printf 'receipt only\n' > "$fixture_repo/test/jdbc/chdb_durable_throughput_test.clj"
git -C "$fixture_repo" add test/jdbc/chdb_durable_throughput_test.clj
git -C "$fixture_repo" commit -q -m receipt-only
receipt_commit=$(git -C "$fixture_repo" rev-parse HEAD)
check_output "real Git receipt-only commit skips exhaustive" false "$fixture_repo" \
  --diff "$fast_commit" "$receipt_commit"
check_fast "real Git receipt-only commit skips fast tier" false "$fixture_repo" \
  --diff "$fast_commit" "$receipt_commit"
printf 'runtime again\n' > "$fixture_repo/src/jdbc/chdb/durable/reader.clj"
git -C "$fixture_repo" add src/jdbc/chdb/durable/reader.clj
git -C "$fixture_repo" commit -q -m receipt-plus-runtime
receipt_runtime_commit=$(git -C "$fixture_repo" rev-parse HEAD)
check_fast "receipt plus runtime commit retains fast tier" true "$fixture_repo" \
  --diff "$receipt_commit" "$receipt_runtime_commit"

printf 'model\n' > "$fixture_repo/formal/quint/future-model.unknown"
git -C "$fixture_repo" add formal/quint/future-model.unknown
git -C "$fixture_repo" commit -q -m model
model_commit=$(git -C "$fixture_repo" rev-parse HEAD)
check_output "real Git unknown formal/quint model input" true "$fixture_repo" \
  --diff "$fast_commit" "$model_commit"
check_fast "real Git unknown model input keeps fast tier" true "$fixture_repo" \
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
# `git archive HEAD` intentionally excludes this branch's uncommitted new
# literate source while this self-test is run before the implementation commit.
# Copy it explicitly so the isolated effective-input fixture represents the
# same declared inventory the fingerprinter validates.
cp "$repo_root/formal/quint/durable-file-wal-spool.md" \
  "$effective_repo/formal/quint/durable-file-wal-spool.md"
cp "$repo_root/scripts/check-durable-file-wal-spool-quint.sh" \
  "$effective_repo/scripts/check-durable-file-wal-spool-quint.sh"
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
check_fast "prose-only literate edit retains fast tier" true "$effective_repo" \
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
check_fast "changed extracted model retains fast tier" true "$effective_repo" \
  --diff "$prose_head" "$model_head"
git -C "$effective_repo" checkout -q "$prose_head"
sed -i 's/module durablePersistenceObservation/module durablePersistenceObservationChanged/' \
  "$effective_repo/formal/quint/durable-persistence-observation.md"
git -C "$effective_repo" add .
git -C "$effective_repo" commit -q -m changed-persistence-observation-model
persistence_observation_head=$(git -C "$effective_repo" rev-parse HEAD)
check_output "changed persistence observation model requests exhaustive" true "$effective_repo" \
  --diff "$prose_head" "$persistence_observation_head"
check_fast "changed persistence observation model retains fast tier" true "$effective_repo" \
  --diff "$prose_head" "$persistence_observation_head"
git -C "$effective_repo" checkout -q "$prose_head"
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
