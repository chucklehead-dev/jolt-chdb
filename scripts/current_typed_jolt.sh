#!/usr/bin/env bash
# Invoke only the authenticated runtime. For S3, replace the exact driver path
# and add the trusted script root; retain all other paths, dependencies and argv.
set -euo pipefail
test "$1" = "${BENCH_JOLT_BIN:?}"
test "$(sha256sum "$1" | cut -d ' ' -f1)" = "${QUALIFIED_RUNTIME_BINARY_SHA256:?}"
task_args=()
task_replacements=0
task_deps_replacements=0
task_expect_deps=false
task_scripts=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
case "$task_scripts" in *'"'*|*'\'*|*$'\n'*|*$'\r'*) exit 64 ;; esac
for task_arg in "$@"; do
  if [[ "${SDK_STORAGE_ARM:-}" == s3 && "$task_expect_deps" == true ]]; then
    # The exact immutable launcher supplies this closed EDN prefix. Add only
    # the trusted workflow script root; retain its paths/dependency graph.
    task_prefix='{:paths ['
    [[ "$task_arg" == "$task_prefix"* ]] || { echo unexpected-storage-deps-shape >&2; exit 64; }
    task_arg="${task_prefix}\"$task_scripts\" ${task_arg#"$task_prefix"}"
    task_deps_replacements=$((task_deps_replacements + 1))
    task_expect_deps=false
  elif [[ "$task_arg" == -Sdeps ]]; then
    task_expect_deps=true
  fi
  if [[ "${SDK_STORAGE_ARM:-}" == s3 && "$task_arg" == "${TASK_ORIGINAL_TYPED_DRIVER:?}" ]]; then
    task_arg=${TASK_TYPED_BUDGET_DRIVER:?}
    task_replacements=$((task_replacements + 1))
  fi
  task_args+=("$task_arg")
done
if [[ "${SDK_STORAGE_ARM:-}" == s3 ]]; then
  # The immutable launcher probes --version first. Every other S3 invocation
  # must route exactly one driver; reject path drift before any cloud effects.
  if [[ "$#" != 2 || "$2" != --version ]]; then
    test "$task_replacements" -eq 1 || { echo unmetered-s3-driver-rejected >&2; exit 64; }
    test "$task_deps_replacements" -eq 1 || { echo unrooted-s3-driver-rejected >&2; exit 64; }
  fi
fi
exec "${task_args[@]}"
