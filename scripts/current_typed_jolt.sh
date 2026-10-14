#!/usr/bin/env bash
# Invoke only the authenticated runtime. Keep argv intact except the exact S3
# driver path, which is replaced by the reviewed request-budget wrapper.
set -euo pipefail
test "$1" = "${BENCH_JOLT_BIN:?}"
test "$(sha256sum "$1" | cut -d ' ' -f1)" = "${QUALIFIED_RUNTIME_BINARY_SHA256:?}"
task_args=()
task_replacements=0
for task_arg in "$@"; do
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
  fi
fi
exec "${task_args[@]}"
