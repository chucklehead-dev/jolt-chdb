#!/usr/bin/env bash
# Invoke only the authenticated runtime. Keep argv intact except the exact S3
# driver path, which is replaced by the reviewed request-budget wrapper.
set -euo pipefail
test "$1" = "${BENCH_JOLT_BIN:?}"
test "$(sha256sum "$1" | cut -d ' ' -f1)" = "${QUALIFIED_RUNTIME_BINARY_SHA256:?}"
task_args=()
for task_arg in "$@"; do
  if [[ "${SDK_STORAGE_ARM:-}" == s3 && "$task_arg" == "${TASK_ORIGINAL_TYPED_DRIVER:?}" ]]; then
    task_arg=${TASK_TYPED_BUDGET_DRIVER:?}
  fi
  task_args+=("$task_arg")
done
exec "${task_args[@]}"
