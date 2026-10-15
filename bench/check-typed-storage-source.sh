#!/usr/bin/env bash
# Closed source profiles; no caller-selected ref or unreviewed production diff.
set -euo pipefail
test "$#" -eq 1
task_repo=$1
case "${QUALIFIED_STORAGE_SOURCE_PROFILE:-baseline}" in
  baseline)
    test -z "${SDK_S3_EXPECT_WAIT_MS:-}"
    task_source=291e1f43a859428f737211c0e9853e1616b54097 ;;
  expect-continue-ab)
    test "${SDK_STORAGE_ARM:-}" = s3
    case "${SDK_S3_EXPECT_WAIT_MS:-}" in 1000|10) ;; *) exit 64 ;; esac
    task_source=a9ffed1e8887f149f417e0f2f1e87aef62394864 ;;
  *) exit 64 ;;
esac
git -C "$task_repo" merge-base --is-ancestor "$task_source" HEAD
git -C "$task_repo" diff --quiet "$task_source" -- src resources deps.edn
