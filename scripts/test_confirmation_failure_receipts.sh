#!/usr/bin/env bash
# Exercise the actual runner function without invoking native code or AWS.
set -euo pipefail
task_repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
task_test=$(mktemp -d /tmp/confirmation-failure-control.XXXXXXXX)
cleanup() {
  if [[ "$task_test" =~ ^/tmp/confirmation-failure-control\.[A-Za-z0-9]+$ ]]; then
    rm -rf -- "$task_test"
  fi
}
trap cleanup EXIT
# Only the function from the trusted runner; do not execute its cloud workflow.
source <(sed -n '/^run_phase() {/,/^}/p' "$task_repo/scripts/run_wal_confirmation_ab.sh")
declare -F run_phase >/dev/null
export -f run_phase
task_storage=s3 task_arm=baseline
task_driver="$task_repo/scripts/fixtures/confirmation/failing-driver.sh"
for task_case in error timeout; do
  task_root="$task_test/$task_case/work"
  task_output="$task_test/$task_case/receipts"
  mkdir -p "$task_root" "$task_output"
  if [[ "$task_case" == timeout ]]; then
    export TEST_FAILURE_TIMEOUT=true
    task_expected=124
  else
    export TEST_FAILURE_TIMEOUT=false
    task_expected=7
  fi
  export task_storage task_arm task_driver task_root task_output
  task_actual=0
  # A fresh shell preserves the production bare-call/errexit behavior; calling
  # the function under || here would disable errexit inside the function.
  bash -c 'set -euo pipefail; run_phase window 1' || task_actual=$?
  test "$task_actual" = "$task_expected"
  grep -Fq ":exit-code $task_expected}" "$task_output/process-failure.edn"
  if [[ "$task_case" == error ]]; then
    test -f "$task_output/driver-failure.edn"
  else
    test ! -e "$task_output/driver-failure.edn"
  fi
  if grep -Rq 'PRIVATE-CANARY-DO-NOT-RETAIN' "$task_output"; then exit 1; fi
  python3 "$task_repo/scripts/check_typed_receipt_privacy.py" "$task_output"
done
# Failed diagnostic writes/copies must not replace the primary exit7.
export TEST_FAILURE_TIMEOUT=false
task_output="$task_test/not-directory"
touch "$task_output"
task_actual=0
bash -c 'set -euo pipefail; run_phase window 1' 2> "$task_root/diagnostic-error.log" || task_actual=$?
test "$task_actual" = 7
echo 'process failure/timeout and diagnostic-write failure preserve primary status without raw logs'
