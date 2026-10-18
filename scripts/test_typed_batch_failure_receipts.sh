#!/usr/bin/env bash
# Exercise the actual workflow function with a synthetic non-network process.
set -euo pipefail
task_repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
task_test=$(mktemp -d /tmp/typed-batch-failure-control.XXXXXXXX)
cleanup() {
  if [[ "$task_test" =~ ^/tmp/typed-batch-failure-control\.[A-Za-z0-9]+$ ]]; then
    rm -rf -- "$task_test"
  fi
}
trap cleanup EXIT
source <(sed -n '/^              run_phase() {/,/^              }/p' \
  "$task_repo/.github/workflows/typed-batch-research.yml")
declare -F run_phase >/dev/null
export -f run_phase
driver="$task_repo/scripts/fixtures/confirmation/failing-driver.sh"
task_size=10000
for task_case in error timeout; do
  task_root="$task_test/$task_case/work"
  task_report="$task_test/$task_case/receipts"
  mkdir -p "$task_root" "$task_report"
  if [[ "$task_case" == timeout ]]; then
    export TEST_FAILURE_TIMEOUT=true
    task_expected=124
  else
    export TEST_FAILURE_TIMEOUT=false
    task_expected=7
  fi
  export driver task_size task_root task_report
  task_actual=0
  bash -c 'set -euo pipefail; run_phase window 1' || task_actual=$?
  test "$task_actual" = "$task_expected"
  grep -Fq ":exit-code $task_expected}" "$task_report/process-failure.edn"
  if [[ "$task_case" == error ]]; then
    test -f "$task_report/driver-failure.edn"
  else
    test ! -e "$task_report/driver-failure.edn"
  fi
  if grep -Rq 'PRIVATE-CANARY-DO-NOT-RETAIN' "$task_report"; then exit 1; fi
  python3 "$task_repo/scripts/check_typed_receipt_privacy.py" "$task_report"
done
export TEST_FAILURE_TIMEOUT=false
task_report="$task_test/not-directory"
touch "$task_report"
task_actual=0
bash -c 'set -euo pipefail; run_phase window 1' 2> "$task_root/diagnostic-error.log" || task_actual=$?
test "$task_actual" = 7
echo 'batch failure/timeout receipts preserve primary status without retaining raw logs'
