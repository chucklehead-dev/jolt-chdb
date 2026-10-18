#!/usr/bin/env bash
# Synthetic process-failure control, not a native/cloud workload.
set -euo pipefail
if [[ -n "${TEST_FAILURE_EXPECT_SAMPLES:-}" ]]; then
  test "$4" = "$TEST_FAILURE_EXPECT_SAMPLES"
fi
printf '%s\n' 'PRIVATE-CANARY-DO-NOT-RETAIN' >&2
if [[ "${TEST_FAILURE_EXPECT_SAMPLES:-}" == 100 ]]; then
  printf '%s\n' '{:scope :typed-batch-export-progress :diagnostic-only true :expected-exports 104 :counts {:started 9 :returned 8 :confirmed 8} :observation :export-started :elapsed-ms 1}' > "$2/export-progress.edn"
fi
if [[ "${TEST_FAILURE_TIMEOUT:-false}" == true ]]; then sleep 5; fi
printf '%s\n' '{:scope :wal-confirmation-failure :scope-returned false}' > "$2/driver-failure.edn"
exit 7
