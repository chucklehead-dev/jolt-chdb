#!/usr/bin/env bash
# Synthetic process-failure control, not a native/cloud workload.
set -euo pipefail
printf '%s\n' 'PRIVATE-CANARY-DO-NOT-RETAIN' >&2
if [[ "${TEST_FAILURE_TIMEOUT:-false}" == true ]]; then sleep 5; fi
printf '%s\n' '{:scope :wal-confirmation-failure :scope-returned false}' > "$2/driver-failure.edn"
exit 7
