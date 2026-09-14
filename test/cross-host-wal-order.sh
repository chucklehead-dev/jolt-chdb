#!/usr/bin/env bash
set -euo pipefail

script=$(cd "$(dirname "$0")/.." && pwd -P)/scripts/benchmark-cross-host-wal.sh

BENCH_VALIDATE_ORDER_ONLY=1 "$script" >/dev/null
BENCH_VALIDATE_JVM_STATUS_ONLY=1 "$script" >/dev/null
BENCH_VALIDATE_CHECKPOINT_GUARD_ONLY=1 "$script" >/dev/null
BENCH_VALIDATE_BB_LAUNCH_ONLY=1 "$script" >/dev/null
BENCH_VALIDATE_ORDER_ONLY=1 \
  BENCH_MATRIX_ORDER="jvm-cheshire jvm-upstream jvm-casselc babashka jolt" \
  "$script" >/dev/null

for invalid in \
  "jolt babashka jvm-casselc jvm-upstream" \
  "jolt babashka jvm-casselc jvm-upstream jvm-upstream" \
  "jolt babashka jvm-casselc jvm-upstream unknown"; do
  if BENCH_VALIDATE_ORDER_ONLY=1 BENCH_MATRIX_ORDER="$invalid" \
       "$script" >/dev/null 2>&1; then
    echo "invalid matrix order was accepted: $invalid" >&2
    exit 1
  fi
done

echo "cross-host WAL matrix-order contract passed"
