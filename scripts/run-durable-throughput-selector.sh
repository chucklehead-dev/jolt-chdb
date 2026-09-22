#!/usr/bin/env bash
set -euo pipefail

# Run exactly one local Durable throughput selector in its own process and
# retain the three artifacts needed to interpret that measurement.  This is
# deliberately a local/manual tool: remote S3 runs use the redacted CI path.

selector=${1:?usage: run-durable-throughput-selector.sh SELECTOR OUTPUT-DIR}
output_dir=${2:?usage: run-durable-throughput-selector.sh SELECTOR OUTPUT-DIR}

case "$selector" in
  scale-512|scale-1000|scale-5000|scale-10000|stage-512|stage-smoke|recovery-512-10|recovery-512-25|recovery-512-50)
    ;;
  *)
    echo "unsupported isolated Durable selector" >&2
    exit 2
    ;;
esac

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

# This selector is also used as the workload beneath the opt-in forensic
# launcher. Keep the normal path behavior unchanged: lifecycle markers are
# emitted only when a caller supplies a fresh, private directory. They are
# launch diagnostics, not benchmark evidence.
forensics_dir=${DURABLE_SELECTOR_FORENSICS_DIR:-}
forensics_events=
forensics_enabled=false
timed_pid=
if [[ -n "$forensics_dir" ]]; then
  [[ "$forensics_dir" = /* && -d "$forensics_dir" && ! -L "$forensics_dir" ]] || {
    echo "forensics directory must be an existing absolute non-symlink directory" >&2
    exit 2
  }
  forensics_events="$forensics_dir/selector.events"
  [[ ! -e "$forensics_events" ]] || {
    echo "forensics event file must not already exist" >&2
    exit 2
  }
  : > "$forensics_events"
  forensics_enabled=true
  forensic_event() {
    # No environment or command arguments are recorded: those can contain
    # provider credentials. The event file only identifies lifecycle phase,
    # PID, and integer exit/signal state.
    printf '%s pid=%s phase=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$$" "$1" \
      >> "$forensics_events"
  }
  forensic_exit() {
    local code=$?
    trap - EXIT
    forensic_event "selector-exit status=$code"
    exit "$code"
  }
  forensic_signal() {
    local signal=$1
    forensic_event "selector-signal signal=$signal"
    # The diagnostic child is a separate session. Forward a catchable signal
    # to its whole group, then reap its session leader before recording the
    # selector's terminal signal status. The ordinary selector path never
    # backgrounds a process and does not use this machinery.
    if [[ -n "$timed_pid" ]]; then
      forensic_event "time-child-signal-forwarded signal=$signal pid=$timed_pid"
      kill -"$signal" -- "-$timed_pid" 2>/dev/null || true
      local child_status
      if wait "$timed_pid"; then
        child_status=0
      else
        child_status=$?
      fi
      forensic_event "time-child-reaped status=$child_status"
    fi
    exit $((128 + signal))
  }
  trap forensic_exit EXIT
  trap 'forensic_signal 1' HUP
  trap 'forensic_signal 2' INT
  trap 'forensic_signal 15' TERM
  forensic_event "selector-started"
fi

for required in JOLT_WRAPPER BENCH_JOLT_BIN BENCH_JOLT_SOURCE_SHA JOLT_CHDB_LIB; do
  if [[ -z ${!required:-} ]]; then
    echo "missing required benchmark provenance" >&2
    exit 2
  fi
done

[[ -x "$JOLT_WRAPPER" && -x "$BENCH_JOLT_BIN" && -f "$JOLT_CHDB_LIB" ]] || {
  echo "benchmark executable or native library is unavailable" >&2
  exit 2
}
[[ -x /usr/bin/time ]] || {
  echo "GNU /usr/bin/time is required for peak RSS evidence" >&2
  exit 2
}
[[ ! -e "$output_dir" ]] || {
  echo "output directory must not already exist" >&2
  exit 2
}

# A receipt's `BENCH_GIT_STATUS=clean` covers all nonignored worktree state,
# not only tracked blobs. `git diff` omits untracked files, which could
# otherwise let a locally added workload/helper be exercised while the receipt
# still says clean. Ignored generated/cache/output paths are deliberately
# outside this Git-status claim; the launcher separately requires explicit
# executable and native-library provenance inputs.
[[ -z $(git -C "$repo_root" status --porcelain --untracked-files=all) ]] || {
  echo "benchmark checkout must be clean" >&2
  exit 2
}

mkdir -p "$(dirname "$output_dir")"
mkdir "$output_dir"

report="$output_dir/report.edn"
log="$output_dir/run.log"
timing="$output_dir/time-v.txt"
receipt_root="$output_dir/receipts"
mkdir "$receipt_root"
receipt_root=$(cd "$receipt_root" && pwd -P)
export BENCH_PERSISTENT_RECEIPT_ROOT="$receipt_root"

export BENCH_JOLT_VERSION
BENCH_JOLT_VERSION=$("$JOLT_WRAPPER" "$BENCH_JOLT_BIN" --version)
export BENCH_GIT_HEAD BENCH_GIT_PARENT BENCH_GIT_TREE BENCH_GIT_STATUS BENCH_STARTED_AT
BENCH_GIT_HEAD=$(git -C "$repo_root" rev-parse HEAD)
BENCH_GIT_PARENT=$(git -C "$repo_root" rev-parse HEAD^)
BENCH_GIT_TREE=$(git -C "$repo_root" rev-parse HEAD^{tree})
BENCH_GIT_STATUS=clean
BENCH_STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

cd "$repo_root"
set +e
if "$forensics_enabled"; then
  command -v setsid >/dev/null 2>&1 || {
    echo "setsid is required for forensic signal forwarding" >&2
    exit 2
  }
  forensic_event "jolt-command-about-to-start"
  LC_ALL=C setsid /usr/bin/time -v -o "$timing" \
    "$JOLT_WRAPPER" "$BENCH_JOLT_BIN" -M:durable-throughput "$selector" "$report" \
    >"$log" 2>&1 &
  timed_pid=$!
  forensic_event "time-child-session-started pid=$timed_pid"
  wait "$timed_pid"
  run_status=$?
  forensic_event "time-child-returned status=$run_status"
else
  LC_ALL=C /usr/bin/time -v -o "$timing" \
    "$JOLT_WRAPPER" "$BENCH_JOLT_BIN" -M:durable-throughput "$selector" "$report" \
    >"$log" 2>&1
  run_status=$?
fi
set -e

if "$forensics_enabled"; then
  forensic_event "artifact-validation-about-to-start"
fi
scripts/check-durable-throughput-artifacts.sh "$report" "$log" "$timing"
# The 512 selector is an acceptance gate.  Check the persisted structured
# receipt before propagating Jolt's nonzero status, so a miss never looks like
# an unqualified launcher failure and the timing/RSS receipt remains intact.
if [[ "$selector" == scale-512 ]]; then
  grep -Eq ':encoding-inclusive-512-acceptance[[:space:]]+\{:status :passed' "$report" || {
    echo "Durable encoding-inclusive 512 acceptance receipt is not passing" >&2
    exit 1
  }
fi
if "$forensics_enabled"; then
  forensic_event "artifact-validation-passed"
fi
test "$run_status" -eq 0

echo "Durable throughput selector completed; retain $output_dir as one evidence unit"
