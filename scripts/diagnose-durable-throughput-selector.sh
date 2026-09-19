#!/usr/bin/env bash
# Opt-in launch forensics for scripts/run-durable-throughput-selector.sh.
#
# This delegates the selector unchanged: it neither changes the selector set,
# workload, ordering, provenance inputs, nor turns a diagnostic result into
# performance evidence. It retains only lifecycle metadata, an optional
# syscall trace, and presence/size inventory of selector output.
set -euo pipefail
umask 077

selector=${1:?usage: diagnose-durable-throughput-selector.sh SELECTOR OUTPUT-DIR}
output_dir=${2:?usage: diagnose-durable-throughput-selector.sh SELECTOR OUTPUT-DIR}

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
selector_runner="$repo_root/scripts/run-durable-throughput-selector.sh"
command -v setsid >/dev/null 2>&1 || {
  echo "setsid is required for forensic signal forwarding" >&2
  exit 2
}
[[ "$output_dir" = /* ]] || {
  echo "output directory must be absolute so forensic artifacts have stable provenance" >&2
  exit 2
}
[[ ! -e "$output_dir" ]] || {
  echo "output directory must not already exist" >&2
  exit 2
}

forensics_dir="${output_dir}.launch-forensics"
[[ ! -e "$forensics_dir" ]] || {
  echo "forensics directory must not already exist" >&2
  exit 2
}
mkdir -p "$(dirname "$forensics_dir")"
mkdir "$forensics_dir"
events="$forensics_dir/launcher.events"
selector_pid=

event() {
  # Never serialize the ambient environment, command line, benchmark log, or
  # credentials. Optional strace output is separate and may contain execve
  # argument paths, but not an environment dump.
  printf '%s pid=%s phase=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$$" "$1" >> "$events"
}

finish() {
  local code=$?
  trap - EXIT
  event "launcher-exit status=$code"
  exit "$code"
}
on_signal() {
  local signal=$1
  event "launcher-signal signal=$signal"
  # The selector is launched in a dedicated session, so forwarding to its
  # process group reaches strace (if present), the selector, GNU time, and the
  # Jolt workload without signalling this launcher. Reap the leader before
  # returning the signal status to prevent a diagnostic orphan.
  if [[ -n "$selector_pid" ]]; then
    event "selector-child-signal-forwarded signal=$signal pid=$selector_pid"
    kill -"$signal" -- "-$selector_pid" 2>/dev/null || true
    local child_status
    if wait "$selector_pid"; then
      child_status=0
    else
      child_status=$?
    fi
    event "selector-child-reaped status=$child_status"
  fi
  exit $((128 + signal))
}
trap finish EXIT
trap 'on_signal 1' HUP
trap 'on_signal 2' INT
trap 'on_signal 15' TERM

event "launcher-started"
trace_prefix="$forensics_dir/execve"
use_strace=false
strace_probe="$forensics_dir/strace-usability-probe"
strace_usable() {
  # Finding strace on PATH is not enough: containers can expose the binary
  # while denying its PTRACE_TRACEME setup.  Probe it with no traced syscalls
  # and discard the private probe before the actual capture.  This never
  # executes the selector or serializes its arguments/environment.
  local status
  if strace -qq -e trace=none -o "$strace_probe" true >/dev/null 2>&1; then
    status=0
  else
    status=$?
  fi
  rm -f -- "$strace_probe"
  return "$status"
}
case "${DURABLE_SELECTOR_FORENSICS_STRACE:-auto}" in
  auto)
    if command -v strace >/dev/null 2>&1; then
      if strace_usable; then
        use_strace=true
      else
        event "strace-unusable"
      fi
    fi
    ;;
  1|true|yes)
    command -v strace >/dev/null 2>&1 || {
      echo "strace was requested but is unavailable" >&2
      exit 2
    }
    if ! strace_usable; then
      event "strace-unusable"
      echo "strace was requested but cannot trace a child" >&2
      exit 2
    fi
    use_strace=true
    ;;
  0|false|no)
    ;;
  *)
    echo "DURABLE_SELECTOR_FORENSICS_STRACE must be auto, 1, or 0" >&2
    exit 2
    ;;
esac

set +e
if "$use_strace"; then
  event "strace-enabled"
  DURABLE_SELECTOR_FORENSICS_DIR="$forensics_dir" \
    setsid strace -ff -qq -s 256 -e trace=process,signal -o "$trace_prefix" \
    "$selector_runner" "$selector" "$output_dir" &
else
  if [[ ${DURABLE_SELECTOR_FORENSICS_STRACE:-auto} == auto ]] && \
     command -v strace >/dev/null 2>&1; then
    event "strace-unusable-fallback"
  elif [[ ${DURABLE_SELECTOR_FORENSICS_STRACE:-auto} == auto ]]; then
    event "strace-unavailable"
  else
    event "strace-disabled"
  fi
  DURABLE_SELECTOR_FORENSICS_DIR="$forensics_dir" \
    setsid "$selector_runner" "$selector" "$output_dir" &
fi
selector_pid=$!
event "selector-child-started pid=$selector_pid"
wait "$selector_pid"
selector_status=$?
set -e
event "selector-child-returned status=$selector_status"

inventory="$forensics_dir/output-inventory.txt"
for relative in report.edn run.log time-v.txt receipts; do
  path="$output_dir/$relative"
  if [[ -f "$path" && ! -L "$path" ]]; then
    printf '%s file bytes=%s\n' "$relative" "$(wc -c < "$path")" >> "$inventory"
  elif [[ -d "$path" && ! -L "$path" ]]; then
    # Receipt names/content can be sensitive or workload-specific. Preserve
    # only the immediate entry count, not paths or content.
    printf '%s directory entries=%s\n' "$relative" \
      "$(find "$path" -mindepth 1 -maxdepth 1 -printf . | wc -c)" >> "$inventory"
  else
    printf '%s missing\n' "$relative" >> "$inventory"
  fi
done
event "output-inventory-written"

if [[ "$selector_status" -ne 0 ]]; then
  echo "selector failed with status $selector_status; inspect $forensics_dir (not performance evidence)" >&2
fi
exit "$selector_status"
