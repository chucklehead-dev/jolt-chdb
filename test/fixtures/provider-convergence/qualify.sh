#!/bin/sh
set -eu

fixture_root=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
workspace_root=$(CDPATH= cd -- "$fixture_root/../../.." && pwd)
jolt_wrapper=${JOLT_WRAPPER:-/home/chuck/ai-src/tools/jolt-with-chez-10.4.1}
jolt_bin=${JOLT_BIN:-jolt}
work=$(mktemp -d "${TMPDIR:-/tmp}/jolt-chdb-provider-convergence.XXXXXX")
trap 'rm -rf "$work"' EXIT

# The historical pins must fail the same exact-one oracle for the causal reason
# recorded in issue #108: two physical roots provide every shared namespace.
(cd "$fixture_root/red" && "$jolt_wrapper" "$jolt_bin" -Spath > "$work/red.spath")
if "$fixture_root/assert-exact-one.sh" "$work/red.spath" "$work/red.providers" \
     > "$work/red.out" 2> "$work/red.err"; then
  echo "divergent provider control unexpectedly passed" >&2
  exit 1
fi
grep -q 'db/sqlite.clj has 2 source providers' "$work/red.err" || {
  echo "divergent provider control failed for the wrong reason" >&2
  exit 1
}
echo "causal red control rejected two db/sqlite.clj providers"

(cd "$fixture_root/green" && "$jolt_wrapper" "$jolt_bin" -Stree > "$work/green.stree")
(cd "$fixture_root/green" && "$jolt_wrapper" "$jolt_bin" -Spath > "$work/green.spath")
grep -q 'jolt-lang/db 802ba50 :newer-version' "$work/green.stree" || {
  echo "green graph did not select the reviewed integrated provider revision" >&2
  exit 1
}
grep -q '802ba50948a231594fa7a94193d8c30b99032c7e' "$workspace_root/deps.edn" || {
  echo "jolt-chdb does not pin the complete integrated provider revision" >&2
  exit 1
}
"$fixture_root/assert-exact-one.sh" "$work/green.spath" "$work/green.providers"
echo "green graph has one physical db/JDBC provider root"

(cd "$fixture_root/green" && "$jolt_wrapper" "$jolt_bin" -M:run)

if [ -n "${PROVIDER_CONVERGENCE_EVIDENCE_DIR:-}" ]; then
  mkdir -p "$PROVIDER_CONVERGENCE_EVIDENCE_DIR"
  cp "$work/green.stree" "$PROVIDER_CONVERGENCE_EVIDENCE_DIR/green.stree"
  cp "$work/green.spath" "$PROVIDER_CONVERGENCE_EVIDENCE_DIR/green.spath"
fi
