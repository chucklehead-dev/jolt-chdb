#!/bin/sh
set -eu

fixture_root=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
workspace_root=$(CDPATH= cd -- "$fixture_root/../../.." && pwd)
jolt_wrapper=${JOLT_WRAPPER:-/home/chuck/ai-src/tools/jolt-with-chez-10.4.1}
jolt_bin=${JOLT_BIN:-jolt}
provider_sha=2de2ce8e19b889eed2ad3d1f56077d14516eb447
work=$(mktemp -d "${TMPDIR:-/tmp}/jolt-chdb-provider-convergence.XXXXXX")
trap 'rm -rf "$work"' EXIT

# Dependency resolution and compilation caches are run-scoped. In particular,
# a cpcache created while deps.edn selected cdc6931 must not answer a later
# request for 2de2ce8e.
mkdir -p "$work/jolt-cache" "$work/jolt-gitlibs"
jolt_cmd() {
  env JOLT_CACHE_DIR="$work/jolt-cache" JOLT_GITLIBS_DIR="$work/jolt-gitlibs" \
    "$jolt_wrapper" "$jolt_bin" "$@"
}

(cd "$fixture_root/green" && jolt_cmd -Stree > "$work/green.stree")
(cd "$fixture_root/green" && jolt_cmd -Spath > "$work/green.spath")
grep -q 'jolt-lang/db 2de2ce8 :newer-version' "$work/green.stree" || {
  echo "green graph did not select the reviewed integrated provider revision" >&2
  exit 1
}
grep -Fq ":git/sha \"$provider_sha\"" "$workspace_root/deps.edn" || {
  echo "jolt-chdb does not pin the complete integrated provider revision" >&2
  exit 1
}

provider_root=
classpath=$(cat "$work/green.spath")
old_ifs=$IFS
IFS=:
for root in $classpath; do
  case "$root" in
    *"/$provider_sha/clj")
      if [ -f "$root/db/sqlite.clj" ]; then
        if [ -n "$provider_root" ]; then
          echo "green graph exposes the exact provider revision more than once" >&2
          exit 1
        fi
        provider_root=$root
      fi
      ;;
  esac
done
IFS=$old_ifs
if [ -z "$provider_root" ]; then
  echo "green classpath does not expose the full reviewed provider SHA $provider_sha" >&2
  exit 1
fi

"$fixture_root/assert-exact-one.sh" \
  "$work/green.spath" "$work/green.providers" "$provider_root"
echo "green graph has one physical db/JDBC provider root at full SHA $provider_sha"

# Prove that the oracle discovers a namespace added after this fixture was
# written instead of trusting a hand-maintained allowlist.
mkdir -p "$work/oracle/reference/db" "$work/oracle/reference/next" \
  "$work/oracle/a/db" "$work/oracle/b/db"
: > "$work/oracle/reference/db/new_provider_control.clj"
: > "$work/oracle/a/db/new_provider_control.clj"
: > "$work/oracle/b/db/new_provider_control.clj"
printf '%s:%s\n' "$work/oracle/a" "$work/oracle/b" > "$work/oracle/duplicate.spath"
if "$fixture_root/assert-exact-one.sh" \
     "$work/oracle/duplicate.spath" "$work/oracle/duplicate.providers" \
     "$work/oracle/reference" > "$work/oracle/duplicate.out" \
     2> "$work/oracle/duplicate.err"; then
  echo "newly added namespace control unexpectedly passed" >&2
  exit 1
fi
grep -q 'db/new_provider_control.clj has 2 source providers' \
  "$work/oracle/duplicate.err" || {
  echo "newly added namespace control failed for the wrong reason" >&2
  exit 1
}
echo "derived-source oracle rejected a newly added duplicate namespace"

# The historical pins must fail the same exact-one oracle for the causal reason
# recorded in issue #108: two physical roots provide the shared namespaces.
(cd "$fixture_root/red" && jolt_cmd -Spath > "$work/red.spath")
if "$fixture_root/assert-exact-one.sh" "$work/red.spath" "$work/red.providers" \
     "$provider_root" \
     > "$work/red.out" 2> "$work/red.err"; then
  echo "divergent provider control unexpectedly passed" >&2
  exit 1
fi
grep -q 'db/sqlite.clj has 2 source providers' "$work/red.err" || {
  echo "divergent provider control failed for the wrong reason" >&2
  exit 1
}
echo "causal red control rejected two db/sqlite.clj providers"

(cd "$fixture_root/green" && jolt_cmd -M:run)

if [ -n "${PROVIDER_CONVERGENCE_EVIDENCE_DIR:-}" ]; then
  mkdir -p "$PROVIDER_CONVERGENCE_EVIDENCE_DIR"
  cp "$work/green.stree" "$PROVIDER_CONVERGENCE_EVIDENCE_DIR/green.stree"
  cp "$work/green.spath" "$PROVIDER_CONVERGENCE_EVIDENCE_DIR/green.spath"
  cp "$work/green.providers.namespaces" \
    "$PROVIDER_CONVERGENCE_EVIDENCE_DIR/provider-namespaces"
fi
