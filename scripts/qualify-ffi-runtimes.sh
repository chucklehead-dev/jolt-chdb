#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

if (( $# == 0 )); then
  jolt_command=(jolt)
else
  jolt_command=("$@")
fi

compat_value() {
  bb -e '(require (quote [clojure.edn :as edn]))
         (let [pins (edn/read-string
                     (slurp "resources/jdbc/chdb/ffi-compatibility.edn"))]
           (print (get-in pins (mapv keyword *command-line-args*))))' -- "$@"
}

expected_jolt_version=$(compat_value jolt version)
expected_bb_tag=$(compat_value babashka tag)
expected_bb_commit=$(compat_value babashka tag-commit)
expected_jdk_version=$(compat_value jvm jdk-version)

actual_jolt_version=$("${jolt_command[@]}" --version)
case "$actual_jolt_version" in
  "jolt v$expected_jolt_version"|"jolt v$expected_jolt_version"-[0-9]*-g[0-9a-f]*) ;;
  *)
    echo "expected Jolt v$expected_jolt_version release or derived commit, got: $actual_jolt_version" >&2
    exit 1
    ;;
esac
test "$(bb --version)" = "babashka $expected_bb_tag"
test "$(bb describe | bb -i -e '(print (:git/sha (read-string (slurp *in*))))')" = \
  "$expected_bb_commit"
test "$(java -XshowSettings:properties -version 2>&1 | sed -n 's/^ *java.runtime.version = //p')" = \
  "$expected_jdk_version"

# Resolve through the production Jolt-side selector exactly once. Exporting the
# result prevents the BB and JVM adapters from independently reconstructing a
# default path when JOLT_CHDB_CACHE_DIR or XDG_CACHE_HOME selects another cache.
library_path_edn=$("${jolt_command[@]}" -e \
  '(do (require (quote [jdbc.chdb.native :as native])) (native/library-path))')
JOLT_CHDB_LIB=$(bb -e '(print (read-string (first *command-line-args*)))' \
  -- "$library_path_edn")
export JOLT_CHDB_LIB

"${jolt_command[@]}" -M:abi-test
bb -cp src:resources:test -m jdbc.chdb-abi-babashka-test
clojure -Srepro -M:abi-babashka-jvm-test
