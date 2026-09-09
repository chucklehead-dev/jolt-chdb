#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

if (( $# == 0 )); then
  jolt_command=(jolt)
else
  jolt_command=("$@")
fi

test "$(bb --version)" = "babashka v1.13.220"
test "$(bb describe | bb -i -e '(print (:git/sha (read-string (slurp *in*))))')" = \
  "b98575c98a0ef4df77775ff25fd7fc7b591b1afd"
test "$(java -XshowSettings:properties -version 2>&1 | sed -n 's/^ *java.runtime.version = //p')" = \
  "25.0.2+10-LTS"

"${jolt_command[@]}" -M:abi-test
bb -cp src:resources:test -m jdbc.chdb-abi-babashka-test
clojure -Srepro -M:abi-babashka-jvm-test
