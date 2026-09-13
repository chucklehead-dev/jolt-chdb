#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 8 ]]; then
  echo "usage: $0 OUTPUT_DIR WAL_JSONL WAL_SHA256 RECORD_ORDINAL WARMUPS SAMPLES JOLT_BIN JOLT_SOURCE_SHA" >&2
  exit 2
fi

output_dir=$(realpath -m "$1")
wal=$(realpath "$2")
wal_sha=$3
record_ordinal=$4
warmups=$5
samples=$6
jolt_bin=$(realpath "$7")
jolt_source_sha=$8
repo_root=$(cd "$(dirname "$0")/.." && pwd -P)
wrapper=/home/chuck/ai-src/tools/jolt-with-chez-10.4.1
compat="$repo_root/resources/jdbc/chdb/ffi-compatibility.edn"
stable_lib=/home/chuck/.cache/jolt-chdb/26.7.0/linux-amd64/libchdb.so
stable_marker="$stable_lib.archive-sha256"
cd "$repo_root"

if [[ -e "$output_dir" ]] && find "$output_dir" -mindepth 1 -print -quit | grep -q .; then
  echo "output directory must be absent or empty" >&2
  exit 2
fi
mkdir -p "$output_dir"

read_pin() {
  bb -e '(require (quote [clojure.edn :as edn]))
         (print (get-in (edn/read-string (slurp (first *command-line-args*)))
                        (mapv keyword (rest *command-line-args*))))' \
     -- "$compat" "$@"
}

expected_jolt=$(read_pin jolt version)
expected_bb=$(read_pin babashka tag)
expected_bb_commit=$(read_pin babashka tag-commit)
expected_jdk=$(read_pin jvm jdk-version)
expected_native=$(read_pin native version)
expected_archive=$(bb -e '(require (quote [clojure.edn :as edn]))
                           (print (get-in
                                   (edn/read-string (slurp (first *command-line-args*)))
                                   [:native :archive-sha256 [:linux "amd64"]]))' \
                         -- "$compat")
data_json_sha=$(bb -e '(require (quote [clojure.edn :as edn]))
                       (print (get-in (edn/read-string (slurp "deps.edn"))
                                      [:deps (quote org.clojure/data.json) :git/sha]))')
upstream_data_json_jar=$(clojure -Srepro -Spath -M:cross-host-wal-upstream-data-json | \
  tr ':' '\n' | grep '/org/clojure/data.json/2.5.2/data.json-2.5.2.jar$')
cheshire_jar=$(clojure -Srepro -Spath -M:cross-host-wal-cheshire | \
  tr ':' '\n' | grep '/cheshire/cheshire/6.2.0/cheshire-6.2.0.jar$')
[[ -f "$upstream_data_json_jar" && -f "$cheshire_jar" ]]
upstream_data_json_sha=$(sha256sum "$upstream_data_json_jar" | cut -d' ' -f1)
cheshire_sha=$(sha256sum "$cheshire_jar" | cut -d' ' -f1)

jolt_version=$($wrapper "$jolt_bin" --version)
case "$jolt_version" in
  "jolt v$expected_jolt"|"jolt v$expected_jolt"-[0-9]*-g[0-9a-f]*) ;;
  *) echo "Jolt compiler is outside the compatible release line: $jolt_version" >&2; exit 2 ;;
esac
jolt_revision=${jolt_version##*-g}
[[ "$jolt_revision" =~ ^[0-9a-f]{8,40}$ ]] || {
  echo "benchmark requires a revision-bearing Jolt compiler" >&2; exit 2; }
[[ "$jolt_source_sha" =~ ^[0-9a-f]{40}$ && "$jolt_source_sha" = "$jolt_revision"* ]] || {
  echo "Jolt banner revision differs from caller-asserted source SHA" >&2; exit 2; }
[[ "$(bb --version)" = "babashka $expected_bb" ]]
[[ "$(bb describe | bb -i -e '(print (:git/sha (read-string (slurp *in*))))')" = "$expected_bb_commit" ]]
[[ "$(java -XshowSettings:properties -version 2>&1 | sed -n 's/^ *java.runtime.version = //p')" = "$expected_jdk" ]]
[[ -f "$stable_lib" && -f "$stable_marker" ]]
[[ "$(tr -d '[:space:]' < "$stable_marker")" = "$expected_archive" ]]

native_version=$(JOLT_CHDB_LIB="$stable_lib" bb -cp "$repo_root/src:$repo_root/resources:$repo_root/test" -e \
  '(require (quote [babashka.ffi :as ffi]) (quote [jdbc.chdb.abi :as abi]))
   (let [lib (ffi/load-library (System/getenv "JOLT_CHDB_LIB"))
         spec (abi/function-spec :version)
         f (ffi/cfn lib (:symbol spec) (:args spec) (:return spec))]
     (print (f)))')
[[ "$native_version" = "$expected_native" ]]
[[ "$(sha256sum "$wal" | cut -d' ' -f1)" = "$wal_sha" ]]

if [[ "${BENCH_PREFLIGHT_ONLY:-0}" = 1 ]]; then
  printf 'cross-host WAL preflight complete: jolt=%s babashka=%s jvm=%s native=%s\n' \
    "$jolt_version" "$expected_bb" "$expected_jdk" "$native_version"
  exit 0
fi

jolt_executable_sha=$(sha256sum "$jolt_bin" | cut -d' ' -f1)
native_library_sha=$(sha256sum "$stable_lib" | cut -d' ' -f1)
bb_bin=$(realpath "$(command -v bb)")
java_bin=$(realpath "$(command -v java)")
jolt_common=(BENCH_JOLT_SOURCE_SHA="$jolt_source_sha"
             BENCH_JOLT_EXECUTABLE_SHA256="$jolt_executable_sha"
             BENCH_NATIVE_VERSION="$native_version"
             BENCH_NATIVE_LIBRARY_SHA256="$native_library_sha")

common=("$wal" "$wal_sha" "$record_ordinal" "$warmups" "$samples")

run_jolt() {
  env "${jolt_common[@]}" BENCH_RUNTIME=jolt BENCH_JSON_PARSER=casselc-data-json \
      BENCH_RUNTIME_VERSION="$jolt_version" \
      BENCH_RUNTIME_REVISION="$jolt_revision" BENCH_DATA_JSON_GIT_SHA="$data_json_sha" \
      BENCH_RUNTIME_EXECUTABLE_SHA256="$jolt_executable_sha" \
    "$wrapper" "$jolt_bin" -Srepro -M:cross-host-wal-benchmark \
    "${common[@]}" "$output_dir/jolt.edn"
}

run_bb() {
  env "${jolt_common[@]}" BENCH_RUNTIME=babashka \
      BENCH_JSON_PARSER=babashka-bundled-cheshire BENCH_RUNTIME_VERSION="$(bb --version)" \
      BENCH_RUNTIME_REVISION="$expected_bb_commit" BENCH_DATA_JSON_GIT_SHA="$data_json_sha" \
      BENCH_RUNTIME_EXECUTABLE_SHA256="$(sha256sum "$bb_bin" | cut -d' ' -f1)" \
    bb --config "$repo_root/deps.edn" --deps-root "$repo_root" \
       -Sdeps '{:paths ["src" "resources" "bench"]}' \
       -m jdbc.chdb-cross-host-wal \
       "${common[@]}" "$output_dir/babashka.edn"
}

run_jvm() {
  local profile=$1 alias=$2 version=$3 artifact_sha=$4 raw=$5 output=$6
  env "${jolt_common[@]}" BENCH_RUNTIME=jvm BENCH_JSON_PARSER="$profile" \
      BENCH_JSON_PARSER_VERSION="$version" BENCH_MEASURE_RAW="$raw" \
      BENCH_JSON_PARSER_ARTIFACT_SHA256="$artifact_sha" \
      BENCH_RUNTIME_VERSION="$expected_jdk" \
      BENCH_RUNTIME_REVISION="$expected_jdk" BENCH_DATA_JSON_GIT_SHA="$data_json_sha" \
      BENCH_RUNTIME_EXECUTABLE_SHA256="$(sha256sum "$java_bin" | cut -d' ' -f1)" \
    clojure -Srepro -M:"$alias" "${common[@]}" "$output"
}

# One fresh process per matrix row. The order is counterbalanced by the caller
# across repeated invocations; this bounded script itself never merges samples.
run_jolt
run_bb
run_jvm casselc-data-json cross-host-wal-benchmark "$data_json_sha" not-applicable 1 \
  "$output_dir/jvm-casselc-data-json.edn"
run_jvm upstream-data-json cross-host-wal-upstream-data-json 2.5.2 \
  "$upstream_data_json_sha" 0 \
  "$output_dir/jvm-upstream-data-json.edn"
run_jvm jvm-cheshire cross-host-wal-cheshire 6.2.0 "$cheshire_sha" 0 \
  "$output_dir/jvm-cheshire.edn"

bb -e '(require (quote [clojure.edn :as edn]))
        (let [reports (mapv (comp edn/read-string slurp)
                            *command-line-args*)
              oracle (mapv #(select-keys (get-in % [:fixture :json])
                                         [:status :sql-chars :sql-sha256])
                           reports)]
          (when-not (and (every? #(= :verified (:status %)) oracle)
                         (apply = oracle))
            (throw (ex-info "cross-host JSON semantic oracle differs"
                            {:oracles oracle}))))' \
   -- "$output_dir"/*.edn

printf 'cross-host WAL characterization complete: %s\n' "$output_dir"
