#!/usr/bin/env bash
set -euo pipefail

validate_matrix_order() {
  local order=$1 row
  read -r -a matrix_rows <<< "$order"
  [[ ${#matrix_rows[@]} -eq 5 ]] || return 1
  declare -A seen_rows=()
  for row in "${matrix_rows[@]}"; do
    case "$row" in
      jolt|babashka|jvm-casselc|jvm-upstream|jvm-cheshire) ;;
      *) return 1 ;;
    esac
    [[ -z "${seen_rows[$row]:-}" ]] || return 1
    seen_rows[$row]=1
  done
  for row in jolt babashka jvm-casselc jvm-upstream jvm-cheshire; do
    [[ -n "${seen_rows[$row]:-}" ]] || return 1
  done
}

summarize_optional_jfr() {
  local profile=$1 basename=$2
  if [[ -s "$basename.jfr" ]] && command -v jfr >/dev/null 2>&1; then
    if jfr summary "$basename.jfr" > "$basename.jfr-summary.txt"; then
      printf '{:event :profile-summary :runtime-row :%s :status :complete}\n' \
        "$profile" >> "$matrix_journal"
    else
      printf '{:event :profile-summary :runtime-row :%s :status :failed}\n' \
        "$profile" >> "$matrix_journal"
    fi
  else
    printf '{:event :profile-summary :runtime-row :%s :status :not-run}\n' \
      "$profile" >> "$matrix_journal"
  fi
}

finish_jvm_run() {
  local benchmark_status=$1 profile=$2 basename=$3
  # Profiling is diagnostic and fail-soft, but it must never replace the
  # authoritative benchmark process status.
  summarize_optional_jfr "$profile" "$basename" || true
  return "$benchmark_status"
}

run_with_first_checkpoint() {
  local journal=$1 timeout_seconds=$2
  shift 2
  checkpoint_guard_reason=
  [[ "$timeout_seconds" =~ ^[1-9][0-9]*$ ]] && (( timeout_seconds <= 300 )) || {
    echo "first-checkpoint timeout must be between 1 and 300 seconds" >&2
    return 2
  }
  setsid "$@" &
  local pid=$! started=$SECONDS status=0
  while kill -0 "$pid" 2>/dev/null; do
    if [[ -s "$journal" ]]; then
      wait "$pid" || status=$?
      if (( status != 0 )); then
        checkpoint_guard_reason=child-exit-after-first-checkpoint
      fi
      return "$status"
    fi
    if (( SECONDS - started >= timeout_seconds )); then
      kill -TERM -- "-$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      checkpoint_guard_reason=first-checkpoint-timeout
      echo "benchmark host produced no first checkpoint within ${timeout_seconds}s" >&2
      return 124
    fi
    sleep 0.1
  done
  wait "$pid" || status=$?
  if [[ -s "$journal" ]]; then
    if (( status != 0 )); then
      checkpoint_guard_reason=child-exit-after-first-checkpoint
    fi
    return "$status"
  fi
  if (( status == 0 )); then
    checkpoint_guard_reason=missing-first-checkpoint
    echo "benchmark host exited successfully without a first checkpoint" >&2
    return 125
  fi
  checkpoint_guard_reason=child-exit-before-first-checkpoint
  return "$status"
}

if [[ "${BENCH_VALIDATE_CHECKPOINT_GUARD_ONLY:-0}" = 1 ]]; then
  guard_tmp=$(mktemp -d)
  trap 'rm -rf "$guard_tmp"' EXIT
  run_with_first_checkpoint "$guard_tmp/ok.journal" 1 \
    bash -c 'printf "checkpoint\n" > "$1"' _ "$guard_tmp/ok.journal"
  [[ -z "$checkpoint_guard_reason" ]]
  status=0
  run_with_first_checkpoint "$guard_tmp/missing.journal" 1 true || status=$?
  [[ "$status" = 125 && "$checkpoint_guard_reason" = missing-first-checkpoint ]] || exit 1
  status=0
  run_with_first_checkpoint "$guard_tmp/failed.journal" 1 \
    bash -c 'exit 17' || status=$?
  [[ "$status" = 17 && "$checkpoint_guard_reason" = child-exit-before-first-checkpoint ]] || exit 1
  status=0
  run_with_first_checkpoint "$guard_tmp/checkpointed-failure.journal" 1 \
    bash -c 'printf "checkpoint\n" > "$1"; exit 19' \
    _ "$guard_tmp/checkpointed-failure.journal" || status=$?
  [[ "$status" = 19 && "$checkpoint_guard_reason" = child-exit-after-first-checkpoint ]] || exit 1
  status=0
  run_with_first_checkpoint "$guard_tmp/late.journal" 1 \
    bash -c 'sleep 5' || status=$?
  [[ "$status" = 124 && "$checkpoint_guard_reason" = first-checkpoint-timeout ]] || exit 1
  echo "first-checkpoint liveness guard passed"
  exit 0
fi

if [[ "${BENCH_VALIDATE_JVM_STATUS_ONLY:-0}" = 1 ]]; then
  matrix_journal=/dev/null
  summarize_optional_jfr() { return 0; }
  status=0
  finish_jvm_run 17 test-profile /tmp/not-used || status=$?
  [[ "$status" = 17 ]] || exit 1
  echo "failed JVM benchmark status survives a successful profile summary"
  exit 0
fi

matrix_order=${BENCH_MATRIX_ORDER:-"jolt babashka jvm-casselc jvm-upstream jvm-cheshire"}
if ! validate_matrix_order "$matrix_order"; then
  echo "BENCH_MATRIX_ORDER must contain all five known rows exactly once" >&2
  exit 2
fi
if [[ "${BENCH_VALIDATE_ORDER_ONLY:-0}" = 1 ]]; then
  printf 'valid matrix order: %s\n' "$matrix_order"
  exit 0
fi

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
benchmark_pins="$repo_root/resources/jdbc/chdb/cross-host-benchmark.edn"
stable_lib=/home/chuck/.cache/jolt-chdb/26.7.0/linux-amd64/libchdb.so
stable_marker="$stable_lib.archive-sha256"
cd "$repo_root"

case "$output_dir/" in
  "$repo_root/"*) echo "output directory must be outside the checkout" >&2; exit 2 ;;
esac
[[ -z "$(git status --porcelain=v1)" ]] || {
  echo "benchmark checkout must be clean" >&2; exit 2; }
repo_head=$(git rev-parse HEAD)
repo_parent=$(git rev-parse HEAD^)
repo_tree=$(git rev-parse HEAD^{tree})

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

read_benchmark_pin() {
  bb -e '(require (quote [clojure.edn :as edn]))
         (print (get-in (edn/read-string (slurp (first *command-line-args*)))
                        (mapv keyword (rest *command-line-args*))))' \
     -- "$benchmark_pins" "$@"
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
expected_jolt_source=$(read_benchmark_pin jolt source-sha)
upstream_data_json_version=$(read_benchmark_pin parsers upstream-data-json version)
expected_upstream_sha=$(read_benchmark_pin parsers upstream-data-json artifact-sha256)
cheshire_version=$(read_benchmark_pin parsers jvm-cheshire version)
expected_cheshire_sha=$(read_benchmark_pin parsers jvm-cheshire artifact-sha256)
data_json_sha=$(bb -e '(require (quote [clojure.edn :as edn]))
                       (print (get-in (edn/read-string (slurp "deps.edn"))
                                      [:deps (quote org.clojure/data.json) :git/sha]))')
upstream_data_json_jar=$(clojure -Srepro -Spath -M:cross-host-wal-upstream-data-json | \
  tr ':' '\n' | grep "/org/clojure/data.json/$upstream_data_json_version/data.json-$upstream_data_json_version.jar$")
cheshire_jar=$(clojure -Srepro -Spath -M:cross-host-wal-cheshire | \
  tr ':' '\n' | grep "/cheshire/cheshire/$cheshire_version/cheshire-$cheshire_version.jar$")
[[ -f "$upstream_data_json_jar" && -f "$cheshire_jar" ]]
upstream_data_json_sha=$(sha256sum "$upstream_data_json_jar" | cut -d' ' -f1)
cheshire_sha=$(sha256sum "$cheshire_jar" | cut -d' ' -f1)
[[ "$upstream_data_json_sha" = "$expected_upstream_sha" ]]
[[ "$cheshire_sha" = "$expected_cheshire_sha" ]]

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
[[ "$jolt_source_sha" = "$expected_jolt_source" ]] || {
  echo "Jolt source differs from the benchmark's canonical pin" >&2; exit 2; }
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
expected_record_count=$(wc -l < "$wal")
[[ "$expected_record_count" =~ ^[1-9][0-9]*$ ]]

if [[ "${BENCH_PREFLIGHT_ONLY:-0}" = 1 ]]; then
  printf 'cross-host WAL preflight complete: jolt=%s babashka=%s jvm=%s native=%s\n' \
    "$jolt_version" "$expected_bb" "$expected_jdk" "$native_version"
  exit 0
fi

jolt_executable_sha=$(sha256sum "$jolt_bin" | cut -d' ' -f1)
native_library_sha=$(sha256sum "$stable_lib" | cut -d' ' -f1)
bb_bin=$(realpath "$(command -v bb)")
java_bin=$(realpath "$(command -v java)")
wal_source_sha=$(sha256sum bench/jdbc/chdb_cross_host_wal.clj | cut -d' ' -f1)
report_source_sha=$(sha256sum bench/jdbc/chdb_cross_host_report.clj | cut -d' ' -f1)
jolt_metrics_source_sha=$(sha256sum bench/jdbc/chdb_cross_host_jolt_metrics.clj | cut -d' ' -f1)
jvm_metrics_source_sha=$(sha256sum bench/jdbc/chdb_cross_host_jvm_metrics.clj | cut -d' ' -f1)
jvm_profile_source_sha=$(sha256sum bench/jdbc/chdb_cross_host_jvm_profile.clj | cut -d' ' -f1)
jvm_scan_source_sha=$(sha256sum bench/jdbc/chdb_cross_host_jvm_scan.clj | cut -d' ' -f1)
runner_sha=$(sha256sum scripts/benchmark-cross-host-wal.sh | cut -d' ' -f1)
matrix_journal="$output_dir/matrix.journal.edn"
first_checkpoint_timeout=${BENCH_FIRST_CHECKPOINT_TIMEOUT_SECONDS:-300}
: > "$matrix_journal"

verify_checkout_provenance() {
  if [[ -n "$(git status --porcelain=v1)" ]] ||
     [[ "$(git rev-parse HEAD)" != "$repo_head" ]] ||
     [[ "$(git rev-parse HEAD^)" != "$repo_parent" ]] ||
     [[ "$(git rev-parse HEAD^{tree})" != "$repo_tree" ]] ||
     [[ "$(sha256sum bench/jdbc/chdb_cross_host_wal.clj | cut -d' ' -f1)" != "$wal_source_sha" ]] ||
     [[ "$(sha256sum bench/jdbc/chdb_cross_host_report.clj | cut -d' ' -f1)" != "$report_source_sha" ]] ||
     [[ "$(sha256sum bench/jdbc/chdb_cross_host_jolt_metrics.clj | cut -d' ' -f1)" != "$jolt_metrics_source_sha" ]] ||
     [[ "$(sha256sum bench/jdbc/chdb_cross_host_jvm_metrics.clj | cut -d' ' -f1)" != "$jvm_metrics_source_sha" ]] ||
     [[ "$(sha256sum bench/jdbc/chdb_cross_host_jvm_profile.clj | cut -d' ' -f1)" != "$jvm_profile_source_sha" ]] ||
     [[ "$(sha256sum bench/jdbc/chdb_cross_host_jvm_scan.clj | cut -d' ' -f1)" != "$jvm_scan_source_sha" ]] ||
     [[ "$(sha256sum scripts/benchmark-cross-host-wal.sh | cut -d' ' -f1)" != "$runner_sha" ]]; then
    printf '{:event :matrix :status :failed :reason :checkout-provenance-drift}\n' >> "$matrix_journal"
    echo "benchmark checkout provenance changed during the matrix; completed reports preserved" >&2
    exit 2
  fi
}

jolt_common=(BENCH_JOLT_SOURCE_SHA="$jolt_source_sha"
             BENCH_JOLT_EXECUTABLE_SHA256="$jolt_executable_sha"
             BENCH_NATIVE_VERSION="$native_version"
             BENCH_NATIVE_LIBRARY_SHA256="$native_library_sha"
             BENCH_REPO_HEAD="$repo_head"
             BENCH_REPO_PARENT="$repo_parent"
             BENCH_REPO_TREE="$repo_tree"
             BENCH_WAL_SOURCE_SHA256="$wal_source_sha"
             BENCH_REPORT_SOURCE_SHA256="$report_source_sha"
             BENCH_JOLT_METRICS_SOURCE_SHA256="$jolt_metrics_source_sha"
             BENCH_JVM_METRICS_SOURCE_SHA256="$jvm_metrics_source_sha"
             BENCH_JVM_PROFILE_SOURCE_SHA256="$jvm_profile_source_sha"
             BENCH_JVM_SCAN_SOURCE_SHA256="$jvm_scan_source_sha"
             BENCH_RUNNER_SHA256="$runner_sha"
             BENCH_EXPECTED_RECORD_COUNT="$expected_record_count")

common=("$wal" "$wal_sha" "$record_ordinal" "$warmups" "$samples")

run_jolt() {
  local output=$1 position=$2
  run_with_first_checkpoint "${output%.edn}.journal.edn" "$first_checkpoint_timeout" \
    env "${jolt_common[@]}" BENCH_RUNTIME=jolt BENCH_JSON_PARSER=casselc-data-json \
      BENCH_MATRIX_ORDER="$matrix_order" BENCH_MATRIX_POSITION="$position" \
      BENCH_RUNTIME_VERSION="$jolt_version" \
      BENCH_RUNTIME_REVISION="$jolt_revision" BENCH_DATA_JSON_GIT_SHA="$data_json_sha" \
      BENCH_RUNTIME_EXECUTABLE_SHA256="$jolt_executable_sha" \
    "$wrapper" "$jolt_bin" -Srepro -M:cross-host-wal-benchmark \
    "${common[@]}" "$output"
}

run_bb() {
  local output=$1 position=$2
  run_with_first_checkpoint "${output%.edn}.journal.edn" "$first_checkpoint_timeout" \
    env "${jolt_common[@]}" BENCH_RUNTIME=babashka \
      BENCH_MATRIX_ORDER="$matrix_order" BENCH_MATRIX_POSITION="$position" \
      BENCH_JSON_PARSER=babashka-bundled-cheshire BENCH_RUNTIME_VERSION="$(bb --version)" \
      BENCH_RUNTIME_REVISION="$expected_bb_commit" BENCH_DATA_JSON_GIT_SHA="$data_json_sha" \
      BENCH_RUNTIME_EXECUTABLE_SHA256="$(sha256sum "$bb_bin" | cut -d' ' -f1)" \
    bb --config "$repo_root/deps.edn" --deps-root "$repo_root" \
       -Sdeps '{:paths ["src" "resources" "bench"]}' \
       -m jdbc.chdb-cross-host-wal \
       "${common[@]}" "$output"
}

run_jvm() {
  local profile=$1 alias=$2 version=$3 artifact_sha=$4 output=$5 position=$6 reuse=${7:-}
  local basename=${output%.edn} benchmark_status=0
  local reuse_env=()
  # Counterbalanced orders may encounter a secondary JVM parser before the
  # primary report exists. In that case it performs and reports its own scan;
  # no scan is silently duplicated or represented as reused.
  if [[ -n "$reuse" && -s "$reuse" ]]; then
    reuse_env=(BENCH_REUSE_BOUNDARY_REPORT="$reuse")
  fi
  run_with_first_checkpoint "$basename.journal.edn" "$first_checkpoint_timeout" \
    env "${jolt_common[@]}" "${reuse_env[@]}" BENCH_RUNTIME=jvm BENCH_JSON_PARSER="$profile" \
      BENCH_MATRIX_ORDER="$matrix_order" BENCH_MATRIX_POSITION="$position" \
      BENCH_JSON_PARSER_VERSION="$version" \
      BENCH_JSON_PARSER_ARTIFACT_SHA256="$artifact_sha" \
      BENCH_RUNTIME_VERSION="$expected_jdk" \
      BENCH_RUNTIME_REVISION="$expected_jdk" BENCH_DATA_JSON_GIT_SHA="$data_json_sha" \
      BENCH_RUNTIME_EXECUTABLE_SHA256="$(sha256sum "$java_bin" | cut -d' ' -f1)" \
      BENCH_JFR_PATH="$basename.jfr" \
    clojure -Srepro -M:"$alias" "${common[@]}" "$output" || benchmark_status=$?
  finish_jvm_run "$benchmark_status" "$profile" "$basename"
}

# One fresh process per matrix row. BENCH_MATRIX_ORDER is recorded and lets
# repeated invocations use counterbalanced orders; one invocation is one order.
position=0
for row in "${matrix_rows[@]}"; do
  position=$((position + 1))
  checkpoint_guard_reason=
  verify_checkout_provenance
  printf '{:event :host :runtime-row :%s :status :started :matrix-position %s}\n' \
    "$row" "$position" >> "$matrix_journal"
  row_status=0
  case "$row" in
    jolt) run_jolt "$output_dir/jolt.edn" "$position" || row_status=$? ;;
    babashka) run_bb "$output_dir/babashka.edn" "$position" || row_status=$? ;;
    jvm-casselc)
      run_jvm casselc-data-json cross-host-wal-benchmark "$data_json_sha" \
        not-applicable "$output_dir/jvm-casselc-data-json.edn" "$position" || row_status=$? ;;
    jvm-upstream)
      run_jvm upstream-data-json cross-host-wal-upstream-data-json \
        "$upstream_data_json_version" "$upstream_data_json_sha" \
        "$output_dir/jvm-upstream-data-json.edn" "$position" \
        "$output_dir/jvm-casselc-data-json.edn" || row_status=$? ;;
    jvm-cheshire)
      run_jvm jvm-cheshire cross-host-wal-cheshire "$cheshire_version" \
        "$cheshire_sha" "$output_dir/jvm-cheshire.edn" "$position" \
        "$output_dir/jvm-casselc-data-json.edn" || row_status=$? ;;
  esac
  if (( row_status != 0 )); then
    failure_reason=${checkpoint_guard_reason:-child-nonzero}
    printf '{:event :host :runtime-row :%s :status :failed :exit %s :reason :%s :matrix-position %s}\n' \
      "$row" "$row_status" "$failure_reason" "$position" >> "$matrix_journal"
    exit "$row_status"
  fi
  printf '{:event :host :runtime-row :%s :status :complete :matrix-position %s}\n' \
    "$row" "$position" >> "$matrix_journal"
  # Test-only fault injection: preserve completed row artifacts, then make the
  # matrix fail before the next row begins.
  if [[ "${BENCH_INJECT_FAILURE_AFTER_ROW:-}" = "$row" ]]; then
    printf '{:event :matrix :status :failed :reason :injected-late-failure :after-row :%s}\n' \
      "$row" >> "$matrix_journal"
    exit 86
  fi
  verify_checkout_provenance
done

verify_checkout_provenance

reports=("$output_dir/jolt.edn" "$output_dir/babashka.edn"
         "$output_dir/jvm-casselc-data-json.edn"
         "$output_dir/jvm-upstream-data-json.edn"
         "$output_dir/jvm-cheshire.edn")
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
   -- "${reports[@]}"

printf '{:event :matrix :status :complete :hosts 5}\n' >> "$matrix_journal"

printf 'cross-host WAL characterization complete: %s\n' "$output_dir"
