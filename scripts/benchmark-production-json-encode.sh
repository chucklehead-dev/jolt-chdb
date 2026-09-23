#!/usr/bin/env bash
set -euo pipefail

# Encoder only: same Durable 512-row ClickStack log fixture on five profiles.
# `verify` builds the fixture and checks parity without timing samples.
if [[ $# -ne 2 || ( $1 != verify && $1 != measure && $1 != jolt-verify && $1 != jolt-measure ) ]]; then
  echo "usage: $0 {verify|measure|jolt-verify|jolt-measure} OUTPUT_DIR" >&2
  exit 2
fi

mode=$1
root=$(cd "$(dirname "$0")/.." && pwd -P)
output=$(realpath -m "$2")
wrapper=/home/chuck/ai-src/tools/jolt-with-chez-10.4.1
data_json_sha=0f51b99101bc5e840f957c073f87b6f877309a25
cheshire_version=6.2.0
warmups=${BENCH_WARMUPS:-3}
samples=${BENCH_SAMPLES:-10}
if [[ $mode == verify || $mode == jolt-verify ]]; then samples=verify; warmups=1; fi

[[ ! -e $output ]] || { echo "output directory already exists" >&2; exit 2; }
[[ -z $(git -C "$root" status --porcelain=v1) ]] || {
  echo "benchmark checkout must be clean" >&2; exit 2; }
grep -Fq ":git/sha \"$data_json_sha\"" "$root/deps.edn" || {
  echo "data.json pin changed; update this benchmark's dependency identity" >&2; exit 2; }
for command in bb sha256sum; do
  command -v "$command" >/dev/null || { echo "missing $command" >&2; exit 2; }
done
if [[ $mode != jolt-measure && $mode != jolt-verify ]]; then
  command -v clojure >/dev/null || { echo "missing clojure" >&2; exit 2; }
fi
if [[ -z ${BENCH_JOLT_BIN:-} ]]; then
  command -v jolt >/dev/null || { echo "missing jolt" >&2; exit 2; }
elif [[ $BENCH_JOLT_BIN != /* ]]; then
  echo "BENCH_JOLT_BIN must be absolute" >&2; exit 2
fi
"$wrapper" >/dev/null
jolt_bin=$(realpath -e "${BENCH_JOLT_BIN:-$(command -v jolt)}")
[[ -x $jolt_bin ]] || { echo "selected Jolt is not executable" >&2; exit 2; }
jolt_sha=$(sha256sum "$jolt_bin" | cut -d' ' -f1)
if [[ ( $mode == jolt-measure || $mode == jolt-verify ) && -z ${BENCH_JOLT_EXPECT_SHA256:-} ]]; then
  echo "Jolt-only mode requires BENCH_JOLT_EXPECT_SHA256" >&2; exit 2
fi
if [[ -n ${BENCH_JOLT_EXPECT_SHA256:-} && $jolt_sha != "$BENCH_JOLT_EXPECT_SHA256" ]]; then
  echo "selected Jolt executable SHA does not match expectation" >&2; exit 2
fi
jolt_version=$("$wrapper" "$jolt_bin" --version | head -1)
[[ $jolt_version == jolt* ]] || { echo "selected Jolt version is invalid" >&2; exit 2; }
mkdir -p "$output"

head=$(git -C "$root" rev-parse HEAD)
fixture_sha=$(sha256sum "$root/bench/jdbc/chdb_durable_log_fixture.cljc" | cut -d' ' -f1)
paths='["src" "bench"]'
if [[ -n ${BENCH_DATA_JSON_LOCAL_ROOT:-} ]]; then
  local_sha=$(git -C "$BENCH_DATA_JSON_LOCAL_ROOT" rev-parse HEAD)
  [[ $local_sha == "$data_json_sha" && -z $(git -C "$BENCH_DATA_JSON_LOCAL_ROOT" status --porcelain=v1) ]] || {
    echo "local data.json checkout must be clean at $data_json_sha" >&2; exit 2; }
  data_coordinate="{:local/root \"$BENCH_DATA_JSON_LOCAL_ROOT\"}"
else
  data_coordinate="{:git/url \"https://github.com/casselc/data.json.git\" :git/sha \"$data_json_sha\"}"
fi
data_deps="{:paths $paths :deps {org.clojure/data.json $data_coordinate}}"
cheshire_deps="{:paths $paths :deps {org.clojure/data.json $data_coordinate cheshire/cheshire {:mvn/version \"$cheshire_version\"}}}"
common=(BENCH_HARNESS_HEAD="$head" BENCH_FIXTURE_SOURCE_SHA256="$fixture_sha")

cd "$root"
if [[ $mode == jolt-measure || $mode == jolt-verify ]]; then
  for profile in data-json-production ordered-four; do
    env "${common[@]}" BENCH_RUNTIME_VERSION="$jolt_version" \
      BENCH_RUNTIME_BINARY_SHA256="$jolt_sha" BENCH_CODEC_VERSION="$data_json_sha" \
      "$wrapper" "$jolt_bin" -Srepro -Sdeps "$data_deps" -M \
      -m jdbc.chdb-production-json-encode \
      "$profile" "$warmups" "$samples" "$output/jolt-$profile.edn"
  done
  bb -e '
  (require (quote [clojure.edn :as edn]))
  (let [reports (mapv (comp edn/read-string slurp) *command-line-args*)
        fixture-sha (mapv #(get-in % [:fixture :utf8-sha256]) reports)]
    (when-not (and (apply = fixture-sha)
                   (= "3aaa66f49724fd83d501f3f42cd9f167857f50a64008b66b2999fa2ce81a0793"
                      (first fixture-sha))
                   (apply = (map #(get-in % [:fixture :semantic-sha256]) reports))
                   (apply = (map :runtime-binary-sha256 reports))
                   (every? #(= 348836 (get-in % [:fixture :utf8-bytes])) reports)
                   (every? #(= :passed (get-in % [:fixture :decoded-value-parity])) reports))
      (throw (ex-info "Jolt-only parity failed" {:fixture-sha fixture-sha})))
    (println (pr-str {:status :passed :scope :encode-only
                      :runtime-binary-sha256 (:runtime-binary-sha256 (first reports))
                      :data-json-utf8-sha256 (first fixture-sha)
                      :profiles (mapv :profile reports)})))' \
    "$output/jolt-data-json-production.edn" \
    "$output/jolt-ordered-four.edn" > "$output/summary.edn"
  cat "$output/summary.edn"
  exit 0
fi

env "${common[@]}" BENCH_RUNTIME_VERSION="$(bb --version)" \
  BENCH_CODEC_VERSION="babashka-bundled-cheshire" \
  bb -cp "$root/src:$root/bench" -m jdbc.chdb-production-json-encode \
  cheshire-production "$warmups" "$samples" "$output/bb-cheshire.edn"

env "${common[@]}" BENCH_RUNTIME_VERSION="$(clojure -Sdescribe | sed -n 's/.*:version "\([^"]*\)".*/clojure-cli-\1/p')" \
  BENCH_CODEC_VERSION="$data_json_sha" \
  clojure -Srepro -Sdeps "$data_deps" -M -m jdbc.chdb-production-json-encode \
  data-json-production "$warmups" "$samples" "$output/jvm-data-json.edn"

env "${common[@]}" BENCH_RUNTIME_VERSION="$(clojure -Sdescribe | sed -n 's/.*:version "\([^"]*\)".*/clojure-cli-\1/p')" \
  BENCH_CODEC_VERSION="$cheshire_version" \
  clojure -Srepro -Sdeps "$cheshire_deps" -M -m jdbc.chdb-production-json-encode \
  cheshire-production "$warmups" "$samples" "$output/jvm-cheshire.edn"

env "${common[@]}" BENCH_RUNTIME_VERSION="$jolt_version" \
  BENCH_RUNTIME_BINARY_SHA256="$jolt_sha" BENCH_CODEC_VERSION="$data_json_sha" \
  "$wrapper" "$jolt_bin" -Srepro -Sdeps "$data_deps" -M -m jdbc.chdb-production-json-encode \
  data-json-production "$warmups" "$samples" "$output/jolt-data-json.edn"

env "${common[@]}" BENCH_RUNTIME_VERSION="$jolt_version" \
  BENCH_RUNTIME_BINARY_SHA256="$jolt_sha" BENCH_CODEC_VERSION="$data_json_sha" \
  "$wrapper" "$jolt_bin" -Srepro -Sdeps "$data_deps" -M -m jdbc.chdb-production-json-encode \
  ordered-four "$warmups" "$samples" "$output/jolt-ordered-four.edn"

bb -e '
(require (quote [clojure.edn :as edn]))
(let [reports (mapv (comp edn/read-string slurp) *command-line-args*)
      fixtures (mapv :fixture reports)
      semantic (mapv :semantic-sha256 fixtures)
      bytes (mapv :utf8-sha256 fixtures)]
  (when-not (and (apply = semantic)
                 (apply = (map :fixture-source-sha256 reports))
                 (= (nth bytes 1) (nth bytes 3) (nth bytes 4)))
    (throw (ex-info "cross-host parity failed"
                    {:semantic-sha256 semantic :utf8-sha256 bytes})))
  (println (pr-str {:status :passed :scope :encode-only
                    :semantic-sha256 (first semantic)
                    :data-json-utf8-sha256 (nth bytes 1)
                    :byte-parity :jolt-and-jvm-data-json
                    :value-parity :all-five-profiles})))' \
  "$output/bb-cheshire.edn" "$output/jvm-data-json.edn" \
  "$output/jvm-cheshire.edn" "$output/jolt-data-json.edn" \
  "$output/jolt-ordered-four.edn" > "$output/summary.edn"

cat "$output/summary.edn"
