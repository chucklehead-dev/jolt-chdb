(ns jdbc.chdb-stream-storage-test
  (:require [clojure.test :refer [deftest is]]
            [jdbc.chdb-stream-storage :as storage]
            [jdbc.chdb.durable.local-posix :as local]
            [jdbc.chdb.durable.s3-curl :as s3]))

(def valid-input
  {"CHDB_BENCH_STORAGE" "s3"
   "JOLT_CHDB_S3_REGION" "us-east-2"
   "JOLT_CHDB_S3_BUCKET" "test-qualification-bucket"
   "JOLT_CHDB_S3_PREFIX" "ci/jolt-chdb/123-1/stream-26-9"
   "JOLT_CHDB_S3_ENDPOINT" "https://s3.us-east-2.amazonaws.com"
   "JOLT_CHDB_S3_ACCESS_KEY" "synthetic-access"
   "JOLT_CHDB_S3_SECRET_KEY" "synthetic-secret"
   "JOLT_CHDB_S3_SESSION_TOKEN" "synthetic-token"})

(deftest local-selection-is-explicit-and-independent-of-aws-inputs
  (is (= :local (storage/kind {})))
  (is (= :local (storage/kind {"CHDB_BENCH_STORAGE" "local"})))
  (is (nil? (storage/configuration :local valid-input)))
  (is (thrown? clojure.lang.ExceptionInfo (storage/kind {"CHDB_BENCH_STORAGE" "typo"}))))

(deftest s3-requires-the-scoped-prefix-endpoint-and-every-credential
  (is (= :s3 (storage/kind valid-input)))
  (let [config (storage/configuration :s3 valid-input)]
    (is (= #{:endpoint :bucket :prefix :region :access-key :secret-key :session-token}
           (set (keys config))))
    (is (= "ci/jolt-chdb/123-1/stream-26-9" (:prefix config))))
  (doseq [input (concat
                 (map #(dissoc valid-input %) (remove #{"CHDB_BENCH_STORAGE"} (keys valid-input)))
                 (map #(assoc valid-input "JOLT_CHDB_S3_PREFIX" %)
                      ["" "ci/jolt-chdb/" "ci/oscope/123-1/stream-26-9"
                       "ci/jolt-chdb/123-1/stream-26-9/../shared" "production/data"])
                 [(assoc valid-input "JOLT_CHDB_S3_ENDPOINT" "http://example.invalid")
                  (assoc valid-input "JOLT_CHDB_S3_BUCKET" "test/path")])]
    (let [failure (try (storage/configuration :s3 input) nil
                       (catch clojure.lang.ExceptionInfo error error))]
      (is (some? failure))
      (is (= "Invalid storage qualification configuration" (ex-message failure)))
      (is (= {:type :jdbc.chdb-stream-storage/invalid-configuration} (ex-data failure))))))

(deftest malformed-selection-does-not-open-either-backend
  (let [opens (atom [])]
    (with-redefs [local/local-backend (fn [& _] (swap! opens conj :local))
                  s3/s3-backend (fn [& _] (swap! opens conj :s3))]
      (is (thrown? clojure.lang.ExceptionInfo
            (storage/options "/unused" "writer" (dissoc valid-input "JOLT_CHDB_S3_SESSION_TOKEN")))))
    (is (empty? @opens))))
