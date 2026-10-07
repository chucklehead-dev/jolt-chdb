(ns jdbc.chdb-exporter-storage-qualification-test
  (:require [clojure.test :refer [deftest is]]
            [jdbc.chdb-exporter-storage-qualification :as qualification]
            [jdbc.chdb.durable.local-posix :as local]
            [jdbc.chdb.durable.s3-curl :as s3]))

(deftest invalid-s3-prefix-never-constructs-any-backend
  (doseq [prefix ["" "ci/jolt-chdb/" "ci/oscope/not-this-role" "other/path"]]
    (let [calls (atom [])]
      (with-redefs-fn
        {#'qualification/required-env (fn [_] prefix)
         #'s3/s3-backend (fn [_] (swap! calls conj :s3))
         #'local/local-backend (fn [_] (swap! calls conj :local))}
        #(is (thrown? AssertionError
                       (#'qualification/namespace-backend "s3" "/tmp/not-used"))))
      (is (empty? @calls)))))

(deftest s3-options-are-flat-with-no-local-fallback
  (let [names ["JOLT_CHDB_S3_ENDPOINT" "JOLT_CHDB_S3_BUCKET" "JOLT_CHDB_S3_REGION"
               "JOLT_CHDB_S3_ACCESS_KEY" "JOLT_CHDB_S3_SECRET_KEY"
               "JOLT_CHDB_S3_SESSION_TOKEN"]
        inputs (assoc (zipmap names (repeat "fake-test-only"))
                      "JOLT_CHDB_S3_PREFIX" "ci/jolt-chdb/test-only")
        seen (atom nil)]
    (with-redefs-fn
      {#'qualification/required-env inputs
       #'s3/s3-backend (fn [opts] (reset! seen opts) :stubbed-s3)
       #'local/local-backend (fn [_] (throw (ex-info "Local fallback forbidden" {})))}
      #(is (= :stubbed-s3
              (#'qualification/namespace-backend "s3" "/tmp/not-used"))))
    (is (= #{:endpoint :bucket :prefix :region :access-key :secret-key :session-token}
           (set (keys @seen))))
    (is (= "ci/jolt-chdb/test-only" (:prefix @seen)))))

(deftest local-backend-never-reads-provider-environment
  (with-redefs-fn
    {#'qualification/required-env (fn [_] (throw (ex-info "No provider env" {})))
     #'local/local-backend identity
     #'s3/s3-backend (fn [_] (throw (ex-info "No provider calls" {})))}
    #(is (= "/tmp/test-only/objects"
            (#'qualification/namespace-backend "local" "/tmp/test-only")))))
