(ns jdbc.chdb-exporter-storage-qualification-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.data.json.jolt-native :as native]
            [clojure.string :as str]
            [jdbc.chdb-exporter-storage-qualification :as qualification]
            [jdbc.chdb.durable.local-posix :as local]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb-durable-throughput-metrics :as metrics]
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

(deftest mismatched-loaded-source-is-rejected
  (with-redefs-fn
    {#'qualification/required-env (fn [_] "/tmp/qualification-unexpected-source-root")
     #'native/load-writer! (fn [] nil)}
    #(let [error (try
                   (#'qualification/provenance!
                     (str "/tmp/qualification-negative-" (java.util.UUID/randomUUID)))
                   nil
                   (catch AssertionError error error))]
       (is (some? error))
       ;; A later resource/hash failure must not make this source guard vacuous.
       (is (and error
                (str/includes? (str error)
                               "Benchmark resolved a different source checkout"))))))

(deftest observed-s3-keeps-production-semantics-and-redacted-coverage
  (let [recorder (metrics/recorder)
        inputs (fn [name]
                 (case name
                   "JOLT_CHDB_S3_PREFIX" "ci/jolt-chdb/test-only"
                   "JOLT_CHDB_S3_ENDPOINT" "https://synthetic-endpoint.invalid"
                   "JOLT_CHDB_S3_REGION" "us-east-2"
                   "synthetic-secret-canary"))
        calls (atom 0)]
    (metrics/set-phase! recorder :writer-run)
    (with-redefs-fn
      {#'qualification/required-env inputs
       #'s3/request-function
       (fn [_]
         (fn [_]
           (swap! calls inc)
           {:status 200 :headers {"etag" "synthetic-etag-canary"}
            :body (.getBytes "synthetic-body-canary" "UTF-8")}))
       #'local/local-backend (fn [_] (throw (ex-info "Local fallback forbidden" {})))}
      (fn []
        (let [observed (#'qualification/namespace-backend "s3" "/tmp/not-used" recorder)]
          (is (= "synthetic-body-canary"
                 (String. (backend/get-bytes observed "synthetic-key-canary") "UTF-8"))))))
    (let [report (metrics/report recorder)]
      (is (= 1 @calls))
      (is (= 1 (get-in report [:logical :writer-run :get :calls])))
      (is (= 1 (get-in report [:transport :writer-run :get :calls])))
      (is (nil? (metrics/assert-transport-coverage! report)))
      (is (nil? (metrics/assert-redacted!
                  report "" "" ["synthetic-secret-canary" "synthetic-etag-canary"
                                 "synthetic-body-canary" "synthetic-key-canary"]))))))

(deftest local-attribution-does-not-wrap-or-erase-provider-capabilities
  (let [delegate (backend/memory-backend)]
    (with-redefs-fn
      {#'qualification/required-env (fn [_] (throw (ex-info "No provider env" {})))
       #'local/local-backend (constantly delegate)}
      #(is (identical? delegate
                       (#'qualification/namespace-backend
                         "local" "/tmp/not-used" (metrics/recorder)))))))
