(ns jdbc.chdb-durable-upload-confirmation-native-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb.durable.s3-checksum :as checksum]
            [jdbc.chdb.durable.s3-curl :as curl]
            [jdbc.chdb-durable-selected-publication-test :as selected]
            [jdbc.chdb-durable-s3-checksum-test :as checksum-test]
            [jdbc.chdb-durable-upload-confirmation-test :as confirmation-test])
  (:import [java.nio.file Files OpenOption]))

(def endpoint (atom nil))

(defn- store [prefix enabled? timeout-ms]
  (backend/object-backend
    (curl/s3-backend
      (cond-> {:endpoint @endpoint :bucket "bucket" :prefix prefix
               :region "us-east-1" :access-key "ACCESS" :secret-key "SECRET"
               :session-token "SESSION" :max-attempts 1 :connect-timeout-ms 2000
               :timeout-ms timeout-ms}
        enabled? (assoc :wal-upload-confirmation :sha256-single-put)))
    "object"))

(deftest real-single-put-confirmation-and-unchanged-reader
  (selected/with-file
    (fn [path]
      (doseq [[prefix enabled? expected-verifies]
              [["checksum-qualified" true 0] ["checksum-default" false 1]
               ["checksum-missing" true 1] ["checksum-composite" true 1]]]
        (let [s (store prefix enabled? 10000)
              token (:token (control/acquire! s selected/options))
              events (atom [])
              publication (control/publish-wal-file! s token path
                            {:phase-observe! #(swap! events conj %)})]
          (is (= :published (:status publication)))
          (is (= expected-verifies
                 (count (filter #(and (= :wal-immutable-verify (:phase %))
                                      (= :complete (:status %))) @events))))
          (is (= :committed
                 (:status (control/commit-reference! s token
                            {:kind :wal :reference (:reference publication)
                             :verified-publication publication
                             :verify-reference! (fn [& _] (throw (ex-info "duplicate verification" {})))}))))
          (is (control/verify-file-reference! s (:reference publication))))))))

(deftest ambiguous-real-upload-retains-readback
  (selected/with-file
    (fn [path]
      (let [s (store "timeout-after-commit" true 100)
            token (:token (control/acquire! s selected/options))
            events (atom [])
            publication (control/publish-wal-file! s token path
                          {:phase-observe! #(swap! events conj %)})]
        (is (= :reconciled (:status publication)))
        (is (some #(= :wal-immutable-verify (:phase %)) @events))
        (is (control/verify-file-reference! s (:reference publication)))))))

(deftest conflicting-real-response-does-not-publish-or-advance-head
  (selected/with-file
    (fn [path]
      (let [s (store "checksum-conflict" true 10000)
            token (:token (control/acquire! s selected/options))
            error (try (control/publish-wal-file! s token path) nil
                       (catch Throwable e e))]
        (is (= ::checksum/invalid-confirmation (:type (ex-data error))))
        (is (= 0 (get-in (control/read-head! s) [:head "manifest" "seq"])))))))

(deftest changed-file-is-rejected-by-provider-before-confirmation
  (selected/with-file
    (fn [path]
      (let [s (store "checksum-changed-file" true 10000)
            token (:token (control/acquire! s selected/options))
            original backend/put-file-with-checksum-if-absent!
            changed (atom false)
            error (with-redefs [backend/put-file-with-checksum-if-absent!
                               (fn [b k p n sha]
                                 (reset! changed true)
                                 (Files/write p (.getBytes "abd" "UTF-8") (make-array OpenOption 0))
                                 (original b k p n sha))]
                    (try (control/publish-wal-file! s token path) nil
                         (catch Throwable e e)))]
        (is @changed)
        (is (= [::s3/provider :put-file-with-checksum-if-absent 400]
               ((juxt :type :operation :status) (ex-data error))))
        (is (= 0 (get-in (control/read-head! s) [:head "manifest" "seq"])))))))

(defn -main [url ca-path]
  (reset! endpoint url)
  ;; Test trust must be supplied explicitly; never disable peer/host checks.
  (let [probe {:method :get :url (str url "/__fixture__/stats") :response-body :bytes
               :auth {:access-key "ACCESS" :secret-key "SECRET" :session-token "SESSION"}
               :region "us-east-1" :connect-timeout-ms 2000 :timeout-ms 10000}
        code (try (curl/request! probe) nil (catch Throwable e (:curl-code (ex-data e))))]
    (when-not (= 60 code) (throw (ex-info "Untrusted test CA was not rejected" {}))))
  (let [original @#'curl/set-string!]
    (with-redefs-fn
      {#'curl/set-string!
       (fn [handle option value stage]
         (when (= stage :url) (original handle 10065 ca-path :test-ca))
         (original handle option value stage))}
      (fn []
        (checksum-test/run-checks!)
        (confirmation-test/run-checks!)
        (let [result (run-tests 'jdbc.chdb-durable-upload-confirmation-native-test)]
          (when (pos? (+ (:fail result) (:error result)))
            (throw (ex-info "Native upload confirmation controls failed" {}))))))))
