(ns jdbc.chdb-durable-witness-head-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]))

(def options
  {:owner "writer" :instance "instance" :expires-at 200M :now 100M
   :clock-skew 5M :database "default" :engine-version "26.7.3"
   :backup-format 1 :min-reader "26.7.3"})

(defn fixture [mode]
  (let [delegate (backend/memory-backend)
        token (:token (control/acquire! delegate options))
        active? (atom false) head-reads (atom 0) replacements (atom 0)
        verify-calls (atom 0)
        store
        (reify backend/ObjectBackend
          (get-bytes [_ key] (backend/get-bytes delegate key))
          (get-with-etag [_ key]
            (let [snapshot (backend/get-with-etag delegate key)]
              (when (and @active? (= control/head-key key))
                (when (= 1 (swap! head-reads inc))
                  ;; The returned snapshot stays old; CAS must detect the
                  ;; actual intervening mutation, not trust its old ownership.
                  (case mode
                    :renew (control/renew! delegate token 300M)
                    :takeover (control/acquire!
                               delegate (assoc options :owner "other"
                                               :instance "other-instance"
                                               :now 256M :expires-at 400M))
                    nil)))
              snapshot))
          (put-file-if-absent! [_ key path]
            (backend/put-file-if-absent! delegate key path))
          (put-bytes-if-absent! [_ key bytes]
            (backend/put-bytes-if-absent! delegate key bytes))
          (replace-if-match! [_ key bytes etag]
            (when @active? (swap! replacements inc))
            (if (and @active? (= :ambiguous-drop mode))
              {:status :ambiguous}
              (let [result (backend/replace-if-match! delegate key bytes etag)]
                (if (and @active? (= :ambiguous-land mode)
                         (= :replaced (:status result)))
                  (do (control/renew! delegate token 300M)
                      {:status :ambiguous})
                  result))))
          (download-to-file! [_ key path]
            (backend/download-to-file! delegate key path)))
        publication (control/publish-wal-bytes! store token (.getBytes "abc" "UTF-8"))]
    (reset! active? true)
    {:store store :delegate delegate :token token :publication publication
     :head-reads head-reads :replacements replacements :verify-calls verify-calls}))

(defn commit! [f witnessed?]
  (control/commit-reference!
   (:store f) (:token f)
   {:kind :wal :reference (:reference (:publication f))
    :verified-publication (when witnessed? (:publication f))
    :verify-reference! (fn [store reference]
                         (swap! (:verify-calls f) inc)
                         (control/verify-byte-reference! store reference))
    :max-attempts 2 :await-backoff! (fn [_] nil)}))

(deftest witnessed-commit-needs-one-fresh-head-not-two
  (doseq [witnessed? [true false]]
    (let [f (fixture :normal) result (commit! f witnessed?)]
      (is (= :committed (:status result)))
      (is (= (if witnessed? 1 2) @(:head-reads f)))
      (is (= (if witnessed? 0 1) @(:verify-calls f)))
      (is (= 1 @(:replacements f)))
      (is (= [(:reference (:publication f))]
             (get-in (:head result) ["manifest" "wal"]))))))

(deftest renewal-between-read-and-cas-is-not-overwritten
  (let [f (fixture :renew) result (commit! f true)]
    (is (= :committed (:status result)))
    (is (= 2 @(:head-reads f)))
    (is (= 2 @(:replacements f)))
    (is (= 0 @(:verify-calls f)))
    (is (= [300 1 [(:reference (:publication f))]]
           [(get-in (:head result) ["lease" "expires_at"])
            (get-in (:head result) ["manifest" "seq"])
            (get-in (:head result) ["manifest" "wal"])]))))

(deftest takeover-between-read-and-cas-fences-without-advancement
  (let [f (fixture :takeover)]
    (is (= ::control/lease-fenced
           (try (commit! f true) nil
                (catch Throwable e (:type (ex-data e))))))
    (is (= 1 @(:replacements f)))
    (is (= 0 @(:verify-calls f)))
    (let [head (:head (control/read-head! (:delegate f)))]
      (is (= "other" (get-in head ["lease" "owner"])))
      (is (= 0 (get-in head ["manifest" "seq"])))
      (is (empty? (get-in head ["manifest" "wal"]))))))

(deftest ambiguous-cas-is-reconciled-never-reissued
  (doseq [mode [:ambiguous-land :ambiguous-drop]]
    (let [f (fixture mode)
          result (try (commit! f true)
                      (catch Throwable e (:type (ex-data e))))]
      (is (= 1 @(:replacements f)))
      (is (= 0 @(:verify-calls f)))
      (if (= :ambiguous-land mode)
        (do (is (= :reconciled (:status result)))
            (is (= [300 1]
                   [(get-in (:head result) ["lease" "expires_at"])
                    (get-in (:head result) ["manifest" "seq"])])))
        (do (is (= ::control/commit-ambiguous result))
            (is (= 0 (get-in (:head (control/read-head! (:delegate f)))
                              ["manifest" "seq"]))))))))

(defn run-checks! []
  (let [result (run-tests 'jdbc.chdb-durable-witness-head-test)]
    (when-not (zero? (+ (:fail result) (:error result)))
      (throw (ex-info "Verified WAL head-read controls failed" result)))
    true))

(defn -main [& _] (run-checks!))
