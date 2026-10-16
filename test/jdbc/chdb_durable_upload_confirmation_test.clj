(ns jdbc.chdb-durable-upload-confirmation-test
  "Semantic capability controls; the provider premise is deliberately trusted.
  These tests do not qualify a real HTTP provider or native persistence."
  (:require [clojure.test :refer [deftest is run-tests testing]]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb-durable-selected-publication-test :as selected]))

(defn- fixture [mode]
  (let [delegate (backend/memory-backend)
        counts (atom {:put 0 :confirm 0 :readback 0})
        namespace
        (reify
          backend/ObjectBackend
          (get-bytes [_ key] (backend/get-bytes delegate key))
          (get-with-etag [_ key] (backend/get-with-etag delegate key))
          (put-file-if-absent! [_ key path]
            (swap! counts update :put inc)
            (backend/put-file-if-absent! delegate key path))
          (put-bytes-if-absent! [_ key bytes]
            (backend/put-bytes-if-absent! delegate key bytes))
          (replace-if-match! [_ key bytes etag]
            (backend/replace-if-match! delegate key bytes etag))
          (download-to-file! [_ key path]
            (swap! counts update :readback inc)
            (backend/download-to-file! delegate key path))
          backend/ObjectUploadConfirmation
          (put-file-with-checksum-if-absent! [this key path n sha]
            (swap! counts update :confirm inc)
            (if (= :unsupported mode)
              {:status :unsupported}
              (let [_ (swap! counts update :put inc)
                    result (backend/put-file-if-absent! delegate key path)]
                (case mode
                  :plain (assoc result :verified true)
                  :wrong-store (backend/confirmed-upload-result (Object.) key n sha result)
                  :wrong-key (backend/confirmed-upload-result this (str key "-other") n sha result)
                  :wrong-size (backend/confirmed-upload-result this key (inc n) sha result)
                  :wrong-digest (backend/confirmed-upload-result this key n "wrong" result)
                  :ambiguous (assoc (backend/confirmed-upload-result this key n sha result)
                                    :status :ambiguous)
                  :exists (assoc (backend/confirmed-upload-result this key n sha result)
                                 :status :precondition-failed)
                  (backend/confirmed-upload-result this key n sha result))))))
        store (backend/object-backend namespace "object")
        token (:token (control/acquire! store selected/options))]
    {:store store :token token :counts counts}))

(defn- commit! [f publication]
  (control/commit-reference!
    (:store f) (:token f)
    {:kind :wal :reference (:reference publication) :verified-publication publication
     :verify-reference! (fn [& _] (throw (ex-info "unexpected commit verification" {})))}))

(deftest qualified-upload-scopes-and-commits-without-publication-readback
  (selected/with-file
    (fn [path]
      (let [f (fixture :qualified)
            publication (control/publish-wal-file! (:store f) (:token f) path)]
        (is (= :published (:status publication)))
        (is (= {:put 1 :confirm 1 :readback 0} @(:counts f)))
        (is (= :committed (:status (commit! f publication))))
        (is (= 0 (:readback @(:counts f))))
        ;; Reader verification is unchanged even after confirmed publication.
        (is (control/verify-file-reference! (:store f) (:reference publication)))
        (is (= 1 (:readback @(:counts f))))))))

(deftest unsupported-and-forged-or-misbound-evidence-retain-readback
  (selected/with-file
    (fn [path]
      (doseq [mode [:unsupported :plain :wrong-store :wrong-key :wrong-size :wrong-digest]]
        (testing (name mode)
          (let [f (fixture mode)
                publication (control/publish-wal-file! (:store f) (:token f) path)]
            (is (= :published (:status publication)))
            (is (= {:put 1 :confirm 1 :readback 1} @(:counts f)))
            (is (= :committed (:status (commit! f publication))))))))))

(deftest nested-scopes-rebind-evidence-at-every-boundary
  (selected/with-file
    (fn [path]
      (let [f (fixture :qualified)
            nested (backend/object-backend (:store f) "nested")
            token (:token (control/acquire! nested selected/options))
            publication (control/publish-wal-file! nested token path)]
        (is (= :published (:status publication)))
        (is (= {:put 1 :confirm 1 :readback 0} @(:counts f)))
        (is (= :committed (:status (commit! (assoc f :store nested :token token) publication))))
        (is (control/verify-file-reference! nested (:reference publication)))))))

(deftest ambiguous-and-existing-cannot-reuse-sealed-created-proof
  (selected/with-file
    (fn [path]
      (doseq [[mode expected] [[:ambiguous :reconciled] [:exists :already-published]]]
        (let [f (fixture mode)
              publication (control/publish-wal-file! (:store f) (:token f) path)]
          (is (= expected (:status publication)))
          (is (= {:put 1 :confirm 1 :readback 1} @(:counts f)))
          (is (= :committed (:status (commit! f publication)))))))))

(deftest confirmed-upload-does-not-confer-current-lease-authority
  (selected/with-file
    (fn [path]
      (let [f (fixture :qualified)
            publication (control/publish-wal-file! (:store f) (:token f) path)
            replacement (control/acquire! (:store f)
                          (assoc selected/options :owner "replacement" :instance "two"
                                 :now 300M :expires-at 400M))
            error (try (commit! f publication) nil (catch Throwable e e))]
        (is (some? (:token replacement)))
        (is (= ::control/lease-fenced (:type (ex-data error))))
        (is (= {:put 1 :confirm 1 :readback 0} @(:counts f)))))))

(defn run-checks! []
  (let [result (run-tests 'jdbc.chdb-durable-upload-confirmation-test)]
    (when (pos? (+ (:fail result) (:error result)))
      (throw (ex-info "Upload confirmation controls failed" {})))
    result))

(defn -main [& _] (run-checks!))
