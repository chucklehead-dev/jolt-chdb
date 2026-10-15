(ns jdbc.chdb-durable-selection-writer-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb-durable-writer-test-support :as support]))

(defn fixture [custom-publication?]
  (let [delegate (backend/memory-backend)
        token (:token (control/acquire! delegate support/base-options))
        reads (atom 0) hooks (atom 0) phases (atom [])
        closes (atom 0) calls (atom []) verify-failure? (atom false)
        store (reify backend/ObjectBackend
                (get-bytes [_ k] (backend/get-bytes delegate k))
                (get-with-etag [_ k]
                  (when (= k control/head-key) (swap! reads inc))
                  (backend/get-with-etag delegate k))
                (put-file-if-absent! [_ k p]
                  (backend/put-file-if-absent! delegate k p))
                (put-bytes-if-absent! [_ k b]
                  (backend/put-bytes-if-absent! delegate k b))
                (replace-if-match! [_ k b e]
                  (backend/replace-if-match! delegate k b e))
                (download-to-file! [_ k p]
                  (if @verify-failure?
                    (throw (ex-info "injected readback failure"
                                    {:type ::control/object-unverified}))
                    (backend/download-to-file! delegate k p))))
        operations
        (cond-> (assoc (support/model-checkpoint-operations calls closes)
                       :writer-phase! #(swap! phases conj (:phase %)))
          custom-publication?
          (assoc :publish-wal-file!
                 (fn [s t p]
                   (swap! hooks inc)
                   (if (fn? custom-publication?)
                     (custom-publication? s t p)
                     (control/publish-wal-file! s t p)))))
        w (writer/start! {:store store :token token :handle :fake-handle
                          :database "default"
                          :engine-metadata {:version "26.7.3" :backup-format 1
                                            :min-reader "26.7.3"}
                          :checkpoint-wal-reference-threshold 2
                          :operations operations})]
    {:writer w :store store :delegate delegate :token token :reads reads
     :hooks hooks :phases phases :closes closes :verify-failure? verify-failure?}))

(deftest selection-and-reference-generation-share-head-but-commit-is-fresh
  (doseq [custom? [false true]]
    (let [{:keys [writer reads hooks store closes]} (fixture custom?)]
      (try
        (writer/execute! writer "INSERT INTO t VALUES (1)")
        (reset! reads 0)
        (is (= :committed (:status (writer/flush! writer))))
        ;; Native execute is local; verification GET is an immutable object,
        ;; not head. The distinct commit head read MUST remain present.
        (is (= (if custom? 3 2) @reads))
        (is (= (if custom? 1 0) @hooks))
        (is (= 1 (count (get-in (:head (control/read-head! store))
                                ["manifest" "wal"]))))
        (is (= 0 (:pending-statements (writer/status writer))))
        (finally (writer/close! writer)))
      (is (= 1 @closes)))))

(deftest threshold-diversion-does-not-seal-another-wal
  (let [{:keys [writer store phases]} (fixture false)]
    (try
      (writer/execute! writer "INSERT INTO t VALUES (1)")
      (is (= :committed (:status (writer/flush! writer))))
      (is (= 1 (count (filter #{:wal-join} @phases))))
      (reset! phases [])
      (writer/execute! writer "INSERT INTO t VALUES (2)")
      (is (= :committed (:status (writer/flush! writer))))
      (is (not-any? #{:wal-join} @phases))
      (let [document (:head (control/read-head! store))]
        (is (= 2 (get-in document ["manifest" "seq"])))
        (is (some? (get-in document ["manifest" "base"])))
        (is (= [] (get-in document ["manifest" "wal"]))))
      (is (= 0 (:pending-statements (writer/status writer))))
      (finally (writer/close! writer)))))

(deftest malformed-optional-operation-rejects-before-worker-start
  (let [closes (atom 0)]
    (is (= ::writer/invalid-options
           (try
             (writer/start!
              {:store (backend/memory-backend) :token {} :handle :fake-handle
               :database "default"
               :operations (assoc (support/fake-operations (atom []) closes)
                                  :publish-wal-file-selected! :not-callable)})
             nil
             (catch Throwable e (:type (ex-data e))))))
    (is (zero? @closes))))

(deftest legacy-nil-publication-is-not-a-checkpoint-selection
  (let [{:keys [writer store closes]} (fixture (fn [& _] nil))]
    (try
      (writer/execute! writer "INSERT INTO t VALUES (1)")
      (is (= ::control/object-unverified
             (try (writer/flush! writer) nil
                  (catch Throwable e (:type (ex-data e))))))
      (let [document (:head (control/read-head! store))]
        (is (= 0 (get-in document ["manifest" "seq"])))
        (is (nil? (get-in document ["manifest" "base"]))))
      (is (= 1 (:pending-statements (writer/status writer))))
      (finally
        ;; Close still performs owned cleanup after this deliberately broken
        ;; custom publisher's final flush fails; no successful close is claimed.
        (try (writer/close! writer) (catch Throwable _))))
    (is (= 1 @closes))))

(deftest selected-readback-failure-retains-spool-until-confirmed-retry
  (let [{:keys [writer store verify-failure?]} (fixture false)]
    (try
      (writer/execute! writer "INSERT INTO t VALUES (1)")
      (reset! verify-failure? true)
      (is (= ::control/object-unverified
             (try (writer/flush! writer) nil
                  (catch Throwable e (:type (ex-data e))))))
      (is (= 1 (:pending-statements (writer/status writer))))
      (is (true? (get-in @(:wal-state writer) [:spool :sealed?])))
      (is (= 0 (get-in (:head (control/read-head! store)) ["manifest" "seq"])))
      (reset! verify-failure? false)
      (is (= :committed (:status (writer/flush! writer))))
      (is (= 0 (:pending-statements (writer/status writer))))
      (is (nil? (:spool @(:wal-state writer))))
      (is (= 1 (get-in (:head (control/read-head! store)) ["manifest" "seq"])))
      (finally
        (reset! verify-failure? false)
        (writer/close! writer)))))

(defn -main [& _]
  (let [r (run-tests 'jdbc.chdb-durable-selection-writer-test)]
    (when-not (zero? (+ (:fail r) (:error r)))
      (throw (ex-info "WAL selection writer controls failed" r)))))
