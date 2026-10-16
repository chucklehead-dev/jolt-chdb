(ns jdbc.chdb-confirmation-profile-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [typed-storage-confirmation-profile :as profile]))

(defn thrown [f] (try (f) nil (catch Throwable e e)))

(deftest closed-selector-preserves-reader-and-default-options
  (doseq [[arm phase expected]
          [["baseline" "window" {}] ["candidate" "window" {:wal-upload-confirmation :sha256-single-put}]
           ["baseline" "reader" {}] ["candidate" "reader" {}]]]
    (is (= (merge {:other :retained} expected)
           ((profile/options-wrapper (constantly {:other :retained}) arm phase)))))
  (is (thrown #(profile/mode "arbitrary" "window")))
  (is (thrown #((profile/options-wrapper
                 (constantly {:wal-upload-confirmation :sha256-single-put}) "baseline" "window")))))

(deftest observers-preserve-real-results-errors-and-existing-observer
  (let [events (atom []) counts (atom (profile/initial-counts)) result {:status :published}
        owner (Object.) proof (backend/confirmed-upload-result owner "key" 3 "hash" {:status :created})
        publish (fn [s _ _ options]
                  (is (identical? owner s))
                  ((:phase-observe! options) {:phase :wal-immutable-put :status :complete})
                  (is (backend/confirmed-upload? proof owner "key" 3 "hash"))
                  result)]
    (with-redefs [control/publish-wal-file! publish]
      (is (identical? result
            (profile/with-observers! counts
              #(control/publish-wal-file! owner :token :path {:phase-observe! (fn [e] (swap! events conj e))})))))
    (is (= 1 (count @events)))
    (is (= 1 (:valid-proof-observations (profile/report! counts "candidate" "window")))))
  (let [counts (atom (profile/initial-counts)) error (ex-info "private-error" {})]
    (with-redefs [control/publish-wal-file! (fn [& _] (throw error))]
      (is (identical? error (thrown #(profile/with-observers! counts
                                     (fn [] (control/publish-wal-file! :s :t :p)))))))
    (is (= 1 (:publication-errors @counts)))
    (is (thrown #(profile/report! counts "baseline" "window")))))

(deftest selection-oracle-rejects-silent-fallback-and-vacuous-runs
  (let [base (merge (profile/initial-counts)
               {:scope :wal-upload-confirmation-selection :scope-returned true
                :arm "candidate" :phase "window" :mode :sha256-single-put
                :file-publications 1 :completed-publications 1 :put-completions 1
                :valid-proof-observations 1})]
    (is (= base (profile/validate! base "candidate" "window")))
    (doseq [bad [(assoc base :valid-proof-observations 0)
                 (assoc base :verification-completions 1)
                 (assoc base :completed-publications 0)
                 (assoc base :publication-errors 1)
                 (assoc base :unexpected "CANARY")]]
      (is (thrown #(profile/validate! bad "candidate" "window"))))
    (is (thrown #(profile/report! (atom (profile/initial-counts)) "candidate" "window")))
    (is (= :readback (:mode (profile/report! (atom (profile/initial-counts)) "candidate" "reader"))))
    (is (thrown #(profile/validate! (assoc base :arm "baseline" :mode :readback) "baseline" "window")))))

(deftest selected-publication-boundary-is-observed-and-empty-selection-is-not-a-write
  (let [counts (atom (profile/initial-counts)) selector (fn [_] nil)]
    (with-redefs [control/publish-wal-file-selected!
                  (fn [_ _ actual options]
                    (is (identical? selector actual))
                    (is (fn? (:phase-observe! options)))
                    nil)]
      (is (nil? (profile/with-observers! counts
                  #(control/publish-wal-file-selected! :store :token selector)))))
    (is (= (profile/initial-counts) @counts)))
  (let [counts (atom (profile/initial-counts))]
    (with-redefs [control/publish-wal-file-selected!
                  (fn [_ _ _ options]
                    (doseq [phase [:wal-immutable-put :wal-immutable-verify]]
                      ((:phase-observe! options) {:phase phase :status :complete}))
                    {:status :published})]
      (profile/with-observers! counts
        #(control/publish-wal-file-selected! :store :token identity {})))
    (is (= 1 (:file-publications (profile/report! counts "baseline" "window"))))))

(defn -main [& _]
  (let [r (run-tests 'jdbc.chdb-confirmation-profile-test)]
    (when (pos? (+ (:fail r) (:error r)))
      (throw (ex-info "Confirmation profile controls failed" {})))))
