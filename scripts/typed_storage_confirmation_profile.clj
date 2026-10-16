(ns typed-storage-confirmation-profile
  "Closed benchmark selector and scalar selection witnesses; never retain data."
  (:require [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [typed-storage-confirmation-receipt :as receipt]))

(def mode receipt/mode)
(def initial-counts receipt/initial-counts)
(def validate! receipt/validate!)

(defn options-wrapper [original arm phase]
  (let [selected (mode arm phase)]
    (fn []
      (let [options (original)]
        (when (contains? options :wal-upload-confirmation)
          (throw (ex-info "Unexpected benchmark confirmation configuration" {})))
        (cond-> options
          (= selected :sha256-single-put)
          (assoc :wal-upload-confirmation selected))))))

(defn with-observers! [counts f]
  ;; Isolated process only. f must close/join all owners before returning.
  ;; Observe the real publication boundary, without replacing its result,
  ;; evidence predicate, CAS, reader, or renewal decisions.
  (let [publish! control/publish-wal-file!
        selected! control/publish-wal-file-selected!
        confirmed? backend/confirmed-upload?
        observed
        (fn [publish store token path-or-selector options]
          (let [observer (:phase-observe! options)]
            (try
              (let [result
                    (publish store token path-or-selector
                      (assoc options :phase-observe!
                        (fn [event]
                          (when observer (observer event))
                          (when (= :complete (:status event))
                            (case (:phase event)
                              :wal-immutable-put (swap! counts update :put-completions inc)
                              :wal-immutable-verify (swap! counts update :verification-completions inc)
                              nil)))))]
                (when result (swap! counts update :file-publications inc))
                (when (contains? #{:published :reconciled} (:status result))
                  (swap! counts update :completed-publications inc))
                result)
              (catch Throwable error
                (swap! counts update :publication-errors inc)
                (throw error)))))]
    (with-redefs [control/publish-wal-file!
                  (fn
                    ([store token path] (observed publish! store token path {}))
                    ([store token path options] (observed publish! store token path options)))
                  control/publish-wal-file-selected!
                  (fn
                    ([store token selector] (observed selected! store token selector {}))
                    ([store token selector options] (observed selected! store token selector options)))
                  backend/confirmed-upload?
                  (fn [& args]
                    (let [result (apply confirmed? args)]
                      (when result (swap! counts update :valid-proof-observations inc))
                      result))]
      (f))))

(defn report! [counts arm phase]
  (validate! (merge @counts {:scope :wal-upload-confirmation-selection
                            :mode (mode arm phase) :arm arm :phase phase
                            :scope-returned true}) arm phase))
