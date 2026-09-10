(ns jdbc.chdb-durable-engine-metadata-itf-test
  (:require [clojure.data.json :as json]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb-durable-open-test-support :as support]))

(def failures (atom 0))

(def ^:private trace-path
  "formal/quint/traces/engine-metadata.itf.json")

(def ^:private initial-options
  {:owner "old-writer" :instance "old-instance"
   :expires-at 100M :now 0M :clock-skew 0M
   :database "default" :engine-version "26.6.0"
   :backup-format 0 :min-reader "26.6.0"})

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- bigint-value [value]
  (bigint (get value "#bigint")))

(defn- model-version [rank]
  (cond
    (= rank 1) "26.6.0"
    (= rank 2) "26.7.2-rc.2"
    :else
    (throw (ex-info "Unsupported engine-metadata model version rank"
                    {:type ::invalid-trace}))))

(defn- model-metadata [state]
  (let [metadata (get state "metadata")]
    {"version" (model-version
                (bigint-value (get metadata "producerVersion")))
     "backup_format" (bigint-value (get metadata "backupFormat"))
     "min_reader" (model-version
                   (bigint-value (get metadata "minReader")))}))

(defn- observed-metadata [store]
  (select-keys (get (:head (control/read-head! store)) "engine")
               ["version" "backup_format" "min_reader"]))

(defn- variant-tag [state key]
  (get-in state [key "tag"]))

(defn- replay! [trace]
  (let [states (get trace "states")]
    (when-not (= ["init" "takeover" "checkpoint"]
                 (mapv #(get % "mbt::actionTaken") states))
      (throw (ex-info "Engine-metadata ITF actions are not canonical"
                      {:type ::invalid-trace})))
    (let [store (backend/memory-backend)
          initial (:token (control/acquire! store initial-options))
          _ (control/release! store initial)
          calls (atom [])
          close-count (atom 0)
          cleanup-count (atom 0)
          operations (support/fake-open-operations
                      calls (atom [200000M 200001M])
                      close-count cleanup-count)
          opened (durable/open-writer!
                  {:store store :owner "new-writer" :instance "new-instance"
                   :database "ignored" :lease-ttl-ms 100000M
                   :operations operations})]
      (try
        (let [takeover-state (get-in states [1 "state"])
              takeover
              [(variant-tag takeover-state "transitionKind")
               (variant-tag takeover-state "expectedResult")
               (model-metadata takeover-state)
               (observed-metadata store)]]
          (writer/checkpoint! opened)
          (let [checkpoint-state (get-in states [2 "state"])]
            [takeover
             [(variant-tag checkpoint-state "transitionKind")
              (variant-tag checkpoint-state "expectedResult")
              (model-metadata checkpoint-state)
              (observed-metadata store)]]))
        (finally (writer/close! opened))))))

(defn run-checks! []
  (reset! failures 0)
  (println "Durable engine-metadata Quint ITF replay")
  (let [trace (json/read-str (slurp trace-path))
        observations (replay! trace)]
    (check "takeover replays the model producer transition"
           ["TakeoverTransition" "AppliedResult"
            {"version" "26.7.2-rc.2"
             "backup_format" 0
             "min_reader" "26.6.0"}
            {"version" "26.7.2-rc.2"
             "backup_format" 0
             "min_reader" "26.6.0"}]
           (first observations))
    (check "checkpoint replays all modeled compatibility metadata"
           ["CheckpointTransition" "AppliedResult"
            {"version" "26.7.2-rc.2"
             "backup_format" 1
             "min_reader" "26.7.2-rc.2"}
            {"version" "26.7.2-rc.2"
             "backup_format" 1
             "min_reader" "26.7.2-rc.2"}]
           (second observations)))
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " engine-metadata ITF checks failed")
                    {:failures @failures})))
  (println "all Durable engine-metadata ITF checks passed")
  true)

(defn -main [& _]
  (run-checks!))
