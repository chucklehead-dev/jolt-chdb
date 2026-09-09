(ns jdbc.chdb-durable-aspect-manifest-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]))

(def ^:private expected-epoch
  "4a0b82119a09fdadb08442cb5d189bdc0474ed86")

(def ^:private expected-selectors
  {:durable/acquire
   ['jdbc.chdb.durable.control/acquire! 2]
   :durable/publish-wal
   ['jdbc.chdb.durable.control/publish-wal-bytes! 4]
   :durable/publish-checkpoint
   ['jdbc.chdb.durable.control/publish-checkpoint-file! 4]
   :durable/commit-reference
   ['jdbc.chdb.durable.control/commit-reference! 3]
   :durable/renew
   ['jdbc.chdb.durable.control/renew! 4]
   :durable/release
   ['jdbc.chdb.durable.control/release! 3]})

(defn- fail! [message data]
  (throw (ex-info message data)))

(defn- target-manifest []
  (let [resource (io/resource
                  "META-INF/jolt/aspects/jolt-chdb-durable.edn")]
    (when-not resource
      (fail! "The target-owned Durable aspect manifest is missing"
             {:type ::missing-manifest}))
    (edn/read-string (slurp resource))))

(defn run-checks! []
  (println "Durable target-owned aspect manifest")
  (let [manifest (target-manifest)
        selectors
        (into {}
              (map (fn [{:keys [id match]}]
                     [id [(:entry match) (:arity match)]]))
              (:aspects manifest))]
    (when-not (= {:id 'io.github.chucklehead-dev/jolt-chdb
                  :version expected-epoch}
                 (:library manifest))
      (fail! "The Durable aspect manifest has the wrong seam epoch"
             {:type ::wrong-epoch}))
    (when-not (= expected-selectors selectors)
      (fail! "The Durable aspect manifest has stale control selectors"
             {:type ::wrong-selectors}))
    (when-not (every? #(= {:matches 1} (:expect %)) (:aspects manifest))
      (fail! "Every Durable aspect selector must match exactly once"
             {:type ::wrong-match-expectation}))
    (println "  ok   retry-aware terminal arities define one seam epoch")
    true))

(defn -main [& _]
  (run-checks!))
