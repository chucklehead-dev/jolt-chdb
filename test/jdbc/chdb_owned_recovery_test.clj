(ns jdbc.chdb-owned-recovery-test
  (:require [clojure.data.json :as json]
            [jdbc.chdb.owned-statement :as owned]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.local-posix :as local]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.writer :as writer]))

(defn- write! [root]
  (let [store (backend/memory-backend)
        frozen (local/local-backend (str root "/frozen"))
        w (durable/open-writer! {:store store :owner "owned-recovery"
                                 :instance "writer" :database "sample"
                                 :scratch-parent root :lease-ttl-ms 60000})
        sql "INSERT INTO samples VALUES (1, 'ready?'), (2, 'line\\nquote\\\"')"
        bytes (.getBytes sql "UTF-8")
        statement (owned/try-snapshot bytes)]
    (try
      (writer/execute-and-flush! w
        "CREATE TABLE samples (n UInt32, s String) ENGINE=MergeTree ORDER BY n")
      (aset-byte bytes 0 (byte 88))
      (with-redefs [owned/text (fn [_] (throw (ex-info "Unexpected SQL text conversion" {})))]
        (writer/execute-owned-and-flush! w statement))
      (assert (= :confirmed (:state (writer/persistence-observation w))))
      ;; Freeze the committed manifest BEFORE normal close can checkpoint.
      ;; Only objects reachable from this head are copied; reader is a fresh
      ;; process using ordinary V1 parsing, not the owned snapshot decoder.
      (let [head (:head (control/read-head! store))
            refs (get-in head ["manifest" "wal"])
            base (get-in head ["manifest" "base"])
            last-line (json/read-str (String. (backend/get-bytes store (get (last refs) "key")) "UTF-8"))]
        (assert (seq refs))
        (assert (= sql (get last-line "sql")))
        (doseq [reference (cond-> (vec refs) base (conj base))]
          (assert (= :created (:status (backend/put-bytes-if-absent!
                                        frozen (get reference "key")
                                        (backend/get-bytes store (get reference "key")))))))
        (assert (= :created (:status (backend/put-bytes-if-absent!
                                      frozen control/head-key
                                      (backend/get-bytes store control/head-key))))))
      (println :owned-writer-confirmed :frozen-wal true)
      (finally (writer/close! w)))))

(defn- read! [root]
  (let [r (durable/open-reader! {:store (local/local-backend (str root "/frozen"))
                                 :scratch-parent root})]
    (try
      (let [rows (:rows (reader/query! r "SELECT n, s FROM samples ORDER BY n" []))]
        (assert (= [["1" "ready?"] ["2" "line\nquote\""]]
                   (mapv (fn [[n s]] [(str n) s]) rows))))
      (println :owned-fresh-stock-reader :rows 2 :passed true)
      (finally (reader/close! r)))))

(defn -main [phase root]
  (case phase "writer" (write! root) "reader" (read! root)))
