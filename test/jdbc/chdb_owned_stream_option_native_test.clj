(ns jdbc.chdb-owned-stream-option-native-test
  "Public writer option, positive stream/fallback witnesses and late-failure recovery."
  (:require [db.jdbc] [jdbc.core :as jdbc] [jdbc.chdb.native :as native]
            [jdbc.chdb.owned-statement :as owned] [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.local-posix :as local] [clojure.data.json :as json]))

(def ticks [0 1 999999999 1000000000 1700000000123456789 9223372036854775807])
(def header "insert into stream_api (`id`, `t`, `events`) SETTINGS input_format_read_datetime_number_as_raw_value=1 FORMAT JSONCompactEachRow\n")
(def query "SELECT id, toString(toUnixTimestamp64Nano(t)) AS ticks, arrayMap(x -> toString(toUnixTimestamp64Nano(x)), events) AS events FROM stream_api ORDER BY id")
(defn expected [copies]
  (conj (vec (mapcat (fn [i n] (repeat copies {:id i :ticks (str n) :events (mapv str ticks)})) (range) ticks))
        {:id 6 :ticks "1" :events ["0" "1"]}))

(defn -main [role outcome root]
  (assert (and (#{"writer" "reader"} role) (#{"success" "uncertain"} outcome) root))
  (let [scratch (str root "/scratch-" role) _ (.mkdirs (java.io.File. scratch))
        spec {:namespace-backend (local/local-backend (str root "/objects"))
              :object-id "owned-stream-api" :scratch-parent scratch}
        copies (if (= outcome "uncertain") 2 1)
        stream-calls (atom 0) original-open native/chdb-stream-insert-n]
    (with-open [connection (jdbc/connection
                           (if (= role "writer")
                             (durable/writer-dbspec (assoc spec :owner "stream-option" :database "default"
                                                          :owned-compact-stream? true))
                             (durable/snapshot-dbspec spec)))]
      (when (= role "writer")
        (jdbc/execute! connection "CREATE TABLE stream_api (id UInt8, t DateTime64(9), events Array(DateTime64(9))) ENGINE=MergeTree ORDER BY id")
        (durable/flush! connection)
        (let [payload (apply str (map-indexed #(str (json/write-str [%1 %2 ticks]) "\n") ticks))
              source (.getBytes (str header payload) "UTF-8") statement (owned/try-snapshot source)]
          (assert (owned/statement? statement))
          (aset-byte source 0 (byte 88))
          ;; Observe real C ABI entry only; no replacement of execution,
          ;; classification, admission, WAL, lease, or publication operations.
          (with-redefs [native/chdb-stream-insert-n (fn [& args] (swap! stream-calls inc) (apply original-open args))]
            (assert (= :committed (:status (durable/execute-owned-and-flush! connection statement))))
            (assert (= 1 @stream-calls))
            (assert (= :wal (:confirmed-boundary (durable/persistence-observation connection))))
            ;; Uppercase header is not the closed producer shape: same owned
            ;; API must keep normal query execution and exact replay semantics.
            (let [fallback (owned/try-snapshot
                            (.getBytes "INSERT INTO stream_api (id,t,events) SETTINGS input_format_read_datetime_number_as_raw_value=1 FORMAT JSONCompactEachRow\n[6,1,[0,1]]\n" "UTF-8"))]
              (assert (= :committed (:status (durable/execute-owned-and-flush! connection fallback))))
              (assert (= 1 @stream-calls)))
            (when (= outcome "uncertain")
              (let [before (durable/persistence-observation connection)
                    destroy native/chdb-destroy-insert-stream
                    error (with-redefs [native/chdb-destroy-insert-stream
                                        (fn [stream]
                                          (destroy stream)
                                          ;; Result already consumed/released,
                                          ;; stream actually retired. Lose ack.
                                          (throw (ex-info "synthetic lost completion" {})))]
                            (try (durable/execute-owned-and-flush! connection statement)
                                 nil (catch Throwable error error)))]
                (assert (= "synthetic lost completion" (.getMessage error)))
                (assert (= 2 @stream-calls))
                (assert (= :unconfirmed (:state (durable/persistence-observation connection))))
                (assert (= (:confirmed-sequence before) (:confirmed-sequence (durable/persistence-observation connection))))
                (durable/flush! connection)
                (let [after (durable/persistence-observation connection)]
                  (assert (= :confirmed (:state after)))
                  (assert (= :checkpoint (:confirmed-boundary after)))
                  (assert (> (:confirmed-sequence after) (:confirmed-sequence before)))))))))
      (assert (= (expected copies) (jdbc/fetch connection query)))
      (assert (= "0" (:value (jdbc/fetch-one connection "SELECT value FROM system.settings WHERE name='input_format_read_datetime_number_as_raw_value'"))))
      (prn {:role role :outcome outcome :copies copies :stream-calls @stream-calls :exact-rows true :passed true}))))
