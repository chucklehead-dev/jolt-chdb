(ns jdbc.chdb-owned-compact-stream-native-test
  "Explicit 26.9 streaming uncertainty probe; writer and reader run separately.
  Reader mode works with the stock executor and never calls the stream helper."
  (:require [db.jdbc] [jdbc.core :as jdbc] [jdbc.chdb :as chdb]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.local-posix :as local]
            [jdbc.chdb.owned-statement :as owned]))

(def header "insert into stream_sample (`n`, `label`) FORMAT JSONCompactEachRow\n")
(def expected [{:n 1 :label "ready?"} {:n 2 :label "quote\"slash\\"}])

(defn -main [mode root]
  (assert (and (#{"writer" "reader"} mode) root))
  (let [spec {:namespace-backend (local/local-backend (str root "/objects"))
              :object-id "owned-stream-uncertainty"
              :scratch-parent (str root "/scratch-" mode)}
        events (atom [])
        original chdb/execute-owned-any-with-query-buffer]
    (with-open [connection (jdbc/connection
                           (if (= mode "writer")
                             (durable/writer-dbspec (assoc spec :owner "stream-probe" :database "default"))
                             (durable/snapshot-dbspec spec)))]
      (when (= mode "writer")
        (jdbc/execute! connection "CREATE TABLE stream_sample (n UInt64, label String) ENGINE=MergeTree ORDER BY n")
        (durable/flush! connection)
        (let [before (durable/persistence-observation connection)
              statement (owned/try-snapshot
                         (.getBytes (str header "[1,\"ready?\"]\n[2,\"quote\\\"slash\\\\\"]\n") "UTF-8"))]
          (assert (= :confirmed (:state before)))
          (with-redefs [chdb/execute-owned-any-with-query-buffer
                        (fn [handle actual buffer]
                          (if (identical? statement actual)
                            (do
                              (chdb/execute-owned-compact-json-stream-with-query-buffer handle actual buffer header)
                              (swap! events conj :native-completed)
                              ;; Native mutation happened. Lose its completion
                              ;; acknowledgement before the writer can stage WAL.
                              (throw (ex-info "synthetic lost completion" {:probe true})))
                            (original handle actual buffer)))]
            (let [failure (try (durable/execute-owned-and-flush! connection statement)
                               nil (catch Throwable error error))]
              (assert failure)
              (assert (= "synthetic lost completion" (.getMessage failure)))))
          (let [uncertain (durable/persistence-observation connection)]
            (assert (= :unconfirmed (:state uncertain)))
            (assert (false? (:view-current? uncertain)))
            (assert (= (:confirmed-sequence before) (:confirmed-sequence uncertain)))
            (swap! events conj :unconfirmed))
          (assert (= expected (jdbc/fetch connection "SELECT * FROM stream_sample ORDER BY n")))
          (durable/flush! connection)
          (let [settled (durable/persistence-observation connection)]
            (assert (= :confirmed (:state settled)))
            (assert (= :checkpoint (:confirmed-boundary settled)))
            (assert (> (:confirmed-sequence settled) (:confirmed-sequence before)))
            (swap! events conj :checkpoint-confirmed))
          (assert (= [:native-completed :unconfirmed :checkpoint-confirmed] @events))))
      (assert (= expected (jdbc/fetch connection "SELECT * FROM stream_sample ORDER BY n")))
      (prn {:mode mode :events @events :exact-rows true :passed true}))))
