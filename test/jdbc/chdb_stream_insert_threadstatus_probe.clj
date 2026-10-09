(ns jdbc.chdb-stream-insert-threadstatus-probe
  (:require [db.jdbc]
            [jdbc.chdb :as chdb]
            [jdbc.core :as jdbc]))

(defn -main [& _]
  ;; Repeatedly exercise the complete native streaming-insert lifecycle in one
  ;; process. A pseudo-terminal wrapper captures ClickHouse diagnostics that do
  ;; not travel through the JDBC exception path.
  (dotimes [iteration 24]
    ;; The current driver retains an engine anchor across logical last close.
    ;; Unique names exercise reconnect without assuming memory-table reset.
    (let [table (str "streamed_" iteration)]
     (with-open [conn (jdbc/connection "chdb::memory:")]
      (jdbc/execute! conn
                     (str "create table " table " (iteration UInt64, chunk UInt64, label String) engine=Memory"))
      (chdb/stream-insert!
       conn
       (str "insert into " table)
       [(str "{\"iteration\":" iteration
             ",\"chunk\":0,\"label\":\"string\"}\n")
        (byte-array
         (map int
              (.getBytes
               (str "{\"iteration\":" iteration
                    ",\"chunk\":1,\"label\":\"bytes\"}\n"))))])
      (when-not (= [{:iteration iteration :chunk 0 :label "string"}
                    {:iteration iteration :chunk 1 :label "bytes"}]
                   (jdbc/fetch conn
                               (str "select iteration, chunk, label from " table " order by chunk")))
        (throw (ex-info "streaming-insert ThreadStatus probe readback mismatch"
                        {:iteration iteration}))))))
  (println "PASS: streaming-insert ThreadStatus probe"))
