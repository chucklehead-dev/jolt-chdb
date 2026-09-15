(ns jdbc.chdb-stream-insert-threadstatus-probe
  (:require [db.jdbc]
            [jdbc.chdb :as chdb]
            [jdbc.core :as jdbc]))

(defn -main [& _]
  ;; Repeatedly exercise the complete native streaming-insert lifecycle in one
  ;; process. A pseudo-terminal wrapper captures ClickHouse diagnostics that do
  ;; not travel through the JDBC exception path.
  (dotimes [iteration 24]
    (with-open [conn (jdbc/connection "chdb::memory:")]
      (jdbc/execute! conn
                     "create table streamed (iteration UInt64, chunk UInt64, label String) engine=Memory")
      (chdb/stream-insert!
       conn
       "insert into streamed"
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
                               "select iteration, chunk, label from streamed order by chunk"))
        (throw (ex-info "streaming-insert ThreadStatus probe readback mismatch"
                        {:iteration iteration})))))
  (println "PASS: streaming-insert ThreadStatus probe"))
