(ns jdbc.chdb-durable-confirmation-metrics-native-test
  "Replay the actual TLS capability/fault controls through benchmark wrappers."
  (:require [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb.durable.s3-curl :as curl]
            [jdbc.chdb-durable-throughput-metrics :as metrics]
            [jdbc.chdb-durable-upload-confirmation-native-test :as native-controls]))

(defn -main [endpoint ca-path]
  (let [recorder (metrics/recorder)
        native-factory curl/request-function
        backend-factory s3/s3-backend]
    ;; These exact wrappers observe scalar counts only and return the original
    ;; response. The native tests must still select confirmation, not fallback.
    (with-redefs [curl/request-function
                  (fn [options]
                    (metrics/instrument-transport recorder (native-factory options)))
                  s3/s3-backend
                  (fn [options]
                    (metrics/instrument-backend recorder (backend-factory options)))]
      (native-controls/-main endpoint ca-path))
    (let [report (metrics/report recorder)
          upload-calls (get-in report [:logical :setup :put-file-with-checksum-if-absent :calls] 0)
          confirmed-calls (get-in report [:logical :setup :put-file-with-checksum-if-absent
                                         :results :checksum-confirmed-created] 0)]
      (metrics/assert-transport-coverage! report)
      (metrics/assert-redacted! report "" ""
        ["ACCESS" "SECRET" "SESSION" "checksum-qualified" "checksum-conflict"
         "checksum-changed-file" "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"])
      ;; Exactly the qualified case has sealed checksum evidence. Missing and
      ;; composite cases are :created but must not be called checksum-confirmed.
      ;; The replayed native controls separately require zero verification GETs
      ;; for that qualified case, proving the seal survives all wrapper scopes.
      (when-not (= 1 confirmed-calls)
        (throw (ex-info "Instrumented checksum confirmation was not selected" {})))
      (println {:instrumented-tls-confirmation-selected true
                :logical-checksum-upload-calls upload-calls
                :checksum-confirmed-created confirmed-calls}))))
