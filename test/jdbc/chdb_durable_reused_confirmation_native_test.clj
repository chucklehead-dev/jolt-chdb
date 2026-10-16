(ns jdbc.chdb-durable-reused-confirmation-native-test
  "Run the real TLS/fault controls through the measured lexical curl owner."
  (:require [jdbc.chdb.durable.s3-curl :as curl]
            [jdbc.chdb-durable-upload-confirmation-native-test :as native]))

(defn -main [url ca-path]
  (let [calls (atom 0)]
    (curl/with-reused-transport!
      (fn [send!]
        (with-redefs [curl/request-function
                      (fn [options]
                        (let [selected (select-keys options
                                        [:connect-timeout-ms :timeout-ms
                                         :max-response-bytes])]
                          ;; Transparent adapter, not a new provenance mint.
                          (with-meta
                            (fn [request]
                              (swap! calls inc)
                              (send! (merge selected request)))
                            (meta send!))))]
          (native/-main url ca-path))))
    (when-not (pos? @calls)
      (throw (ex-info "Reused confirmation control did not exercise transport" {})))
    (println {:reused-native-confirmation-controls true :requests @calls})))
