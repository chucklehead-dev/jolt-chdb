(ns jdbc.chdb-durable-selected-publication-native-test
  (:require [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.s3-curl :as curl]
            [jdbc.chdb-durable-selected-publication-test :as selected]))

(defn -main [endpoint]
  (let [scopes (atom 0) requests (atom 0) request! curl/request!]
    (with-redefs [curl/request! (fn [request]
                                 (swap! requests inc)
                                 (request! request))
                  backend/memory-backend
                  (fn []
                    (backend/object-backend
                     (curl/s3-backend
                      {:endpoint endpoint :bucket "bucket"
                       :prefix "selected-publication" :region "us-east-1"
                       :access-key "ACCESS" :secret-key "SECRET"
                       :session-token "SESSION" :max-attempts 1
                       :connect-timeout-ms 2000 :timeout-ms 10000})
                     (str "case-" (swap! scopes inc))))]
      (selected/-main))
    ;; Positive transport witnesses reject an accidentally memory-only pass.
    (assert (= 13 @scopes))
    (assert (>= @requests 40))
    (prn {:scope :loopback-selected-publication :cases @scopes :requests @requests})))
