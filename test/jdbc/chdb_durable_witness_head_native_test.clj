(ns jdbc.chdb-durable-witness-head-native-test
  "Run the same witnessed CAS race controls through actual libcurl requests."
  (:require [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.s3-curl :as curl]
            [jdbc.chdb-durable-witness-head-test :as witness-tests]))

(defn -main [endpoint]
  (let [scopes (atom 0) requests (atom 0) request! curl/request!]
    ;; The controls construct their store through this factory. Substitute only
    ;; in this isolated synchronous test process; no production factory changes.
    ;; Each case needs its own object namespace and lease, never shared state.
    (with-redefs [curl/request! (fn [request]
                                 (swap! requests inc)
                                 (request! request))
                  backend/memory-backend
                  (fn []
                    (backend/object-backend
                     (curl/s3-backend
                      {:endpoint endpoint :bucket "bucket"
                       :prefix "witness-head" :region "us-east-1"
                       :access-key "ACCESS" :secret-key "SECRET"
                       :session-token "SESSION" :max-attempts 1
                       :connect-timeout-ms 2000 :timeout-ms 10000})
                     (str "case-" (swap! scopes inc))))]
      (witness-tests/run-checks!))
    ;; A passing memory-only run is not evidence of exercising the transport.
    (assert (= 6 @scopes))
    (assert (>= @requests 30))
    (prn {:scope :loopback-witnessed-cas :cases @scopes :requests @requests})))
