(ns jdbc.chdb-s3-expect-native-test
  (:require [jdbc.chdb.durable.s3-curl :as curl]))

(defn -main [endpoint]
  (let [bytes (byte-array 1048577)
        send (fn [wait]
               (let [start (System/nanoTime)
                     response (curl/request!
                               {:method :put :url (str endpoint "/bucket/ignore-continue/" wait)
                                :request-body {:bytes bytes :byte-count 1048577}
                                :headers {"x-amz-content-sha256" "UNSIGNED-PAYLOAD"}
                                :auth {:access-key "ACCESS" :secret-key "SECRET"
                                       :session-token "SESSION"}
                                :region "us-east-1" :timeout-ms 5000
                                :expect-100-timeout-ms wait})]
                 ;; Fixture returns200 only for the exact byte count and body.
                 (assert (= 200 (:status response)))
                 (/ (- (System/nanoTime) start) 1e6)))
        baseline (send 1000)
        candidate (send 10)]
    (assert (> baseline 800))
    (assert (< candidate 500))
    (prn {:scope :native-libcurl-ignored-continue-not-aws
          :body-bytes 1048577 :exact-body true
          :default-ms baseline :bounded-wait-ms candidate})))
