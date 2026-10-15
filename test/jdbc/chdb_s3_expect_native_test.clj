(ns jdbc.chdb-s3-expect-native-test
  (:require [jdbc.chdb.durable.s3-curl :as curl]
            [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb.durable.backend :as backend]))

(defn rejection-controls [endpoint bytes]
  (doseq [wait [1000 10]]
    (curl/with-reused-transport!
      (fn [owner]
        (let [send (curl/request-function {:expect-100-timeout-ms wait :timeout-ms 5000} owner)
              store (backend/object-backend
                     (s3/s3-backend {:endpoint endpoint :bucket "bucket" :prefix "continue-reject"
                                     :region "us-east-1" :access-key "ACCESS" :secret-key "SECRET"
                                     :session-token "SESSION" :max-attempts 1 :timeout-ms 5000
                                     :retry-deadline-ms 5000 :request! send})
                     "object")]
          (doseq [key ["early412" "late417"]]
            (let [before (backend/get-with-etag store key)
                  _ (assert (= "protected-original" (String. (:bytes before) "UTF-8")))
                  result (try (backend/put-bytes-if-absent! store key bytes)
                              (catch Throwable error {:error error}))
                  after (backend/get-with-etag store key)]
              (assert (not= :created (:status result)))
              (if (= key "early412")
                (assert (= :precondition-failed (:status result)))
                ;; A late417 can surface as an HTTP rejection or ambiguous
                ;; transport/rewind failure. It must never be a committed ACK.
                (assert (or (and (= ::s3/provider (:type (ex-data (:error result))))
                                 (= 417 (:status (ex-data (:error result)))))
                            (= :ambiguous (:status result)))))
              (assert (= (:etag before) (:etag after)))
              (assert (= "protected-original" (String. (:bytes after) "UTF-8")))))
          ;; A real new write/read after both failures uses the same owned handle.
          (let [key (str "success-after-rejection-" wait)
                value (.getBytes "after-rejection" "UTF-8")]
            (assert (= :created (:status (backend/put-bytes-if-absent! store key value))))
            (assert (= "after-rejection" (String. (:bytes (backend/get-with-etag store key)) "UTF-8")))))))))

(defn short-deadline-control [endpoint bytes]
  (let [start (System/nanoTime)
        error (try (curl/request!
                     {:method :put :url (str endpoint "/bucket/ignore-continue/deadline")
                      :request-body {:bytes bytes :byte-count 1048577}
                      :headers {"x-amz-content-sha256" "UNSIGNED-PAYLOAD"}
                      :auth {:access-key "ACCESS" :secret-key "SECRET" :session-token "SESSION"}
                      :region "us-east-1" :timeout-ms 100 :expect-100-timeout-ms 1000})
                   nil (catch Throwable e e))
        elapsed (/ (- (System/nanoTime) start) 1e6)]
    ;; CURLE_OPERATION_TIMEDOUT, not a mocked allocation/validation failure.
    (assert (= 28 (:curl-code (ex-data error))))
    (assert (< elapsed 800))))

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
    (short-deadline-control endpoint bytes)
    (rejection-controls endpoint bytes)
    (prn {:scope :native-libcurl-ignored-continue-not-aws
          :body-bytes 1048577 :exact-body true
          :default-ms baseline :bounded-wait-ms candidate})))
