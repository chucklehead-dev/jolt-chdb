(ns jdbc.chdb-durable-s3-headers-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.durable.s3-headers :as headers]))

(defn- parsed [lines status]
  (headers/final-headers (reduce headers/accept-line headers/initial-state lines) status))

(defn- error [lines status]
  (try (parsed lines status) nil
       (catch Throwable e {:message (ex-message e) :data (ex-data e)})))

(deftest final-block-isolation
  (is (= {} (parsed ["HTTP/1.1 100 Continue\r\n" "ETag: interim\r\n"
                      "x-amz-checksum-sha256: interim\r\n" "\r\n"
                      "HTTP/1.1 200 OK\r\n" "\r\n"] 200)))
  (is (= {"etag" "final" "x-amz-checksum-sha256" "hash"}
         (parsed ["HTTP/1.1 301 Moved\r\n" "ETag: previous\r\n" "\r\n"
                  "HTTP/2 200\r\n" "ETag: final\r\n"
                  "X-Amz-Checksum-Sha256: hash\r\n" "\r\n"] 200)))
  (is (= {"etag" "final"}
         (parsed ["HTTP/1.1 100 Continue\r\n" "ETag: first\r\n"
                  "ETag: duplicate-interim\r\n" "\r\n"
                  "HTTP/1.1 200 OK\r\n" "ETag: final\r\n" "\r\n"] 200))))

(deftest trailers-and-unselected-headers-are-not-retained
  (is (= {"etag" "final"}
         (parsed ["ETag: before-status\r\n" "HTTP/1.1 200 OK\r\n"
                  "Authorization: FAKE-SECRET\r\n" "ETag: final\r\n" "\r\n"
                  "ETag: trailer\r\n" "x-amz-checksum-sha256: trailer\r\n"] 200)))
  (is (= {"x-amz-checksum-type" "FULL_OBJECT"}
         (parsed ["HTTP/3 200\r\n" "X-Amz-Checksum-Type: \tFULL_OBJECT \t\r\n"
                  "\r\n"] 200))))

(deftest duplicate-selected-fields-fail-closed
  (doseq [name ["ETag" "x-amz-checksum-sha256" "x-amz-checksum-type"]
          second-value ["same" "different"]]
    (is (= {:message "S3 response headers are invalid"
            :data {:category :transport :definitely-not-sent? false}}
           (error ["HTTP/1.1 200 OK\r\n" (str name ": same\r\n")
                   (str name ": " second-value "\r\n") "\r\n"] 200)))))

(deftest incomplete-mismatched-and-malformed-blocks-fail-closed
  (doseq [[lines status]
          [[["HTTP/1.1 200 OK\r\n" "ETag: final\r\n"] 200]
           [["HTTP/1.1 100 Continue\r\n" "\r\n"] 200]
           [["HTTP/1.1 invalid\r\n" "\r\n"] 200]
           [["ETag: no-status\r\n" "\r\n"] 200]
           [["HTTP/1.1 200 OK\r\n" "ETag: value\r\n" " continuation\r\n" "\r\n"] 200]
           [["HTTP/1.1 200 OK\r\n" "ETag: value\nInjected: secret\r\n" "\r\n"] 200]]]
    (is (some? (error lines status)))))

(deftest bounded-allowlist
  (doseq [[name limit] [["ETag" 1024] ["x-amz-checksum-sha256" 256]
                       ["x-amz-checksum-type" 64]]]
    (let [value (apply str (repeat limit "x"))]
      (is (= 1 (count (parsed ["HTTP/1.1 200 OK\r\n" (str name ": " value "\r\n") "\r\n"] 200))))
      (is (some? (error ["HTTP/1.1 200 OK\r\n" (str name ": " value "x\r\n") "\r\n"] 200))))))

(defn run-checks! []
  (let [result (run-tests 'jdbc.chdb-durable-s3-headers-test)]
    (when (pos? (+ (:fail result) (:error result)))
      (throw (ex-info "S3 response-header checks failed" {})))
    result))

(defn -main [& _] (run-checks!))
