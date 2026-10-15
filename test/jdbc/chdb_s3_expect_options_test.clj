(ns jdbc.chdb-s3-expect-options-test
  (:require [clojure.test :as t :refer [deftest is]]
            [jdbc.chdb.durable.s3-curl :as curl]
            [jolt.ffi :as ffi]))

(deftest defaults-remain-live-and-controls-are-forwarded
  (let [send (curl/request-function {:expect-100-timeout-ms 10 :unrelated :not-forwarded})]
    (with-redefs [curl/request! identity]
      (is (= {:method :put :expect-100-timeout-ms 10} (send {:method :put})))
      (is (= {:method :get :expect-100-timeout-ms 50}
             (send {:method :get :expect-100-timeout-ms 50}))))))

(deftest explicit-owner-remains-the-sender
  (let [requests (atom [])
        send (curl/request-function {:expect-100-timeout-ms 10 :timeout-ms 100}
                                   (fn [r] (swap! requests conj r) :owner-response))]
    (is (= :owner-response (send {:method :put})))
    (is (= [{:method :put :expect-100-timeout-ms 10 :timeout-ms 100}] @requests))))

(deftest invalid-expect-waits-reject-before-native-handle-allocation
  (doseq [value [0 -1 1001 nil "10" 10.5]]
    (let [calls (atom 0)]
      (with-redefs [curl/curl-easy-init (fn [] (swap! calls inc))]
        (let [error (try (curl/request! {:method :put :url "http://127.0.0.1/fixture"
                                        :region "us-east-1"
                                        :auth {:access-key "fixture" :secret-key "fixture"}
                                        :expect-100-timeout-ms value})
                         nil (catch Throwable e e))]
          (is (= :transport (:category (ex-data error))))
          (is (true? (:definitely-not-sent? (ex-data error))))
          (is (= 0 @calls)))))))

(deftest accepted-boundaries-reach-allocation-without-real-handles
  (doseq [value [1 1000]]
    (let [calls (atom 0)]
      (with-redefs [curl/curl-easy-init (fn [] (swap! calls inc) ffi/null)]
        (let [error (try (curl/request! {:method :get :url "http://127.0.0.1/fixture"
                                        :region "us-east-1" :timeout-ms 100
                                        :auth {:access-key "fixture" :secret-key "fixture"}
                                        :expect-100-timeout-ms value})
                         nil (catch Throwable e e))]
          (is (= 1 @calls))
          (is (= "libcurl request allocation failed" (.getMessage error))))))))

(defn -main [& _]
  (let [r (t/run-tests 'jdbc.chdb-s3-expect-options-test)]
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
