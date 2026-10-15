(ns test-typed-storage-curl-profile
  (:require [clojure.test :as t :refer [deftest is]]
            [typed-storage-curl-profile :as profile]
            [jdbc.chdb.durable.s3-curl :as curl]
            [jolt.ffi :as ffi]))

(deftest classes-retain-only-bounded-symbolic-metadata
  (is (= [:get :none] (profile/request-class {:method :get :url "secret-url"})))
  (is (= [:put :bytes :small]
         (profile/request-class {:method :put :request-body {:bytes "secret" :byte-count 4096}})))
  (is (= [:put :file :medium]
         (profile/request-class {:method :put :request-body {:file "secret-path" :byte-count 4097}})))
  (is (= [:put :bytes :large]
         (profile/request-class {:method :put :request-body {:bytes "secret" :byte-count 1048577}})))
  (is (= [:put :other :unknown] (profile/request-class {:method :put})))
  (is (= [:other :none] (profile/request-class {:method :secret}))))

(deftest wrapped-results-and-failures-remain-original
  (let [counts (atom {}) marker (Object.) failure (ex-info "secret-error" {})
        perform (profile/wrap-perform (constantly 28) counts
                                     (fn [_] {:curl-total-seconds 0.1 :new-connections 1}))
        request (profile/wrap-request (fn [_] (is (= 28 (perform ffi/null))) marker) counts)]
    (is (identical? marker (request {:method :get :url "secret-url"})))
    (is (= 1 (get-in (profile/report! counts) [:classes [:get :none] :perform-failures])))
    (is (not (.contains (pr-str @counts) "secret")))
    (let [request (profile/wrap-request (fn [_] (throw failure)) counts)]
      (is (identical? failure (try (request {:method :get}) (catch Throwable e e)))))
    (is (= 1 (get-in @counts [[:get :none] :request-failures])))))

(deftest getinfo-failure-does-not-change-transfer-but-invalidates-profile
  (let [counts (atom {})
        perform (profile/wrap-perform (constantly 0) counts (fn [_] (throw (ex-info "secret" {}))))
        request (profile/wrap-request (fn [_] (perform ffi/null)) counts)]
    (is (= 0 (request {:method :get})))
    (is (= 1 (get-in @counts [[:get :none] :sample-errors])))
    (is (not (.contains (pr-str @counts) "secret")))
    (is (= ::profile/incomplete
           (try (profile/report! counts) nil (catch Throwable e (:type (ex-data e))))))))

(deftest native-result-types-and-option-ids
  (let [calls (atom [])]
    (with-redefs [curl/curl-easy-getinfo-pointer
                  (fn [_ option p]
                    (swap! calls conj option)
                    (if (= option 2097178) (ffi/write p :long 2)
                        (ffi/write p :double 0.125))
                    0)]
      (let [r (profile/read-info! ffi/null)]
        (is (= 2 (:new-connections r)))
        (is (= 0.125 (:curl-total-seconds r)))
        (is (= 7 (count r)))
        (is (= [3145731 3145732 3145733 3145734 3145745 3145761 2097178] @calls))))))

(deftest failed-native-getinfo-is-not-an-initialized-measurement
  (with-redefs [curl/curl-easy-getinfo-pointer (fn [& _] 48)]
    (is (= ::profile/unavailable
           (try (profile/read-info! ffi/null) nil (catch Throwable e (:type (ex-data e))))))))

(defn -main [& _]
  (let [r (t/run-tests 'test-typed-storage-curl-profile)]
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
