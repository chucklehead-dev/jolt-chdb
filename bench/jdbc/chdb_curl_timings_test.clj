(ns jdbc.chdb-curl-timings-test
  (:require [clojure.test :refer [deftest is]]
            [jolt.ffi :as ffi]
            [jdbc.chdb.durable.s3-curl :as curl]
            [jdbc.chdb-curl-timings :as timing]))

(def ^:private values
  {0x600032 5000 0x600033 11 0x600034 22
   0x600035 44 0x600036 444 0x600038 33 0x20001a 1})

(defn- fake-info [_ option out]
  (ffi/write out (if (= option 0x20001a) :long :int64) (get values option))
  0)

(deftest typed-scalar-milestones-and-connection-count
  (let [stats (timing/recorder) calls (atom 0)]
    (with-redefs-fn
      {#'curl/curl-easy-perform (fn [_] (swap! calls inc) 0)
       #'curl/curl-easy-getinfo-pointer fake-info}
      #(is (= 0 (timing/observe! stats
                  (fn [] ((timing/instrument-request stats
                            (fn [_] (#'curl/curl-easy-perform ffi/null)))
                          {:operation :get-with-etag :url "synthetic-secret-url"
                           :headers {"authorization" "synthetic-secret-header"}}))))))
    (let [report (timing/report stats) entry (get-in report [:operations :get-with-etag])]
      (is (= 1 @calls))
      (is (= 1 (:calls entry)))
      (is (= 1 (:new-connections entry)))
      (is (zero? (:diagnostic-declines report)))
      (doseq [[field expected] [[:total 5000] [:dns 11] [:connect 22]
                                [:pretransfer 44] [:first-byte 444] [:tls 33]]]
        (is (= expected (get-in entry [:milestones field :p50-us]))))
      (is (not (.contains (pr-str report) "synthetic-secret"))))))

(deftest diagnostic-failure-cannot-change-perform-result
  (doseq [info [(fn [& _] 1) (fn [& _] (throw (ex-info "synthetic-secret-error" {})))]]
    (let [stats (timing/recorder)]
      (with-redefs-fn {#'curl/curl-easy-perform (fn [_] 0)
                      #'curl/curl-easy-getinfo-pointer info}
        #(is (= 0 (timing/observe! stats (fn [] (#'curl/curl-easy-perform ffi/null))))))
      (is (= 1 (:diagnostic-declines (timing/report stats)))))))

(deftest perform-errors-and-exceptions-are-not-replaced
  (let [stats (timing/recorder) reads (atom 0)
        error (ex-info "synthetic-perform-error" {:synthetic true})]
    (with-redefs-fn {#'curl/curl-easy-perform (fn [_] 7)
                    #'curl/curl-easy-getinfo-pointer (fn [& _] (swap! reads inc) 0)}
      #(is (= 7 (timing/observe! stats (fn [] (#'curl/curl-easy-perform ffi/null))))))
    (with-redefs-fn {#'curl/curl-easy-perform (fn [_] (throw error))}
      #(is (identical? error (try (timing/observe! stats
                                  (fn [] (#'curl/curl-easy-perform ffi/null)))
                                 nil (catch Throwable e e)))))
    (is (zero? @reads))
    (is (empty? (:operations (timing/report stats))))))

(deftest bounded-samples-and-closed-operation-labels
  (let [stats (timing/recorder)]
    (with-redefs-fn {#'curl/curl-easy-perform (fn [_] 0)
                    #'curl/curl-easy-getinfo-pointer fake-info}
      #(timing/observe! stats
         (fn []
           (let [request! (timing/instrument-request stats
                            (fn [_] (#'curl/curl-easy-perform ffi/null)))]
             (dotimes [_ 2050] (request! {:operation "synthetic-secret-label"}))))))
    (let [entry (get-in (timing/report stats) [:operations :unknown])]
      (is (= 2050 (:calls entry)))
      (is (= 2050 (:new-connections entry)))
      (is (true? (:samples-truncated? entry)))
      (is (= 2048 (get-in entry [:milestones :dns :samples]))))))

(deftest coverage-rejects-compiled-bypass-and-unavailable-readouts
  (let [provider {:transport {:writer-run {:get {:results {200 2 404 1 :transport 1}}}}}
        complete {:diagnostic-declines 0 :operations {:get {:calls 3}}}]
    (is (nil? (timing/assert-coverage! complete provider)))
    (doseq [report [(assoc complete :operations {})
                    (assoc complete :diagnostic-declines 1)
                    (assoc-in complete [:operations :get :calls] 2)]]
      (is (thrown? clojure.lang.ExceptionInfo (timing/assert-coverage! report provider))))))

(deftest actual-request-call-site-reaches-observer-without-network
  ;; Use a genuine curl handle/cleanup and file:///dev/null. Even if the
  ;; compiler bypassed the perform mock, this can only read an empty local
  ;; file, not send fake credentials or start an HTTP connection. Such bypass
  ;; must fail the observer/response assertions rather than vacuously pass.
  (let [stats (timing/recorder) performs (atom 0)
        request! (timing/instrument-request stats (curl/request-function {}))]
    (with-redefs-fn
      {#'curl/curl-easy-perform (fn [_] (swap! performs inc) 0)
       #'curl/curl-easy-getinfo-pointer
       (fn [handle option out]
         (if (= option 0x200002)
           (do (ffi/write out :long 200) 0)
           (fake-info handle option out)))}
      #(let [response (timing/observe! stats
                       (fn [] (request! {:operation :get :method :get
                                         :url "file:///dev/null" :region "us-east-2"
                                         :auth {:access-key "synthetic-key"
                                                :secret-key "synthetic-secret"}})))]
         (is (= 200 (:status response)))))
    (is (= 1 @performs))
    (is (= 1 (get-in (timing/report stats) [:operations :get :calls])))))
