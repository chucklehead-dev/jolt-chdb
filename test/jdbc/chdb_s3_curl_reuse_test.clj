(ns jdbc.chdb-s3-curl-reuse-test
  (:require [clojure.test :as test :refer [deftest is]]
            [jdbc.chdb.durable.s3-curl :as curl]
            [jolt.ffi :as ffi]))

(def ^:dynamic *endpoint* nil)

(defn- request [method]
  {:method method :url (str *endpoint* "/bucket/reuse-probe")
   :region "us-east-1"
   :auth {:access-key "ACCESS" :secret-key "SECRET" :session-token "SESSION"}
   :request-body (when (= :put method) {:bytes (byte-array [7 9]) :byte-count 2})
   :response-body :bytes :connect-timeout-ms 2000 :timeout-ms 10000})

(defn- connections [run]
  (let [observed (atom []) original @#'curl/curl-easy-perform]
    (with-redefs-fn
      {#'curl/curl-easy-perform
       (fn [handle]
         (let [code (original handle)]
           (ffi/with-out [out :long]
             (assert (zero? (#'curl/curl-easy-getinfo-pointer handle 0x20001a out)))
             (swap! observed conj (ffi/read out :long)))
           code))}
      run)
    @observed))

(deftest actual-transfer-reuses-connection-and-resets-method
  (let [baseline (connections #(dotimes [_ 3] (curl/request! (request :get))))
        reused (connections
                 #(curl/with-reused-transport!
                    (fn [send]
                      (is (= 200 (:status (send (request :put)))))
                      (dotimes [_ 3]
                        (let [response (send (request :get))]
                          (is (= 200 (:status response)))
                          (is (= [7 9] (vec (:body response)))))))))]
    (is (= [1 1 1] baseline))
    (is (= [1 0 0 0] reused))
    (prn {:scope :synthetic-http-loopback-not-s3-throughput
          :fresh-handle-new-connections baseline :reused-handle-new-connections reused})))

(deftest scope-closes-on-error-and-retained-request-cannot-use-handle
  (let [send (atom nil) cleanups (atom 0) resets (atom 0)
        original-cleanup @#'curl/curl-easy-cleanup
        original-reset @#'curl/curl-easy-reset
        error (ex-info "synthetic scope error" {})]
    (with-redefs-fn
      {#'curl/curl-easy-cleanup (fn [h] (swap! cleanups inc) (original-cleanup h))
       #'curl/curl-easy-reset (fn [h] (swap! resets inc) (original-reset h))}
      (fn []
        (is (identical? error
                       (try (curl/with-reused-transport!
                              (fn [request!]
                                (reset! send request!)
                                (request! (request :get))
                                (throw error)))
                            nil (catch Throwable e e))))
        (is (= 1 @cleanups))
        (is (= 1 @resets))
        (is (= true (:definitely-not-sent?
                     (ex-data (try (@send (request :get)) nil
                                   (catch Throwable e e))))))
        (is (= 1 @cleanups))
        (is (= 1 @resets))))))

(deftest reentrant-request-rejects-and-reset-precedes-arena-close
  (let [send (atom nil) arena (atom nil) open-at-reset (atom [])
        original-arena ffi/shared-arena original-reset @#'curl/curl-easy-reset]
    (with-redefs-fn
      {#'ffi/shared-arena (fn [] (let [a (original-arena)] (reset! arena a) a))
       #'curl/curl-easy-perform
       (fn [_]
         (is (= true (:definitely-not-sent?
                      (ex-data (try (@send (request :get)) nil
                                    (catch Throwable e e))))))
         0)
       #'curl/curl-easy-getinfo-pointer (fn [_ _ out] (ffi/write out :long 200) 0)
       #'curl/curl-easy-reset
       (fn [h] (swap! open-at-reset conj (ffi/arena-open? @arena)) (original-reset h))}
      #(curl/with-reused-transport!
         (fn [request!] (reset! send request!) (request! (request :get)))))
    (is (= [true] @open-at-reset))
    (is (false? (ffi/arena-open? @arena)))))

(deftest configuration-changes-reject-before-native-work
  (let [performs (atom 0) original @#'curl/curl-easy-perform]
    (with-redefs-fn
      {#'curl/curl-easy-perform (fn [h] (swap! performs inc) (original h))}
      #(curl/with-reused-transport!
         (fn [send]
           (send (request :get))
           (doseq [changed [(assoc (request :get) :region "different-region")
                            (assoc-in (request :get) [:auth :access-key] "DIFFERENT")
                            (assoc (request :get) :url "http://localhost:1/not-contacted")]]
             (is (= true (:definitely-not-sent?
                          (ex-data (try (send changed) nil (catch Throwable e e)))))))
           (is (= 1 @performs)))))))

(defn run-tests! [endpoint]
  (binding [*endpoint* endpoint]
    (let [result (test/run-tests 'jdbc.chdb-s3-curl-reuse-test)]
      (when-not (zero? (+ (:fail result) (:error result)))
        (throw (ex-info "libcurl owned reuse checks failed"
                        {:failures (:fail result) :errors (:error result)}))))))
