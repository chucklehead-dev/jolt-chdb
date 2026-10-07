(ns jdbc.chdb-s3-curl-reuse-test
  (:require [clojure.test :as test :refer [deftest is]]
            [jdbc.chdb.durable.s3-curl :as curl]
            [jolt.ffi :as ffi]
            [jolt.fibers :as fibers]))

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

(deftest failed-request-resets-and-next-request-uses-the-same-live-handle
  (doseq [mode [:setup-error :perform-error :arena-error]]
    (let [first? (atom true) inits (atom 0) resets (atom 0) cleanups (atom 0)
          arena (atom nil) reset-states (atom [])
          original-init @#'curl/curl-easy-init original-reset @#'curl/curl-easy-reset
          original-cleanup @#'curl/curl-easy-cleanup
          original-setopt @#'curl/curl-easy-setopt-string
          original-arena ffi/shared-arena error (ex-info "synthetic arena failure" {})]
      (with-redefs-fn
        {#'curl/curl-easy-init (fn [] (swap! inits inc) (original-init))
         #'curl/curl-easy-reset
         (fn [h]
           (swap! resets inc)
           (swap! reset-states conj (if @arena (ffi/arena-open? @arena) :no-arena))
           (original-reset h))
         #'curl/curl-easy-cleanup (fn [h] (swap! cleanups inc) (original-cleanup h))
         #'ffi/shared-arena
         (fn []
           (if (and (= :arena-error mode) (compare-and-set! first? true false))
             (throw error)
             (let [a (original-arena)] (reset! arena a) a)))
         #'curl/curl-easy-perform
         (fn [_] (if (and (= :perform-error mode) (compare-and-set! first? true false)) 7 0))
         #'curl/curl-easy-setopt-string
         (fn [h option value]
           (if (and (= :setup-error mode) (compare-and-set! first? true false))
             43 (original-setopt h option value)))
         #'curl/curl-easy-getinfo-pointer (fn [_ _ out] (ffi/write out :long 200) 0)}
        #(curl/with-reused-transport!
           (fn [send]
             (let [failure (try (send (request :get)) nil (catch Throwable e e))]
               (if (= :arena-error mode)
                 (is (identical? error failure))
                 (is (= :transport (:category (ex-data failure))))))
             (is (= 200 (:status (send (request :get)))))
             (is (zero? @cleanups)))))
      (is (= 1 @inits))
      (is (= 2 @resets))
      (is (= 1 @cleanups))
      (is (= (if (= :arena-error mode) [:no-arena true] [true true]) @reset-states)))))

(deftest scope-exit-waits-for-admitted-worker-and-closes-admission
  (let [entered (promise) release (promise) waiting (promise)
        transfer-finished (promise) expected-done (atom nil) send (atom nil)
        worker (atom nil) cleanups (atom 0) deferred-free (atom nil)
        original-deref deref original-cas compare-and-set!
        original-cleanup @#'curl/curl-easy-cleanup]
    (with-redefs-fn
      {#'clojure.core/compare-and-set!
       (fn [cell before after]
         (let [changed? (original-cas cell before after)]
           (when (and changed? (map? before) (contains? before :active)
                      (map? after) (= :closing (:phase after)))
             (reset! expected-done (:done (:active before))))
           changed?))
       #'clojure.core/deref
       (fn
         ([value]
          (when (identical? value (original-deref expected-done))
            (deliver waiting :draining))
          (original-deref value))
         ([value timeout default] (original-deref value timeout default)))
       #'curl/curl-easy-perform
       (fn [_] (deliver entered true) @release (deliver transfer-finished true) 0)
       #'curl/curl-easy-getinfo-pointer (fn [_ _ out] (ffi/write out :long 200) 0)
       #'curl/curl-easy-cleanup
       (fn [handle]
         (swap! cleanups inc)
         (is (realized? transfer-finished) "cleanup cannot race an admitted transfer")
         ;; Broken controls observe premature cleanup without freeing live
         ;; native memory; physical free is deferred until the worker joins.
         (if (realized? transfer-finished)
           (original-cleanup handle)
           (reset! deferred-free handle)))}
      (fn []
        (let [owner (fibers/spawn
                      #(curl/with-reused-transport!
                         (fn [request!]
                           (reset! send request!)
                           (reset! worker (fibers/spawn #(request! (request :get))))
                           @entered
                           :scope-result)))]
          (try
            (is (= :draining (original-deref waiting 2000 :timeout))
                "causal hook must observe the actual active-ticket drain")
            (is (zero? @cleanups))
            (is (= true (:definitely-not-sent?
                         (ex-data (try (@send (request :get)) nil (catch Throwable e e))))))
            (finally (deliver release true)))
          (is (= 200 (:status (fibers/join @worker))))
          (is (= :scope-result (fibers/join owner)))
          (when @deferred-free (original-cleanup @deferred-free))
          (is (= 1 @cleanups)))))))

(defn run-tests! [endpoint]
  (binding [*endpoint* endpoint]
    (let [result (test/run-tests 'jdbc.chdb-s3-curl-reuse-test)]
      (when-not (zero? (+ (:fail result) (:error result)))
        (throw (ex-info "libcurl owned reuse checks failed"
                        {:failures (:fail result) :errors (:error result)}))))))
