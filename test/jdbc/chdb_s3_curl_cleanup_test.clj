(ns jdbc.chdb-s3-curl-cleanup-test
  (:require [clojure.test :refer [deftest is]]
            [jolt.ffi :as ffi]
            [jdbc.chdb.durable.s3-curl :as curl]))

(defn- request []
  {:method :get :url "file:///dev/null" :region "us-east-2"
   :auth {:access-key "synthetic-key" :secret-key "synthetic-secret"}})

(deftest handle-cleanup-happens-while-callback-arena-is-open
  (doseq [mode [:success :perform-error :setup-error]]
    (let [arena (atom nil) cleanups (atom 0) state-at-cleanup (atom [])
          original-arena ffi/shared-arena
          original-cleanup @#'curl/curl-easy-cleanup
          original-setopt @#'curl/curl-easy-setopt-string]
      (with-redefs-fn
        {#'ffi/shared-arena (fn [] (let [a (original-arena)] (reset! arena a) a))
         #'curl/curl-easy-cleanup
         (fn [handle]
           (swap! cleanups inc)
           (swap! state-at-cleanup conj (ffi/arena-open? @arena))
           (original-cleanup handle))
         #'curl/curl-easy-perform (fn [_] (if (= :perform-error mode) 7 0))
         #'curl/curl-easy-setopt-string
         (fn [handle option value]
           (if (= :setup-error mode) 43 (original-setopt handle option value)))
         #'curl/curl-easy-getinfo-pointer
         (fn [_ _ out] (ffi/write out :long 200) 0)}
        (fn []
          (let [outcome (try {:response (curl/request! (request))}
                             (catch Throwable e {:error e}))]
            (if (= :success mode)
              (is (= 200 (get-in outcome [:response :status])))
              (is (= :transport (:category (ex-data (:error outcome)))))))))
      (is (= 1 @cleanups))
      (is (= [true] @state-at-cleanup))
      (is (false? (ffi/arena-open? @arena))))))

(deftest arena-construction-failure-still-cleans-real-handle-once
  (let [calls (atom 0) original @#'curl/curl-easy-cleanup
        error (ex-info "synthetic arena failure" {})]
    (with-redefs-fn
      {#'ffi/shared-arena (fn [] (throw error))
       #'curl/curl-easy-cleanup (fn [h] (swap! calls inc) (original h))}
      #(is (identical? error (try (curl/request! (request))
                                 nil (catch Throwable e e)))))
    (is (= 1 @calls))))
