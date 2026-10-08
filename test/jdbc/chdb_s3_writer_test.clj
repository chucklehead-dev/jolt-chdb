(ns jdbc.chdb-s3-writer-test
  (:require [clojure.test :refer [deftest is]]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb.durable.s3-curl :as curl]
            [jdbc.chdb.durable.s3-writer :as owner]
            [jdbc.chdb.durable.writer :as writer]))

(defn- thrown [f]
  (try (f) nil (catch Throwable error error)))

(defn- exercise [callback close-error startup-error]
  (let [events (atom []) scopes (atom 0) opened (atom nil) renewal (atom nil)
        w :writer token {:generation 7} retry {:max-attempts 3}]
    (with-redefs
      [curl/with-reused-transport!
       (fn [f]
         (let [n (swap! scopes inc)]
           (swap! events conj [:enter n])
           (try (f (fn [_] n))
                (finally (swap! events conj [:exit n])))))
       s3/s3-backend identity
       backend/object-backend (fn [namespace object-id] [namespace object-id])
       control/renew! (fn [store t expiry r] (reset! renewal [store t expiry r]) :renewed)
       durable/open-writer!
       (fn [opts]
         (reset! opened opts)
         (swap! events conj :open)
         (when startup-error (throw startup-error))
         (is (= :renewed ((get-in opts [:operations :renew-control!])
                          :ignored token 120.001M retry)))
         w)
       writer/close! (fn [value]
                       (is (= w value))
                       (swap! events conj :close)
                       (when close-error (throw close-error)))]
      (let [outcome (try
                      {:value (owner/with-writer!
                                {:bucket "bucket" :prefix "one"}
                                {:object-id "object" :owner "writer"
                                 :operations {:now-ms (constantly 0)}}
                                (fn [value]
                                  (is (= w value))
                                  (swap! events conj :callback)
                                  (callback value)))}
                      (catch Throwable error {:error error}))]
        (when-not startup-error
          (is (= "object" (second (first @renewal))))
          (is (= "one" (:prefix (first (first @renewal)))))
          (is (not (identical? (get-in @opened [:namespace-backend :request!])
                               (:request! (first (first @renewal))))))
          (is (= [token 120.001M retry] (subvec @renewal 1)))
          (is (fn? (get-in @opened [:operations :now-ms]))))
        {:outcome outcome :events @events}))))

(deftest owner-closes-producers-before-handles
  (let [r (exercise (constantly :done) nil nil)]
    (is (= {:value :done} (:outcome r)))
    (is (= [[:enter 1] [:enter 2] :open :callback :close [:exit 2] [:exit 1]]
           (:events r)))))

(deftest owner-preserves-primary-and-close-errors
  (let [primary (ex-info "callback failed" {}) close (ex-info "close failed" {})]
    (is (identical? primary (get-in (exercise (fn [_] (throw primary)) close nil)
                                    [:outcome :error])))
    (is (identical? close (get-in (exercise (constantly :done) close nil)
                                  [:outcome :error])))))

(deftest startup-failure-unwinds-both-scopes
  (let [error (ex-info "startup failed" {}) r (exercise identity nil error)]
    (is (identical? error (get-in r [:outcome :error])))
    (is (= [[:enter 1] [:enter 2] :open [:exit 2] [:exit 1]] (:events r)))))

(deftest owner-rejects-routing-overrides-before-handle-creation
  (let [entered (atom 0)]
    (with-redefs [curl/with-reused-transport! (fn [_] (swap! entered inc))]
      (doseq [[s3-options writer-options f]
              [[nil {} identity] [{} nil identity] [{} {} nil]
               [{:request! identity} {} identity]
               [{} {:store :wrong} identity]
               [{} {:namespace-backend :wrong} identity]
               [{} {:operations false} identity]
               [{} {:operations {:renew-control! identity}} identity]]]
        (is (= ::owner/invalid-options
               (:type (ex-data (thrown #(owner/with-writer! s3-options writer-options f)))))))
      (is (zero? @entered)))))

(deftest renewal-control-hook-validates-before-storage-effects
  (let [reads (atom 0)]
    (with-redefs [control/read-head! (fn [_] (swap! reads inc))]
      (is (= ::durable/invalid-options
             (:type (ex-data
                      (thrown #(durable/open-writer!
                                 {:operations {:renew-control! false}}))))))
      (is (zero? @reads)))))
