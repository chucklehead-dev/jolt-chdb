(ns jdbc.chdb-typed-storage-support-test
  (:require [clojure.test :refer [deftest is testing run-tests]]
            [jdbc.core :as jdbc]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.local-posix :as local]
            [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb.durable.s3-writer :as owner]
            [jdbc.chdb-typed-storage-support :as support]))

(defn thrown [f] (try (f) nil (catch Throwable e e)))

(deftest local-connection-closes-after-callback-and-on-failure
  (doseq [fail? [false true]]
    (let [events (atom []) error (ex-info "callback failure" {})
          connection (reify java.io.Closeable
                       (close [_] (swap! events conj :close)))]
      (with-redefs [support/arm (constantly :local)
                    durable/writer-dbspec identity
                    jdbc/connection (fn [options]
                                      (is (= {:choice :local} options))
                                      (swap! events conj :open) connection)]
        (let [invoke #(support/with-writer! {:choice :local}
                                           (fn [] (throw (ex-info "unused S3 observer" {})))
                                           (fn [actual]
                                             (is (identical? connection actual))
                                             (swap! events conj :use)
                                             (if fail? (throw error) :value)))]
          (if fail? (is (identical? error (thrown invoke)))
              (is (= :value (invoke))))
          (is (= [:open :use :close] @events)))))))

(deftest s3-retains-owned-routing-and-real-renewal-outcomes
  (let [events (atom []) observed (atom 0) seen (atom nil)
        checkpoint (fn [& _] :checkpoint)
        json-reader (fn [& _] :json)
        options {:namespace-backend :must-not-escape
                 :object-id "typed-compact-prefix"
                 :operations {:renew-control! (fn [& _] (throw (ex-info "wrong store" {})))
                              :create-checkpoint! checkpoint :json-reader json-reader}}]
    (with-redefs [support/arm (constantly :s3)
                  support/s3-options (constantly {:configuration :fake})
                  control/renew! (fn [& args] (reset! seen args) :real-renewal)
                  owner/with-connection!
                  (fn [aws selected f]
                    (is (= {:configuration :fake} aws))
                    (is (not (contains? selected :namespace-backend)))
                    (is (not (contains? (:operations selected) :renew-control!)))
                    (is (identical? checkpoint (get-in selected [:operations :create-checkpoint!])))
                    (is (identical? json-reader (get-in selected [:operations :json-reader])))
                    (swap! events conj :scope-open)
                    (try (f :borrowed-connection)
                         (finally (swap! events conj :scope-close))))]
      (is (= :value
             (support/with-writer! options #(swap! observed inc)
               (fn [connection]
                 (is (= :borrowed-connection connection))
                 (swap! events conj :use)
                 (is (= :real-renewal (control/renew! :renewal-store :token 42 {})))
                 :value))))
      (is (= [:scope-open :use :scope-close] @events))
      (is (= 1 @observed))
      (is (= [:renewal-store :token 42 {}] @seen))
      ;; with-redefs must be unwound before a future owner uses the control Var.
      (is (= :real-renewal (control/renew! :another-store :token 43 {})))
      (is (= 1 @observed)))))

(deftest failed-real-renewal-is-not-counted-or-replaced
  (let [error (ex-info "real renewal failure" {}) observed (atom 0)]
    (with-redefs [support/arm (constantly :s3)
                  support/s3-options (constantly {})
                  control/renew! (fn [& _] (throw error))
                  owner/with-connection! (fn [_ _ f] (f :connection))]
      (is (identical? error
            (thrown #(support/with-writer! {} (fn [] (swap! observed inc))
                       (fn [_] (control/renew! :store :token 42 {}))))))
      (is (zero? @observed)))))

(deftest snapshot-backend-is-selected-without-retaining-writer-scope
  (let [root (str (System/getProperty "java.io.tmpdir") "/typed-storage-support-test-"
                  (System/nanoTime))]
    (with-redefs [support/arm (constantly :s3)
                  support/s3-options (constantly {:configuration :fake})
                  s3/s3-backend (fn [options]
                                  (is (= {:configuration :fake} options))
                                  :fresh-snapshot-backend)]
      (try
        (is (= :fresh-snapshot-backend (:namespace-backend (support/options root "reader"))))
        (is (not (contains? (support/options root "writer") :namespace-backend)))
        (finally
          ;; Only these exact task-created empty directories; no recursive
          ;; cleanup or deletion of a caller's benchmark/data directory.
          (.delete (java.io.File. (str root "/scratch-reader")))
          (.delete (java.io.File. (str root "/scratch-writer")))
          (.delete (java.io.File. root)))))))

(deftest s3-configuration-refuses-out-of-scope-prefix-without-secret-data
  (let [secret "never-in-error-data"]
    (with-redefs [support/required-env (fn [_ field] (if (= field :prefix) "other-prefix" secret))]
      (let [error (thrown support/s3-options)]
        (is (= {:type ::support/configuration :field :prefix} (ex-data error)))
        (is (not (.contains (str (ex-data error)) secret)))))))

(defn -main [& _]
  (let [result (run-tests 'jdbc.chdb-typed-storage-support-test)]
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
