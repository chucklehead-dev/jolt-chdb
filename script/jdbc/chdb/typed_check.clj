(ns jdbc.chdb.typed-check
  "Fail-closed runner for the bounded, JVM-only Typed Clojure pilot."
  (:require [clojure.string :as str]
            [jdbc.chdb.typed.abi]
            [typed.clojure :as t]))

(def valid-namespaces
  ['jdbc.chdb.typed.abi
   'jdbc.chdb.typed.driver])

(def mutant-controls
  [{:name :malformed-abi-descriptor-lookup
    :namespace 'jdbc.chdb.typed.controls.malformed-abi-descriptor-lookup
    :evidence ["abi/function-spec"
               "typed-abi/FunctionId"
               "(t/Val :not-a-chdb-function)"]}
   {:name :durable-result-state-mismatch
    :namespace 'jdbc.chdb.typed.controls.durable-result-state-mismatch
    :evidence ["driver/stable-unsupported"
               "SupportedCapability"
               "UnsupportedCapability"]}])

(def expected-mutant-registry
  #{:malformed-abi-descriptor-lookup :durable-result-state-mismatch})
(def expected-mutant-count 2)

(def expected-function-registry
  #{:version :set-signal-handlers-enabled :connect :close-conn
    :query-with-params-n :destroy-query-result :result-buffer :result-length
    :result-elapsed :result-rows-read :result-bytes-read
    :result-storage-rows-read :result-storage-bytes-read
    :result-rows-written :result-bytes-written :result-error
    :stream-insert-n :stream-append
    :stream-done :stream-cancel-insert :stream-insert-error
    :destroy-insert-stream :backup-database-n :restore-database-n
    :classify-query-n})

(def expected-durable-registry
  #{:version :connect :close-conn :destroy-query-result :result-error
    :backup-database-n :restore-database-n :classify-query-n})

(defn- require! [description condition]
  (when-not condition
    (throw (ex-info (str "typed-check failed: " description)
                    {:description description})))
  (println "PASS" description))

(defn- errors-of [value]
  (when (map? value)
    (or (:delayed-errors value) (:errors value))))

(defn- normalize-report [result]
  (let [errors (errors-of result)]
    (if (seq errors)
      {:ok? false :errors errors}
      {:ok? true :errors []})))

(defn- check-ns-report [ns-sym]
  ;; Requiring outside the catch keeps load/read/analyzer setup failures from
  ;; being mistaken for an expected mutant rejection.
  (require ns-sym :reload)
  (try
    (normalize-report (t/check-ns-clj ns-sym))
    (catch clojure.lang.ExceptionInfo error
      (let [errors (errors-of (ex-data error))]
        (if (seq errors)
          {:ok? false :errors errors}
          (throw (ex-info (str "check-ns-clj crashed on " ns-sym)
                          {:namespace ns-sym :kind ::checker-crash}
                          error)))))))

(defn- expected-type-error? [errors evidence]
  (and (= 1 (count errors))
       (let [error (first errors)
             report (str error)]
         (and (instance? clojure.lang.ExceptionInfo error)
              (= :clojure.core.typed.errors/type-error
                 (:type-error (ex-data error)))
              (every? #(str/includes? report %) evidence)))))

(defn -main [& _]
  (let [names (set (map :name mutant-controls))]
    (require! (str "registers exactly " expected-mutant-count
                   " named mutant controls")
              (and (= expected-mutant-count (count mutant-controls))
                   (= expected-mutant-registry names))))
  (doseq [ns-sym valid-namespaces]
    (let [{:keys [ok? errors]} (check-ns-report ns-sym)]
      (require! (str ns-sym " type-checks with zero errors ("
                     (count errors) " found)")
                ok?)))
  (let [summary ((requiring-resolve 'jdbc.chdb.typed.driver/durable-summary))
        stable @(requiring-resolve 'jdbc.chdb.typed.driver/stable-unsupported)
        stable-state @(requiring-resolve 'jdbc.chdb.typed.driver/stable-state)
        descriptor ((requiring-resolve 'jdbc.chdb.abi/descriptor))]
    (require! "positive consumer loads the production Durable V1 contract"
              (= 1 (get-in summary [:contract :version])))
    (require! "typed FunctionId registry exactly matches the production ABI"
              (= expected-function-registry (set (keys (:functions descriptor)))))
    (require! "elapsed statistics retains its double ABI return"
              (= :double (get-in descriptor
                                 [:functions :result-elapsed :return])))
    (require! "positive consumer reads the exact Durable ABI registry"
              (= expected-durable-registry (set (keys (:functions summary)))))
    (require! "unsupported sample carries every real capability symbol"
              (= expected-durable-registry (set (keys (:symbols stable)))))
    (require! "unsupported sample localizes the three absent stock symbols"
              (= (set (:missing stable))
                 (into #{} (keep (fn [[function-id status]]
                                   (when-not (:available? status) function-id)))
                       (:symbols stable))))
    (require! "positive consumer maps the stock-library result to unavailable"
              (= :unavailable stable-state)))
  (doseq [{:keys [name namespace evidence]} mutant-controls]
    (let [{:keys [ok? errors]} (check-ns-report namespace)]
      (require! (str name " is rejected by its mutation-specific type error")
                (and (not ok?) (expected-type-error? errors evidence)))))
  (flush))
