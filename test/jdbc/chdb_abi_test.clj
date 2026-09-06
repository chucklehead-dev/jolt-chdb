(ns jdbc.chdb-abi-test
  (:require [jdbc.chdb.abi :as abi]
            [jdbc.chdb.native :as native]
            [jolt.ffi :as ffi]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- rejected-data [value]
  (try
    (abi/validate-descriptor! value)
    nil
    (catch Throwable error (ex-data error))))

(def expected-source
  {:repository "https://github.com/chdb-io/chdb-core.git"
   :release "v26.7.2-rc.2"
   :commit "30488a59b2700188ee36ecbced7713081a909f56"
   :header "programs/local/chdb.h"
   :c-oracle "examples/chdbDurableAbiTest.c"
   :python-oracle "tests/test_durable_backup_restore_classify.py"})

(def expected-durable-functions
  {:backup-database-n
   {:symbol "chdb_backup_database_n"
    :args [:pointer :pointer :size_t :pointer :size_t :pointer :size_t]
    :return :pointer :blocking? true :contracts [:durable-v1]}
   :classify-query-n
   {:symbol "chdb_classify_query_n"
    :args [:pointer :pointer :size_t :pointer :size_t :pointer]
    :return :int :contracts [:durable-v1]}
   :close-conn
   {:symbol "chdb_close_conn" :args [:pointer] :return :void
    :blocking? true :contracts [:driver :durable-v1]}
   :connect
   {:symbol "chdb_connect" :args [:int :pointer] :return :pointer
    :blocking? true :contracts [:driver :durable-v1]}
   :destroy-query-result
   {:symbol "chdb_destroy_query_result" :args [:pointer] :return :void
    :blocking? true :contracts [:driver :durable-v1]}
   :restore-database-n
   {:symbol "chdb_restore_database_n"
    :args [:pointer :pointer :size_t :pointer :size_t]
    :return :pointer :blocking? true :contracts [:durable-v1]}
   :result-error
   {:symbol "chdb_result_error" :args [:pointer] :return :string
    :contracts [:driver :durable-v1]}
   :version
   {:symbol "chdb_version" :args [] :return :string
    :contracts [:driver :durable-v1]}})

(defn- run-descriptor-checks []
  (println "chDB versioned ABI descriptor")
  (let [descriptor (abi/descriptor)]
    (check "descriptor validates as schema 1" descriptor
           (abi/validate-descriptor! descriptor))
    (check "provenance names the exact upstream header and oracle commit"
           expected-source (abi/source-provenance))
    (check "Durable signatures independently match the pinned chdb.h oracle"
           expected-durable-functions (abi/contract-functions :durable-v1))
    (check "Durable contract is explicitly V1 at the first native release"
           {:version 1 :minimum-native-version "26.7.2"}
           (abi/contract-spec :durable-v1))
    (check "query class enum preserves fail-closed ordering"
           {:read-only 0 :mutating 1 :mutating-global 2 :control 3 :unknown 4}
           (get-in descriptor [:enums :query-class]))
    (check "analysis flags independently match the pinned chdb.h bits"
           {:has-secrets 1 :writes-only-target-database 2
            :changes-database-lifecycle 4}
           (get-in descriptor [:enums :query-analysis-flag]))
    (check "query analysis struct is exactly four uint32 fields / 16 bytes"
           {:kind :struct :size 16
            :fields [[:struct-size :uint32]
                     [:statement-count :uint32]
                     [:flags :uint32]
                     [:query-class :uint32]]}
           (get-in descriptor [:types :query-analysis-v1]))
    (check "compiled Jolt query-analysis layout is 16 bytes"
           16 (ffi/layout-size native/query-analysis-layout))

    ;; Each mutant changes a different contract dimension. A validator that
    ;; merely parses EDN, or a test that never reaches it, would let these pass.
    (doseq [[label mutant]
            [["schema mutant is rejected" (assoc descriptor :schema 2)]
             ["contract-version mutant is rejected"
              (assoc-in descriptor [:contracts :durable-v1 :version] 2)]
             ["layout-size mutant is rejected"
              (assoc-in descriptor [:types :query-analysis-v1 :size] 12)]
             ["unknown scalar signature mutant is rejected"
              (assoc-in descriptor [:functions :classify-query-n :args 2]
                        :unsigned-long-ish)]
             ["enum-value mutant is rejected"
              (assoc-in descriptor [:enums :query-class :unknown] 3)]
             ["flag-bit mutant is rejected"
              (assoc-in descriptor
                        [:enums :query-analysis-flag :has-secrets] 2)]
             ["duplicate-symbol mutant is rejected"
              (assoc-in descriptor [:functions :restore-database-n :symbol]
                        "chdb_backup_database_n")]]]
      (check label ::abi/invalid-descriptor (:type (rejected-data mutant))))))

(defn- run-stock-library-checks []
  (println "stock libchdb capability negotiation")
  (check "raw Durable symbols are not callable through the public namespace"
         {}
         (select-keys
          (ns-publics 'jdbc.chdb.native)
          '[chdb-backup-database-n chdb-restore-database-n
            chdb-classify-query-n]))
  (let [driver (native/contract-capability :driver)
        durable (native/durable-capability)]
    (check "stable driver contract is present on the production library"
           :supported (:status driver))
    (check "positive control crosses real chdb_version"
           native/version (:native-version driver))
    (check "every stable driver symbol was actually resolved"
           true (every? :available? (vals (:symbols driver))))
    (check "production libchdb lacks Durable V1 without breaking load"
           :unsupported (:status durable))
    (check "old-library result has the typed unsupported-core identity"
           ::native/unsupported-core (:type durable))
    (check "old-library negative names exactly the three new symbols"
           [:backup-database-n :classify-query-n :restore-database-n]
           (:missing durable))
    (check "old-library report retains its actual native version"
           native/version (:native-version durable)))

  (with-redefs [ffi/find-symbol (fn [_] 1)]
    (let [capability (native/durable-capability)]
      (check "all-symbol positive control reports supported"
             :supported (:status capability))
      (check "supported result has no failure type" nil (:type capability))
      (check "supported result has no missing functions" nil (:missing capability))))

  (let [missing-symbol "chdb_classify_query_n"]
    (with-redefs [ffi/find-symbol (fn [symbol]
                                   (when-not (= missing-symbol symbol) 1))]
      (let [capability (native/durable-capability)]
        (check "single-symbol mutant makes capability red" :unsupported
               (:status capability))
        (check "single-symbol mutant is localized"
               [:classify-query-n] (:missing capability))))))

(defn -main [& _]
  (reset! failures 0)
  (run-descriptor-checks)
  (run-stock-library-checks)
  (if (zero? @failures)
    (println "all ABI checks passed")
    (throw (ex-info (str @failures " ABI checks failed")
                    {:failures @failures}))))
