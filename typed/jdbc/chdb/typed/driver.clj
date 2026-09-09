(ns jdbc.chdb.typed.driver
  "A real positive JVM consumer of the production runtime-neutral ABI API."
  (:require [jdbc.chdb.abi :as abi]
            [jdbc.chdb.typed.abi :as typed-abi]
            [typed.clojure :as t]))

(t/defalias DurableSummary
  (t/HMap :mandatory {:source typed-abi/SourceProvenance
                       :contract typed-abi/DurableContract
                       :functions (t/Map typed-abi/FunctionId
                                         typed-abi/FunctionSpec)
                       :analysis typed-abi/QueryAnalysisType}
          :complete? true))

(t/ann durable-summary [-> DurableSummary])
(defn durable-summary []
  {:source (abi/source-provenance)
   :contract (abi/contract-spec :durable-v1)
   :functions (abi/contract-functions :durable-v1)
   :analysis (abi/type-spec :query-analysis-v1)})

(t/ann capability-state
  [typed-abi/DurableCapability -> (t/U ':ready ':unavailable)])
(defn capability-state [capability]
  (if (= :supported (:status capability))
    :ready
    :unavailable))

(t/ann stable-unsupported typed-abi/UnsupportedCapability)
(def stable-unsupported
  {:status :unsupported
   :type :jdbc.chdb.native/unsupported-core
   :contract :durable-v1
   :native-version "26.7.0"
   :minimum-native-version "26.7.2"
   :symbols
   {:version {:symbol "chdb_version" :available? true}
    :connect {:symbol "chdb_connect" :available? true}
    :close-conn {:symbol "chdb_close_conn" :available? true}
    :destroy-query-result
    {:symbol "chdb_destroy_query_result" :available? true}
    :result-error {:symbol "chdb_result_error" :available? true}
    :backup-database-n
    {:symbol "chdb_backup_database_n" :available? false}
    :classify-query-n
    {:symbol "chdb_classify_query_n" :available? false}
    :restore-database-n
    {:symbol "chdb_restore_database_n" :available? false}}
   :provenance (abi/source-provenance)
   :missing [:backup-database-n :classify-query-n :restore-database-n]})

(t/ann stable-state (t/U ':ready ':unavailable))
(def stable-state (capability-state stable-unsupported))
