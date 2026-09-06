(ns jdbc.chdb.typed.abi
  "Dev-only external types for the JVM-loadable jdbc.chdb.abi data API."
  (:require [jdbc.chdb.abi]
            [typed.clojure :as t]))

(t/defalias ContractId (t/U ':driver ':durable-v1))
(t/defalias TypeId ':query-analysis-v1)
(t/defalias FunctionId
  (t/U ':version ':set-signal-handlers-enabled ':connect ':close-conn
       ':query-with-params-n ':destroy-query-result ':result-buffer
       ':result-length ':result-elapsed ':result-rows-read ':result-bytes-read
       ':result-storage-rows-read ':result-storage-bytes-read
       ':result-rows-written ':result-bytes-written ':result-error ':stream-insert-n
       ':stream-append ':stream-done ':stream-cancel-insert
       ':stream-insert-error ':destroy-insert-stream ':backup-database-n
       ':restore-database-n ':classify-query-n))
(t/defalias ScalarType
  (t/U ':void ':int ':uint32 ':uint64 ':size_t ':double ':pointer ':string))

(t/defalias SourceProvenance
  (t/HMap :mandatory {:repository t/Str
                       :release t/Str
                       :commit t/Str
                       :header t/Str
                       :c-oracle t/Str
                       :python-oracle t/Str}
          :complete? true))
(t/defalias FunctionSpec
  (t/HMap :mandatory {:symbol t/Str
                       :args (t/Vec ScalarType)
                       :return ScalarType
                       :contracts (t/Vec ContractId)}
          :optional {:blocking? t/Bool}
          :complete? true))
(t/defalias QueryAnalysisType
  (t/HMap :mandatory {:kind ':struct
                       :size (t/Val 16)
                       :fields '[[':struct-size ':uint32]
                                 [':statement-count ':uint32]
                                 [':flags ':uint32]
                                 [':query-class ':uint32]]}
          :complete? true))
(t/defalias DriverContract
  (t/HMap :mandatory {:minimum-native-version (t/Val "26.7.0")}
          :complete? true))
(t/defalias DurableContract
  (t/HMap :mandatory {:version (t/Val 1)
                       :minimum-native-version (t/Val "26.7.2")}
          :complete? true))
(t/defalias ContractSpec (t/U DriverContract DurableContract))
(t/defalias QueryClassEnum
  (t/HMap :mandatory {:read-only (t/Val 0)
                       :mutating (t/Val 1)
                       :mutating-global (t/Val 2)
                       :control (t/Val 3)
                       :unknown (t/Val 4)}
          :complete? true))
(t/defalias QueryAnalysisFlagEnum
  (t/HMap :mandatory {:has-secrets (t/Val 1)
                       :writes-only-target-database (t/Val 2)
                       :changes-database-lifecycle (t/Val 4)}
          :complete? true))
(t/defalias Descriptor
  (t/HMap :mandatory
          {:schema (t/Val 1)
           :source SourceProvenance
           :contracts (t/HMap :mandatory {:driver DriverContract
                                           :durable-v1 DurableContract}
                              :complete? true)
           :enums (t/HMap :mandatory {:query-class QueryClassEnum
                                       :query-analysis-flag QueryAnalysisFlagEnum}
                              :complete? true)
           :types (t/Map TypeId QueryAnalysisType)
           :functions (t/Map FunctionId FunctionSpec)}
          :complete? true))

(t/defalias SymbolStatus
  (t/HMap :mandatory {:symbol t/Str :available? t/Bool} :complete? true))
;; Capability shapes are deliberately Durable V1-specific; the pilot does not
;; claim to type the native namespace's separate :driver report.
(t/defalias SupportedCapability
  (t/HMap :mandatory {:status ':supported
                       :contract ':durable-v1
                       :native-version t/Str
                       :minimum-native-version (t/Val "26.7.2")
                       :symbols (t/Map FunctionId SymbolStatus)
                       :provenance SourceProvenance}
          :complete? true))
(t/defalias UnsupportedCapability
  (t/HMap :mandatory {:status ':unsupported
                       :type ':jdbc.chdb.native/unsupported-core
                       :contract ':durable-v1
                       :native-version t/Str
                       :minimum-native-version (t/Val "26.7.2")
                       :symbols (t/Map FunctionId SymbolStatus)
                       :provenance SourceProvenance
                       :missing (t/Vec FunctionId)}
          :complete? true))
(t/defalias DurableCapability (t/U SupportedCapability UnsupportedCapability))

;; These signatures are an explicit trusted seam: the unchanged production
;; bodies are verified by the stock Jolt ABI/JDBC gates, while this pilot checks
;; JVM consumers of their runtime-neutral data. validate-descriptor! alone must
;; accept t/Any because its job is to refine untrusted EDN or throw; the runtime
;; descriptor mutants verify that trusted refinement boundary.
(t/ann jdbc.chdb.abi/descriptor [-> Descriptor])
(t/ann jdbc.chdb.abi/validate-descriptor! [t/Any -> Descriptor])
(t/ann jdbc.chdb.abi/source-provenance [-> SourceProvenance])
(t/ann jdbc.chdb.abi/function-spec [FunctionId -> FunctionSpec])
(t/ann jdbc.chdb.abi/type-spec [TypeId -> QueryAnalysisType])
(t/ann jdbc.chdb.abi/contract-spec
  (t/IFn [':driver -> DriverContract]
         [':durable-v1 -> DurableContract]))
(t/ann jdbc.chdb.abi/contract-functions
  [ContractId -> (t/Map FunctionId FunctionSpec)])
