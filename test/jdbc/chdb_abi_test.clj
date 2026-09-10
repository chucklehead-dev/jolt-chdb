(ns jdbc.chdb-abi-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jdbc.chdb.abi :as abi]
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

(def expected-result-statistics-functions
  {:result-elapsed
   {:symbol "chdb_result_elapsed" :args [:pointer] :return :double
    :contracts [:driver]}
   :result-rows-read
   {:symbol "chdb_result_rows_read" :args [:pointer] :return :uint64
    :contracts [:driver]}
   :result-bytes-read
   {:symbol "chdb_result_bytes_read" :args [:pointer] :return :uint64
    :contracts [:driver]}
   :result-storage-rows-read
   {:symbol "chdb_result_storage_rows_read" :args [:pointer] :return :uint64
    :contracts [:driver]}
   :result-storage-bytes-read
   {:symbol "chdb_result_storage_bytes_read" :args [:pointer] :return :uint64
    :contracts [:driver]}
   :result-rows-written
   {:symbol "chdb_result_rows_written" :args [:pointer] :return :uint64
    :contracts [:driver]}
   :result-bytes-written
   {:symbol "chdb_result_bytes_written" :args [:pointer] :return :uint64
    :contracts [:driver]}})

(def smoke-function-ids
  [:version :connect :close-conn :query-with-params-n
   :destroy-query-result :result-buffer :result-length :result-error])

(defn- read-edn [path]
  (-> path io/file slurp edn/read-string))

(defn- hosted-jdk-pin-matches?
  [pins workflow]
  (let [{:keys [corretto-version setup-java-cache-version url sha256]}
        (get-in pins [:jvm :hosted-linux-x64-archive])]
    (every? #(str/includes? workflow %)
            [corretto-version setup-java-cache-version url sha256
             "distribution: jdkfile"
             "architecture: x64"])))

(defn- hosted-bb-pin-matches?
  [pins workflow]
  (let [{:keys [linkage url sha256]}
        (get-in pins [:babashka :hosted-linux-x64-archive])]
    (and (= :dynamic linkage)
         (every? #(str/includes? workflow %) [url sha256])
         (not (str/includes? workflow "-linux-amd64-static.tar.gz"))
         (not (re-find #"(?m)^\s+bb:" workflow)))))

(defn- allocated-bytes! [allocated value]
  (let [bytes (.getBytes (str value) "UTF-8")
        pointer (ffi/alloc (max 1 (alength bytes)))]
    (swap! allocated conj pointer)
    (when (pos? (alength bytes))
      (ffi/write-array pointer bytes))
    {:pointer pointer :length (alength bytes)}))

(defn- pointer-array! [allocated pointer]
  (let [array (ffi/alloc (ffi/sizeof :pointer))]
    (swap! allocated conj array)
    (ffi/write array :pointer pointer 0)
    array))

(defn- length-array! [allocated length]
  (let [array (ffi/alloc (ffi/sizeof :size_t))]
    (swap! allocated conj array)
    (ffi/write array :size_t length 0)
    array))

(defn- run-owned! [f]
  (let [outcome (promise)
        thread (Thread.
                (fn []
                  (deliver outcome
                           (try {:value (f)}
                                (catch Throwable error {:error error})))))]
    (.setName thread "jolt-chdb-jolt-ffi-smoke")
    (.start thread)
    (.join thread)
    (let [{:keys [value error]} @outcome]
      (if error (throw error) value))))

(defn- run-jolt-native-smoke []
  (run-owned!
   (fn []
     (native/ensure-loaded!)
     (let [native-version (native/chdb-version)
           capabilities
           (mapv (fn [{:keys [function symbol]}]
                   {:function function
                    :symbol symbol
                    :available? (boolean (ffi/find-symbol symbol))})
                 (abi/binding-specs smoke-function-ids))]
       (when-not (every? :available? capabilities)
         (throw (ex-info "selected libchdb lacks a required smoke symbol"
                         {:capabilities capabilities})))
       (let [handle (native/open! ":memory:")
             destroyed (atom 0)
             closed (atom 0)
             copied (atom nil)]
         (try
           (native/with-live-handle
            handle
            (fn [connection]
              (let [allocated (atom [])]
                (try
                  (let [query (allocated-bytes! allocated "SELECT {p1:UInt64}")
                      format (allocated-bytes! allocated "CSV")
                      name (allocated-bytes! allocated "p1")
                      value (allocated-bytes! allocated "42")
                      names (pointer-array! allocated (:pointer name))
                      name-lengths (length-array! allocated (:length name))
                      values (pointer-array! allocated (:pointer value))
                      value-lengths (length-array! allocated (:length value))
                      result
                      (native/chdb-query-with-params-n
                       connection
                       (:pointer query) (:length query)
                       (:pointer format) (:length format)
                       names name-lengths values value-lengths 1)]
                    (when (ffi/null? result)
                      (throw (ex-info "chDB returned a null query result" {})))
                    (try
                      (when-let [message (native/chdb-result-error result)]
                        (throw (ex-info (str "chDB query failed: " message) {})))
                      (let [length (native/chdb-result-length result)]
                        (reset! copied
                                (vec (ffi/read-array
                                      (native/chdb-result-buffer result) length))))
                      (finally
                        (native/chdb-destroy-query-result result)
                        (swap! destroyed inc))))
                  (finally
                    (doseq [pointer (reverse @allocated)]
                      (ffi/free pointer)))))))
           (finally
             (native/close! handle)
             (swap! closed inc)))
         ;; No native pointer or arena crosses the positive thread join.
         {:native-version native-version
          :capabilities capabilities
          :bytes @copied
          :destroyed @destroyed
          :closed @closed})))))

(defn- run-descriptor-checks []
  (println "chDB versioned ABI descriptor")
  (let [descriptor (abi/descriptor)
        pins (read-edn "resources/jdbc/chdb/ffi-compatibility.edn")
        deps (read-edn "deps.edn")
        workflow (slurp ".github/workflows/tests.yml")
        ffi-path (get-in pins [:jvm :ffi-dependency :deps-path])
        platform (select-keys (native/platform) [:os :arch])]
    (check "descriptor validates as schema 1" descriptor
           (abi/validate-descriptor! descriptor))
    (check "provenance names the exact upstream header and oracle commit"
           expected-source (abi/source-provenance))
    (check "Durable signatures independently match the pinned chdb.h oracle"
           expected-durable-functions (abi/contract-functions :durable-v1))
    (check "result statistics signatures match the pinned chdb.h oracle"
           expected-result-statistics-functions
           (select-keys (abi/contract-functions :driver)
                        (keys expected-result-statistics-functions)))
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
    (check "ordered binding generation is derived from the canonical descriptor"
           smoke-function-ids
           (mapv :function (abi/binding-specs smoke-function-ids)))
    (check "compatibility Jolt version agrees with deps.edn"
           (:jolt/min-version deps) (get-in pins [:jolt :version]))
    (check "JVM FFI revision agrees with the selected deps.edn alias"
           (get-in deps ffi-path)
           (get-in pins [:jvm :ffi-dependency :commit]))
    (check "hosted BB uses the manifest-pinned dynamic artifact"
           true (hosted-bb-pin-matches? pins workflow))
    (check "a static hosted BB selection turns the guard red"
           false
           (hosted-bb-pin-matches?
            (assoc-in pins [:babashka :hosted-linux-x64-archive :linkage]
                      :static)
            workflow))
    (check "hosted exact-JDK archive agrees with the compatibility manifest"
           true (hosted-jdk-pin-matches? pins workflow))
    (check "hosted JDK checksum drift turns the guard red"
           false
           (hosted-jdk-pin-matches?
            (assoc-in pins [:jvm :hosted-linux-x64-archive :sha256]
                      (apply str (repeat 64 "0")))
            workflow))
    (check "hosted JDK archive-version drift turns the guard red"
           false
           (hosted-jdk-pin-matches?
            (assoc-in pins
                      [:jvm :hosted-linux-x64-archive :corretto-version]
                      "25.0.2.10.0")
            workflow))
    (check "compatibility native version agrees with the production selector"
           native/version (get-in pins [:native :version]))
    (check "compatibility archive digest agrees with the production selector"
           (get-in native/assets [[(:os platform) (:arch platform)] :sha256])
           (get-in pins [:native :archive-sha256
                         [(:os platform) (:arch platform)]]))
    (check "the running native platform is explicitly qualified"
           true
           (boolean (some #{platform}
                          (get-in pins [:native :qualified-platforms]))))

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

(defn- run-jolt-smoke-checks []
  (println "Jolt descriptor-driven owned-thread ABI smoke")
  (let [result (run-jolt-native-smoke)]
    (check "positive control crosses real chdb_version"
           "26.7.0" (:native-version result))
    (check "parameterized SELECT 42 is copied exactly before destruction"
           [52 50 10] (:bytes result))
    (check "query result is destroyed exactly once" 1 (:destroyed result))
    (check "connection owner is closed exactly once" 1 (:closed result))
    (check "only descriptor-selected capabilities are exercised"
           smoke-function-ids (mapv :function (:capabilities result)))
    (check "every descriptor-selected symbol is available"
           true (every? :available? (:capabilities result)))
    (check "no native owner, result, buffer, or arena escapes the worker"
           #{:native-version :capabilities :bytes :destroyed :closed}
           (set (keys result)))))

(defn -main [& _]
  (reset! failures 0)
  (run-descriptor-checks)
  (run-stock-library-checks)
  (run-jolt-smoke-checks)
  (if (zero? @failures)
    (println "all ABI checks passed")
    (throw (ex-info (str @failures " ABI checks failed")
                    {:failures @failures}))))
