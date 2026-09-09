(ns jdbc.chdb-abi-babashka-test
  "Phase 0 characterization only. Phase 1 must delete this temporary binding
  path after folding its host adapter into jdbc.chdb.native."
  (:require [babashka.ffi :as ffi]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jdbc.chdb.abi :as abi]))

(def smoke-function-ids
  [:version :connect :close-conn :query-with-params-n
   :destroy-query-result :result-buffer :result-length :result-error])

(defn- compatibility []
  (-> "jdbc/chdb/ffi-compatibility.edn" io/resource slurp edn/read-string))

(defn- library-path []
  (or (some-> (System/getenv "JOLT_CHDB_LIB") str/trim not-empty)
      (str (System/getProperty "user.home")
           "/.cache/jolt-chdb/26.7.0/linux-amd64/libchdb.so")))

(defn- selected-library! [path]
  (when-not (.isFile (io/file path))
    (throw (ex-info "selected libchdb does not exist"
                    {:type ::library-missing :path path})))
  (ffi/load-library path))

(defn- bindings [library]
  (into {}
        (map (fn [{:keys [function symbol args return]}]
               [function (ffi/cfn library symbol args return)]))
        (abi/binding-specs smoke-function-ids)))

(defn- bytes! [arena value]
  (let [data (.getBytes (str value) "UTF-8")
        pointer (ffi/alloc arena (max 1 (alength data)))]
    (when (pos? (alength data))
      (ffi/write-array pointer :byte data))
    {:pointer pointer :length (alength data)}))

(defn- run-native-smoke [library]
  (let [{:keys [version connect close-conn query-with-params-n
                destroy-query-result result-buffer result-length result-error]}
        (bindings library)
        native-version (version)
        capabilities
        (mapv (fn [{:keys [function symbol]}]
                {:function function
                 :symbol symbol
                 :available? (boolean (ffi/find-symbol library symbol))})
              (abi/binding-specs smoke-function-ids))
        _ (when-not (every? :available? capabilities)
            (throw (ex-info "selected libchdb lacks a required smoke symbol"
                            {:capabilities capabilities})))
        destroyed (atom 0)
        closed (atom 0)
        copied-result (atom nil)
        owner (connect 0 ffi/null)]
    (when (ffi/null? owner)
      (throw (ex-info "chDB returned a null owner" {})))
    (try
      (let [pointer-width (ffi/sizeof :pointer)
            connection (ffi/read (ffi/reinterpret owner pointer-width) :pointer)]
        (when (ffi/null? connection)
          (throw (ex-info "chDB returned a null connection" {})))
        (with-open [arena (ffi/confined-arena)]
          (let [query (bytes! arena "SELECT {p1:UInt64}")
                format (bytes! arena "CSV")
                name (bytes! arena "p1")
                value (bytes! arena "42")
                names (ffi/alloc arena pointer-width)
                name-lengths (ffi/alloc arena (ffi/sizeof :size_t))
                values (ffi/alloc arena pointer-width)
                value-lengths (ffi/alloc arena (ffi/sizeof :size_t))]
            (ffi/write names :pointer (:pointer name))
            (ffi/write name-lengths :size_t (:length name))
            (ffi/write values :pointer (:pointer value))
            (ffi/write value-lengths :size_t (:length value))
            (let [result (query-with-params-n
                          connection
                          (:pointer query) (:length query)
                          (:pointer format) (:length format)
                          names name-lengths values value-lengths 1)]
              (when (ffi/null? result)
                (throw (ex-info "chDB returned a null query result" {})))
              (try
                (when-let [message (result-error result)]
                  (throw (ex-info (str "chDB query failed: " message) {})))
                (let [length (result-length result)
                      buffer (result-buffer result)
                      missing-reinterpret-rejected?
                      (try
                        ;; Required red control: C result pointers have size
                        ;; zero and must not be copied until bounded exactly.
                        (ffi/read-array buffer :byte length)
                        false
                        (catch Throwable _ true))
                      copied (ffi/read-array
                              (ffi/reinterpret buffer length) :byte length)]
                  (reset! copied-result
                          {:native-version native-version
                           :capabilities capabilities
                           :bytes (vec copied)
                           :missing-reinterpret-rejected?
                           missing-reinterpret-rejected?}))
                (finally
                  (destroy-query-result result)
                  (swap! destroyed inc)))))))
      (finally
        (close-conn owner)
        (swap! closed inc)))
    (assoc @copied-result :destroyed @destroyed :closed @closed)))

(defn- run-owned! [f]
  (let [outcome (promise)
        thread (Thread. ^Runnable
                        (fn []
                          (deliver outcome
                                   (try {:value (f)}
                                        (catch Throwable error
                                          {:error error})))))]
    (.setName thread "jolt-chdb-babashka-ffi-smoke")
    (.start thread)
    (.join thread)
    (let [{:keys [value error]} @outcome]
      (if error (throw error) value))))

(defn- rejected? [f]
  (try (f) false (catch Throwable _ true)))

(defn- runtime-id []
  (if (System/getProperty "babashka.version") :babashka :jvm))

(defn- validate-compatibility! [pins runtime]
  (assert (= 1 (:schema pins)))
  (assert (= :test-only (get-in pins [:temporary-binding :scope])))
  (assert (= :phase-1 (get-in pins [:temporary-binding :retire-in])))
  (assert (= {:tag "v1.13.220"
              :tag-commit "b98575c98a0ef4df77775ff25fd7fc7b591b1afd"
              :embedded-ffi-commit
              "aacb153618bc39ca1e4c397b8f30fb81c76d0c4c"}
             (:babashka pins)))
  (assert (= "aacb153618bc39ca1e4c397b8f30fb81c76d0c4c"
             (get-in pins [:jvm :ffi-commit])))
  (assert (= "26.7.0" (get-in pins [:native :version])))
  (assert (= [{:os :linux :arch "amd64"}]
             (get-in pins [:native :qualified-platforms])))
  (assert (= "Linux" (System/getProperty "os.name")))
  (assert (= "amd64" (System/getProperty "os.arch")))
  (when (= :jvm runtime)
    (assert (= :corretto (get-in pins [:jvm :jdk-distribution])))
    (assert (= "Amazon.com Inc." (System/getProperty "java.vendor")))
    (assert (= (get-in pins [:jvm :jdk-version])
               (System/getProperty "java.runtime.version"))))
  pins)

(defn -main [& _]
  (let [pins (compatibility)
        runtime (runtime-id)
        expected-bb (get-in pins [:babashka :tag])
        actual-bb (some-> (System/getProperty "babashka.version") (#(str "v" %)))
        path (library-path)
        library (selected-library! path)
        smoke (run-owned!
               (fn []
                 (let [result (run-native-smoke library)]
                   ;; Native pointers and arenas are lexical to the worker.
                   ;; Only this closed immutable value crosses the join.
                   (assoc result :result :completed))))
        missing-path (str path ".required-missing-control")
        wrong-library (ffi/load-system-library "z")
        wrong-library-rejected?
        (rejected?
         #((ffi/cfn wrong-library
                    (:symbol (abi/function-spec :version))
                    (:args (abi/function-spec :version))
                    (:return (abi/function-spec :version)))))]
    (validate-compatibility! pins runtime)
    (when (= :babashka runtime)
      (assert (= expected-bb actual-bb)
              (str "expected Babashka " expected-bb ", got " actual-bb)))
    (assert (= "26.7.0" (:native-version smoke)))
    (assert (= [52 50 10] (:bytes smoke)))
    (assert (every? :available? (:capabilities smoke)))
    (assert (= smoke-function-ids
               (mapv :function (:capabilities smoke))))
    (assert (:missing-reinterpret-rejected? smoke))
    (assert (= 1 (:destroyed smoke)))
    (assert (= 1 (:closed smoke)))
    (assert (= :completed (:result smoke)))
    (assert (= #{:result :native-version :capabilities :bytes
                 :missing-reinterpret-rejected? :destroyed :closed}
               (set (keys smoke)))
            "native pointers must not escape the owned worker")
    (assert (rejected? #(selected-library! missing-path))
            "a missing selected library must reject")
    (assert wrong-library-rejected?
            "an explicitly wrong library must not fall back globally")
    (println "ABI FFI characterization passed"
             {:runtime runtime
              :native-version (:native-version smoke)
              :destroyed (:destroyed smoke)
              :closed (:closed smoke)
              :os (System/getProperty "os.name")
              :arch (System/getProperty "os.arch")
              :java (System/getProperty "java.version")})))
