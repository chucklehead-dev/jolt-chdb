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

(defn- project-deps []
  (-> "deps.edn" io/file slurp edn/read-string))

(defn- library-path []
  (or (some-> (System/getenv "JOLT_CHDB_LIB") str/trim not-empty)
      (throw (ex-info "qualification requires one Jolt-selected libchdb path"
                      {:type ::library-selection-missing}))))

(defn- loader-causes [error]
  (loop [cause (ex-cause error)
         depth 0
         summaries []]
    (if (and cause (< depth 4))
      (recur (ex-cause cause)
             (inc depth)
             (conj summaries
                   {:class (.getName ^Class (class cause))
                    :message (ex-message cause)}))
      summaries)))

(defn- selected-library! [path]
  (when-not (.isFile (io/file path))
    (throw (ex-info "selected libchdb does not exist"
                    {:type ::library-missing :path path})))
  (try
    (ffi/load-library path)
    (catch clojure.lang.ExceptionInfo error
      (throw (ex-info "selected libchdb could not be loaded"
                      {:type ::library-load-failed
                       :path path
                       :causes (loader-causes error)}
                      error)))))

(defn- bindings [library]
  {:functions
   (into {}
         (map (fn [{:keys [function symbol args return]}]
                [function (ffi/cfn library symbol args return)]))
         (abi/binding-specs smoke-function-ids))
   :capabilities
   (mapv (fn [{:keys [function symbol]}]
           {:function function
            :symbol symbol
            :available? (boolean (ffi/find-symbol library symbol))})
         (abi/binding-specs smoke-function-ids))})

(defn- bytes! [arena value]
  (let [data (.getBytes (str value) "UTF-8")
        pointer (ffi/alloc arena (max 1 (alength data)))]
    (when (pos? (alength data))
      (ffi/write-array pointer :byte data))
    {:pointer pointer :length (alength data)}))

(defn- run-native-smoke [{:keys [functions capabilities]}]
  (let [{:keys [version connect close-conn query-with-params-n
                destroy-query-result result-buffer result-length result-error]}
        functions
        native-version (version)
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
                      missing-reinterpret-error
                      (try
                        ;; Required red control: C result pointers have size
                        ;; zero and must not be copied until bounded exactly.
                        (ffi/read-array buffer :byte length)
                        nil
                        (catch clojure.lang.ExceptionInfo error
                          {:message (ex-message error)
                           :data-keys (set (keys (ex-data error)))}))
                      copied (ffi/read-array
                              (ffi/reinterpret buffer length) :byte length)]
                  (reset! copied-result
                          {:native-version native-version
                           :capabilities capabilities
                           :bytes (vec copied)
                           :missing-reinterpret-error
                           missing-reinterpret-error}))
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

(defn- exception-info [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo error error)))

(defn- runtime-id []
  (if (System/getProperty "babashka.version") :babashka :jvm))

(defn- validate-compatibility! [pins runtime]
  (let [deps (project-deps)
        ffi-path (get-in pins [:jvm :ffi-dependency :deps-path])
        platform {:os (case (System/getProperty "os.name")
                        "Linux" :linux
                        nil)
                  :arch (case (System/getProperty "os.arch")
                          ("amd64" "x86_64") "amd64"
                          ("aarch64" "arm64") "arm64"
                          nil)}]
    (assert (= 1 (:schema pins)))
    (assert (= :test-only (get-in pins [:temporary-binding :scope])))
    (assert (= :phase-1 (get-in pins [:temporary-binding :retire-in])))
    (assert (= (get-in pins [:jolt :version]) (:jolt/min-version deps)))
    (assert (= (get-in pins [:jvm :ffi-dependency :commit])
               (get-in deps ffi-path)))
    (assert (some #{platform} (get-in pins [:native :qualified-platforms]))
            (str "unqualified native platform " (pr-str platform)))
    (assert (false? (get-in pins
                            [:babashka :embedded-ffi-source
                             :runtime-verifiable?])))
    (when (= :jvm runtime)
      (assert (= :corretto (get-in pins [:jvm :jdk-distribution])))
      (assert (= "Amazon.com Inc." (System/getProperty "java.vendor")))
      (assert (= (get-in pins [:jvm :jdk-version])
                 (System/getProperty "java.runtime.version")))))
  pins)

(defn -main [& _]
  (let [pins (compatibility)
        runtime (runtime-id)
        expected-bb (get-in pins [:babashka :tag])
        actual-bb (some-> (System/getProperty "babashka.version") (#(str "v" %)))
        _ (validate-compatibility! pins runtime)
        path (library-path)
        library (selected-library! path)
        binding-setup (bindings library)
        smoke (run-owned!
               (fn []
                 (let [result (run-native-smoke binding-setup)]
                   ;; Native pointers and arenas are lexical to the worker.
                   ;; Only this closed immutable value crosses the join.
                   (assoc result :result :completed))))
        missing-path (str path ".required-missing-control")
        wrong-library (ffi/load-system-library "z")
        missing-library-error
        (exception-info #(selected-library! missing-path))
        wrong-library-error
        (exception-info
         #((ffi/cfn wrong-library
                    (:symbol (abi/function-spec :version))
                    (:args (abi/function-spec :version))
                    (:return (abi/function-spec :version)))))
        loader-diagnostic-error
        (exception-info
         #(with-redefs
            [ffi/load-library
             (fn [_]
               (throw (ex-info "bounded loader failure" {}
                               (IllegalArgumentException. "dlopen cause"))))]
            (selected-library! path)))]
    (when (= :babashka runtime)
      (assert (= expected-bb actual-bb)
              (str "expected Babashka " expected-bb ", got " actual-bb)))
    (assert (= (get-in pins [:native :version]) (:native-version smoke)))
    (assert (= [52 50 10] (:bytes smoke)))
    (assert (every? :available? (:capabilities smoke)))
    (assert (= smoke-function-ids
               (mapv :function (:capabilities smoke))))
    (assert (= #{:pointer}
               (get-in smoke [:missing-reinterpret-error :data-keys])))
    (assert (re-find #"has size 0; give it a size with reinterpret"
                     (get-in smoke [:missing-reinterpret-error :message])))
    (assert (= 1 (:destroyed smoke)))
    (assert (= 1 (:closed smoke)))
    (assert (= :completed (:result smoke)))
    (assert (= #{:result :native-version :capabilities :bytes
                 :missing-reinterpret-error :destroyed :closed}
               (set (keys smoke)))
            "native pointers must not escape the owned worker")
    (assert (= ::library-missing (:type (ex-data missing-library-error)))
            "a missing selected library must reject with library-missing")
    (assert (= missing-path (:path (ex-data missing-library-error))))
    (assert (= (:symbol (abi/function-spec :version))
               (:symbol (ex-data wrong-library-error)))
            "an explicitly wrong library must reject its missing symbol")
    (assert (= (str "babashka.ffi: symbol not found: "
                    (:symbol (abi/function-spec :version)))
               (ex-message wrong-library-error)))
    (assert (= ::library-load-failed
               (:type (ex-data loader-diagnostic-error))))
    (assert (= path (:path (ex-data loader-diagnostic-error))))
    (assert (= [{:class "java.lang.IllegalArgumentException"
                 :message "dlopen cause"}]
               (:causes (ex-data loader-diagnostic-error)))
            "loader diagnostics expose only bounded class/message causes")
    (println "ABI FFI characterization passed"
             {:runtime runtime
              :native-version (:native-version smoke)
              :destroyed (:destroyed smoke)
              :closed (:closed smoke)
              :os (System/getProperty "os.name")
              :arch (System/getProperty "os.arch")
              :java (System/getProperty "java.version")})))
