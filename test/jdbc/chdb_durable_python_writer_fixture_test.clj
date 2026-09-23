(ns jdbc.chdb-durable-python-writer-fixture-test
  "Read one immutable logical object emitted by the pinned Python Durable writer."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.digest :as digest]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.native :as native])
  (:import [java.io File]
           [java.nio.file CopyOption Files LinkOption Path Paths]
           [java.security MessageDigest]))

(def failures (atom 0))
(def ^:private expected-source "66643e5030fb73c30ac5cdd31d4c7858ea040ed0")
(def ^:private expected-tree "a61323f3f7246c83f9083ca8d4f733a7c4402c50")
(def ^:private expected-engine "26.7.3")
(def ^:private expected-source-archive
  "4269548e589fa34497c207e84e660785399b52edecb7294cc929be796b04b381")
(def ^:private expected-core-wheel
  "b10b96f9599fab42ba51d9be80333e1819782bdc8a91b2b26979149693ba431f")
(def ^:private expected-protocol
  "82538d958d2f522bea6e4a6ccbc27c6bb6e230e19a1a386c605d43a0c02d11ba")
(def ^:private expected-suite
  "5b9a97a3bccc0c29b10aca6b6e9a72edceea6a79e19d9a54c900b10d24f5eee6")
(def ^:private expected-extension
  "15aae3d06f0f074ea91fe126983918ae7868fa791b999af112b3142413dc2cc0")
(def ^:private expected-aggregate
  {:label_bytes "32" :max_n "3" :min_n "1" :n "3" :sum_n "6" :true_count "2"})
(def ^:private no-link-options (make-array LinkOption 0))
(def ^:private no-copy-options (make-array CopyOption 0))

(defn- fail! [message]
  (throw (ex-info message {:type ::invalid-fixture})))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do (swap! failures inc)
        (println "  FAIL" label "- expected" (pr-str expected)
                 "got" (pr-str actual)))))

(defn- rejected [f]
  (try (f) nil (catch Throwable error error)))

(defn- exact-keys! [label expected value]
  (when-not (and (map? value) (= expected (set (keys value))))
    (fail! (str label " has missing or unknown fields")))
  value)

(defn- path [value]
  (Paths/get (str value) (make-array String 0)))

(defn- safe-key! [key]
  (let [parts (when (string? key) (str/split key #"/" -1))]
    (when-not (and (seq parts)
                   (not (str/starts-with? key "/"))
                   (not (str/includes? key "\\"))
                   (every? #(and (not (str/blank? %))
                                 (not (contains? #{"." ".."} %)))
                           parts))
      (fail! "fixture key is not a safe relative path"))
    key))

(defn- resolve-key [^Path root key]
  (let [resolved (.normalize (.resolve root (safe-key! key)))]
    (when-not (.startsWith resolved root)
      (fail! "fixture key escaped its namespace root"))
    resolved))

(deftype ^:private RawReadOnlyBackend [^Path root]
  backend/ObjectBackend
  (get-bytes [_ key]
    (let [target (resolve-key root key)]
      (when (Files/exists target no-link-options)
        (Files/readAllBytes target))))
  (get-with-etag [this key]
    (when-let [bytes (backend/get-bytes this key)]
      {:bytes bytes :etag (digest/sha256-file (resolve-key root key))}))
  (put-file-if-absent! [_ _ _] (fail! "Python fixture backend is read-only"))
  (put-bytes-if-absent! [_ _ _] (fail! "Python fixture backend is read-only"))
  (replace-if-match! [_ _ _ _] (fail! "Python fixture backend is read-only"))
  (download-to-file! [_ key destination]
    (let [source (resolve-key root key)
          target (path destination)]
      (if (Files/exists source no-link-options)
        (do (Files/copy source target no-copy-options)
            {:status :downloaded :byte-count (Files/size target)})
        {:status :not-found}))))

(defn- raw-read-only-backend [root]
  (let [root (.normalize (.toAbsolutePath (path root)))]
    (when-not (Files/isDirectory root no-link-options)
      (fail! "fixture root is not a directory"))
    (RawReadOnlyBackend. root)))

(defn- file-identity [file]
  (let [file (File. (str file))]
    (when-not (.isFile file) (fail! "provenance artifact is not a file"))
    {:file_name (.getName file)
     :bytes (.length file)
     :sha256 (digest/sha256-file (.toPath file))}))

(defn- byte->hex [value]
  (format "%02x" (bit-and 255 value)))

(defn- sha256-text [text]
  (let [hash (.digest (MessageDigest/getInstance "SHA-256")
                      (.getBytes text "UTF-8"))]
    (apply str (map byte->hex hash))))

(defn- inventory [root]
  (let [root (path root)]
    (->> (file-seq (.toFile root))
         (keep (fn [^File file]
                 (let [target (.toPath file)]
                   (cond
                     (Files/isSymbolicLink target)
                     (fail! "logical fixture contains a symbolic link")

                     (Files/isRegularFile target no-link-options)
                     {:key (str/replace (str (.relativize root target)) "\\" "/")
                      :bytes (Files/size target)
                      :sha256 (digest/sha256-file target)}

                     :else nil))))
         (sort-by :key)
         vec)))

(defn- inventory-sha [entries]
  (sha256-text
   (apply str (map #(str (:key %) "\u0000" (:bytes %) "\u0000" (:sha256 %) "\n")
                   entries))))

(defn- validate-provenance!
  [descriptor fixture-root source-archive core-wheel native-library native-header]
  (exact-keys! "descriptor" #{:schema_version :source :python :jolt_native
                               :provider_boundary :fixture} descriptor)
  (exact-keys! "source" #{:repository :commit :tree :archive
                           :protocol_sha256 :suite_sha256} (:source descriptor))
  (exact-keys! "Python" #{:implementation :version :durable_module :core_version
                           :engine_version :core_wheel :extension} (:python descriptor))
  (exact-keys! "Jolt native" #{:expected_engine_version :library :header}
               (:jolt_native descriptor))
  (exact-keys! "provider boundary" #{:producer :consumer
                                      :direct_provider_compatibility
                                      :excluded_provider_private_keys}
               (:provider_boundary descriptor))
  (exact-keys! "fixture" #{:object_id :database :expected :manifest
                            :logical_inventory :inventory_sha256}
               (:fixture descriptor))
  (exact-keys! "expected aggregate"
               #{:label_bytes :max_n :min_n :n :sum_n :true_count}
               (get-in descriptor [:fixture :expected]))
  (exact-keys! "manifest" #{:base :db :seq :wal}
               (get-in descriptor [:fixture :manifest]))
  (when-let [base (get-in descriptor [:fixture :manifest :base])]
    (exact-keys! "base reference" #{:key :size :sha256} base))
  (doseq [reference (get-in descriptor [:fixture :manifest :wal])]
    (exact-keys! "WAL reference" #{:key :size :sha256} reference))
  (doseq [[label value] [["source archive" (get-in descriptor [:source :archive])]
                         ["core wheel" (get-in descriptor [:python :core_wheel])]
                         ["Python extension" (get-in descriptor [:python :extension])]
                         ["native library" (get-in descriptor [:jolt_native :library])]
                         ["native header" (get-in descriptor [:jolt_native :header])]]]
    (exact-keys! label #{:file_name :bytes :sha256} value))
  (doseq [entry (get-in descriptor [:fixture :logical_inventory])]
    (exact-keys! "inventory entry" #{:key :bytes :sha256} entry))
  (when-not (and (= 1 (:schema_version descriptor))
                 (= "https://github.com/chdb-io/chdb.git"
                    (get-in descriptor [:source :repository]))
                 (= expected-source (get-in descriptor [:source :commit]))
                 (= expected-tree (get-in descriptor [:source :tree]))
                 (= expected-source-archive
                    (get-in descriptor [:source :archive :sha256]))
                 (= expected-core-wheel
                    (get-in descriptor [:python :core_wheel :sha256]))
                 (= expected-protocol (get-in descriptor [:source :protocol_sha256]))
                 (= expected-suite (get-in descriptor [:source :suite_sha256]))
                 (= "cpython" (get-in descriptor [:python :implementation]))
                 (= "chdb/durable/__init__.py"
                    (get-in descriptor [:python :durable_module]))
                 (= expected-engine (get-in descriptor [:python :core_version]))
                 (= expected-engine (get-in descriptor [:python :engine_version]))
                 (= expected-extension (get-in descriptor [:python :extension :sha256]))
                 (= expected-engine (get-in descriptor [:jolt_native :expected_engine_version]))
                 (= "libchdb.so" (get-in descriptor [:jolt_native :library :file_name]))
                 (= "chdb.h" (get-in descriptor [:jolt_native :header :file_name]))
                 (= "python-local-raw-mtime-size-etag"
                    (get-in descriptor [:provider_boundary :producer]))
                 (= "jolt-test-only-raw-read-only"
                    (get-in descriptor [:provider_boundary :consumer]))
                 (false? (get-in descriptor [:provider_boundary :direct_provider_compatibility]))
                 (let [excluded (get-in descriptor [:provider_boundary :excluded_provider_private_keys])
                       checkpoint? (some? (get-in descriptor [:fixture :manifest :base]))]
                   (and (vector? excluded)
                        (= (count excluded) (if checkpoint? 3 2))
                        (contains? (set excluded) "head.json.lock")
                        (contains? (set excluded) "unreferenced-provider.canary")
                        (or (not checkpoint?)
                            (= 1 (count (filter #(str/starts-with? % "wal/") excluded))))))
                 (= "python-writer" (get-in descriptor [:fixture :object_id]))
                 (= "fixture" (get-in descriptor [:fixture :database]))
                 (= expected-aggregate (get-in descriptor [:fixture :expected])))
    (fail! "fixture provenance or provider boundary differs"))
  (doseq [[label expected actual]
          [["source archive" (get-in descriptor [:source :archive])
            (file-identity source-archive)]
           ["core wheel" (get-in descriptor [:python :core_wheel])
            (file-identity core-wheel)]
           ["native library" (get-in descriptor [:jolt_native :library])
            (file-identity native-library)]
           ["native header" (get-in descriptor [:jolt_native :header])
            (file-identity native-header)]]]
    (when-not (= expected actual) (fail! (str label " identity differs"))))
  (when-not (= expected-engine (:native-version (native/durable-capability)))
    (fail! "running Jolt chDB engine differs from the Python fixture engine"))
  descriptor)

(defn- validate-descriptor!
  [descriptor fixture-root source-archive core-wheel native-library native-header]
  (validate-provenance! descriptor fixture-root source-archive core-wheel native-library native-header)
  (let [object-root (.resolve (path fixture-root)
                              (get-in descriptor [:fixture :object_id]))
        actual (inventory object-root)]
    (when-not (= actual (get-in descriptor [:fixture :logical_inventory]))
      (fail! "logical fixture inventory differs"))
    (when-not (= (inventory-sha actual)
                 (get-in descriptor [:fixture :inventory_sha256]))
      (fail! "logical fixture inventory digest differs"))
    (let [head-bytes (Files/readAllBytes (.resolve object-root "head.json"))
          head (json/read-str (String. head-bytes "UTF-8") :key-fn keyword)
          keys (set (map :key actual))
          referenced (set (concat ["head.json"]
                                  (map :key (get-in head [:manifest :wal]))
                                  (when-let [base (get-in head [:manifest :base])] [(:key base)])))]
      (when-not (and (= expected-engine (get-in head [:engine :version]))
                     (= expected-engine (get-in head [:engine :min_reader]))
                     (= 1 (get-in head [:engine :backup_format]))
                     (nil? (get-in head [:lease :owner]))
                     (= (if (get-in head [:manifest :base]) 3 1) (get-in head [:manifest :seq]))
                     (= 1 (count (get-in head [:manifest :wal]))))
        (fail! "head fixture shape differs"))
      (when-not (and (= (get-in descriptor [:fixture :manifest]) (:manifest head))
                     (= keys referenced)
                     (= (int \{) (bit-and 255 (aget head-bytes 0))))
        (fail! "logical fixture includes an envelope, private key, or unreferenced key")))
    actual))

(defn- observed [opened]
  (let [row (-> (reader/query!
                 opened
                 (str "SELECT count(), sum(n), countIf(ok), sum(length(label)), "
                      "min(n), max(n) FROM fixture.events")
                 []) :rows first)
        keys [:n :sum_n :true_count :label_bytes :min_n :max_n]]
    (into {} (map (fn [key value] [key (str value)]) keys row))))

(defn- require-aggregate! [expected actual]
  (when-not (= expected actual)
    (fail! "recovered aggregate differs from the Python writer readback"))
  actual)

(defn- require-rows! [opened expected]
  (let [rows (:rows (reader/query! opened "SELECT n, ok, label FROM fixture.events ORDER BY n" []))]
    (when-not (= expected rows) (fail! "exact typed rows differ"))
    (doseq [[n ok label] rows]
      (when-not (and (integer? n) (instance? Boolean ok) (string? label))
        (fail! "recovered row types differ")))))

(defn- checkpoint-controls! [fixture-root descriptor artifact-paths]
  (let [object-id (get-in descriptor [:fixture :object_id])
        object-root (.resolve (path fixture-root) object-id)
        base (get-in descriptor [:fixture :manifest :base])
        wal (first (get-in descriptor [:fixture :manifest :wal]))
        snapshot-root (.resolve (.getParent (path fixture-root)) "base-only-store")
        snapshot-object (.resolve snapshot-root object-id)
        snapshot-before (inventory snapshot-object)
        snapshot-head (json/read-str (slurp (str (.resolve snapshot-object "head.json"))) :key-fn keyword)]
    (when-not (and (= 2 (get-in snapshot-head [:manifest :seq]))
                   (= base (get-in snapshot-head [:manifest :base]))
                   (empty? (get-in snapshot-head [:manifest :wal]))
                   (= #{"head.json" (:key base)} (set (map :key snapshot-before))))
      (fail! "invalid base-only snapshot"))
    (doseq [[label ref action] [["missing-base" base :missing] ["corrupt-base" base :corrupt]
                               ["truncated-base" base :truncated] ["missing-wal" wal :missing]
                               ["corrupt-wal" wal :corrupt] ["backup-format" nil :format]]]
      (let [scenario-root (.resolve (.getParent (path fixture-root)) (str "controls/" label))
            scenario (.resolve scenario-root object-id)]
        (doseq [entry (inventory object-root)]
          (let [target (.resolve scenario (safe-key! (:key entry)))]
            (Files/createDirectories (.getParent target) (make-array java.nio.file.attribute.FileAttribute 0))
            (Files/copy (.resolve object-root (:key entry)) target no-copy-options)))
        (if (= action :format)
          (let [target (.resolve scenario "head.json")
                head (json/read-str (slurp (str target)) :key-fn keyword)]
            (spit (str target) (json/write-str (assoc-in head [:engine :backup_format] 2))))
          (let [target (.resolve scenario (:key ref))]
          (if (= action :missing) (Files/delete target)
            (let [bytes (Files/readAllBytes target)
                  value (if (= action :truncated)
                          (let [value (byte-array (dec (alength bytes)))]
                            (System/arraycopy bytes 0 value 0 (alength value)) value)
                          bytes)]
              (when (= action :corrupt) (aset-byte value 0 (unchecked-byte (bit-xor 1 (aget value 0)))))
              (Files/write target value (make-array java.nio.file.OpenOption 0))))))
        (when (= action :format)
          (let [entries (inventory scenario)
                mutant (-> descriptor (assoc-in [:fixture :logical_inventory] entries)
                           (assoc-in [:fixture :inventory_sha256] (inventory-sha entries)))
                called? (atom false)
                error (with-redefs [durable/open-reader! (fn [& _] (reset! called? true) (fail! "reader unexpectedly opened"))]
                        (rejected #(apply validate-descriptor! mutant scenario-root artifact-paths)))]
            (when-not (and (= ::invalid-fixture (:type (ex-data error))) (false? @called?))
              (fail! "backup-format mismatch was not rejected before reader open"))
            (println "ok backup-format mismatch rejected before reader open")))
        (println "prepared independent Jolt control" label)))))

(defn- run-control! [fixture-root label]
  (let [object-id "python-writer"
        root (.resolve (path fixture-root) object-id)
        before (inventory root)
        error (try
                (let [opened (durable/open-reader! {:namespace-backend (raw-read-only-backend fixture-root) :object-id object-id})]
                  (try (when (= label "base-only")
                         (require-rows! opened [[1 true "snowman ☃"] [2 false "question ?"]]))
                       (finally (reader/close! opened))))
                nil (catch Throwable error error))]
    (if (= label "base-only")
      (when error (throw error))
      (when-not (= [::durable/startup-failed :recover ::durable/corrupt]
                   [(:type (ex-data error))
                    (:jdbc.chdb.durable/startup-stage (ex-data error))
                    (:type (some-> error .getCause ex-data))])
        (fail! (str "expected recover-stage corruption rejection: " label))))
    (when-not (= before (inventory root)) (fail! "control recovery changed protocol bytes"))
    (println "ok independent fresh-process Jolt control" label)))

(defn- valid-main! [args]
  (when-not (= 6 (count args))
    (fail! "usage: FIXTURE_ROOT DESCRIPTOR SOURCE_ARCHIVE CORE_WHEEL LIBCHDB HEADER"))
  (reset! failures 0)
  (let [workflow (slurp ".github/workflows/durable-python-fixture.yml")
        runner (slurp "scripts/verify-durable-python-writer-fixture.sh")
        [fixture-root descriptor-path source-archive core-wheel native-library native-header]
        args
        descriptor (json/read-str (slurp descriptor-path) :key-fn keyword)
        before (validate-descriptor! descriptor fixture-root source-archive core-wheel
                                     native-library native-header)
        store (raw-read-only-backend fixture-root)
        opened (durable/open-reader!
                {:namespace-backend store
                 :object-id (get-in descriptor [:fixture :object_id])})
        actual (try
                 (when (get-in descriptor [:fixture :manifest :base])
                   (require-rows! opened [[1 true "snowman ☃"] [2 false "question ?"] [3 true "comma,quote"]]))
                 (observed opened) (finally (reader/close! opened)))
        after (validate-descriptor! descriptor fixture-root source-archive core-wheel
                                    native-library native-header)]
    (check "hosted fixture exchange uses the already-qualified cached Jolt"
           [false false]
           [(str/includes? workflow "/opt/chez")
            (str/includes? workflow "JOLT_WRAPPER=")])
    (check "local qualification retains an explicit optional wrapper seam"
           true
           (and (str/includes? runner "${JOLT_WRAPPER:-}")
                (str/includes? runner "jolt_command=(\"$wrapper\" \"$jolt_bin\")")))
    (check "Python writer aggregate survives Jolt WAL recovery" actual
           (require-aggregate! (get-in descriptor [:fixture :expected]) actual))
    (check "read-only recovery preserves every logical object byte" before after)
    (when (get-in descriptor [:fixture :manifest :base])
      (checkpoint-controls! fixture-root descriptor [source-archive core-wheel native-library native-header]))
    (doseq [[label mutant]
            [["source commit mutant is rejected"
              (assoc-in descriptor [:source :commit] (str "0" (subs expected-source 1)))]
             ["core wheel identity mutant is rejected"
              (assoc-in descriptor [:python :core_wheel :sha256] (apply str (repeat 64 "0")))]
             ["inventory digest mutant is rejected"
              (assoc-in descriptor [:fixture :inventory_sha256] (apply str (repeat 64 "0")))]
             ["engine pin mutant is rejected"
              (assoc-in descriptor [:jolt_native :expected_engine_version] "26.7.2-rc.2")]]]
      (let [called? (atom false)
            error (with-redefs [durable/open-reader! (fn [& _] (reset! called? true) (fail! "reader unexpectedly opened"))]
                    (rejected #(validate-descriptor!
                                 mutant fixture-root source-archive core-wheel
                                 native-library native-header)))]
        (check label [::invalid-fixture false] [(:type (ex-data error)) @called?])))
    (check "aggregate mutant is rejected after real recovery"
           true
           (boolean (rejected #(require-aggregate! (assoc actual :n "4") actual)))))
  (if (zero? @failures)
    (println "all Python-writer logical fixture checks passed")
    (throw (ex-info (str @failures " Python-writer fixture checks failed")
                    {:failures @failures}))))

(defn -main [& args]
  (if (= 7 (count args))
    (let [label (last args)]
      (when-not (contains? #{"base-only" "missing-base" "corrupt-base" "truncated-base" "missing-wal" "corrupt-wal"} label)
        (fail! "unknown control"))
      (apply validate-provenance! (json/read-str (slurp (second args)) :key-fn keyword) (first args) (take 4 (drop 2 args)))
      (run-control! (first args) label))
    (valid-main! args)))
