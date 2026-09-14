(ns jdbc.chdb-durable-linux-compatibility-test
  "Separate-process Linux x86-64 archive compatibility matrix cells."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.digest :as digest]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb.native :as native])
  (:import [java.io File]
           [java.nio.file CopyOption Files LinkOption OpenOption Path Paths]
           [java.security MessageDigest]))

(def ^:private no-link-options (make-array LinkOption 0))
(def ^:private no-copy-options (make-array CopyOption 0))
(def ^:private no-open-options (make-array OpenOption 0))
(def ^:private expected-aggregate
  {:n "3" :sum_n "6" :label_bytes "18" :min_n "1" :max_n "3"})

(def ^:private descriptor-keys
  #{:schema_version :producer :producer_release :matrix_sha256
    :manifest :expected :inventory :inventory_sha256})

(defn- fail! [message]
  (throw (ex-info message {:type ::invalid-matrix})))

(defn- check! [condition message]
  (when-not condition (fail! message)))

(defn- source-contract! []
  (let [workflow (slurp ".github/workflows/durable-linux-compatibility.yml")
        runner (slurp "scripts/verify-durable-linux-compatibility.sh")]
    (check! (and (not (str/includes? workflow "/opt/chez"))
                 (not (str/includes? workflow "JOLT_WRAPPER=")))
            "hosted matrix must use the already-qualified cached Jolt")
    (check! (and (str/includes? runner "${JOLT_WRAPPER:-}")
                 (str/includes? runner "jolt_command=(\"$wrapper\" \"$jolt_bin\")"))
            "local matrix must retain an explicit optional wrapper seam")))

(defn- rejected [f]
  (try (f) nil (catch Throwable error error)))

(defn- path [value]
  (Paths/get (str value) (make-array String 0)))

(defn- safe-key! [key]
  (let [parts (when (string? key) (str/split key #"/" -1))]
    (check! (and (seq parts) (not (str/starts-with? key "/"))
                 (not (str/includes? key "\\"))
                 (every? #(and (not (str/blank? %))
                               (not (contains? #{"." ".."} %))) parts))
            "matrix fixture key is unsafe")
    key))

(defn- resolve-key [^Path root key]
  (let [resolved (.normalize (.resolve root (safe-key! key)))]
    (check! (.startsWith resolved root) "matrix fixture key escaped its root")
    resolved))

(deftype ^:private RawReadOnlyBackend [^Path root downloads]
  backend/ObjectBackend
  (get-bytes [_ key]
    (let [target (resolve-key root key)]
      (when (Files/exists target no-link-options) (Files/readAllBytes target))))
  (get-with-etag [this key]
    (when-let [bytes (backend/get-bytes this key)]
      {:bytes bytes :etag (digest/sha256-file (resolve-key root key))}))
  (put-file-if-absent! [_ _ _] (fail! "matrix fixture is read-only"))
  (put-bytes-if-absent! [_ _ _] (fail! "matrix fixture is read-only"))
  (replace-if-match! [_ _ _ _] (fail! "matrix fixture is read-only"))
  (download-to-file! [_ key destination]
    (swap! downloads inc)
    (let [source (resolve-key root key)
          target (path destination)]
      (if (Files/exists source no-link-options)
        (do (Files/copy source target no-copy-options)
            {:status :downloaded :byte-count (Files/size target)})
        {:status :not-found}))))

(defn- raw-backend [root downloads]
  (let [root (.normalize (.toAbsolutePath (path root)))]
    (check! (Files/isDirectory root no-link-options) "matrix fixture root is absent")
    (RawReadOnlyBackend. root downloads)))

(defn- file-identity [file]
  (let [file (File. (str file))]
    (check! (.isFile file) "matrix artifact is absent")
    {:file_name (.getName file) :bytes (.length file)
     :sha256 (digest/sha256-file (.toPath file))}))

(defn- byte->hex [value]
  (format "%02x" (bit-and 255 value)))

(defn- sha256-text [text]
  (apply str (map byte->hex
                  (.digest (MessageDigest/getInstance "SHA-256")
                           (.getBytes text "UTF-8")))))

(defn- inventory [root]
  (let [root (path root)]
    (->> (file-seq (.toFile root))
         (keep (fn [^File file]
                 (let [target (.toPath file)]
                   (cond
                     (Files/isSymbolicLink target) (fail! "matrix fixture has a symlink")
                     (Files/isRegularFile target no-link-options)
                     {:key (str/replace (str (.relativize root target)) "\\" "/")
                      :bytes (Files/size target)
                      :sha256 (digest/sha256-file target)}
                     :else nil))))
         (sort-by :key) vec)))

(defn- inventory-sha [entries]
  (sha256-text
   (apply str (map #(str (:key %) "\u0000" (:bytes %) "\u0000" (:sha256 %) "\n")
                   entries))))

(defn- matrix [file]
  (let [value (json/read-str (slurp file) :key-fn keyword)]
    (check! (= 1 (:schema_version value)) "matrix schema differs")
    (check! (= {:os "linux" :arch "x86_64" :provider_compatibility false}
               (:scope value))
            "matrix scope differs")
    value))

(defn- release! [matrix release library header]
  (let [declared (get-in matrix [:releases release])]
    (check! declared "matrix release is unknown")
    (check! (= (:library declared) (file-identity library))
            "matrix library identity differs")
    (check! (= (:header declared) (file-identity header))
            "matrix header identity differs")
    (let [capability (native/durable-capability)]
      (check! (= :supported (:status capability)) "matrix library lacks Durable V1")
      (check! (= (:version declared) (:native-version capability))
              "matrix runtime version differs"))
    declared))

(defn- write-bytes! [root key bytes]
  (let [target (resolve-key root key)]
    (when-let [parent (.getParent target)]
      (Files/createDirectories parent (make-array java.nio.file.attribute.FileAttribute 0)))
    (Files/write target bytes no-open-options)))

(defn- produce! [matrix-file output release library header]
  (let [matrix (matrix matrix-file)
        declared (release! matrix release library header)
        output (.normalize (.toAbsolutePath (path output)))]
    (check! (not (Files/exists output no-link-options)) "producer output already exists")
    (Files/createDirectories output (make-array java.nio.file.attribute.FileAttribute 0))
    (let [store (backend/memory-backend)
          opened (durable/open-writer!
                  {:store store :owner "linux-compat-matrix"
                   :instance (str "producer-" (name release))
                   :database "compat" :lease-ttl-ms 30000})]
      (try
        (writer/execute! opened
                         (str "CREATE TABLE rows (n Int64, label String) "
                              "ENGINE=MergeTree ORDER BY n"))
        (writer/execute! opened
                         (str "INSERT INTO rows VALUES (1,'one'),(2,'two?'),"
                              "(3,'snowman ☃')"))
        (check! (= :committed (:status (writer/checkpoint! opened)))
                "checkpoint did not commit")
        (finally (writer/close! opened)))
      (let [head-bytes (backend/get-bytes store control/head-key)
            head (json/read-str (String. head-bytes "UTF-8") :key-fn keyword)
            manifest (:manifest head)
            reference (:base manifest)
            fixture-root (.resolve output "fixture-store")]
        (check! (and (= (:version declared) (get-in head [:engine :version]))
                     (= (:version declared) (get-in head [:engine :min_reader]))
                     (= 1 (get-in head [:engine :backup_format]))
                     (= "compat" (:db manifest)) reference (empty? (:wal manifest)))
                "producer head metadata differs")
        (write-bytes! fixture-root "head.json" head-bytes)
        (write-bytes! fixture-root (:key reference)
                      (backend/get-bytes store (:key reference)))
        (let [items (inventory fixture-root)
              descriptor {:schema_version 1 :producer (name release)
                          :producer_release declared
                          :matrix_sha256 (digest/sha256-file (path matrix-file))
                          :manifest manifest :expected expected-aggregate
                          :inventory items :inventory_sha256 (inventory-sha items)}]
          (spit (.toFile (.resolve output "fixture.json"))
                (str (json/write-str descriptor) "\n"))
          (println "PRODUCED" (name release) (:version declared)
                   (count items) "logical objects"))))))

(defn- observed [opened]
  (let [row (-> (reader/query!
                 opened
                 (str "SELECT count(), sum(n), sum(length(label)), min(n), max(n) "
                      "FROM compat.rows") []) :rows first)]
    (zipmap [:n :sum_n :label_bytes :min_n :max_n] (map str row))))

(defn- descriptor-valid! [matrix-file matrix descriptor fixture-root producer]
  (let [items (inventory fixture-root)
        head-bytes (Files/readAllBytes (.resolve (path fixture-root) "head.json"))
        head (json/read-str (String. head-bytes "UTF-8") :key-fn keyword)
        reference (get-in descriptor [:manifest :base])]
    (check! (= descriptor-keys (set (keys descriptor)))
            "fixture descriptor has missing or unknown fields")
    (check! (= 1 (:schema_version descriptor)) "fixture descriptor schema differs")
    (check! (= (digest/sha256-file (path matrix-file)) (:matrix_sha256 descriptor))
            "fixture matrix identity differs")
    (check! (= (name producer) (:producer descriptor)) "fixture producer differs")
    (check! (= (get-in matrix [:releases producer]) (:producer_release descriptor))
            "fixture producer provenance differs")
    (check! (= expected-aggregate (:expected descriptor)) "fixture aggregate differs")
    (check! (= (:manifest descriptor) (:manifest head)) "fixture head manifest differs")
    (check! (= #{"head.json" (:key reference)} (set (map :key items)))
            "fixture contains an envelope, private key, or unreferenced object")
    (check! (= (:size reference)
               (:bytes (first (filter #(= (:key reference) (:key %)) items))))
            "fixture base reference size differs")
    (check! (= (:sha256 reference)
               (:sha256 (first (filter #(= (:key reference) (:key %)) items))))
            "fixture base reference digest differs")
    (check! (= items (:inventory descriptor)) "fixture inventory differs")
    (check! (= (inventory-sha items) (:inventory_sha256 descriptor))
            "fixture inventory digest differs")
    items))

(defn- archive-cell [matrix producer reader]
  (let [matches (filter #(and (= (name producer) (:producer %))
                              (= (name reader) (:reader %)))
                        (:archive_cells matrix))]
    (check! (= 1 (count matches)) "archive matrix cell is missing or duplicated")
    (first matches)))

(defn- copy-to-memory [fixture-root descriptor mutate-base? mutate-min-reader?]
  (let [store (backend/memory-backend)
        head-path (.resolve (path fixture-root) "head.json")
        head (json/read-str (slurp (.toFile head-path)) :key-fn keyword)
        head (if mutate-min-reader? (assoc-in head [:engine :min_reader] "99.0.0") head)
        base-key (get-in descriptor [:manifest :base :key])
        base (Files/readAllBytes (.resolve (path fixture-root) base-key))
        base (if mutate-base?
               (let [mutant (aclone base)]
                 (aset-byte mutant 0 (byte (bit-xor 1 (aget mutant 0))))
                 mutant)
               base)]
    (backend/put-bytes-if-absent! store control/head-key
                                  (.getBytes (json/write-str head) "UTF-8"))
    (backend/put-bytes-if-absent! store base-key base)
    store))

(defn- run-mutants! [fixture-root descriptor before]
  (let [archive-error
        (rejected #(let [opened (durable/open-reader!
                                 {:store (copy-to-memory fixture-root descriptor true false)})]
                     (reader/close! opened)))]
    (check! (= ::durable/corrupt (:type (ex-data archive-error)))
            "wrong archive identity mutant was not corrupt"))
  (let [min-reader-error
        (rejected #(let [opened (durable/open-reader!
                                 {:store (copy-to-memory fixture-root descriptor false true)})]
                     (reader/close! opened)))]
    (check! (= ::durable/engine-incompatible (:type (ex-data min-reader-error)))
            "unsupported minimum-reader mutant was not refused"))
  (check! (= before (inventory fixture-root)) "mutants changed authoritative fixture bytes")
  [{:id "wrong-archive-identity" :expected "refuse" :actual "refuse"
    :error "corrupt" :authoritative_inventory_unchanged true}
   {:id "unsupported-min-reader" :expected "refuse" :actual "refuse"
    :error "engine-incompatible" :authoritative_inventory_unchanged true}])

(defn- read! [matrix-file fixture-root descriptor-file producer reader-release library header report]
  (let [matrix (matrix matrix-file)
        declared (release! matrix reader-release library header)
        descriptor (json/read-str (slurp descriptor-file) :key-fn keyword)
        before (descriptor-valid! matrix-file matrix descriptor fixture-root producer)
        cell (archive-cell matrix producer reader-release)
        downloads (atom 0)
        store (raw-backend fixture-root downloads)
        result
        (if (= "accept" (:expected cell))
          (let [opened (durable/open-reader! {:store store})
                actual (try (observed opened) (finally (reader/close! opened)))]
            (check! (= expected-aggregate actual) "accepted archive aggregate differs")
            {:actual "accept" :aggregate actual :downloads @downloads})
          (let [active-before (native/active-storage)
                error (rejected #(let [opened (durable/open-reader! {:store store})]
                                   (reader/close! opened)))]
            (check! (= ::durable/engine-incompatible (:type (ex-data error)))
                    "incompatible archive was not refused")
            (check! (zero? @downloads) "minimum-reader refusal downloaded the archive")
            (check! (= active-before (native/active-storage))
                    "minimum-reader refusal opened native storage")
            {:actual "refuse" :error "engine-incompatible" :downloads @downloads}))
        after (descriptor-valid! matrix-file matrix descriptor fixture-root producer)
        controls (when (= "rc2-to-release" (:id cell))
                   (run-mutants! fixture-root descriptor before))]
    (check! (= (:expected cell) (:actual result)) "archive cell result differs")
    (check! (= before after) "archive cell changed protocol bytes")
    (spit report (str (json/write-str
                       {:schema_version 1 :cell cell :reader_release declared
                        :result result :inventory_unchanged true
                        :causal_controls (or controls [])}) "\n"))
    (println "PASSED" (:id cell) (:expected cell) "-" (:reason cell))))

(defn -main [& args]
  (source-contract!)
  (let [[command matrix-file & rest] args]
    (case command
      "produce"
      (let [[output release library header] rest]
        (check! (= 4 (count rest)) "produce arguments differ")
        (produce! matrix-file output (keyword release) library header))

      "read"
      (let [[fixture-root descriptor producer reader-release library header report] rest]
        (check! (= 7 (count rest)) "read arguments differ")
        (read! matrix-file fixture-root descriptor (keyword producer)
               (keyword reader-release) library header report))

      (fail! "matrix command must be produce or read"))))
