(ns jdbc.chdb.durable
  "Public Durable V1 reader/writer open and recovery orchestration."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [db.driver :as driver]
            [db.export :as export]
            [db.jdbc-shim :as shim]
            [jdbc.chdb :as chdb]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.digest :as digest]
            [jdbc.chdb.durable.policy :as policy]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb.native :as native]
            [jdbc.proto :as proto])
  (:import [java.io File]
           [java.nio.file CopyOption Files Path Paths StandardCopyOption]
           [java.nio.file.attribute FileAttribute PosixFilePermissions]
           [java.util UUID]))

(def reader-backup-format 1)
(def default-lease-ttl-ms 30000)
(def default-clock-skew-ms 0)

(def ^:private private-directory-attributes
  (into-array
   FileAttribute
   [(PosixFilePermissions/asFileAttribute
     (PosixFilePermissions/fromString "rwx------"))]))

(def ^:private atomic-move-options
  (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE]))

(defn- fail! [type message]
  (throw (ex-info message {:type type})))

(defn- version-parts [version]
  (when (string? version)
    (when-let [[_ major minor patch prerelease]
               (re-matches
                #"(?i)^v?(\d+)\.(\d+)\.(\d+)(?:-([0-9a-z.-]+))?(?:\+[0-9a-z.-]+)?$"
                version)]
      {:core [(bigint major) (bigint minor) (bigint patch)]
       :prerelease (when prerelease (str/split prerelease #"\."))})))

(defn- compare-prerelease-part [left right]
  (let [left-number? (boolean (re-matches #"\d+" left))
        right-number? (boolean (re-matches #"\d+" right))]
    (cond
      (and left-number? right-number?)
      (compare (bigint left) (bigint right))

      left-number? -1
      right-number? 1
      :else (compare left right))))

(defn- compare-prerelease [left right]
  (cond
    (and (nil? left) (nil? right)) 0
    (nil? left) 1
    (nil? right) -1
    :else
    (loop [left left right right]
      (cond
        (and (empty? left) (empty? right)) 0
        (empty? left) -1
        (empty? right) 1
        :else (let [comparison (compare-prerelease-part
                                (first left) (first right))]
                (if (zero? comparison)
                  (recur (next left) (next right))
                  comparison))))))

(defn compare-release-versions
  "Compare two chDB releases by numeric release/prerelease precedence."
  [left right]
  (let [left (or (version-parts left)
                 (fail! ::engine-incompatible
                        "The running chDB release cannot be compared"))
        right (or (version-parts right)
                  (fail! ::engine-incompatible
                         "The Durable minimum reader release cannot be compared"))
        core-comparison (compare (:core left) (:core right))]
    (if (zero? core-comparison)
      (compare-prerelease (:prerelease left) (:prerelease right))
      core-comparison)))

(defn check-engine-compatibility!
  "Fail closed unless this reader can restore the manifest's engine state."
  [document running-version]
  (let [engine (get document "engine")]
    (when (> (get engine "backup_format") reader-backup-format)
      (fail! ::engine-incompatible
             "The Durable backup format requires a newer reader"))
    (when (neg? (compare-release-versions running-version
                                          (get engine "min_reader")))
      (fail! ::engine-incompatible
             "The Durable object requires a newer chDB release"))
    document))

(defn- quote-identifier [database]
  (str "`" (str/replace database "`" "``") "`"))

(defn- default-create-database! [handle database]
  (chdb/execute-any handle
                    (str "CREATE DATABASE IF NOT EXISTS "
                         (quote-identifier database)) []))

(defn- default-use-database! [handle database]
  (chdb/execute-any handle (str "USE " (quote-identifier database)) []))

(defn- default-scratch! [scratch-parent]
  (let [parent (Paths/get scratch-parent (make-array String 0))]
    (when-not (Files/isDirectory parent (make-array java.nio.file.LinkOption 0))
      (fail! ::invalid-options "Durable scratch parent must be a directory"))
    (Files/createTempDirectory parent "jolt-chdb-" private-directory-attributes)))

(defn- delete-tree-pass! [path]
  (let [file (.toFile ^Path path)]
    (when (.exists file)
      (doseq [child (or (.listFiles file) (make-array File 0))]
        (delete-tree-pass! (.toPath child)))
      (Files/deleteIfExists ^Path path))))

(defn- delete-tree! [path]
  (when path
    (loop [attempt 1]
      (let [error (try (delete-tree-pass! path) nil
                       (catch Throwable error error))]
        (cond
          (not (Files/exists ^Path path (make-array java.nio.file.LinkOption 0)))
          nil

          (< attempt 4)
          (do (Thread/yield) (recur (inc attempt)))

          :else
          (throw (ex-info "Durable scratch cleanup failed"
                          {:type ::cleanup-failed
                           :cause-class (some-> error class str)}))))))
  nil)

(defn- download-reference! [store scratch label reference max-bytes]
  (let [size (get reference "size")]
    (when (and max-bytes (> size max-bytes))
      (fail! ::limit-exceeded "A Durable recovery object exceeds its limit"))
    (let [attempt (.resolve ^Path scratch
                            (str "." label "-" (UUID/randomUUID) ".part"))
          final (.resolve ^Path scratch label)]
      (try
        (let [result (backend/download-to-file! store (get reference "key") attempt)]
          (when-not (= :downloaded (:status result))
            (fail! ::corrupt "A referenced Durable object is missing"))
          (when-not (and (= size (:byte-count result))
                         (= size (Files/size attempt)))
            (fail! ::corrupt "A Durable recovery object has the wrong size"))
          (when-not (= (get reference "sha256") (digest/sha256-file attempt))
            (fail! ::corrupt "A Durable recovery object has the wrong digest"))
          (Files/move attempt final atomic-move-options)
          final)
        (catch Throwable error
          (Files/deleteIfExists attempt)
          (throw error))))))

(defn- decode-wal! [path]
  (let [bytes (Files/readAllBytes ^Path path)
        length (alength bytes)
        text (String. bytes "UTF-8")]
    (when (or (zero? length) (not= 10 (bit-and 255 (aget bytes (dec length)))))
      (fail! ::corrupt "A Durable WAL is not newline terminated"))
    (when-not (= (vec bytes) (vec (.getBytes text "UTF-8")))
      (fail! ::corrupt "A Durable WAL is not canonical UTF-8"))
    (try
      (mapv
       (fn [line]
         (let [record (json/read-str line)
               sql (get record "sql")]
           (when-not (and (map? record) (= #{"sql"} (set (keys record)))
                          (string? sql))
             (fail! ::corrupt "A Durable WAL record is invalid"))
           (when (> (alength (.getBytes sql "UTF-8")) writer/max-statement-bytes)
             (fail! ::limit-exceeded "A Durable WAL statement exceeds 64 MiB"))
           sql))
       (butlast (str/split text #"\n" -1)))
      (catch Throwable error
        (if (:type (ex-data error))
          (throw error)
          (fail! ::corrupt "A Durable WAL record cannot be decoded"))))))

(defn- required-operation! [operations key]
  (when-not (fn? (get operations key))
    (fail! ::invalid-options "Durable open operations must be functions")))

(defn- resolve-store!
  [{:keys [store namespace-backend object-id]}]
  (cond
    (and store (or namespace-backend object-id))
    (fail! ::invalid-options
           "Choose either store or namespace-backend with object-id")

    store store

    (and namespace-backend object-id)
    (backend/object-backend namespace-backend object-id)

    (or namespace-backend object-id)
    (fail! ::invalid-options
           "namespace-backend and object-id must be supplied together")

    :else
    (fail! ::invalid-options
           "store or namespace-backend with object-id is required")))

(defn- default-open-operations []
  {:now-ms #(System/currentTimeMillis)
   :durable-capability native/durable-capability
   :create-scratch! default-scratch!
   :cleanup-scratch! delete-tree!
   :open-native! (fn [scratch]
                   (native/open!
                    (str (.resolve ^Path scratch "data"))
                    {:backups-allowed-path (str scratch)}))
   :close-native! native/close!
   :restore-database! native/restore-database!
   :create-checkpoint!
   (fn [handle database scratch]
     (let [path (.resolve ^Path scratch
                          (str "checkpoint-" (UUID/randomUUID) ".tar.gz"))]
       (native/backup-database! handle database (str path))
       path))
   :delete-checkpoint! (fn [path] (Files/deleteIfExists ^Path path))
   :create-database! default-create-database!
   :use-database! default-use-database!
   :analyze-query! policy/analyze-query!
   :analyze-execute! policy/analyze-execute!
   :classification-sql! chdb/classification-sql
   :classify! native/classify-query!
   :query-native! (fn [handle sql params]
                    (chdb/execute-any handle sql params))
   :query-bytes-native! chdb/execute-query-bytes-handle
   :execute-native! (fn [handle sql params]
                      (chdb/execute-any handle sql params))})

(def ^:private recovery-operation-keys
  [:create-scratch! :cleanup-scratch! :open-native! :close-native!
   :restore-database! :create-database! :use-database! :analyze-execute!
   :execute-native!])

(defn- recover-snapshot!
  "Restore exactly `document`'s manifest into an already opened private handle."
  [store document operations scratch handle]
  (let [manifest (get document "manifest")
        logical-database (get manifest "db")]
    (if-let [base (get manifest "base")]
      (let [archive (download-reference!
                     store scratch "base.tar.gz" base nil)]
        (try
          ((:restore-database! operations)
           handle logical-database (str archive))
          (catch Throwable _
            (fail! ::engine-incompatible
                   "chDB could not restore a compatible Durable archive"))))
      ((:create-database! operations) handle logical-database))
    ((:use-database! operations) handle logical-database)
    (doseq [[index reference] (map-indexed vector (get manifest "wal"))]
      (let [path (download-reference!
                  store scratch (str "wal-" index ".jsonl") reference
                  writer/max-wal-segment-bytes)]
        (doseq [sql (decode-wal! path)]
          ((:analyze-execute! operations) handle sql logical-database)
          ((:execute-native! operations) handle sql []))))
    logical-database))

(defn open-reader!
  "Open one immutable Durable V1 head snapshot without acquiring a lease."
  [{:keys [scratch-parent operations]
    :or {scratch-parent (System/getProperty "java.io.tmpdir")}
    :as options}]
  (let [store (resolve-store! options)
        operations (merge (default-open-operations) operations)]
    (doseq [key (concat [:durable-capability :classification-sql!
                         :query-native!
                         :query-bytes-native!]
                        recovery-operation-keys)]
      (required-operation! operations key))
    (let [capability ((:durable-capability operations))]
      (when-not (= :supported (:status capability))
        (fail! ::engine-incompatible "The running chDB core lacks Durable V1"))
      (let [snapshot (or (control/read-head-read-only! store)
                         (fail! ::not-found "The Durable object does not exist"))
            document (:head snapshot)
            scratch (atom nil)
            handle (atom nil)]
        (check-engine-compatibility! document (:native-version capability))
        (try
          (reset! scratch ((:create-scratch! operations) scratch-parent))
          (reset! handle ((:open-native! operations) @scratch))
          (let [database (recover-snapshot!
                          store document operations @scratch @handle)]
            (reader/start!
             {:handle @handle :database database
              :operations
              (assoc operations
                     :cleanup-scratch!
                     (fn [] ((:cleanup-scratch! operations) @scratch)))}))
          (catch Throwable primary
            (when @handle
              (try ((:close-native! operations) @handle) (catch Throwable _)))
            (when @scratch
              (try ((:cleanup-scratch! operations) @scratch) (catch Throwable _)))
            (throw primary)))))))

(defn open-writer!
  "Acquire, recover, renew, and return a serialized Durable V1 writer."
  [{:keys [owner instance database lease-ttl-ms clock-skew-ms force?
           heartbeat-interval-ms scratch-parent operations]
    :or {lease-ttl-ms default-lease-ttl-ms
         clock-skew-ms default-clock-skew-ms
         force? false
         scratch-parent (System/getProperty "java.io.tmpdir")}
    :as options}]
  (when-not (and (number? lease-ttl-ms) (pos? lease-ttl-ms))
    (fail! ::invalid-options "lease-ttl-ms must be positive"))
  (let [store (resolve-store! options)
        operations
        (merge (default-open-operations) operations)
        required [:now-ms :durable-capability :create-scratch! :cleanup-scratch!
                  :open-native! :close-native! :restore-database!
                  :create-checkpoint! :delete-checkpoint!
                  :create-database! :use-database! :analyze-query!
                  :analyze-execute! :classification-sql! :classify!
                  :query-native!
                  :query-bytes-native!
                  :execute-native!]]
    (doseq [key required] (required-operation! operations key))
    (let [capability ((:durable-capability operations))]
      (when-not (= :supported (:status capability))
        (fail! ::engine-incompatible "The running chDB core lacks Durable V1"))
      (let [running-version (:native-version capability)
            existing (control/read-head! store)]
        (when existing
          (check-engine-compatibility! (:head existing) running-version))
        (let [now ((:now-ms operations))
              acquired (control/acquire!
                        store {:owner owner :instance instance
                               :expires-at (+ now lease-ttl-ms)
                               :now now :clock-skew clock-skew-ms :force? force?
                               :database database :engine-version running-version
                               :backup-format reader-backup-format
                               :min-reader running-version})
              token (:token acquired)
              document (:head acquired)
              scratch (atom nil)
              handle (atom nil)]
          (try
            (check-engine-compatibility! document running-version)
            (reset! scratch ((:create-scratch! operations) scratch-parent))
            (reset! handle ((:open-native! operations) @scratch))
            (let [logical-database
                  (recover-snapshot! store document operations @scratch @handle)]
              (let [renew-now ((:now-ms operations))
                    current-expiry (get-in document ["lease" "expires_at"])
                    renewed-expiry (max (inc current-expiry)
                                        (+ renew-now lease-ttl-ms))]
                (control/renew! store token renewed-expiry)
                (writer/start!
                 {:store store :token token :handle @handle
                  :database logical-database
                  :lease-expiry renewed-expiry
                  :lease-ttl-ms (long lease-ttl-ms)
                  :heartbeat-interval-ms
                  (long (or heartbeat-interval-ms
                            (max 1 (quot lease-ttl-ms 3))))
                  :operations
                  (assoc operations
                         :create-checkpoint!
                         (fn [handle database]
                           ((:create-checkpoint! operations)
                            handle database @scratch))
                         :cleanup-scratch!
                         (fn [] ((:cleanup-scratch! operations) @scratch)))})))
            (catch Throwable primary
              (when @handle
                (try ((:close-native! operations) @handle) (catch Throwable _)))
              (try (control/release! store token) (catch Throwable _))
              (when @scratch
                (try ((:cleanup-scratch! operations) @scratch) (catch Throwable _)))
              (throw primary))))))))

(defn connection-role
  "Return `:writer` or `:reader` for an open Durable JDBC connection.

  Other driver types and closed connections fail at the JDBC extension
  boundary. This is a non-publishing capability check for integrations that
  must reject the wrong connection before performing schema or data writes."
  [connection]
  (shim/extension-operation
   #(let [shim-connection (proto/connection connection)
          {:keys [handle]}
          (shim/driver-context shim-connection :chdb-durable)]
      (if (reader/reader? handle) :reader :writer))))

(defn- jdbc-writer-handle [connection]
  (let [shim-connection (proto/connection connection)
        {:keys [handle]}
        (shim/driver-context shim-connection :chdb-durable)]
    (when (reader/reader? handle)
      (fail! ::read-only-required
             "A read-only Durable connection cannot publish state"))
    handle))

(defn flush!
  "Commit the pending recovery state of a Durable JDBC writer connection.

  Materialized mutations publish statement WAL. If a native bound mutation is
  pending, the call publishes a full checkpoint instead. It returns only after
  the manifest CAS is confirmed or reconciled. Other driver types and read-only
  Durable connections fail closed."
  [connection]
  (shim/extension-operation
   #(writer/flush! (jdbc-writer-handle connection))))

(defn checkpoint!
  "Publish and commit a full checkpoint for a Durable JDBC writer connection.

  Pending statement WAL is cleared only after the checkpoint head transition
  is confirmed or reconciled."
  [connection]
  (shim/extension-operation
   #(writer/checkpoint! (jdbc-writer-handle connection))))

(def durable-driver
  "`jdbc.core` adapter for map dbspecs carrying a scoped Durable backend.

  Reads and mutations retain native bound values. Because V1 statement WAL has
  no typed-parameter record, a successful parameterized mutation makes the next
  flush or close publish a full checkpoint."
  (reify driver/Driver
    (descriptor [_]
      {:id :chdb-durable
       :aliases #{"chdb-durable"}
       :uri-prefixes []
       :product-name "ClickHouse (chDB Durable V1)"
       :capabilities {:transactions :none :generated-keys :none
                      :query-bytes chdb/query-bytes-capability}
       :constraints {:active-storage-paths :one-per-process
                     :mutation-parameters :checkpoint-fallback}
       :schema-sql nil})
    (open-handle [_ spec]
      (when-not (map? spec)
        (fail! ::invalid-options "Durable chDB requires a map dbspec"))
      (let [common {:store (:backend spec)
                    :namespace-backend (:namespace-backend spec)
                    :object-id (:object-id spec)
                    :scratch-parent (or (:scratch-parent spec)
                                        (System/getProperty "java.io.tmpdir"))
                    :operations (:operations spec)}]
        (if (:read-only? spec)
          (open-reader! common)
          (open-writer!
           (merge common
                  {:owner (:owner spec)
                   :instance (:instance spec)
                   :database (:database spec)
                   :lease-ttl-ms (or (:lease-ttl-ms spec)
                                     default-lease-ttl-ms)
                   :clock-skew-ms (or (:clock-skew-ms spec)
                                      default-clock-skew-ms)
                   :heartbeat-interval-ms (:heartbeat-interval-ms spec)
                   :force? (boolean (:force? spec))})))))
    (close-handle [_ handle]
      (if (reader/reader? handle)
        (reader/close! handle)
        (writer/close! handle)))
    (execute-handle [_ handle sql params]
      (if (reader/reader? handle)
        (reader/query! handle sql params)
        (writer/sql! handle sql params)))

    export/QueryBytesDriver
    (query-bytes-handle [_ handle sql params options]
      (if (reader/reader? handle)
        (reader/query-bytes! handle sql params options)
        (writer/query-bytes! handle sql params options)))))

(driver/register! durable-driver)
