(ns jdbc.chdb.durable.backend
  "Durable V1 object-backend contract and runtime-neutral reference backend."
  (:require [clojure.string :as str])
  (:import [java.nio.file CopyOption Files LinkOption OpenOption Path Paths
            StandardCopyOption StandardOpenOption]
           [java.nio.file.attribute FileAttribute]))

(defprotocol ObjectBackend
  (get-bytes [backend key]
    "Return an owned byte array for key, or nil when it does not exist.")
  (get-with-etag [backend key]
    "Return {:bytes owned-bytes :etag opaque-token}, or nil.")
  (put-file-if-absent! [backend key local-path]
    "Atomically create key by streaming local-path, returning {:status
    :created :etag token} or {:status :precondition-failed}.")
  (put-bytes-if-absent! [backend key bytes]
    "Atomically create key, returning {:status :created :etag token} or
    {:status :precondition-failed}.")
  (replace-if-match! [backend key bytes etag]
    "Atomically replace key only when its opaque ETag still matches, returning
    {:status :replaced :etag token}, {:status :precondition-failed}, or
    {:status :ambiguous} when a remote provider cannot prove whether its
    conditional write landed. Callers must reconcile the latter by reread.")
  (download-to-file! [backend key local-path]
    "Stream key into a caller-owned unique local path without overwriting it,
    returning {:status :downloaded :byte-count n}, or {:status :not-found}."))

(defprotocol LocalDurability
  (with-exclusive-lock [platform lock-path f]
    "Run f while holding the provider's cross-process exclusive lock.")
  (sync-file! [platform path]
    "Make the contents of path durable before publication.")
  (sync-directory! [platform path]
    "Make directory-entry changes under path durable.")
  (create-private-directory! [platform path]
    "Atomically create path with private permissions; return true when created.")
  (create-private-temp-file! [platform parent]
    "Atomically create and return a private staging file under parent.")
  (create-private-file! [platform path]
    "Atomically create path with private permissions; return true when created.")
  (copy-file-range! [platform source target source-offset target-offset]
    "Copy source bytes after source-offset into target whose current size is
    exactly target-offset, and return the copied byte count without
    payload-sized managed allocation."))

(defn- fail! [type message]
  ;; Object keys are intentionally absent: callers may eventually map remote
  ;; provider paths containing tenant data through this boundary.
  (throw (ex-info message {:type type})))

(defn- checked-key [key]
  (let [parts (when (string? key) (str/split key #"/" -1))]
    (when-not (and (seq parts)
                   (not (str/starts-with? key "/"))
                   (not (str/includes? key "\\"))
                   (every? #(and (not (str/blank? %))
                                 (not (contains? #{"." ".."} %)))
                           parts))
      (fail! ::invalid-key "Durable backend object key is not a safe relative key"))
    key))

(defn- owned-bytes [value]
  (when-not (bytes? value)
    (fail! ::invalid-bytes "Durable backend value must be a byte array"))
  (let [length (alength value)
        copy (byte-array length)]
    (System/arraycopy value 0 copy 0 length)
    copy))

(defn- new-etag []
  ;; Reference tokens are deliberately unrelated to content. Consumers may
  ;; compare or return an ETag, but must never parse it or treat it as a digest.
  (str (gensym "opaque-etag-")))

(defn- conflict []
  {:status :precondition-failed})

(deftype ^:private MemoryBackend [objects]
  ObjectBackend
  (get-bytes [_ key]
    (checked-key key)
    (some-> (get @objects key) :bytes owned-bytes))

  (get-with-etag [_ key]
    (checked-key key)
    (when-let [{:keys [bytes etag]} (get @objects key)]
      {:bytes (owned-bytes bytes) :etag etag}))

  (put-file-if-absent! [this key local-path]
    ;; The in-memory implementation remains a semantic oracle, not an
    ;; advertised streaming provider.
    (put-bytes-if-absent! this key (Files/readAllBytes
                                    (Paths/get (str local-path)
                                               (into-array String [])))))

  (put-bytes-if-absent! [_ key bytes]
    (checked-key key)
    (let [stored (owned-bytes bytes)
          etag (new-etag)]
      (loop []
        (let [before @objects]
          (if (contains? before key)
            (conflict)
            (if (compare-and-set! objects before
                                  (assoc before key
                                         {:bytes stored :etag etag}))
              {:status :created :etag etag}
              (recur)))))))

  (replace-if-match! [_ key bytes expected-etag]
    (checked-key key)
    (let [stored (owned-bytes bytes)
          next-etag (new-etag)]
      (loop []
        (let [before @objects
              current (get before key)]
          (if (or (nil? current)
                  (not= expected-etag (:etag current)))
            (conflict)
            (if (compare-and-set! objects before
                                  (assoc before key
                                         {:bytes stored :etag next-etag}))
              {:status :replaced :etag next-etag}
              (recur)))))))

  (download-to-file! [this key local-path]
    (if-let [bytes (get-bytes this key)]
      (do
        (Files/write (Paths/get (str local-path) (into-array String []))
                     bytes
                     (into-array OpenOption [StandardOpenOption/CREATE_NEW
                                             StandardOpenOption/WRITE]))
        {:status :downloaded :byte-count (alength bytes)})
      {:status :not-found})))

(defn memory-backend
  "Create the deterministic in-process semantic oracle for backend contracts.

  This is not an advertised Durable storage provider: it exists to exercise
  atomic conditional semantics without filesystem or object-store behavior."
  []
  (MemoryBackend. (atom {})))

;; The local provider stores the value and its opaque generation token in one
;; atomically published file. A fixed-size token keeps parsing independent of a
;; host byte-buffer implementation.
(def ^:private envelope-magic
  (byte-array [74 67 72 68 66 48 49 0])) ; "JCHDB01\0"
(def ^:private etag-bytes 36)             ; canonical random UUID spelling
(def ^:private envelope-header-bytes (+ (alength envelope-magic) etag-bytes))
(def ^:private no-link-options (into-array LinkOption []))
(def ^:private nofollow-link-options
  (into-array LinkOption [LinkOption/NOFOLLOW_LINKS]))
(def ^:private no-open-options (into-array OpenOption []))
(def ^:private no-file-attributes (into-array FileAttribute []))
(def ^:private publish-options
  (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE
                          StandardCopyOption/REPLACE_EXISTING]))

(defn- path-exists? [path]
  (Files/exists path no-link-options))

(defn- local-io [operation f]
  (try
    (f)
    (catch Throwable error
      (if (:type (ex-data error))
        (throw error)
        ;; java.nio exceptions normally include their path in the message.
        ;; Preserve useful classification without disclosing tenant paths.
        (throw (ex-info (str "Durable local " (name operation) " failed")
                        {:type ::local-io-failed
                         :operation operation
                         :cause-class (str (class error))}))))))

(defn- encode-envelope [bytes etag]
  (let [body (owned-bytes bytes)
        token (.getBytes etag "UTF-8")
        result (byte-array (+ envelope-header-bytes (alength body)))]
    (when-not (= etag-bytes (alength token))
      (fail! ::invalid-etag "Durable backend generated an invalid opaque ETag"))
    (System/arraycopy envelope-magic 0 result 0 (alength envelope-magic))
    (System/arraycopy token 0 result (alength envelope-magic) etag-bytes)
    (System/arraycopy body 0 result envelope-header-bytes (alength body))
    result))

(defn- decode-envelope [encoded]
  (let [length (alength encoded)]
    (when (< length envelope-header-bytes)
      (fail! ::invalid-envelope "Durable local object has a truncated envelope"))
    (doseq [i (range (alength envelope-magic))]
      (when-not (= (aget envelope-magic i) (aget encoded i))
        (fail! ::invalid-envelope "Durable local object has an invalid envelope")))
    (let [token (byte-array etag-bytes)
          body-length (- length envelope-header-bytes)
          body (byte-array body-length)]
      (System/arraycopy encoded (alength envelope-magic) token 0 etag-bytes)
      (System/arraycopy encoded envelope-header-bytes body 0 body-length)
      (let [etag (String. token "UTF-8")]
        (when-not (re-matches
                   #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
                   etag)
          (fail! ::invalid-envelope
                 "Durable local object has an invalid opaque ETag"))
        {:etag etag :bytes body}))))

(defn- existing-object-path [^Path objects key]
  ;; Inspect every existing component without following a symbolic link. This
  ;; runs under the provider lock; a directory modified by non-cooperating
  ;; software is explicitly outside the local provider's trust boundary.
  (let [parts (str/split (checked-key key) #"/" -1)]
    (loop [parent objects
           [part & more] parts]
      (let [child (.resolve ^Path parent part)]
        (when (Files/isSymbolicLink child)
          (fail! ::unsafe-local-root
                 "Durable local object hierarchy contains a symbolic link"))
        (if (seq more)
          (cond
            (not (path-exists? child)) nil
            (not (Files/isDirectory child nofollow-link-options))
            (fail! ::unsafe-local-root
                   "Durable local object hierarchy contains a non-directory")
            :else (recur child more))
          (when (path-exists? child) child))))))

(defn- ensure-directory-tree! [platform ^Path base parts]
  ;; Publish directory entries before any object can depend on them. Calling
  ;; this under the provider lock also closes the create/check race.
  (reduce
   (fn [^Path parent part]
     (let [child (.resolve parent part)]
       (if (path-exists? child)
         (when (or (Files/isSymbolicLink child)
                   (not (Files/isDirectory child nofollow-link-options)))
           (fail! ::unsafe-local-root
                  "Durable local object hierarchy contains an unsafe entry"))
         (do
           (when-not (create-private-directory! platform child)
             (fail! ::unsafe-local-root
                    "Durable local object directory creation raced"))
           (sync-directory! platform child)
           (sync-directory! platform parent)))
       child))
   base parts))

(defn- read-local-envelope [path]
  (when (path-exists? path)
    (when (Files/isSymbolicLink path)
      (fail! ::unsafe-local-root
             "Durable local object path is a symbolic link"))
    (decode-envelope (Files/readAllBytes path))))

(defn- write-envelope-from-file! [platform source target etag]
  (let [header (encode-envelope (byte-array 0) etag)]
    (Files/write target header no-open-options)
    (copy-file-range! platform source target 0 (alength header))))

(defn- read-exact! [input bytes]
  (loop [offset 0]
    (if (= offset (alength bytes))
      bytes
      (let [n (.read input bytes offset (- (alength bytes) offset))]
        (when (not (pos? n))
          (fail! ::invalid-envelope
                 "Durable local object has a truncated envelope"))
        (recur (+ offset n))))))

(defn- stream-envelope-to-file! [platform source target]
  (with-open [input (Files/newInputStream source no-open-options)]
    ;; Parsing the fixed header before copying proves this is a provider object;
    ;; content size/digest verification belongs to the caller before it
    ;; atomically publishes this unique scratch path.
    (decode-envelope (read-exact! input (byte-array envelope-header-bytes))))
  (copy-file-range! platform source target envelope-header-bytes 0))

(defn- publish-local-with! [platform ^Path objects key write-staging!]
  (let [parts (str/split key #"/" -1)
        parent-parts (butlast parts)
        parent (ensure-directory-tree! platform objects parent-parts)
        target (.resolve parent (last parts))
        temporary (create-private-temp-file! platform parent)
        etag (str (random-uuid))]
    (let [outcome (try
                    (write-staging! temporary etag)
                    (sync-file! platform temporary)
                    (Files/move temporary target publish-options)
                    (sync-directory! platform parent)
                    {:value etag}
                    (catch Throwable error {:error error}))
          cleanup-error (try
                          (Files/deleteIfExists temporary)
                          nil
                          (catch Throwable error error))]
      (cond
        (:error outcome) (throw (:error outcome))
        cleanup-error (throw cleanup-error)
        :else (:value outcome)))))

(defn- publish-local! [platform objects key bytes]
  (publish-local-with!
   platform objects key
   #(Files/write %1 (encode-envelope bytes %2) no-open-options)))

(defn- download-local! [platform source target]
  (when-not (create-private-file! platform target)
    (fail! ::destination-exists
           "Durable download destination already exists"))
  (try
    (let [byte-count (stream-envelope-to-file! platform source target)]
      (sync-file! platform target)
      {:status :downloaded :byte-count byte-count})
    (catch Throwable primary-error
      (try
        (Files/deleteIfExists target)
        (catch Throwable cleanup-error
          (throw
           (ex-info "Durable local download cleanup failed"
                    {:type ::download-cleanup-failed
                     :primary-type (:type (ex-data primary-error))
                     :cleanup-cause-class (str (class cleanup-error))}))))
      (throw primary-error))))

(deftype ^:private LocalBackend [^Path objects ^Path lock-path platform]
  ObjectBackend
  (get-bytes [_ key]
    (checked-key key)
    (local-io
     :get
     #(with-exclusive-lock platform lock-path
        (fn []
          (when-let [path (existing-object-path objects key)]
            (some-> (read-local-envelope path) :bytes owned-bytes))))))

  (get-with-etag [_ key]
    (checked-key key)
    (local-io
     :get-with-etag
     #(with-exclusive-lock platform lock-path
        (fn []
          (when-let [path (existing-object-path objects key)]
            (when-let [{:keys [bytes etag]} (read-local-envelope path)]
              {:bytes (owned-bytes bytes) :etag etag}))))))

  (put-file-if-absent! [_ key local-path]
    (let [key (checked-key key)
          source (Paths/get (str local-path) (into-array String []))]
      (local-io
       :put-file-if-absent
       (fn []
         (when-not (Files/isRegularFile source nofollow-link-options)
           (fail! ::invalid-local-file
                  "Durable upload source is not a regular file"))
         (with-exclusive-lock
          platform lock-path
          (fn []
            (if (existing-object-path objects key)
              (conflict)
              {:status :created
               :etag
               (publish-local-with!
                platform objects key
                (fn [target etag]
                  (write-envelope-from-file!
                   platform source target etag)))})))))))

  (put-bytes-if-absent! [_ key bytes]
    (let [key (checked-key key)
          stored (owned-bytes bytes)]
      ;; Validate/copy before taking a potentially contended process lock.
      (local-io
       :put-if-absent
       #(with-exclusive-lock platform lock-path
          (fn []
            (if (existing-object-path objects key)
              (conflict)
              {:status :created
               :etag (publish-local! platform objects key stored)}))))))

  (replace-if-match! [_ key bytes expected-etag]
    (let [key (checked-key key)
          stored (owned-bytes bytes)]
      (local-io
       :replace-if-match
       #(with-exclusive-lock platform lock-path
          (fn []
            (let [current-path (existing-object-path objects key)
                  current (when current-path
                            (read-local-envelope current-path))]
              (if (or (nil? current) (not= expected-etag (:etag current)))
                (conflict)
                {:status :replaced
                 :etag (publish-local! platform objects key stored)})))))))

  (download-to-file! [_ key local-path]
    (checked-key key)
    (let [target (Paths/get (str local-path) (into-array String []))]
      (local-io
       :download-to-file
       (fn []
         (with-exclusive-lock
          platform lock-path
          (fn []
            (if-let [source (existing-object-path objects key)]
              (download-local! platform source target)
              {:status :not-found}))))))))

(defn local-backend
  "Create a single-host Durable backend rooted at `root`.

  File operations use the java.nio/babashka.fs-shaped surface shared by Jolt,
  Babashka, and JVM Clojure. `platform` supplies only cross-process exclusion
  and durability barriers. The root must be private to cooperating processes."
  [root platform]
  (local-io
   :initialize
   (fn []
     (let [requested (.normalize (.toAbsolutePath
                                  (Paths/get (str root)
                                             (into-array String []))))
           parent (.getParent requested)]
       (when parent
         (Files/createDirectories parent no-file-attributes))
       (when-not (path-exists? requested)
         ;; Two cooperating processes may initialize a new root concurrently.
         ;; A lost create is acceptable only if the winner left a real directory.
         (create-private-directory! platform requested))
       (when (or (Files/isSymbolicLink requested)
                 (not (Files/isDirectory requested nofollow-link-options)))
         (fail! ::unsafe-local-root "Durable local root is not a safe directory"))
       (let [canonical (.toRealPath requested no-link-options)
             lock-path (.resolve canonical ".jchdb.lock")
             objects (.resolve canonical "objects")]
         (with-exclusive-lock platform lock-path
           #(do
              (ensure-directory-tree! platform canonical ["objects"])
              (sync-directory! platform canonical)))
         (LocalBackend. objects lock-path platform))))))
