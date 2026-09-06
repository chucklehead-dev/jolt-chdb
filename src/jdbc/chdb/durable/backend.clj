(ns jdbc.chdb.durable.backend
  "Durable V1 object-backend contract and runtime-neutral reference backend."
  (:require [clojure.string :as str])
  (:import [java.nio.file CopyOption Files LinkOption OpenOption Path Paths
            StandardCopyOption]
           [java.nio.file.attribute FileAttribute]))

(defprotocol ObjectBackend
  (get-bytes [backend key]
    "Return an owned byte array for key, or nil when it does not exist.")
  (get-with-etag [backend key]
    "Return {:bytes owned-bytes :etag opaque-token}, or nil.")
  (put-bytes-if-absent! [backend key bytes]
    "Atomically create key, returning {:status :created :etag token} or
    {:status :precondition-failed}.")
  (replace-if-match! [backend key bytes etag]
    "Atomically replace key only when its opaque ETag still matches, returning
    {:status :replaced :etag token} or {:status :precondition-failed}."))

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
    "Atomically create and return a private staging file under parent."))

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
              (recur))))))))

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

(defn- publish-local! [platform ^Path objects key bytes]
  (let [parts (str/split key #"/" -1)
        parent-parts (butlast parts)
        parent (ensure-directory-tree! platform objects parent-parts)
        target (.resolve parent (last parts))
        temporary (create-private-temp-file! platform parent)
        etag (str (random-uuid))]
    (let [outcome (try
                    (Files/write temporary (encode-envelope bytes etag)
                                 no-open-options)
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
                 :etag (publish-local! platform objects key stored)}))))))))

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
