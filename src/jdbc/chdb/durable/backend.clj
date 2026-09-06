(ns jdbc.chdb.durable.backend
  "Durable V1 object-backend contract and runtime-neutral reference backend."
  (:require [clojure.string :as str]))

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
