(ns jdbc.chdb.durable.head
  "Frozen Durable V1 head.json codec and fail-closed schema checks.

  Maps deliberately retain JSON string keys so fields unknown to this V1
  reader survive a decode/update/encode cycle unchanged."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]))

(def max-head-bytes (* 1024 1024))
(def max-safe-integer 9007199254740991)
(def max-json-depth
  "Maximum number of nested JSON object/array containers, including the root."
  64)

(def protocol-source
  {:repository "https://github.com/chdb-io/chdb.git"
   :commit "66643e5030fb73c30ac5cdd31d4c7858ea040ed0"
   :document "docs/durable/protocol-v1.mdx"
   :section "head"})

(def ^:private supported-protocol-version 1)
(def ^:private supported-reader-features #{})
(def ^:private supported-writer-features #{})
(def ^:private sha256-pattern #"[0-9a-f]{64}")
(def ^:private decimal-component "(?:0|[1-9][0-9]*)")
(def ^:private redacted-object-path ["<redacted-object-field>"])

(def ^:private reference-json-schema
  {"key" true
   "size" true
   "sha256" true})

(def ^:private head-json-schema
  {"protocol" {"version" true
                "reader_features" [true]
                "writer_features" [true]}
   "engine" {"name" true
             "version" true
             "backup_format" true
             "min_reader" true}
   "lease" {"generation" true
            "owner" true
            "instance" true
            "expires_at" true}
   "manifest" {"db" true
               "base" reference-json-schema
               "wal" [reference-json-schema]
               "seq" true}})

(defn- fail! [type message path]
  (throw (ex-info message {:type type :path path})))

(defn- corrupt! [message path]
  (fail! ::corrupt message path))

(defn- unsupported! [message path]
  (fail! ::protocol-unsupported message path))

(defn- limit! [message path]
  (fail! ::limit-exceeded message path))

(defn- finite-number? [value]
  (and (number? value)
       (= value value)
       (not= value ##Inf)
       (not= value ##-Inf)))

(defn- safe-integer? [value]
  (and (integer? value)
       (<= (- max-safe-integer) value max-safe-integer)))

(defn- valid-json-value! [value path schema depth]
  (cond
    (map? value)
    (do
      (when (>= depth max-json-depth)
        (corrupt! "head.json exceeds the JSON nesting limit" path))
      (doseq [[key child] value]
        (when-not (string? key)
          (corrupt! "head.json object keys must be strings" path))
        ;; Known keys are constants from the frozen schema and are safe to expose.
        ;; Unknown key names may themselves contain credentials, so once traversal
        ;; crosses one, neither that key nor any descendant key reaches error data.
        (let [known? (and (map? schema) (contains? schema key))]
          (valid-json-value! child
                             (if known? (conj path key) redacted-object-path)
                             (when known? (get schema key))
                             (inc depth)))))

    (vector? value)
    (do
      (when (>= depth max-json-depth)
        (corrupt! "head.json exceeds the JSON nesting limit" path))
      (let [element-schema (when (vector? schema) (first schema))]
        (doseq [[index child] (map-indexed vector value)]
          (valid-json-value! child (conj path index) element-schema
                             (inc depth)))))

    (integer? value)
    (when-not (safe-integer? value)
      (corrupt! "head.json integer exceeds the cross-language safe range" path))

    (number? value)
    (when-not (finite-number? value)
      (corrupt! "head.json number must be finite" path))

    (or (nil? value) (string? value) (boolean? value)) nil
    :else (corrupt! "head.json contains a non-JSON value" path))
  value)

(defn- required [object key path]
  (when-not (and (map? object) (contains? object key))
    (corrupt! "head.json is missing a required field" (conj path key)))
  (get object key))

(defn- object! [value path]
  (when-not (map? value)
    (corrupt! "head.json field must be an object" path))
  value)

(defn- array! [value path]
  (when-not (vector? value)
    (corrupt! "head.json field must be an array" path))
  value)

(defn- nonblank-string! [value path]
  (when-not (and (string? value) (not (str/blank? value)))
    (corrupt! "head.json field must be a nonblank string" path))
  value)

(defn- nonnegative-safe-integer! [value path]
  (when-not (and (safe-integer? value) (not (neg? value)))
    (corrupt! "head.json field must be a nonnegative safe integer" path))
  value)

(defn- positive-safe-integer! [value path]
  (when-not (and (safe-integer? value) (pos? value))
    (corrupt! "head.json field must be a positive safe integer" path))
  value)

(defn- feature-list! [value path]
  (array! value path)
  (doseq [[index feature] (map-indexed vector value)]
    (nonblank-string! feature (conj path index)))
  (when-not (= (count value) (count (distinct value)))
    (corrupt! "head.json feature names must be unique" path))
  value)

(defn- reference-parts! [kind reference path]
  (object! reference path)
  (let [key (nonblank-string! (required reference "key" path)
                              (conj path "key"))
        size (nonnegative-safe-integer! (required reference "size" path)
                                        (conj path "size"))
        digest (required reference "sha256" path)
        extension (case kind :checkpoint "tar\\.gz" :wal "jsonl")
        prefix (case kind :checkpoint "checkpoints" :wal "wal")]
    ;; Build the exact V1 spelling without accepting an absolute key, empty
    ;; component, traversal component, leading zero, or non-lowercase hex token.
    (let [key-pattern (re-pattern
                       (str prefix "/(" decimal-component ")-("
                            decimal-component ")-([0-9a-f]{8})\\." extension))
          match (re-matches key-pattern key)]
      (when-not match
        (corrupt! "head.json object reference key is not a canonical V1 key"
                  (conj path "key")))
      (when-not (and (string? digest) (re-matches sha256-pattern digest))
        (corrupt! "head.json object reference has an invalid SHA-256 digest"
                  (conj path "sha256")))
      (let [generation (parse-long (nth match 1))
            seq-number (parse-long (nth match 2))]
        (positive-safe-integer! generation (conj path "key"))
        (positive-safe-integer! seq-number (conj path "key"))
        ;; Referencing size zero is legal at this layer; the download integrity
        ;; gate later proves whether a concrete provider object matches it.
        {:key key :size size :generation generation :seq seq-number}))))

(defn- validate-protocol! [head mode]
  (let [path ["protocol"]
        protocol (object! (required head "protocol" []) path)
        version (required protocol "version" path)
        reader-features (feature-list!
                         (required protocol "reader_features" path)
                         (conj path "reader_features"))
        writer-features (feature-list!
                         (required protocol "writer_features" path)
                         (conj path "writer_features"))]
    (when-not (safe-integer? version)
      (corrupt! "head.json protocol version must be a safe integer"
                (conj path "version")))
    (cond
      (> version supported-protocol-version)
      (unsupported! "head.json requires a newer protocol reader"
                    (conj path "version"))

      (not= version supported-protocol-version)
      (corrupt! "head.json protocol version is not V1" (conj path "version")))
    (when (seq (remove supported-reader-features reader-features))
      (unsupported! "head.json requires an unknown reader feature"
                    (conj path "reader_features")))
    (when (and (= :writer mode)
               (seq (remove supported-writer-features writer-features)))
      (unsupported! "head.json requires an unknown writer feature"
                    (conj path "writer_features")))))

(defn- validate-engine! [head]
  (let [path ["engine"]
        engine (object! (required head "engine" []) path)]
    (when-not (= "chdb" (required engine "name" path))
      (corrupt! "head.json engine name must be chdb" (conj path "name")))
    (nonblank-string! (required engine "version" path) (conj path "version"))
    (nonnegative-safe-integer! (required engine "backup_format" path)
                               (conj path "backup_format"))
    (nonblank-string! (required engine "min_reader" path)
                      (conj path "min_reader"))))

(defn- validate-lease! [head]
  (let [path ["lease"]
        lease (object! (required head "lease" []) path)
        generation (positive-safe-integer!
                    (required lease "generation" path)
                    (conj path "generation"))
        owner (required lease "owner" path)
        instance (required lease "instance" path)
        expires-at (required lease "expires_at" path)
        released? (and (nil? owner) (nil? instance) (nil? expires-at))
        active? (and (string? owner) (not (str/blank? owner))
                     (string? instance) (not (str/blank? instance))
                     (finite-number? expires-at) (not (neg? expires-at)))]
    (when-not (or released? active?)
      (corrupt! "head.json lease must be wholly active or wholly released" path))
    generation))

(defn- validate-manifest! [head lease-generation]
  (let [path ["manifest"]
        manifest (object! (required head "manifest" []) path)
        db (required manifest "db" path)
        base (required manifest "base" path)
        wal (array! (required manifest "wal" path) (conj path "wal"))
        seq-number (nonnegative-safe-integer!
                    (required manifest "seq" path) (conj path "seq"))
        base-parts (when-not (nil? base)
                     (reference-parts! :checkpoint base (conj path "base")))
        wal-parts (mapv (fn [index reference]
                          (reference-parts! :wal reference
                                            (conj path "wal" index)))
                        (range (count wal)) wal)
        parts (cond-> [] base-parts (conj base-parts) true (into wal-parts))
        sequences (mapv :seq parts)
        expected-seq (if (seq sequences) (peek sequences) 0)]
    (nonblank-string! db (conj path "db"))
    (when-not (every? true? (map < sequences (rest sequences)))
      (corrupt! "head.json WAL references are not in strict replay order"
                (conj path "wal")))
    (when-not (= expected-seq seq-number)
      (corrupt! "head.json manifest sequence does not name its final reference"
                (conj path "seq")))
    (when (some #(> (:generation %) lease-generation) parts)
      (corrupt! "head.json reference generation exceeds the lease generation"
                path))))

(defn validate!
  "Validate a decoded V1 head and return the identical map.

  `mode` is `:read-only` or `:writer`. Unknown writer features remain readable
  in read-only mode but prevent writer acquisition. Unknown map fields remain
  present because validation never rebuilds the input map."
  ([head] (validate! head :read-only))
  ([head mode]
   (when-not (contains? #{:read-only :writer} mode)
     (throw (ex-info "Durable head validation mode is invalid"
                     {:type ::invalid-mode :mode mode})))
   (valid-json-value! head [] head-json-schema 0)
   (object! head [])
   (validate-protocol! head mode)
   (validate-engine! head)
   (let [generation (validate-lease! head)]
     (validate-manifest! head generation))
   head))

(defn- input-bytes [input]
  (cond
    (string? input) (.getBytes input "UTF-8")
    (bytes? input) input
    :else (corrupt! "head.json input must be UTF-8 bytes or a string" [])))

(declare scan-json-value)

(defn- json-whitespace? [character]
  (contains? #{\space \tab \newline \return} character))

(defn- skip-json-whitespace [text start]
  (loop [index start]
    (if (and (< index (count text))
             (json-whitespace? (.charAt text index)))
      (recur (inc index))
      index)))

(defn- scan-json-string [text start]
  (loop [index (inc start)]
    (when (>= index (count text))
      (throw (ex-info "unterminated JSON string" {})))
    (case (.charAt text index)
      \" (inc index)
      \\ (recur (+ index 2))
      (recur (inc index)))))

(defn- decode-json-key [text start end]
  ;; Delegate escape and surrogate handling to the same parser used below, so
  ;; spellings such as "owner" and "ow\u006eer" compare as the same key.
  (json/read-str (subs text start end)))

(defn- scan-json-object [text start depth]
  (loop [index (skip-json-whitespace text (inc start))
         seen #{}]
    (when (>= index (count text))
      (throw (ex-info "unterminated JSON object" {})))
    (if (= \} (.charAt text index))
      (inc index)
      (do
        (when-not (= \" (.charAt text index))
          (throw (ex-info "invalid JSON object key" {})))
        (let [key-end (scan-json-string text index)
              key (decode-json-key text index key-end)]
          (when (contains? seen key)
            (throw (ex-info "duplicate JSON object key" {})))
          (let [colon (skip-json-whitespace text key-end)]
            (when-not (and (< colon (count text))
                           (= \: (.charAt text colon)))
              (throw (ex-info "missing JSON object colon" {})))
            (let [value-end (scan-json-value text (inc colon) depth)
                  delimiter (skip-json-whitespace text value-end)]
              (when (>= delimiter (count text))
                (throw (ex-info "unterminated JSON object" {})))
              (case (.charAt text delimiter)
                \, (recur (skip-json-whitespace text (inc delimiter))
                           (conj seen key))
                \} (inc delimiter)
                (throw (ex-info "invalid JSON object delimiter" {}))))))))))

(defn- scan-json-array [text start depth]
  (loop [index (skip-json-whitespace text (inc start))]
    (when (>= index (count text))
      (throw (ex-info "unterminated JSON array" {})))
    (if (= \] (.charAt text index))
      (inc index)
      (let [value-end (scan-json-value text index depth)
            delimiter (skip-json-whitespace text value-end)]
        (when (>= delimiter (count text))
          (throw (ex-info "unterminated JSON array" {})))
        (case (.charAt text delimiter)
          \, (recur (skip-json-whitespace text (inc delimiter)))
          \] (inc delimiter)
          (throw (ex-info "invalid JSON array delimiter" {})))))))

(defn- scan-json-primitive [text start]
  (loop [index start]
    (if (or (>= index (count text))
            (contains? #{\, \] \}} (.charAt text index))
            (json-whitespace? (.charAt text index)))
      index
      (recur (inc index)))))

(defn- scan-json-value
  ([text start] (scan-json-value text start 0))
  ([text start depth]
   (let [index (skip-json-whitespace text start)]
     (when (>= index (count text))
       (throw (ex-info "missing JSON value" {})))
     (case (.charAt text index)
       \{ (do (when (>= depth max-json-depth)
                (throw (ex-info "JSON nesting limit exceeded" {})))
              (scan-json-object text index (inc depth)))
       \[ (do (when (>= depth max-json-depth)
                (throw (ex-info "JSON nesting limit exceeded" {})))
              (scan-json-array text index (inc depth)))
       \" (scan-json-string text index)
       (scan-json-primitive text index)))))

(defn- single-json-value-bounds [text]
  ;; data.json intentionally resolves duplicate keys last-value-wins. Scan the
  ;; original token stream first so another binding cannot reach a different
  ;; lease/fencing decision from byte-identical input. The same scan proves
  ;; that JSON whitespace surrounds exactly one value; only that value is
  ;; passed to data.json because parser handling of trailing whitespace differs
  ;; across runtimes. All scanner detail is discarded because an object key may
  ;; itself contain a credential.
  (try
    (let [start (skip-json-whitespace text 0)
          end (scan-json-value text start)
          document-end (skip-json-whitespace text end)]
      (when-not (= document-end (count text))
        (throw (ex-info "second JSON value" {})))
      [start end])
    (catch Throwable _
      (corrupt! "head.json is not valid unambiguous JSON" []))))

(defn decode
  "Decode and validate a bounded UTF-8 head.json document."
  ([input] (decode input :read-only))
  ([input mode]
   (let [bytes (input-bytes input)
         byte-count (alength bytes)]
     (when (> byte-count max-head-bytes)
       (limit! "head.json exceeds the 1 MiB limit" []))
     (when (and (>= byte-count 3)
                (= [-17 -69 -65] (subvec (vec bytes) 0 3)))
       (corrupt! "head.json must not contain a UTF-8 byte-order mark" []))
     (let [text (String. bytes "UTF-8")]
       (when-not (= (vec bytes) (vec (.getBytes text "UTF-8")))
         (corrupt! "head.json is not canonical UTF-8" []))
       (let [[start end] (single-json-value-bounds text)
             head (try
                    (json/read-str (subs text start end)
                                   :bigdec true
                                   :extra-data-fn json/on-extra-throw)
                    (catch Throwable _
                      ;; Do not retain the parser exception: it may quote a
                      ;; secret-bearing fragment of the invalid document.
                      (corrupt! "head.json is not valid JSON" [])))]
         (validate! head mode))))))

(defn encode
  "Validate and encode a writer-compatible head as UTF-8 JSON bytes."
  [head]
  (validate! head :writer)
  (let [text (try
               (json/write-str head)
               (catch Throwable _
                 (corrupt! "head.json could not be encoded" [])))
        bytes (.getBytes text "UTF-8")]
    (when (> (alength bytes) max-head-bytes)
      (limit! "head.json exceeds the 1 MiB limit" []))
    bytes))
