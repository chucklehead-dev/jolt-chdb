(ns jdbc.chdb
  "Explicit chDB driver registration and high-level streaming insert API."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [db.driver :as driver]
            [db.jdbc-shim :as shim]
            [jdbc.chdb.native :as native]
            [jdbc.proto :as proto]
            [jolt.ffi :as ffi]))

(defrecord TypedParam [type value])

(defn- balanced-type? [s]
  (and (re-matches #"[A-Za-z][A-Za-z0-9_(), ]*" s)
       (loop [xs (seq s) depth 0]
         (if-let [c (first xs)]
           (let [depth (cond (= c \() (inc depth) (= c \)) (dec depth) :else depth)]
             (and (not (neg? depth)) (recur (next xs) depth)))
           (zero? depth)))))

(defn typed-param
  "Supply an explicit ClickHouse type, primarily for nullable nil parameters."
  [type value]
  (let [type (str type)]
    (when-not (balanced-type? type)
      (throw (ex-info "unsafe or invalid ClickHouse parameter type" {:type type})))
    (->TypedParam type value)))

(defn- parameter [value]
  (let [[type value] (if (instance? TypedParam value)
                       [(:type value) (:value value)]
                       [nil value])
        inferred (cond
                   type type
                   (nil? value)
                   (throw (ex-info "chDB cannot infer the type of nil; use jdbc.chdb/typed-param"
                                   {:value value :jdbc/sql-error true}))
                   (boolean? value) "Bool"
                   (integer? value) "Int64"
                   (number? value) "Float64"
                   (or (string? value) (bytes? value)) "String"
                   :else
                   (throw (ex-info "unsupported chDB parameter type"
                                   {:value value :class (str (class value))
                                    :jdbc/sql-error true})))
        encoded (cond
                  (nil? value) "\\N"
                  (true? value) "1"
                  (false? value) "0"
                  (bytes? value) value
                  :else (str value))]
    {:type inferred :value encoded}))

(defn- rewrite-placeholders [sql params]
  (let [parameters (mapv parameter params)
        n (count sql)]
    (loop [i 0 mode :code block-depth 0 pindex 0 out (transient [])]
      (if (= i n)
        (do
          (when-not (= pindex (count parameters))
            (throw (ex-info "more chDB parameters than SQL placeholders"
                            {:placeholders pindex :parameters (count parameters)
                             :jdbc/sql-error true})))
          {:sql (apply str (persistent! out)) :parameters parameters})
        (let [c (nth sql i)
              next-c (when (< (inc i) n) (nth sql (inc i)))]
          (case mode
            :code
            (cond
              (= c \?)
              (do
                (when (>= pindex (count parameters))
                  (throw (ex-info "more chDB placeholders than parameters"
                                  {:placeholders (inc pindex) :parameters (count parameters)
                                   :jdbc/sql-error true})))
                (let [p (nth parameters pindex)]
                  (recur (inc i) :code 0 (inc pindex)
                         (conj! out (str "{p" (inc pindex) ":" (:type p) "}")))))

              (= c \') (recur (inc i) :single 0 pindex (conj! out c))
              (= c \u0022) (recur (inc i) :double 0 pindex (conj! out c))
              (= c \`) (recur (inc i) :backtick 0 pindex (conj! out c))
              (and (= c \-) (= next-c \-))
              (recur (+ i 2) :line 0 pindex (-> out (conj! c) (conj! next-c)))
              (and (= c \/) (= next-c \*))
              (recur (+ i 2) :block 1 pindex (-> out (conj! c) (conj! next-c)))
              :else (recur (inc i) :code 0 pindex (conj! out c)))

            :line
            (recur (inc i) (if (= c \newline) :code :line) 0 pindex (conj! out c))

            :block
            (cond
              (and (= c \/) (= next-c \*))
              (recur (+ i 2) :block (inc block-depth) pindex
                     (-> out (conj! c) (conj! next-c)))
              (and (= c \*) (= next-c \/))
              (let [depth (dec block-depth)]
                (recur (+ i 2) (if (zero? depth) :code :block) depth pindex
                       (-> out (conj! c) (conj! next-c))))
              :else (recur (inc i) :block block-depth pindex (conj! out c)))

            ;; All remaining modes are quoted identifiers or string literals.
            (let [quote (case mode :single \' :double \u0022 :backtick \`)]
              (cond
                (and (= c \\) next-c)
                (recur (+ i 2) mode block-depth pindex
                       (-> out (conj! c) (conj! next-c)))
                (and (= c quote) (= next-c quote))
                (recur (+ i 2) mode block-depth pindex
                       (-> out (conj! c) (conj! next-c)))
                (= c quote)
                (recur (inc i) :code 0 pindex (conj! out c))
                :else
                (recur (inc i) mode block-depth pindex (conj! out c))))))))))

(defn- allocate-encoded! [allocated value]
  (if (bytes? value)
    (let [n (alength value)
          ptr (ffi/alloc (max 1 n))]
      (swap! allocated conj ptr)
      (ffi/write-array ptr value)
      {:pointer ptr :length n})
    (let [s (str value)
          ptr (ffi/alloc (max 1 (inc (* 4 (count s)))))]
      (swap! allocated conj ptr)
      {:pointer ptr :length (ffi/write-bytes ptr s)})))

(defn- pointer-array! [allocated buffers]
  (if (empty? buffers)
    ffi/null
    (let [width (ffi/sizeof :pointer)
          ptr (ffi/alloc (* width (count buffers)))]
      (swap! allocated conj ptr)
      (doseq [[i buffer] (map-indexed vector buffers)]
        (ffi/write ptr :pointer (* i width) (:pointer buffer)))
      ptr)))

(defn- length-array! [allocated buffers]
  (if (empty? buffers)
    ffi/null
    (let [width (ffi/sizeof :size_t)
          ptr (ffi/alloc (* width (count buffers)))]
      (swap! allocated conj ptr)
      (doseq [[i buffer] (map-indexed vector buffers)]
        (ffi/write ptr :size_t (* i width) (:length buffer)))
      ptr)))

(defn- decode-compact-json [data]
  (let [lines (vec (remove str/blank? (str/split-lines data)))]
    (if (empty? lines)
      {:labels [] :rows []}
      (do
        (when (< (count lines) 2)
          (throw (ex-info "chDB compact JSON result lacks name/type headers"
                          {:data data :jdbc/sql-error true})))
        (let [labels (vec (json/read-str (nth lines 0)))
              types (vec (json/read-str (nth lines 1)))
              rows (mapv (fn [line] (vec (json/read-str line))) (subvec lines 2))]
          (when-not (= (count labels) (count types))
            (throw (ex-info "chDB result name/type header mismatch"
                            {:labels labels :types types :jdbc/sql-error true})))
          {:labels labels :rows rows})))))

(defn- consume-result [result]
  (when (ffi/null? result)
    (throw (ex-info "chDB returned a null result" {:jdbc/sql-error true})))
  (try
    (when-let [message (native/chdb-result-error result)]
      (throw (ex-info (str "chDB query failed: " message) {:jdbc/sql-error true})))
    (let [length (native/chdb-result-length result)
          buffer (native/chdb-result-buffer result)
          data (if (zero? length) "" (ffi/read-bytes buffer length))
          decoded (decode-compact-json data)]
      (assoc decoded :count (if (seq (:labels decoded))
                              0
                              (native/chdb-result-rows-written result))))
    (finally (native/chdb-destroy-query-result result))))

(defn execute-any [handle sql params]
  (let [{rewritten :sql parameters :parameters} (rewrite-placeholders sql params)]
    (native/with-live-handle
     handle
     (fn [connection]
       (let [allocated (atom [])]
         (try
           (let [query-buffer (allocate-encoded! allocated rewritten)
                 format-buffer (allocate-encoded! allocated "JSONCompactEachRowWithNamesAndTypes")
                 name-buffers (mapv (fn [i] (allocate-encoded! allocated (str "p" (inc i))))
                                    (range (count parameters)))
                 value-buffers (mapv (fn [p] (allocate-encoded! allocated (:value p))) parameters)
                 names (pointer-array! allocated name-buffers)
                 name-lengths (length-array! allocated name-buffers)
                 values (pointer-array! allocated value-buffers)
                 value-lengths (length-array! allocated value-buffers)]
             (consume-result
              (native/chdb-query-with-params-n
               connection
               (:pointer query-buffer) (:length query-buffer)
               (:pointer format-buffer) (:length format-buffer)
               names name-lengths values value-lengths (count parameters))))
           (finally
             (doseq [ptr (reverse @allocated)] (ffi/free ptr)))))))))

(defn- spec-path [spec]
  (let [path (if (string? spec)
               (subs spec (count "chdb:"))
               (or (:subname spec) (:name spec) (:dbname spec) ":memory:"))
        path (str path)]
    (cond
      (or (empty? path) (= path ":memory:")) ":memory:"
      (str/starts-with? path "//") (subs path 2)
      :else path)))

(def chdb-driver
  (reify driver/Driver
    (descriptor [_]
      {:id :chdb
       :aliases #{"chdb"}
       :uri-prefixes ["chdb:"]
       :product-name "ClickHouse (chDB)"
       :capabilities {:transactions :none :generated-keys :none}
       :constraints {:active-storage-paths :one-per-process}
       :schema-sql nil})
    (open-handle [_ spec] (native/open! (spec-path spec)))
    (close-handle [_ handle] (native/close! handle))
    (execute-handle [_ handle sql params] (execute-any handle sql params))))

(driver/register! chdb-driver)

(defn stream-insert!
  "Insert bounded format-encoded chunks. The native stream never escapes this
  call; it is finalized or canceled and destroyed on every path."
  ([conn query chunks] (stream-insert! conn query chunks {:format "JSONEachRow"}))
  ([conn query chunks {:keys [format] :or {format "JSONEachRow"}}]
   (let [shim-conn (proto/connection conn)
         {:keys [handle]} (shim/driver-context shim-conn :chdb)]
     (native/with-live-handle
      handle
      (fn [connection]
        (let [allocated (atom [])]
          (try
            (let [query-buffer (allocate-encoded! allocated query)
                  format-buffer (allocate-encoded! allocated format)
                  stream (native/chdb-stream-insert-n
                          connection
                          (:pointer query-buffer) (:length query-buffer)
                          (:pointer format-buffer) (:length format-buffer))
                  finalized? (atom false)]
              (when (ffi/null? stream)
                (throw (ex-info "chDB returned a null insert stream" {:jdbc/sql-error true})))
              (try
                (when-let [message (native/chdb-stream-insert-error stream)]
                  (throw (ex-info (str "chDB insert stream failed: " message)
                                  {:jdbc/sql-error true})))
                (doseq [chunk chunks]
                  ;; A stream may be arbitrarily long. Keep only the current
                  ;; chunk alive across the native append call.
                  (let [chunk-allocated (atom [])]
                    (try
                      (let [chunk-buffer (allocate-encoded! chunk-allocated chunk)]
                        (when-not (zero? (native/chdb-stream-append
                                          stream (:pointer chunk-buffer) (:length chunk-buffer)))
                          (throw (ex-info (str "chDB stream append failed: "
                                               (or (native/chdb-stream-insert-error stream)
                                                   "unknown error"))
                                          {:jdbc/sql-error true}))))
                      (finally
                        (doseq [ptr (reverse @chunk-allocated)] (ffi/free ptr))))))
                (let [result (native/chdb-stream-done stream)]
                  (reset! finalized? true)
                  (consume-result result))
                (catch Throwable t
                  (when-not @finalized?
                    (native/chdb-stream-cancel-insert stream))
                  (throw t))
                (finally
                  (native/chdb-destroy-insert-stream stream))))
            (finally
              (doseq [ptr (reverse @allocated)] (ffi/free ptr))))))))))
