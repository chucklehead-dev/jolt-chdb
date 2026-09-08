(ns jdbc.chdb
  "Explicit chDB driver registration and high-level chDB extension APIs."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [db.driver :as driver]
            [db.export :as export]
            [db.jdbc-shim :as shim]
            [jdbc.chdb.native :as native]
            [jdbc.proto :as proto]
            [jolt.ffi :as ffi]))

(defrecord TypedParam [type value])

(def max-encoded-result-rows
  "Hard row cap for one `query-bytes` call. Callers may request a lower cap."
  100000)

(def max-encoded-result-bytes
  "Hard serialized/native-result cap for one `query-bytes` call (64 MiB)."
  67108864)

(def default-encoded-result-rows
  "Default row cap for one encoded query. Currently equal to the hard cap."
  max-encoded-result-rows)

(def default-encoded-result-bytes
  "Default byte cap for one encoded query. Currently equal to the hard cap."
  max-encoded-result-bytes)

(def ^:dynamic ^:private *query-statistics-sink* nil)

(defn with-query-statistics
  "Run zero-argument `f` and return `{:result value :queries [stats ...]}`.
  One statistics map is copied from each completed native query result,
  successful or failed, before that result is destroyed. Nested collectors
  both receive the same values; no native pointer escapes. Collection is
  synchronous and thread-bound. This chDB-specific observation surface does
  not change generic JDBC return values."
  [f]
  (let [queries (atom [])
        parent *query-statistics-sink*]
    (binding [*query-statistics-sink*
              (fn
                ([] @queries)
                ([statistics]
                 (swap! queries conj statistics)
                 (when parent (parent statistics))))]
      (let [result (f)]
        {:result result :queries @queries}))))

(def ^:private encoded-formats
  {:arrow {:native-format "Arrow"
           :content-type "application/vnd.apache.arrow.file"
           :extension "arrow"}
   :parquet {:native-format "Parquet"
             :content-type "application/vnd.apache.parquet"
             :extension "parquet"}})

(def query-bytes-capability
  "Neutral db.export capability advertised by the chDB driver."
  {:version 1
   :formats
   (into {} (map (fn [[format metadata]]
                   [format (select-keys metadata [:content-type :extension])]))
         encoded-formats)
   :limits {:max-rows max-encoded-result-rows
            :max-bytes max-encoded-result-bytes
            :default-max-rows default-encoded-result-rows
            :default-max-bytes default-encoded-result-bytes}
   :staging :memory})

(def ^:private hex-digits "0123456789ABCDEF")

(defn- escaped-string-bytes
  "Encode every input byte with ClickHouse's `\\xHH` parameter syntax. The
  chDB parameter parser consumes backslash escapes before applying the declared
  String type, so passing raw bytes would lose `0x5c` and make arbitrary binary
  or strings containing backslashes impossible to round-trip."
  [value]
  (let [octets (if (bytes? value) value (.getBytes (str value) "UTF-8"))]
    (apply str
           (mapcat (fn [b]
                     (let [n (bit-and (int b) 255)]
                       [\\ \x (nth hex-digits (quot n 16))
                        (nth hex-digits (mod n 16))]))
                   octets))))

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
                  (or (string? value) (bytes? value)) (escaped-string-bytes value)
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

(defn classification-sql
  "Return the value-free SQL shape executed for a parameterized query.

  Parameter values remain out of the returned string; only validated native
  parameter types appear in named placeholders. Durable policy uses this form
  so the core classifier sees the same parseable statement shape that native
  execution receives."
  [sql params]
  (:sql (rewrite-placeholders sql params)))

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
        (ffi/write ptr :pointer (:pointer buffer) (* i width)))
      ptr)))

(defn- length-array! [allocated buffers]
  (if (empty? buffers)
    ffi/null
    (let [width (ffi/sizeof :size_t)
          ptr (ffi/alloc (* width (count buffers)))]
      (swap! allocated conj ptr)
      (doseq [[i buffer] (map-indexed vector buffers)]
        (ffi/write ptr :size_t (:length buffer) (* i width)))
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

(defn- result-statistics [result]
  {:elapsed-seconds (native/chdb-result-elapsed result)
   :result-rows (native/chdb-result-rows-read result)
   :result-bytes (native/chdb-result-bytes-read result)
   :storage-rows-read (native/chdb-result-storage-rows-read result)
   :storage-bytes-read (native/chdb-result-storage-bytes-read result)
   :rows-written (native/chdb-result-rows-written result)
   :bytes-written (native/chdb-result-bytes-written result)})

(defn- observe-statistics! [statistics]
  (when *query-statistics-sink*
    (*query-statistics-sink* statistics))
  statistics)

(defn- statistics-diagnostics [statistics]
  (cond-> {:db.chdb/query-statistics statistics}
    *query-statistics-sink*
    (assoc :db.chdb/query-statistics-collected (*query-statistics-sink*))))

(defn- with-owned-result
  "Consume a non-null native result while owning its destruction. `f` must not
  let the result or any pointer derived from it escape."
  [result f]
  (when (ffi/null? result)
    (throw (ex-info "chDB returned a null result" {:jdbc/sql-error true})))
  (try
    (let [message (native/chdb-result-error result)
          ;; Preserve the unobserved success hot path: the seven additional
          ;; native accessors run only inside a collector or for an error whose
          ;; diagnostics retain the statistics.
          statistics
          (when (or *query-statistics-sink* message)
            (result-statistics result))]
      (when statistics (observe-statistics! statistics))
      (when message
        (throw (ex-info (str "chDB query failed: " message)
                        (merge {:jdbc/sql-error true}
                               (statistics-diagnostics statistics)))))
      (f result))
    (finally (native/chdb-destroy-query-result result))))

(defn- consume-json-result [result]
  (with-owned-result
   result
   (fn [result]
     (let [length (native/chdb-result-length result)
           buffer (native/chdb-result-buffer result)
           data (if (zero? length) "" (ffi/read-bytes buffer length))
           decoded (decode-compact-json data)]
       (assoc decoded :count (if (seq (:labels decoded))
                               0
                               (native/chdb-result-rows-written result)))))))

(defn- execute-native [handle sql params format consume]
  (let [{rewritten :sql parameters :parameters} (rewrite-placeholders sql params)]
    (native/with-live-handle
     handle
     (fn [connection]
       (let [allocated (atom [])]
         (try
           (let [query-buffer (allocate-encoded! allocated rewritten)
                 format-buffer (allocate-encoded! allocated format)
                 name-buffers (mapv (fn [i] (allocate-encoded! allocated (str "p" (inc i))))
                                    (range (count parameters)))
                 value-buffers (mapv (fn [p] (allocate-encoded! allocated (:value p))) parameters)
                 names (pointer-array! allocated name-buffers)
                 name-lengths (length-array! allocated name-buffers)
                 values (pointer-array! allocated value-buffers)
                 value-lengths (length-array! allocated value-buffers)]
             (consume handle connection
               (native/chdb-query-with-params-n
               connection
               (:pointer query-buffer) (:length query-buffer)
               (:pointer format-buffer) (:length format-buffer)
               names name-lengths values value-lengths (count parameters))))
           (finally
             (doseq [ptr (reverse @allocated)] (ffi/free ptr)))))))))

(defn execute-any [handle sql params]
  (execute-native handle sql params "JSONCompactEachRowWithNamesAndTypes"
                  (fn [_ _ result] (consume-json-result result))))

(defn- validate-encoded-sql! [sql]
  (let [trimmed (str/trim sql)
        n (count sql)]
    (when-not (re-find #"(?i)^(select|with)\b" trimmed)
      (throw (ex-info "chDB encoded query must begin with SELECT or WITH"
                      {:jdbc/sql-error true :db.chdb/query-bytes true})))
    ;; The SQL is nested inside a generated SELECT. Balanced lexical structure
    ;; prevents caller text from closing that subquery, commenting out its
    ;; suffix, or beginning a second statement that bypasses hard settings.
    (loop [i 0 mode :code block-depth 0 paren-depth 0]
      (if (= i n)
        (do
          (when-not (or (= mode :code) (= mode :line))
            (throw (ex-info "chDB encoded query has an unterminated quoted form or comment"
                            {:mode mode :jdbc/sql-error true
                             :db.chdb/query-bytes true})))
          (when-not (zero? paren-depth)
            (throw (ex-info "chDB encoded query has unbalanced parentheses"
                            {:depth paren-depth :jdbc/sql-error true
                             :db.chdb/query-bytes true})))
          sql)
        (let [c (nth sql i)
              next-c (when (< (inc i) n) (nth sql (inc i)))]
          (case mode
            :code
            (cond
              (= c \;)
              (throw (ex-info "chDB encoded query may not contain a statement separator"
                              {:index i :jdbc/sql-error true
                               :db.chdb/query-bytes true}))
              (= c \() (recur (inc i) :code 0 (inc paren-depth))
              (= c \))
              (if (zero? paren-depth)
                (throw (ex-info "chDB encoded query may not close its generated subquery"
                                {:index i :jdbc/sql-error true
                                 :db.chdb/query-bytes true}))
                (recur (inc i) :code 0 (dec paren-depth)))
              (= c \') (recur (inc i) :single 0 paren-depth)
              (= c \u0022) (recur (inc i) :double 0 paren-depth)
              (= c \`) (recur (inc i) :backtick 0 paren-depth)
              (and (= c \-) (= next-c \-))
              (recur (+ i 2) :line 0 paren-depth)
              (and (= c \/) (= next-c \*))
              (recur (+ i 2) :block 1 paren-depth)
              :else (recur (inc i) :code 0 paren-depth))

            :line
            (recur (inc i) (if (= c \newline) :code :line) 0 paren-depth)

            :block
            (cond
              (and (= c \/) (= next-c \*))
              (recur (+ i 2) :block (inc block-depth) paren-depth)
              (and (= c \*) (= next-c \/))
              (let [depth (dec block-depth)]
                (recur (+ i 2) (if (zero? depth) :code :block)
                       depth paren-depth))
              :else (recur (inc i) :block block-depth paren-depth))

            (let [quote (case mode :single \' :double \u0022 :backtick \`)]
              (cond
                (and (= c \\) next-c)
                (recur (+ i 2) mode block-depth paren-depth)
                (and (= c quote) (= next-c quote))
                (recur (+ i 2) mode block-depth paren-depth)
                (= c quote)
                (recur (inc i) :code 0 paren-depth)
                :else (recur (inc i) mode block-depth paren-depth)))))))))

(defn- bounded-select [sql max-rows max-bytes]
  ;; Placing the caller's query in a FROM subquery makes this a read-only API:
  ;; DDL, INSERT, trailing FORMAT clauses, and multiple statements are invalid
  ;; in that position. Both interpolated settings are validated integers.
  (str "SELECT * FROM (\n" sql "\n) AS jolt_chdb_encoded_result\n"
       "SETTINGS max_result_rows=" max-rows
       ", max_result_bytes=" max-bytes
       ", result_overflow_mode='throw'"))

(defn- reset-output-format-after-error! [connection]
  ;; libchdb 26.7.0 retains a failed encoded query's output format for exactly
  ;; one subsequent query. Consume that stale format with a successful zero-row
  ;; internal query before returning the original error, or ordinary JDBC JSON
  ;; decoding would see Arrow/Parquet bytes. This query is deliberately raw:
  ;; its result is opaque and destroyed without attempting to decode the stale
  ;; format it is expected to receive.
  (let [sql "SELECT 1 WHERE 0"
        format "JSONCompactEachRowWithNamesAndTypes"]
    (ffi/with-c-string [query-buffer sql]
      (ffi/with-c-string [format-buffer format]
        (let [result (native/chdb-query-with-params-n
                      connection
                      query-buffer (count sql)
                      format-buffer (count format)
                      ffi/null ffi/null ffi/null ffi/null 0)]
          (when (ffi/null? result)
            (throw (ex-info "chDB failed to reset output format after encoded query error"
                            {:jdbc/sql-error true :db.chdb/query-bytes true})))
          (try
            (when-let [message (native/chdb-result-error result)]
              (throw (ex-info
                      (str "chDB failed to reset output format after encoded query error: " message)
                      {:jdbc/sql-error true :db.chdb/query-bytes true})))
            (finally
              (native/chdb-destroy-query-result result))))))))

(defn- retire-after-recovery-failure!
  [handle query-message query-statistics recovery-error]
  ;; Once the one-shot format reset fails, no later query may observe this
  ;; connection's uncertain serializer state. native/close! marks the handle
  ;; closed before invoking the C destructor, so even a close failure remains
  ;; fail-closed and retains the process storage claim.
  (let [close-error (try (native/close! handle) nil
                         (catch Throwable error error))]
    (throw
     (ex-info
      "chDB encoded query failed and its output-format recovery failed; connection retired"
      (cond-> (merge {:jdbc/sql-error true :db.chdb/query-bytes true
                      :query-error query-message
                      :recovery-error (ex-message recovery-error)
                      :db.chdb/connection-retired true}
                     (statistics-diagnostics query-statistics))
        ;; Preserve the throwable, not just its message: native close failures
        ;; can carry structured cause data needed to diagnose an uncertain
        ;; process-wide storage claim.
        close-error (assoc :close-error close-error))
      recovery-error))))

(defn- retire-after-result-destroy-failure!
  [handle query-message query-statistics destroy-error]
  ;; A failed encoded result must be destroyed before the one-shot serializer
  ;; reset. If destruction itself fails, resetting while that result may still
  ;; be live is unsafe. Retire the handle under the encompassing reentrant
  ;; with-live-handle lock and retain every diagnostic instead.
  (let [close-error (try (native/close! handle) nil
                         (catch Throwable error error))]
    (throw
     (ex-info
      "chDB encoded query result destruction failed; connection retired"
      (cond-> (merge {:jdbc/sql-error true :db.chdb/query-bytes true
                      :query-error query-message
                      :destroy-error (ex-message destroy-error)
                      :db.chdb/connection-retired true}
                     (statistics-diagnostics query-statistics))
        close-error (assoc :close-error close-error))
      destroy-error))))

(defn- retire-after-null-result! [handle]
  ;; NULL provides neither an error object nor evidence that libchdb cleared
  ;; the requested binary serializer. Do not let a later JSON query discover
  ;; that uncertainty. As above, close marks the handle unusable first.
  (let [close-error (try (native/close! handle) nil
                         (catch Throwable error error))]
    (throw
     (ex-info
      "chDB returned a null encoded result; connection retired"
      (cond-> {:jdbc/sql-error true :db.chdb/query-bytes true
               :db.chdb/connection-retired true}
        close-error (assoc :close-error (ex-message close-error)))
      close-error))))

(defn- consume-encoded-result [handle connection result max-bytes]
  (when (ffi/null? result)
    (retire-after-null-result! handle))
  (if-let [message (native/chdb-result-error result)]
    (let [statistics (observe-statistics! (result-statistics result))]
      ;; Destroy the failed user result before issuing another query on the
      ;; same native connection. The encompassing with-live-handle lock remains
      ;; held across destruction, recovery, and either throw path.
      (try
        (native/chdb-destroy-query-result result)
        (catch Throwable destroy-error
          (retire-after-result-destroy-failure!
           handle message statistics destroy-error)))
      (try
        (reset-output-format-after-error! connection)
        (catch Throwable recovery-error
          (retire-after-recovery-failure!
           handle message statistics recovery-error)))
      (throw (ex-info (str "chDB query failed: " message)
                      (merge {:jdbc/sql-error true :db.chdb/query-bytes true}
                             (statistics-diagnostics statistics)))))
    (with-owned-result
     result
     (fn [result]
       ;; The engine-side max_result_bytes setting bounds the native materialized
       ;; result. Recheck the serialized size before allocating the Jolt-owned
       ;; copy because format overhead can differ from ClickHouse's accounting.
       (let [length (native/chdb-result-length result)]
         (when (> length max-bytes)
           (throw (ex-info "chDB encoded result exceeds its byte cap"
                           {:actual-bytes length :maximum-bytes max-bytes
                            :jdbc/sql-error true :db.chdb/query-bytes true})))
         (let [buffer (native/chdb-result-buffer result)]
           (when (and (pos? length) (ffi/null? buffer))
             (throw (ex-info "chDB encoded result has a null data buffer"
                             {:actual-bytes length :jdbc/sql-error true
                              :db.chdb/query-bytes true})))
           ;; read-array is binary-safe and copies before with-owned-result
           ;; destroys the result. No native pointer crosses this call.
           {:byte-count length
            :bytes (if (zero? length) (byte-array 0)
                       (ffi/read-array buffer length))}))))))

(defn execute-query-bytes-handle
  "Low-level owned-handle implementation used by serialized driver adapters."
  [handle sql params {:keys [format max-rows max-bytes]}]
  (let [native-format (get-in encoded-formats [format :native-format])
        bounded-sql (bounded-select (validate-encoded-sql! sql)
                                    max-rows max-bytes)]
    ;; execute-native enters native/with-live-handle exactly once and retains
    ;; that lock through result destruction and stale-format recovery.
    (execute-native handle bounded-sql params native-format
                    #(consume-encoded-result %1 %2 %3 max-bytes))))

(defn query-bytes
  "Execute one result-bounded SELECT and return an owned Arrow or Parquet byte
  array. SQL is trusted application input; result caps do not bound arbitrary
  SELECT execution cost or table-function access.

  `query` is a SQL string or `[sql & params]`, matching jdbc.core/HoneySQL SQL
  vectors. Options requires `:format` (`:arrow` or `:parquet`); `:max-rows` and
  `:max-bytes` may lower, but never exceed, the public hard caps. The native
  result and its buffer are destroyed before this function returns. No output
  path is accepted: filesystem and HTTP download policy belongs to the caller."
  [conn query options]
  (export/query-bytes conn query options))

(defn- spec-path [spec]
  (let [path (if (string? spec)
               (subs spec (count "chdb:"))
               (or (:subname spec) (:name spec) (:dbname spec) ":memory:"))
        path (str path)]
    (cond
      (or (empty? path) (= path ":memory:")) ":memory:"
      (str/starts-with? path "//") (subs path 2)
      :else path)))

(def ^:private max-database-name-length 255)

(defn- spec-database [spec]
  (when (map? spec)
    (let [database (:database spec)]
      (when-not (or (nil? database) (string? database) (keyword? database))
        (throw (ex-info "chDB :database must be a string or keyword"
                        {:database database :jdbc/sql-error true})))
      (when (and (keyword? database) (namespace database))
        (throw (ex-info "chDB :database keyword must be unqualified"
                        {:database database :jdbc/sql-error true})))
      (let [database (cond
                       (keyword? database) (name database)
                       (string? database) database
                       :else nil)]
        (when-not (str/blank? database)
          ;; Keep interpolation into CREATE DATABASE and USE deliberately more
          ;; restrictive than ClickHouse's full quoted-identifier grammar. A
          ;; logical database in a dbspec is configuration, not raw SQL.
          (when-not (and (<= (count database) max-database-name-length)
                         (re-matches #"[A-Za-z_][A-Za-z0-9_]*" database))
            (throw (ex-info "unsafe or invalid chDB logical database name"
                            {:database database
                             :max-length max-database-name-length
                             :jdbc/sql-error true})))
          database)))))

(defn- open-spec! [spec]
  ;; Validate the identifier before loading libchdb or claiming the process's
  ;; one active storage path.
  (let [database (spec-database spec)
        handle (native/open! (spec-path spec))]
    (try
      (when database
        (let [identifier (str "`" database "`")]
          (execute-any handle (str "CREATE DATABASE IF NOT EXISTS " identifier) [])
          (execute-any handle (str "USE " identifier) [])))
      handle
      (catch Throwable t
        (native/close! handle)
        (throw t)))))

(def chdb-driver
  (reify driver/Driver
    (descriptor [_]
      {:id :chdb
       :aliases #{"chdb"}
       :uri-prefixes ["chdb:"]
       :product-name "ClickHouse (chDB)"
       :capabilities {:transactions :none :generated-keys :none
                      :query-bytes query-bytes-capability}
       :constraints {:active-storage-paths :one-per-process
                     :logical-databases {:spec-key :database
                                         :create-if-missing true
                                         :identifier :ascii-simple}}
       :schema-sql nil})
    (open-handle [_ spec] (open-spec! spec))
    (close-handle [_ handle] (native/close! handle))
    (execute-handle [_ handle sql params] (execute-any handle sql params))

    export/QueryBytesDriver
    (query-bytes-handle [_ handle sql params options]
      (execute-query-bytes-handle handle sql params options))))

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
                  (consume-json-result result))
                (catch Throwable t
                  (when-not @finalized?
                    (native/chdb-stream-cancel-insert stream))
                  (throw t))
                (finally
                  (native/chdb-destroy-insert-stream stream))))
            (finally
              (doseq [ptr (reverse @allocated)] (ffi/free ptr))))))))))
