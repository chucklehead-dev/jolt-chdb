(ns otel.exporter.chdb
  "Direct Jolt OTel exporter for an embedded/in-process chDB database."
  (:require [db.jdbc]
            [db.jdbc-shim :as jdbc-shim]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [jdbc.chdb :as chdb]
            [jdbc.chdb.native :as native]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.json-each-row :as row-encoder]
            [jdbc.chdb.owned-statement :as owned-statement]
            [jdbc.core :as jdbc]
            [jdbc.proto :as jdbc-proto]
            [otel.any-value :as any]
            [otel.context :as context]
            [otel.exporter.chdb.attribute-map-shape :as attribute-map-shape]
            [otel.exporter.chdb.attribute-projection :as attribute-projection]
            [otel.exporter.chdb.compact-format :as compact-format]
            [otel.exporter.chdb.schema :as schema]
            [otel.otlp.any-value :as wire-any]
            [otel.sdk.export :as export]
            [otel.sdk.logs :as logs]))

(def ^:private ^:dynamic *json-backend* :configured)

(def ^:private native-nested-json-writer
  (delay
    (require 'clojure.data.json.jolt-native)
    ;; Retain the Var, not its current function value. Older explicit byte
    ;; dependencies lack this optional adapter and retain the public path.
    (ns-resolve 'clojure.data.json.jolt-native 'write-str)))

(defn- nested-json-string [value]
  (if (= :native-guarded-byte-batch *json-backend*)
    (if-let [write @native-nested-json-writer]
      (write value)
      (json/write-str value))
    (json/write-str value)))

(defn- key-string [k]
  (cond
    (string? k) k
    ;; Preserve the namespace without printing a colon and copying it away.
    (keyword? k) (if-let [prefix (namespace k)]
                   (str prefix "/" (name k))
                   (name k))
    :else (str k)))

(defn- pdata-raw [v]
  (cond
    (any/empty-value? v) nil
    (any/bytes? v) (:bytesValue (wire-any/encode v))
    (map? v) (into (empty v)
                   (map (fn [[key value]] [key (pdata-raw value)]) v))
    (sequential? v) (mapv pdata-raw v)
    :else v))

(defn- canonical-value-string [v]
  (cond
    (any/empty-value? v) ""
    (any/bytes? v) (:bytesValue (wire-any/encode v))
    (or (sequential? v) (map? v)) (json/write-str (pdata-raw v))
    :else (str v)))

(defn- value-string [v]
  ;; Match the collector's pdata Value.AsString for every representable value.
  ;; Canonical special AnyValues are records and must be recognized before the
  ;; generic map branch. Invalid direct-SDK values retain OTel's established
  ;; readable fallback rather than acquiring a different JSON interpretation.
  (if (nil? v)
    ""
    (or (any/try-scalar-string v)
        (let [canonical (any/canonicalize v)]
          (if (:error canonical)
            (pr-str v)
            (canonical-value-string (:value canonical)))))))

(def ^:private original-attribute-key-string key-string)
(def ^:private original-attribute-value-string value-string)

(defn- reusable-string-attrs? [m]
  ;; Only built-in immutable maps: custom maps may have observable traversal.
  ;; The SDK helper fences live canonicalizer/limit changes and oversized
  ;; strings. Merely checking string? would silently bypass that policy.
  ;; This pure native predicate also owns the builtin-map type check; avoid
  ;; repeating reflective class admission and SDK checks for mixed maps.
  (if-some [required (attribute-map-shape/reusable-string-map-required-chars m)]
    (and (identical? key-string original-attribute-key-string)
       (identical? value-string original-attribute-value-string)
       ;; The SDK, not a consumer-load snapshot, owns the default identity.
       ;; A preinstalled replacement may return strings while changing later
       ;; converters, so it must reach the generic path without a trial call.
       (if-some [maximum (any/default-string-value-max-chars)]
         (<= required maximum)
         false))
    false))

(def ^:private small-attribute-transform
  (delay
    (try
      (require 'otel.exporter.chdb.native-attributes)
      ((ns-resolve 'otel.exporter.chdb.native-attributes 'load-transform!)
       #'key-string #'value-string original-attribute-key-string)
      (catch Throwable _
        (throw (ex-info "Native attribute projection is unavailable"
                        {:type ::native-attributes-unavailable}))))))

(def ^:private sdk-scalar-map-converter
  ;; Keep the Var live. Older SDKs simply lack the optional map helper and
  ;; retain the existing native/generic conversion without a dependency floor.
  (delay (ns-resolve 'otel.any-value 'try-scalar-string-map)))

(def ^:private sdk-sorted-scalar-map-converter
  (delay (ns-resolve 'otel.any-value 'try-sorted-scalar-string-map)))

(defn- try-sdk-scalar-attrs [m converter]
  (when (and (identical? key-string original-attribute-key-string)
             (identical? value-string original-attribute-value-string))
    (when-let [convert-var @converter]
      (let [convert @convert-var
            result (convert m)]
        ;; A replaced SDK helper can change exporter policy before returning.
        ;; Do not accept its result past a changed per-entry conversion root.
        (when (and (identical? key-string original-attribute-key-string)
                   (identical? value-string original-attribute-value-string))
          result)))))

(defn- sdk-scalar-attrs [m]
  (or (try-sdk-scalar-attrs m sdk-sorted-scalar-map-converter)
      (try-sdk-scalar-attrs m sdk-scalar-map-converter)))

(def ^:dynamic ^:private *experimental-attribute-transform* nil)

(defn- attrs [m]
  (if (reusable-string-attrs? m)
    ;; Match the old fresh map's metadata without copying immutable entries.
    (with-meta m nil)
    (or (when (= :native-guarded-byte-batch *json-backend*)
          (or (sdk-scalar-attrs m)
              ((or *experimental-attribute-transform* @small-attribute-transform) m)))
        (into {} (map (fn [[k v]] [(key-string k) (value-string v)])) (or m {})))))

(defn- service-name [resource fallback]
  (let [attributes (:attributes resource)]
    (cond
      (contains? attributes "service.name")
      (value-string (get attributes "service.name"))

      (contains? attributes :service.name)
      (value-string (get attributes :service.name))

      :else fallback)))

(def ^:dynamic ^:private *timestamp-wire* :unix-nanos)

(defn- timestamp [nanos]
  ;; Pinned libchdb package 26.7.3 / SQL engine 26.7.2.1 reads JSONEachRow
  ;; integer DateTime64(9) values as raw nanosecond ticks (pre-26.8 semantics).
  ;; 26.9 uses canonical UTC ISO strings instead; both retain the same bounded
  ;; source domain, including epoch zero. Selection belongs to each exporter.
  (when-not (and (integer? nanos) (<= 0 nanos 9223372036854775807))
    (throw (ex-info "Telemetry timestamp exceeds DateTime64 nanosecond domain"
                    {:type ::invalid-timestamp-nanos})))
  (case *timestamp-wire*
    :unix-nanos nanos
    :raw-ticks nanos
    :iso-utc (.toString (java.time.Instant/ofEpochSecond
                         (quot nanos 1000000000) (rem nanos 1000000000)))
    (throw (ex-info "Unsupported telemetry timestamp wire" {:type ::unqualified-timestamp-wire}))))

(defn- metric-timestamp [nanos]
  ;; The pinned ClickStack collector stores metric timestamps as DateTime,
  ;; whose wire value is whole Unix seconds. The pdata zero value remains the
  ;; absent-start sentinel; subsecond precision is intentionally truncated.
  (quot nanos 1000000000))

(defn- trace-state-string [state]
  (cond
    (nil? state) ""
    (string? state) state
    (sequential? state)
    (str/join "," (map (fn [[k v]] (str k "=" v)) state))
    :else (str state)))

(defn- otel-enum-string [value fallback]
  ;; Matches pdata SpanKind.String()/StatusCode.String() used by the pinned
  ;; collector exporter (for example Server, Client, Ok, Error, Unset).
  ;; These finite standard keywords need no intermediate case-converted strings.
  ;; Keep the general name/capitalize path for extensions and other input forms.
  (let [value (or value fallback)]
    (or (when (keyword? value)
          (case value
            :internal "Internal" :server "Server" :client "Client"
            :producer "Producer" :consumer "Consumer"
            :unset "Unset" :ok "Ok" :error "Error"
            nil))
        (str/capitalize (name value)))))

(defn- event-columns
  ([events] (event-columns events false))
  ([events compact?]
   ;; Empty immutable vectors have no row callbacks or seq realization to run.
   ;; Keep generic/lazy inputs on the original traversal and EventsJSON encoding
   ;; in span-row untouched, including live JSONWriter extensions.
   (cond
     (and (vector? events) (empty? events))
     (if compact? [[] [] []]
       {"Events.Timestamp" [] "Events.Name" [] "Events.Attributes" []})

     ;; A builtin one-element vector has the same event in all three passes.
     ;; Keep timestamp, name and attribute conversion in their original order,
     ;; with fresh output vectors but no transient mapv builders/closures.
     ;; Lazy/sequential inputs and larger vectors retain the original passes.
     (and (vector? events) (= 1 (count events)))
     (let [event (nth events 0)
           times [(timestamp (:timestamp-unix-nano event))]
           names [(or (:name event) "")]
           attributes [(attrs (:attributes event))]]
       (if compact? [times names attributes]
         {"Events.Timestamp" times "Events.Name" names "Events.Attributes" attributes}))

     :else
     (let [times (mapv #(timestamp (:timestamp-unix-nano %)) events)
         names (mapv #(or (:name %) "") events)
         attributes (mapv #(attrs (:attributes %)) events)]
     (if compact? [times names attributes]
       {"Events.Timestamp" times "Events.Name" names "Events.Attributes" attributes})))))

(defn- link-columns
  ([links] (link-columns links false))
  ([links compact?]
   (if (and (vector? links) (empty? links))
     (if compact? [[] [] [] []]
       {"Links.TraceId" [] "Links.SpanId" []
        "Links.TraceState" [] "Links.Attributes" []})
     (let [trace-ids (mapv #(or (get-in % [:span-context :trace-id]) "") links)
         span-ids (mapv #(or (get-in % [:span-context :span-id]) "") links)
         states (mapv #(trace-state-string (get-in % [:span-context :trace-state])) links)
         attributes (mapv #(attrs (:attributes %)) links)]
     (if compact? [trace-ids span-ids states attributes]
       {"Links.TraceId" trace-ids "Links.SpanId" span-ids
        "Links.TraceState" states "Links.Attributes" attributes})))))

(defn- uint8 [value]
  ;; pdata values are converted with Go's uint8 cast by the pinned exporter.
  (bit-and (or value 0) 0xff))

(defn- log-timestamp-nanos [record]
  ;; pdata falls back to ObservedTimestamp when Timestamp is its zero value.
  (let [event-time (or (:timestamp-unix-nano record) 0)]
    (if (zero? event-time)
      (or (:observed-time-unix-nano record) 0)
      event-time)))

(defn- span-row
  ([span typed-projector] (span-row span typed-projector false))
  ([span typed-projector compact?]
   (span-row span typed-projector compact? nil))
  ([span typed-projector compact? write-nested-json]
  (let [context (:span-context span)
        scope (:scope span)
        resource (:resource span)
        events (or (:events span) [])
        links (or (:links span) [])
        compact? (and compact? (nil? typed-projector))
        ;; Keep conversion/callback order shared with the named/typed layout.
        time (timestamp (:start-time-unix-nano span))
        trace-id (or (:trace-id context) "") span-id (or (:span-id context) "")
        parent-id (or (:parent-span-id span) "")
        trace-state (trace-state-string (:trace-state context))
        span-name (:name span) kind (otel-enum-string (:kind span) :internal)
        service (service-name resource "unknown_service:jolt")
        resource-attrs (attrs (:attributes resource))
        scope-name (or (:name scope) "") scope-version (or (:version scope) "")
        span-attrs (attrs (:attributes span))
        duration (max 0 (- (:end-time-unix-nano span) (:start-time-unix-nano span)))
        status (otel-enum-string (get-in span [:status :code]) :unset)
        message (or (get-in span [:status :description]) "")
        ;; Nil preserves late Var lookup at EACH original call boundary; do
        ;; not capture the default writer before conversion callbacks run.
        events-json (if write-nested-json (write-nested-json events) (nested-json-string events))
        links-json (if write-nested-json (write-nested-json links) (nested-json-string links))
        common (when-not compact?
                 {"Timestamp" time "TraceId" trace-id "SpanId" span-id
                  "ParentSpanId" parent-id "TraceState" trace-state
                  "SpanName" span-name "SpanKind" kind "ServiceName" service
                  "ResourceAttributes" resource-attrs "ScopeName" scope-name
                  "ScopeVersion" scope-version "SpanAttributes" span-attrs
                  "Duration" duration "StatusCode" status "StatusMessage" message
                  "EventsJSON" events-json "LinksJSON" links-json})
        event-values (if compact? (event-columns events true) (event-columns events))
        link-values (if compact? (link-columns links true) (link-columns links))]
    (if compact?
      [time trace-id span-id parent-id trace-state span-name kind service resource-attrs
       scope-name scope-version span-attrs duration status message
       (nth event-values 0) (nth event-values 1) (nth event-values 2)
       (nth link-values 0) (nth link-values 1) (nth link-values 2) (nth link-values 3)
       events-json links-json]
      (merge common event-values link-values
             (if typed-projector (typed-projector span) {}))))))

(defn- log-row
  ([record typed-projector] (log-row record typed-projector false))
  ([record typed-projector compact?]
   (let [compact? (and compact? (nil? typed-projector))
         scope (:scope record) resource (:resource record)
         time (timestamp (log-timestamp-nanos record))
         trace-id (or (:trace-id record) "") span-id (or (:span-id record) "")
         flags (uint8 (:trace-flags record)) severity (or (:severity-text record) "")
         severity-number (uint8 (:severity-number record))
         ;; Missing log service retains the collector's empty fallback.
         service (service-name resource "") body (value-string (:body record))
         resource-url (or (:schema-url resource) "")
         resource-attrs (attrs (:attributes resource))
         scope-url (or (:schema-url scope) "") scope-name (or (:name scope) "")
         scope-version (or (:version scope) "") scope-attrs (attrs (:attributes scope))
         log-attrs (attrs (:attributes record)) event-name (or (:event-name record) "")]
     (if compact?
       [time trace-id span-id flags severity severity-number service body resource-url
        resource-attrs scope-url scope-name scope-version scope-attrs log-attrs event-name]
       (merge {"Timestamp" time "TraceId" trace-id "SpanId" span-id "TraceFlags" flags
               "SeverityText" severity "SeverityNumber" severity-number
               "ServiceName" service "Body" body "ResourceSchemaUrl" resource-url
               "ResourceAttributes" resource-attrs "ScopeSchemaUrl" scope-url
               "ScopeName" scope-name "ScopeVersion" scope-version
               "ScopeAttributes" scope-attrs "LogAttributes" log-attrs "EventName" event-name}
              (if typed-projector (typed-projector record) {}))))))

(def ^:private log-insert-query
  (str "insert into otel_logs ("
       (str/join ", " schema/clickstack-log-insert-columns)
       ")"))

(defn- selected-log-insert-query [typed-projector]
  ;; A confirmed typed projection adds installer-owned columns beyond the
  ;; pinned compatibility list. The unqualified table insert lets JSONEachRow
  ;; name those additive fields; omitted unrelated table columns keep defaults.
  (if typed-projector "insert into otel_logs" log-insert-query))

(def ^:private max-insert-bytes (* 8 1024 1024))
(def ^:private max-configured-insert-bytes (* 64 1024 1024))
(def ^:dynamic ^:private *insert-byte-limit* nil)

(defn- insert-byte-limit []
  (or *insert-byte-limit* max-insert-bytes))

(defn- insert-limit-exception [limit]
  (ex-info (if (= limit (* 8 1024 1024))
             "chDB telemetry export batch exceeds 8 MiB"
             "chDB telemetry export batch exceeds its configured byte limit")
           {:limit limit}))

(def ^:private untyped-span-shape
  {:name "shape" :start-time-unix-nano 0 :end-time-unix-nano 0
   :events [] :links [] :attributes {} :resource {:attributes {}}})

;; Each accessor is selected once at construction. There is no per-row column
;; name lookup/case dispatch and no materialized outer physical row map.
(def ^:private untyped-span-accessors
  {"Timestamp" #(timestamp (:start-time-unix-nano %))
   "TraceId" #(or (get-in % [:span-context :trace-id]) "")
   "SpanId" #(or (get-in % [:span-context :span-id]) "")
   "ParentSpanId" #(or (:parent-span-id %) "")
   "TraceState" #(trace-state-string (get-in % [:span-context :trace-state]))
   "SpanName" :name
   "SpanKind" #(otel-enum-string (:kind %) :internal)
   "ServiceName" #(service-name (:resource %) "unknown_service:jolt")
   "ResourceAttributes" #(attrs (get-in % [:resource :attributes]))
   "ScopeName" #(or (get-in % [:scope :name]) "")
   "ScopeVersion" #(or (get-in % [:scope :version]) "")
   "SpanAttributes" #(attrs (:attributes %))
   "Duration" #(max 0 (- (:end-time-unix-nano %) (:start-time-unix-nano %)))
   "StatusCode" #(otel-enum-string (get-in % [:status :code]) :unset)
   "StatusMessage" #(or (get-in % [:status :description]) "")
   "EventsJSON" #(json/write-str (or (:events %) []))
   "LinksJSON" #(json/write-str (or (:links %) []))
   "Events.Timestamp" #(mapv (fn [e] (timestamp (:timestamp-unix-nano e))) (or (:events %) []))
   "Events.Name" #(mapv (fn [e] (or (:name e) "")) (or (:events %) []))
   "Events.Attributes" #(mapv (fn [e] (attrs (:attributes e))) (or (:events %) []))
   "Links.TraceId" #(mapv (fn [e] (or (get-in e [:span-context :trace-id]) "")) (or (:links %) []))
   "Links.SpanId" #(mapv (fn [e] (or (get-in e [:span-context :span-id]) "")) (or (:links %) []))
   "Links.TraceState" #(mapv (fn [e] (trace-state-string (get-in e [:span-context :trace-state]))) (or (:links %) []))
   "Links.Attributes" #(mapv (fn [e] (attrs (:attributes e))) (or (:links %) []))})

;; These columns are common enough in trace batches that retaining their
;; already-produced `data.json` bytes within one payload avoids repeatedly
;; rebuilding the same scalar attribute maps.  The cache is deliberately
;; batch-local: it neither retains telemetry after an acknowledged call nor
;; crosses exporter threads.
(def ^:private untyped-span-attribute-accessors
  {"ResourceAttributes" #(get-in % [:resource :attributes])
   "SpanAttributes" :attributes})

(def ^:private untyped-attribute-cache-max-entries 64)
(def ^:private untyped-attribute-cache-max-chars (* 1024 1024))

(def ^:dynamic ^:private *untyped-attribute-wire-cache* nil)

(defn- untyped-attribute-cache []
  {:entries (java.util.HashMap.)
   :chars (java.util.concurrent.atomic.AtomicLong. 0)})

(defn- cached-untyped-attrs-wire [source]
  ;; `data.json` emits map entries in the received map's iteration order.
  ;; Keep that ordered source sequence in the key rather than treating maps
  ;; with equal contents as wire-equivalent.  Cached values are exact output
  ;; from the maintained writer, never a second JSON implementation.
  (let [source (or source {})
        cache *untyped-attribute-wire-cache*]
    (if-not cache
      (json/write-str (attrs source))
      (let [entries (:entries cache)
            key (vec source)
            cached (.get entries key)]
        (if cached
          cached
          (let [encoded (json/write-str (attrs source))
                size (count encoded)
                chars (:chars cache)]
            (when (and (< (.size entries) untyped-attribute-cache-max-entries)
                       (<= (+ (.get chars) size) untyped-attribute-cache-max-chars))
              (.put entries key encoded)
              (.addAndGet chars size))
            encoded))))))

(defn- untyped-scalar? [x] (or (nil? x) (string? x) (boolean? x)
                         (and (integer? x) (<= -9223372036854775808 x 9223372036854775807))))
(defn- untyped-attributes? [x]
  (or (nil? x) (and (map? x) (every? string? (keys x)) (every? untyped-scalar? (vals x)))))
(defn- untyped-span-eligible? [span]
  ;; Narrow, pure shape check. Rejected shapes use the original whole-row path,
  ;; preserving its validation order and errors. No acceptance rules change.
  (and (map? span)
       (every? #(or (nil? %) (map? %)) [(:span-context span) (:scope span) (:resource span) (:status span)])
       (every? untyped-attributes? [(:attributes span) (get-in span [:resource :attributes])])
       (every? #(and (integer? %) (<= 0 % 9223372036854775807))
               [(:start-time-unix-nano span) (:end-time-unix-nano span)])
       (every? #(or (nil? %) (string? %))
               [(:name span) (:parent-span-id span) (get-in span [:span-context :trace-id])
                (get-in span [:span-context :span-id]) (get-in span [:span-context :trace-state])
                (get-in span [:scope :name]) (get-in span [:scope :version])
                (get-in span [:status :description])])
       (every? #(or (nil? %) (keyword? %)) [(:kind span) (get-in span [:status :code])])
       (or (nil? (:links span)) (and (vector? (:links span)) (empty? (:links span))))
       (or (nil? (:events span))
           (and (vector? (:events span))
                (every? #(and (map? %) (integer? (:timestamp-unix-nano %))
                              (<= 0 (:timestamp-unix-nano %) 9223372036854775807)
                              (or (nil? (:name %)) (string? (:name %)))
                              (untyped-attributes? (:attributes %))) (:events span))))))

;; A confirmed typed projector may add physical columns, but it need not force
;; construction of the outer row map. Retain the projector's value/status rules
;; and derive the precise persistent-map iteration order from the same row
;; constructor used by the fallback. If its keys overlap a base column or the
;; shape cannot be compiled, startup keeps the original row-map path.
(defn- compile-untyped-span-encoder
  ([] (compile-untyped-span-encoder nil))
  ([typed-projector]
   (let [order (vec (keys (span-row untyped-span-shape typed-projector)))
        base-columns (set (keys untyped-span-accessors))
        typed-shape (when typed-projector (typed-projector untyped-span-shape))]
    (when (and (= (set order) (into base-columns (keys typed-shape)))
               (empty? (filter base-columns (keys typed-shape))))
      ;; Empty links are still values with live JSONWriter dispatch. Do not
      ;; freeze their bytes when the plan is compiled: later String/Collection
      ;; writers must apply, including to the nested LinksJSON representation.
      (let [slots (mapv (fn [index column]
                          (let [value-at (get untyped-span-accessors column)
                                attribute-at (get untyped-span-attribute-accessors column)
                                write-value
                                (cond
                                  attribute-at
                                  (fn [out span _]
                                    (.write out (cached-untyped-attrs-wire (attribute-at span))))

                                  value-at
                                  (fn [out span _]
                                    (json/write (value-at span) out))

                                  :else
                                  (fn [out _ typed-values]
                                    (json/write (get typed-values column) out)))]
                            [(str (if (zero? index) "{" ",") (json/write-str column) ":")
                             write-value])) (range) order)
            emit (reduce (fn [next [fragment write-value]]
                           (fn [out span typed-values]
                             (.append out fragment)
                             (write-value out span typed-values)
                             (next out span typed-values)))
                         (fn [out _ _] (.append out "}")) (reverse slots))]
        (fn [span]
          (if (untyped-span-eligible? span)
            (let [out (java.io.StringWriter.)
                  typed-values (when typed-projector (typed-projector span))]
              (emit out span typed-values)
              (.toString out))
            (json/write-str (span-row span typed-projector)))))))))

(defn- untyped-span-payload
  ([encoder spans] (untyped-span-payload encoder spans (insert-byte-limit)))
  ([encoder spans limit]
   ;; Do not use mapv/map here: even chunked map can evaluate later rows before
   ;; the current row's limit check. Only request next after acceptance.
   (binding [*untyped-attribute-wire-cache* (untyped-attribute-cache)]
     (loop [remaining limit rows (seq spans) out (StringBuilder.)]
       (if rows
         (let [encoded (encoder (first rows))
               size (inc (alength (.getBytes encoded "UTF-8")))]
           (when (> size remaining)
             (throw (insert-limit-exception limit)))
           (.append out encoded) (.append out "\n")
           (recur (- remaining size) (next rows) out))
         (.toString out))))))


(def ^:private untyped-span-encoder (delay (compile-untyped-span-encoder)))

(defn- compile-typed-span-encoder [projector]
  ;; A nil result is the intentional structural fallback (for example, a
  ;; promoted column collides with a base column). An exception is not an
  ;; eligibility result: fail construction instead of silently disabling the
  ;; fast path. Do not retain the cause, which may contain attribute values.
  (try
    (compile-untyped-span-encoder projector)
    (catch Throwable _
      (throw (ex-info "Typed span encoder compilation failed"
                      {:type ::typed-span-encoder-compilation-failed})))))

(defn- configured-json-each-row-payload
  "Encode one batch as JSONEachRow with the maintained data.json defaults.

  The builder is deliberately local to this call: exporters may run concurrently
  and neither a reusable buffer nor a changed JSON option set is safe at this
  boundary. It retains data.json's per-row string encoding, but eliminates the
  intermediate sequence and final apply/str pass. Each full row is checked
  before it can be appended past the configured payload bound."
  [rows]
  (loop [remaining (insert-byte-limit)
         rows (seq rows)
         out (StringBuilder.)]
    (if-let [row (first rows)]
      (let [encoded (json/write-str row)
            bytes (inc (alength (.getBytes encoded "UTF-8")))]
        (when (> bytes remaining)
          (throw (insert-limit-exception (insert-byte-limit))))
        (.append out encoded)
        (.append out "\n")
        (recur (- remaining bytes) (next rows) out))
      (.toString out))))

(defn- json-each-row-payload
  ([rows] (json-each-row-payload rows nil))
  ([rows prefix] (json-each-row-payload rows prefix false))
  ([rows prefix owned-output?]
   (if (= :configured *json-backend*)
    (let [payload (configured-json-each-row-payload rows)]
      (if (nil? prefix) payload (str prefix payload)))
    ;; A context belongs to this payload, not the exporter: independent SDK
    ;; signals may serialize concurrently. Preserve incremental row effects.
    (let [encoder (row-encoder/open-encoder
                   {:parallelism 1 :json-backend *json-backend*})]
      (try
        (if (nil? prefix)
          (row-encoder/encode-limited-text! encoder rows (insert-byte-limit))
          (if owned-output?
            (row-encoder/encode-limited-prefixed-statement! encoder prefix rows (insert-byte-limit))
            (row-encoder/encode-limited-prefixed-text! encoder prefix rows (insert-byte-limit))))
        (catch clojure.lang.ExceptionInfo e
          (if (= :jdbc.chdb.json-each-row/output-limit (:type (ex-data e)))
            (throw (insert-limit-exception (insert-byte-limit)))
            (throw e)))
        (finally (row-encoder/close! encoder)))))))

(defn- compact-columns! [columns]
  (when-not (and (vector? columns) (seq columns)
                 (every? string? columns)
                 (= (count columns) (count (set columns))))
    (throw (ex-info "Invalid compact telemetry column plan"
                    {:type ::invalid-compact-columns})))
  columns)

(defn- compact-rows [columns rows]
  (compact-columns! columns)
  (let [size (count columns) missing (Object.)]
    (letfn [(step [rows]
              (lazy-seq
                (when-let [rows (seq rows)]
                  (let [row (first rows)]
                    ;; Do not silently discard unknown fields or fill missing
                    ;; fields with null: positional input must match its plan.
                    (when-not (and (map? row) (= size (count row)))
                      (throw (ex-info "Invalid compact telemetry row"
                                      {:type ::invalid-compact-row})))
                    (cons
                     (mapv (fn [column]
                             (let [value (get row column missing)]
                               (when (identical? value missing)
                                 (throw (ex-info "Invalid compact telemetry row"
                                                 {:type ::invalid-compact-row})))
                               value)) columns)
                     ;; Only request the next input row after the current
                     ;; encoded row has passed the serial UTF-8 budget check.
                     (lazy-seq (step (next rows))))))))]
      (step rows))))

(defn- insert-payload
  ([format columns rows]
   (json-each-row-payload
    (if (= :json-compact-each-row format) (compact-rows columns rows) rows)))
  ([format columns rows prefix]
   (json-each-row-payload
    (if (= :json-compact-each-row format) (compact-rows columns rows) rows) prefix)))

(defn- timestamp-insert-query [query]
  ;; Query-local, so the SAME setting is persisted in each exact SQL WAL line.
  ;; Never mutate shared connection settings or rely on replay session state.
  (if (= :raw-ticks *timestamp-wire*)
    (str query " SETTINGS input_format_read_datetime_number_as_raw_value=1")
    query))

(defn- compact-insert-query [table columns]
  (compact-columns! columns)
  (when-not (contains? #{"otel_traces" "otel_logs" "otel_metrics_gauge"
                         "otel_metrics_sum" "otel_metrics_histogram"} table)
    (throw (ex-info "Invalid compact telemetry table"
                    {:type ::invalid-compact-table})))
  (timestamp-insert-query
   (str "insert into " table " ("
        (str/join ", " (map #(str "`" (str/replace % "`" "``") "`") columns)) ")")))

(defn- insert-format-name [format]
  (if (= :json-compact-each-row format) "JSONCompactEachRow" "JSONEachRow"))

(defn- insert-ordinary-payload! [connection table columns format payload]
  (if (= :json-compact-each-row format)
    (jdbc/execute! connection
                   (str (compact-insert-query table columns)
                        " FORMAT JSONCompactEachRow\n" payload))
    (chdb/insert-json-rows! connection table columns payload)))

(defn- insert-json-rows!
  "Insert one SDK-bounded batch through chDB's ordinary query API. libchdb
  26.7's streaming-insert API corrupts ClickHouse ThreadStatus nesting under
  long-lived multi-signal exporters; the query API does not share that path."
  [connection query rows]
  (let [payload (json-each-row-payload rows)]
    (context/with-instrumentation-suppressed
      (jdbc/execute! connection (str (timestamp-insert-query query) " FORMAT JSONEachRow\n" payload)))))

(defn- valid-json-value? [value]
  (cond
    (or (nil? value) (string? value) (boolean? value)) true
    (integer? value) (<= -9223372036854775808 value 9223372036854775807)
    (float? value) (and (= value value)
                        (<= -1.7976931348623157E308 value 1.7976931348623157E308))
    (map? value) (and (every? string? (keys value))
                     (every? valid-json-value? (vals value)))
    (sequential? value) (every? valid-json-value? value)
    :else false))

(defn- uint64? [value]
  (and (integer? value) (<= 0 value 18446744073709551615N)))

(defn- valid-timestamp-value? [value]
  (case *timestamp-wire*
    :unix-nanos (and (integer? value) (<= 0 value 9223372036854775807))
    :raw-ticks (and (integer? value) (<= 0 value 9223372036854775807))
    :iso-utc (and (string? value)
                 (boolean (re-matches #"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?Z" value))
                 (try
                   (let [instant (java.time.Instant/parse value)
                         seconds (.getEpochSecond instant)
                         nanos (.getNano instant)]
                     ;; Some host parsers normalize invalid calendar dates.
                     ;; Accept only the canonical string our producer emits.
                     (and (= value (.toString instant))
                          (<= 0 seconds 9223372036)
                          (or (< seconds 9223372036) (<= nanos 854775807))))
                   (catch Throwable _ false)))
    false))

(defn- valid-row-value? [column value]
  ;; These are exporter-owned physical UInt64 domains, not promoted Int64
  ;; attributes. Preserve the full integer domain supported by JSONEachRow.
  (case column
    "Timestamp" (valid-timestamp-value? value)
    "Events.Timestamp" (and (sequential? value)
                            (every? valid-timestamp-value? value))
    "Duration" (uint64? value)
    "Count" (uint64? value)
    "BucketCounts" (and (sequential? value) (every? uint64? value))
    (valid-json-value? value)))

(declare validate-ordinary-row!)

(defn- ordinary-payload
  ([columns rows] (ordinary-payload columns rows :json-each-row))
  ([columns rows format]
   ;; Check every row before encoding or entering the driver. Error data never
   ;; retains row values, attribute names, or the encoded telemetry payload.
   (let [rows (vec rows)]
     (doseq [row rows]
       (validate-ordinary-row! columns row))
     ;; The batch-local StringBuilder checks each complete maintained data.json
     ;; row plus LF in UTF-8 before appending, preserving the pre-overflow
     ;; rejection boundary while avoiding chunk sequencing/final concatenation.
     (insert-payload format columns rows))))

(defn- jolt-runtime?
  "The direct log writer is a Jolt-only implementation detail.

  JVM Clojure and Babashka keep the maintained map/data.json path even though
  both happen to provide StringBuilder.  The property is set by Jolt itself;
  checking it before forcing the delayed encoder keeps this a capability gate,
  not a host-specific change in JSON behavior."
  []
  (string? (System/getProperty "jolt.version")))

(def ^:private untyped-log-shape
  {:timestamp-unix-nano 0 :observed-time-unix-nano 0
   :attributes {} :resource {:attributes {}} :scope {:attributes {}}})

;; Accessors are selected once when the closed ClickStack log schema is
;; confirmed below.  The accepted path does not construct an intermediate
;; physical row map; unknown SDK shapes retain `log-row` and data.json.
(def ^:private untyped-log-accessors
  {"Timestamp" #(timestamp (log-timestamp-nanos %))
   "TraceId" #(or (:trace-id %) "")
   "SpanId" #(or (:span-id %) "")
   "TraceFlags" #(uint8 (:trace-flags %))
   "SeverityText" #(or (:severity-text %) "")
   "SeverityNumber" #(uint8 (:severity-number %))
   "ServiceName" #(service-name (:resource %) "")
   "Body" #(value-string (:body %))
   "ResourceSchemaUrl" #(or (get-in % [:resource :schema-url]) "")
   "ResourceAttributes" #(attrs (get-in % [:resource :attributes]))
   "ScopeSchemaUrl" #(or (get-in % [:scope :schema-url]) "")
   "ScopeName" #(or (get-in % [:scope :name]) "")
   "ScopeVersion" #(or (get-in % [:scope :version]) "")
   "ScopeAttributes" #(attrs (get-in % [:scope :attributes]))
   "LogAttributes" #(attrs (:attributes %))
   "EventName" #(or (:event-name %) "")})

;; Attribute maps recur heavily in collector log batches: resource and scope
;; maps are normally shared by every record, and application log attributes
;; often repeat too.  Preserve data.json as the only JSON authority, but keep
;; its exact already-produced wire text in a bounded cache for this one
;; payload.  In particular, map equality is not sufficient: data.json follows
;; the supplied map's iteration order, so the ordered entry vector is part of
;; the key.  The dynamic binding below gives the cache request-local lifetime;
;; it cannot retain telemetry across exporter calls or threads.
(def ^:private untyped-log-attribute-accessors
  {"ResourceAttributes" #(get-in % [:resource :attributes])
   "ScopeAttributes" #(get-in % [:scope :attributes])
   "LogAttributes" :attributes})

(def ^:private untyped-log-attribute-cache-max-entries 64)
(def ^:private untyped-log-attribute-cache-max-chars (* 1024 1024))

(def ^:dynamic ^:private *untyped-log-attribute-wire-cache* nil)

(defn- untyped-log-attribute-cache []
  {:entries (java.util.HashMap.)
   :chars (java.util.concurrent.atomic.AtomicLong. 0)})

(defn- utf8-byte-count [text]
  ;; This is deliberately byte based rather than `(count text)`: Jolt's
  ;; strings are not an UTF-8 byte container.  The direct batch renderer uses
  ;; this only for data.json-owned attribute wires; its scalar subset is ASCII
  ;; and has constant-time byte counts below.  Keeping this authority here
  ;; makes a future native byte counter an isolated substitution, not a wire
  ;; semantic change.
  (alength (.getBytes text "UTF-8")))

(defn- cached-untyped-log-attrs-entry [source]
  (let [source (or source {})
        cache *untyped-log-attribute-wire-cache*]
    (if-not cache
      (let [wire (json/write-str (attrs source))]
        {:wire wire :utf8-bytes (utf8-byte-count wire)})
      (let [entries (:entries cache)
            ;; Do not use `source` itself here: associative equality erases
            ;; insertion order, while JSON bytes must preserve it.
            key (vec source)
            cached (.get entries key)]
        (if cached
          cached
          (let [encoded (json/write-str (attrs source))
                size (count encoded)
                entry {:wire encoded :utf8-bytes (utf8-byte-count encoded)}
                chars (:chars cache)]
            (when (and (< (.size entries) untyped-log-attribute-cache-max-entries)
                       (<= (+ (.get chars) size) untyped-log-attribute-cache-max-chars))
              (.put entries key entry)
              (.addAndGet chars size))
            entry))))))

(defn- cached-untyped-log-attrs-wire [source]
  (:wire (cached-untyped-log-attrs-entry source)))

(defn- plain-json-string? [value]
  ;; data.json's default writer has the exact escaping authority.  This tiny
  ;; direct subset deliberately excludes every escaping/Unicode boundary,
  ;; including slash, so all such values continue through data.json.
  (and (string? value)
       (loop [index 0]
         (if (= index (.length value))
           true
           (let [code (int (.charAt value index))]
             (if (and (<= 32 code 126)
                      (not= code 34) (not= code 47) (not= code 92))
               (recur (inc index))
               false))))))

(defn- append-direct-log-scalar! [^StringBuilder out value]
  ;; Do not create a second general JSON encoder.  Dynamic attributes and any
  ;; string that needs escaping stay on the maintained public writer.
  (cond
    (nil? value) (.append out "null")
    (true? value) (.append out "true")
    (false? value) (.append out "false")
    (integer? value) (.append out (str value))
    (plain-json-string? value) (do (.append out (char 34))
                                  (.append out value)
                                  (.append out (char 34)))
    :else (json/write value out)))

(defn- batch-direct-log-scalar-bytes
  "Return an exact UTF-8 byte count for the direct scalar subset, or nil.
  Its strings are ASCII by admission and JSON numbers are ASCII by grammar."
  [value]
  ;; This is not a general replacement for valid-row-value?. The closed log
  ;; accessors produce only checked Timestamp, UInt8 flags/severity, and
  ;; strings for non-attribute columns; attributes retain attrs/data.json.
  ;; A new accessor/value domain must update the admission proof and tests or
  ;; return nil here so the entire batch takes the generic path.
  (cond
    (nil? value) 4
    (true? value) 4
    (false? value) 5
    (integer? value) (count (str value))
    (plain-json-string? value) (+ 2 (count value))
    :else nil))

(defn- append-batch-direct-log-scalar! [^StringBuilder out value]
  (cond
    (nil? value) (.append out "null")
    (true? value) (.append out "true")
    (false? value) (.append out "false")
    (integer? value) (.append out (str value))
    :else (do (.append out (char 34)) (.append out value) (.append out (char 34)))))

(defn- untyped-log-eligible? [record]
  ;; Pure, conservative shape admission.  In particular, maps are accepted
  ;; only where `attrs` has its normal input; unusual application values take
  ;; the old row-construction route and therefore preserve its error/order.
  (and (map? record)
       (every? #(or (nil? %) (map? %)) [(:resource record) (:scope record)])
       (every? #(or (nil? %) (map? %))
               [(:attributes record)
                (get-in record [:resource :attributes])
                (get-in record [:scope :attributes])])
       (every? #(or (nil? %) (and (integer? %) (<= 0 % 9223372036854775807)))
               [(:timestamp-unix-nano record) (:observed-time-unix-nano record)])
       (every? #(or (nil? %) (string? %))
               [(:trace-id record) (:span-id record) (:severity-text record)
                (:event-name record) (get-in record [:resource :schema-url])
                (get-in record [:scope :schema-url]) (get-in record [:scope :name])
                (get-in record [:scope :version])])
       (every? #(or (nil? %) (integer? %))
               [(:trace-flags record) (:severity-number record)])))

(defn- log-key-fragments [order]
  (mapv (fn [index column]
          (str (if (zero? index) "{" ",") (json/write-str column) ":"))
        (range) order))

(defn- validate-ordinary-row! [columns row]
  (let [expected (set columns)]
    (when-not (and (map? row) (= expected (set (keys row)))
                   (every? string? (keys row))
                   (every? (fn [[column value]] (valid-row-value? column value)) row))
      (throw (ex-info "Invalid chDB telemetry row"
                      {:type ::invalid-ordinary-row}))))
  row)

(defn- compile-untyped-log-encoder []
  ;; Set equality is the schema fence; the legacy map construction order is the
  ;; byte-parity oracle. JSONEachRow is name-addressed, so insert-column order
  ;; need not equal data.json's map iteration order. A field addition/removal
  ;; returns nil and selects the generic path until consciously updated.
  (when (jolt-runtime?)
    (let [order (vec (keys (log-row untyped-log-shape nil)))]
      (when (= (set order) (set schema/clickstack-log-insert-columns))
        (when (= (set order) (set (keys untyped-log-accessors)))
          (let [fragments (log-key-fragments order)]
            (fn [record]
              (if-not (untyped-log-eligible? record)
                ;; Defensive private-call behavior only. Public log export
                ;; tests eligibility before invoking this row encoder and
                ;; sends a rejected whole batch through insert-batch!, which
                ;; intentionally has different Durable/ordinary validation.
                (let [row (log-row record nil)]
                  (validate-ordinary-row! order row)
                  (json/write-str row))
                (let [out (StringBuilder.)]
                  (loop [columns order fragments fragments]
                    (when-let [column (first columns)]
                      (let [attribute-at (get untyped-log-attribute-accessors column)
                            value (if attribute-at
                                    (cached-untyped-log-attrs-wire (attribute-at record))
                                    ((get untyped-log-accessors column) record))]
                        ;; Attribute wires come directly from `(attrs source)`
                        ;; and data.json, therefore represent the same valid
                        ;; Map(String,String) value the old accessor supplied.
                        ;; Other columns retain the existing explicit check.
                        (when (and (not attribute-at)
                                   (not (valid-row-value? column value)))
                          (throw (ex-info "Invalid chDB telemetry row"
                                          {:type ::invalid-ordinary-row})))
                        (.append out (first fragments))
                        (if attribute-at
                          (.append out value)
                          (append-direct-log-scalar! out value))
                        (recur (next columns) (next fragments)))))
                  (.append out \})
                  (.toString out))))))))))

(def ^:private untyped-log-encoder (delay (compile-untyped-log-encoder)))

(def ^:private generic-untyped-log-payload ::generic-untyped-log-payload)

(defn- compile-untyped-log-batch-encoder []
  ;; The row encoder above has an intentionally broader admitted subset: it
  ;; can delegate one scalar to data.json and then check the completed row.
  ;; This batch form has a stricter admission rule because it must know every
  ;; record's UTF-8 size *before* looking at the next record, without turning
  ;; each row into a final String.  Escaped/structured scalar values therefore
  ;; select the existing all-generic path for the whole batch.
  (when (jolt-runtime?)
    (let [order (vec (keys (log-row untyped-log-shape nil)))]
      (when (and (= (set order) (set schema/clickstack-log-insert-columns))
                 (= (set order) (set (keys untyped-log-accessors))))
        (let [fragments (log-key-fragments order)
              fragment-bytes (mapv utf8-byte-count fragments)
              emit (fn [records limit]
                     (loop [remaining limit
                   records (seq records)
                   out (StringBuilder.)]
              ;; Do not use `if-let`: false/nil raw records must choose the
              ;; generic route rather than ending the batch early.
              (if records
                (let [record (first records)]
                  (if-not (untyped-log-eligible? record)
                    generic-untyped-log-payload
                    (let [row-start-bytes 2 ; closing brace plus LF
                          row-bytes (volatile! row-start-bytes)
                          direct? (loop [columns order
                                         fragments fragments
                                         fragment-bytes fragment-bytes]
                                    (if-let [column (first columns)]
                                      (let [attribute-at (get untyped-log-attribute-accessors column)
                                            value (if attribute-at
                                                    (cached-untyped-log-attrs-entry
                                                     (attribute-at record))
                                                    ((get untyped-log-accessors column) record))]
                                        ;; No StringBuilder mutation happens
                                        ;; before the scalar's direct-admission
                                        ;; decision. A rejected field returns
                                        ;; the generic sentinel, and the
                                        ;; request-local builder is discarded.
                                        (if attribute-at
                                          (do (.append out (first fragments))
                                              (.append out (:wire value))
                                              (vswap! row-bytes + (first fragment-bytes)
                                                      (:utf8-bytes value))
                                              (recur (next columns) (next fragments)
                                                     (next fragment-bytes)))
                                          (let [scalar-bytes
                                                (batch-direct-log-scalar-bytes value)]
                                            (if (nil? scalar-bytes)
                                              false
                                              (do (.append out (first fragments))
                                                  (append-batch-direct-log-scalar! out value)
                                                  (vswap! row-bytes + (first fragment-bytes)
                                                          scalar-bytes)
                                                  (recur (next columns) (next fragments)
                                                         (next fragment-bytes)))))))
                                      true))]
                      (if-not direct?
                        generic-untyped-log-payload
                        (let [bytes @row-bytes]
                          ;; This is the pre-next-record boundary. The
                          ;; builder is request-local and has not reached a
                          ;; JDBC/Durable/WAL boundary at this point.
                          (when (> bytes remaining)
                            (throw (insert-limit-exception limit)))
                          (.append out \}) (.append out "\n")
                          (recur (- remaining bytes) (next records) out))))))
                (.toString out))))]
          (fn
            ([records] (emit records (insert-byte-limit)))
            ([records limit] (emit records limit))))))))

(def ^:private untyped-log-batch-encoder (delay (compile-untyped-log-batch-encoder)))

(defn- untyped-log-payload
  "Encode an all-eligible batch incrementally, preserving the
  no-later-record-after-overflow rule.

  A non-eligible raw record returns the generic payload sentinel before it is
  encoded. The caller then restarts the whole batch through the old log-row
  path: Durable retains its historical write semantics (which do not run
  ordinary-row validation), while ordinary transport retains its existing
  whole-batch validation. Do not pre-scan the input: that could observe a
  later record after an earlier direct row would overflow."
  [encoder records]
  ;; The selected encoder is Jolt-only; retain that boundary here as well so a
  ;; direct/private caller cannot accidentally change JVM or BB behavior.
  (binding [*untyped-log-attribute-wire-cache*
            (when (jolt-runtime?) (untyped-log-attribute-cache))]
    (loop [remaining (insert-byte-limit) records (seq records) out (StringBuilder.)]
      ;; Test sequence presence, not the record: raw SDK callers can supply a
      ;; falsey record, which must retain the ordinary `log-row` fallback rather
      ;; than terminating this batch and dropping subsequent records.
      (if records
        (let [record (first records)]
          (if-not (untyped-log-eligible? record)
            generic-untyped-log-payload
            (let [encoded (encoder record)
                  bytes (inc (alength (.getBytes encoded "UTF-8")))]
              (when (> bytes remaining)
                (throw (insert-limit-exception (insert-byte-limit))))
              (.append out encoded) (.append out "\n")
              (recur (- remaining bytes) (next records) out))))
        (.toString out)))))

(declare execute-durable-sql! direct-log-layout? insert-direct-log-batch!)

(defn- insert-batch! [connection state table columns query rows]
  (let [snapshot @state
        format (:insert-format snapshot :json-each-row)
        _ (when (= :json-compact-each-row format)
            (compact-format/require-order! (:compact-plans snapshot) connection table columns))
        query (if (= :json-compact-each-row format)
                (compact-insert-query table columns) (timestamp-insert-query query))]
    (if (:durable? snapshot)
      ;; Durable V1 records the exact materialized SQL. Its execute and
      ;; publication acknowledgement are one writer request: splitting this
      ;; into JDBC execute! plus flush! would let another caller intervene.
      (execute-durable-sql!
       connection
       (if (= :native-guarded-byte-batch *json-backend*)
         ;; The qualified byte collector can materialize the SQL once. Its
         ;; Row-local writer view and payload budget exclude the SQL prefix.
         (insert-payload format columns rows
                         (str query " FORMAT " (insert-format-name format) "\n"))
         (str query " FORMAT " (insert-format-name format) "\n"
              (insert-payload format columns rows))))
      (let [payload (ordinary-payload columns rows format)]
        (context/with-instrumentation-suppressed
          (insert-ordinary-payload! connection table columns format payload))))))

(defn- insert-untyped-log-records!
  "Jolt's closed untyped-log encoder is selected only after its schema fence.

  The generic `insert-batch!` remains the sole path for typed logs and for
  JVM/BB.  The selected Jolt path feeds the same exact JSONEachRow bytes into
  the same ordinary/Durable ownership boundaries; it only avoids physical row
  map construction for conservatively admitted SDK records."
  [connection state records]
  (let [snapshot @state
        typed-projector (:typed-log-projector snapshot)
        ;; This fixed writer owns precisely the untyped collector schema. A
        ;; constructed or mutated exporter state with a different ordered
        ;; column vector must retain insert-batch!'s generic validation and
        ;; query construction rather than pairing its payload with that state.
        encoder (when (and (= :json-each-row (:insert-format snapshot :json-each-row))
                           (not typed-projector)
                           (= (:log-insert-columns snapshot)
                              schema/clickstack-log-insert-columns))
                  @untyped-log-encoder)
        batch-encoder (when encoder @untyped-log-batch-encoder)]
    (cond
      (direct-log-layout? snapshot)
      (insert-direct-log-batch! connection state records)

      (not encoder)
      (insert-batch! connection state "otel_logs"
                     (:log-insert-columns snapshot)
                     (selected-log-insert-query typed-projector)
                     (map #(log-row % typed-projector) records))
      :else
      (let [payload (if batch-encoder
                      (binding [*untyped-log-attribute-wire-cache*
                                (untyped-log-attribute-cache)]
                        (batch-encoder records))
                      (untyped-log-payload encoder records))
            query (selected-log-insert-query nil)]
        (if (= generic-untyped-log-payload payload)
          ;; Keep every non-admitted raw shape on the historical path. In
          ;; particular Durable deliberately serializes this path without the
          ;; ordinary transport's physical-row validation.
          (insert-batch! connection state "otel_logs"
                         (:log-insert-columns snapshot) query
                         (map #(log-row % nil) records))
          (if (:durable? snapshot)
            (execute-durable-sql!
             connection (str (timestamp-insert-query query) " FORMAT JSONEachRow\n" payload))
            (context/with-instrumentation-suppressed
              (chdb/insert-json-rows! connection "otel_logs"
                                      (:log-insert-columns snapshot) payload))))))))

(defn- typed-columns [fields]
  (vec (mapcat (fn [field]
                 [(get-in field [:physical :value-column])
                  (get-in field [:physical :status-column])]) fields)))

(defn- temporality-code [value]
  (case value :delta 1 :cumulative 2 0))

(def ^:private empty-metric-exemplars
  {"Exemplars.FilteredAttributes" []
   "Exemplars.TimeUnix" []
   "Exemplars.Value" []
   "Exemplars.SpanId" []
   "Exemplars.TraceId" []})

(defn- metric-row
  ([resource scope metric point projectors]
   (metric-row resource scope metric point projectors false))
  ([resource scope metric point typed-metric-projectors compact?]
   (let [typed-projector (get typed-metric-projectors (:type metric))
         compact? (and compact? (nil? typed-projector))
         typed-context (when typed-projector {:resource resource :scope scope :point point})
         ;; Share conversions and their effect order between layouts. Compact
         ;; metrics do not need a temporary physical map or string-key lookup.
         ra (attrs (:attributes resource))
         rs (or (:schema-url resource) "")
         sn (or (:name scope) "")
         sv (or (:version scope) "")
         sa (attrs (:attributes scope))
         ss (or (:schema-url scope) "")
         service (service-name resource "")
         mn (:name metric)
         md (or (:description metric) "")
         mu (or (:unit metric) "")
         pa (attrs (:attributes point))
         start (metric-timestamp (or (:start-time-unix-nano point) 0))
         time (metric-timestamp (or (:time-unix-nano point) 0))
         common (when-not compact?
                 {"Exemplars.FilteredAttributes" []
                 "Exemplars.TimeUnix" []
                 "Exemplars.Value" []
                 "Exemplars.SpanId" []
                 "Exemplars.TraceId" []
                 "ResourceAttributes" ra
                 "ResourceSchemaUrl" rs
                 "ScopeName" sn
                 "ScopeVersion" sv
                 "ScopeAttributes" sa
                 "ScopeDroppedAttrCount" 0
                 "ScopeSchemaUrl" ss
                 "ServiceName" service
                 "MetricName" mn
                 "MetricDescription" md
                 "MetricUnit" mu
                 "Attributes" pa
                 "StartTimeUnix" start
                 "TimeUnix" time
                 "Flags" 0})
         row (case (:type metric)
               :gauge
               (let [value (double (:value point))]
                 (if compact?
                   [ra rs sn sv sa 0 ss service mn md mu pa start time value 0 [] [] [] [] []]
                   (assoc common "Value" value)))
               :sum
               (let [value (double (:value point))
                     temporality (temporality-code (:temporality metric))
                     monotonic (boolean (:monotonic? metric))]
                 (if compact?
                   [ra rs sn sv sa 0 ss service mn md mu pa start time
                    value 0 [] [] [] [] [] temporality monotonic]
                   (assoc common "Value" value "AggregationTemporality" temporality
                                 "IsMonotonic" monotonic)))
               :histogram
               (let [count (:count point) sum (double (:sum point))
                     buckets (:bucket-counts point) bounds (:explicit-bounds metric)
                     minimum (double (or (:min point) 0.0))
                     maximum (double (or (:max point) 0.0))
                     temporality (temporality-code (:temporality metric))]
                 (if compact?
                   [ra rs sn sv sa 0 ss service mn md mu pa start time
                    count sum buckets bounds [] [] [] [] [] 0 minimum maximum temporality]
                   (assoc common "Count" count "Sum" sum "BucketCounts" buckets
                                 "ExplicitBounds" bounds "Min" minimum "Max" maximum
                                 "AggregationTemporality" temporality))))]
     ;; A confirmed projector still runs once, after all ordinary field
     ;; conversions, and retains the original last-wins merge precedence.
     (if typed-projector (merge row (typed-projector typed-context)) row))))

(defn- metric-rows
  ([resource collected] (metric-rows resource collected nil))
  ([resource collected typed-metric-projectors]
   (for [{:keys [scope metrics]} collected
         metric metrics
         point (:data-points metric)]
     (metric-row resource scope metric point typed-metric-projectors))))

(defn- metric-insert-columns [state type]
  (into (get schema/clickstack-metric-insert-columns type)
        (get-in @state [:typed-metric-columns type] [])))

(defn- metric-insert-query [type columns]
  (str "insert into " (get schema/metric-table-names type)
       " (" (str/join ", " columns) ")"))

(defn- metric-row-groups
  ([resource collected projectors] (metric-row-groups resource collected projectors false))
  ([resource collected projectors compact?]
  ;; Construct/project each point once in input order. Routing is outside the
  ;; wire map: do not assoc/dissoc a temporary field on every persistent row.
  (reduce
    (fn [groups {:keys [scope metrics]}]
      (reduce
        (fn [groups metric]
          (reduce
            (fn [groups point]
              (let [row (metric-row resource scope metric point projectors compact?)
                    type (:type metric)]
                (assoc groups type (conj (get groups type []) row))))
            groups (:data-points metric)))
        groups metrics))
    {} collected)))

(def ^:private direct-metric-columns
  ;; This is the positional program implemented by metric-row, independent
  ;; of the configurable schema plan. A reordered future plan must decline.
  (let [common ["ResourceAttributes" "ResourceSchemaUrl" "ScopeName" "ScopeVersion"
                "ScopeAttributes" "ScopeDroppedAttrCount" "ScopeSchemaUrl" "ServiceName"
                "MetricName" "MetricDescription" "MetricUnit" "Attributes"
                "StartTimeUnix" "TimeUnix"]
        exemplars ["Exemplars.FilteredAttributes" "Exemplars.TimeUnix" "Exemplars.Value"
                   "Exemplars.SpanId" "Exemplars.TraceId"]]
    {:gauge (vec (concat common ["Value" "Flags"] exemplars))
     :sum (vec (concat common ["Value" "Flags"] exemplars
                       ["AggregationTemporality" "IsMonotonic"]))
     :histogram (vec (concat common ["Count" "Sum" "BucketCounts" "ExplicitBounds"]
                             exemplars ["Flags" "Min" "Max" "AggregationTemporality"]))}))

(defn- load-experimental-span-codec! []
  (try
    (require 'jolt.codec.experimental)
    (let [open (ns-resolve 'jolt.codec.experimental 'open-codec)
          encode @(ns-resolve 'jolt.codec.experimental 'encode-prefixed-records)
          nested @(ns-resolve 'jolt.codec.experimental 'prepare-nested-text)
          codec (open)
          folder ((ns-resolve 'jolt.codec.experimental 'native-map-folder) codec)
          _ (require 'otel.exporter.chdb.native-attributes)
          attributes ((ns-resolve 'otel.exporter.chdb.native-attributes 'load-transform!)
                      #'key-string #'value-string original-attribute-key-string folder)]
      {:encode (fn [prefix records project limit] (encode codec prefix records project limit))
       :nested (fn [value] (nested codec value))
       :folder folder :attributes attributes})
    (catch Throwable _
      (throw (ex-info "Experimental span codec requires its source alias and a qualified runtime"
                      {:type ::experimental-span-codec-unavailable})))))

(defn- direct-metric-layout? [snapshot]
  (and (:durable? snapshot)
       (= :json-compact-each-row (:insert-format snapshot))
       (empty? (:typed-metric-projectors snapshot))
       (every? empty? (vals (:typed-metric-columns snapshot)))
       (= direct-metric-columns schema/clickstack-metric-insert-columns)))

(defn- execute-closed-compact-rows!
  ([connection table columns rows checked-snapshot!]
   (execute-closed-compact-rows! connection table columns rows checked-snapshot! nil nil))
  ([connection table columns rows checked-snapshot! codec project]
  (let [initial-snapshot (checked-snapshot!)]
    (compact-format/require-order! (:compact-plans initial-snapshot) connection table columns)
    (letfn [(checked [remaining]
              (lazy-seq
                (when-let [remaining (seq remaining)]
                  (let [row (first remaining)]
                    (when-not (and (vector? row) (= (count columns) (count row)))
                      (throw (ex-info "Invalid direct compact telemetry row"
                                      {:type ::invalid-compact-row})))
                    (cons row (lazy-seq (checked (next remaining))))))))]
      (let [query (compact-insert-query table columns)
            prefix (str query " FORMAT JSONCompactEachRow\n")
            sql (if codec
                  (let [bytes ((:encode codec) prefix rows
                               (fn [record]
                                 (let [row (project record)]
                                   (when-not (and (vector? row) (= (count columns) (count row)))
                                     (throw (ex-info "Invalid direct compact telemetry row"
                                                     {:type ::invalid-compact-row})))
                                   row))
                               (insert-byte-limit))]
                    ;; Same immutable snapshot and fallback as the existing
                    ;; driver encoder. Never replay input producers on decline.
                    (if-let [statement (owned-statement/try-snapshot bytes)]
                      (owned-statement/precompute-wal statement)
                      (String. bytes "UTF-8")))
                  (if (= :native-guarded-byte-batch *json-backend*)
                  (json-each-row-payload (checked rows) prefix
                                        (boolean (:owned-statement-output? initial-snapshot)))
                  (str prefix (json-each-row-payload (checked rows)))))
            snapshot (checked-snapshot!)]
        ;; Custom conversion/JSON callbacks may have changed private state.
        ;; Do not pair completed positional data with a different live plan.
        (compact-format/require-order! (:compact-plans snapshot) connection table columns)
        (execute-durable-sql! connection sql))))))

(defn- insert-direct-metric-batch! [connection state type columns rows]
  (execute-closed-compact-rows!
    connection (get schema/metric-table-names type) columns rows
    (fn []
      (let [snapshot @state]
        (when-not (and (direct-metric-layout? snapshot)
                       (= columns (get direct-metric-columns type)))
          (throw (ex-info "Direct compact metric plan changed"
                          {:type ::invalid-direct-metric-plan})))
        snapshot))))

(def ^:private direct-span-columns
  ["Timestamp" "TraceId" "SpanId" "ParentSpanId" "TraceState" "SpanName" "SpanKind"
   "ServiceName" "ResourceAttributes" "ScopeName" "ScopeVersion" "SpanAttributes"
   "Duration" "StatusCode" "StatusMessage" "Events.Timestamp" "Events.Name"
   "Events.Attributes" "Links.TraceId" "Links.SpanId" "Links.TraceState"
   "Links.Attributes" "EventsJSON" "LinksJSON"])

(defn- direct-span-layout? [snapshot]
  (and (:durable? snapshot)
       (= :json-compact-each-row (:insert-format snapshot))
       (nil? (:typed-span-projector snapshot))
       (= direct-span-columns (:span-insert-columns snapshot))
       (= direct-span-columns (into schema/clickstack-trace-insert-columns ["EventsJSON" "LinksJSON"]))))

(defn- insert-direct-span-batch! [connection state spans]
  ;; Internal constructed vectors only; arbitrary vectors remain rejected by
  ;; the general compact-map projection. Keep input realization/budget ordering.
  (let [codec (:experimental-span-codec @state)]
   (execute-closed-compact-rows!
    connection "otel_traces" direct-span-columns
    (if codec spans (map #(span-row % nil true) spans))
    (fn []
      (let [snapshot @state]
        (when-not (and (direct-span-layout? snapshot)
                       (identical? codec (:experimental-span-codec snapshot)))
          (throw (ex-info "Direct compact span plan changed"
                          {:type ::invalid-direct-span-plan})))
        snapshot))
    codec (when codec #(span-row % nil true (:nested codec))))))

(defn- direct-typed-span-layout? [snapshot]
  (let [{:keys [map-projector vector-projector columns]} (:typed-span-vector-plan snapshot)]
    (and (= :native-guarded-byte-batch *json-backend*)
         (:durable? snapshot)
         (= :json-compact-each-row (:insert-format snapshot))
         (ifn? vector-projector)
         (vector? columns)
         (<= (count direct-span-columns) (count columns))
         (= direct-span-columns (subvec columns 0 (count direct-span-columns)))
         (= direct-span-columns (into schema/clickstack-trace-insert-columns ["EventsJSON" "LinksJSON"]))
         (identical? map-projector (:typed-span-projector snapshot))
         (= columns (:span-insert-columns snapshot)))))

(defn- insert-direct-typed-span-batch! [connection state spans]
  (let [initial @state
        plan (:typed-span-vector-plan initial)
        columns (:columns plan)
        projector (:vector-projector plan)
        codec (:experimental-span-codec initial)]
    (execute-closed-compact-rows!
      connection "otel_traces" columns
      (if codec spans
       (map (fn [span]
             ;; Preserve span-row's base conversion before typed projection.
             (into (span-row span nil true) (projector span))) spans))
      (fn []
        (let [snapshot @state]
          (when-not (and (direct-typed-span-layout? snapshot)
                         (identical? codec (:experimental-span-codec snapshot))
                         (identical? plan (:typed-span-vector-plan snapshot)))
            (throw (ex-info "Direct typed compact span plan changed"
                            {:type ::invalid-direct-typed-span-plan})))
          snapshot))
      codec (when codec
              (fn [span] (into (span-row span nil true (:nested codec)) (projector span)))))))

(def ^:private direct-log-columns
  ["Timestamp" "TraceId" "SpanId" "TraceFlags" "SeverityText" "SeverityNumber"
   "ServiceName" "Body" "ResourceSchemaUrl" "ResourceAttributes" "ScopeSchemaUrl"
   "ScopeName" "ScopeVersion" "ScopeAttributes" "LogAttributes" "EventName"])

(defn- direct-log-layout? [snapshot]
  (and (:durable? snapshot)
       (= :json-compact-each-row (:insert-format snapshot))
       (nil? (:typed-log-projector snapshot))
       (= direct-log-columns (:log-insert-columns snapshot))
       (= direct-log-columns schema/clickstack-log-insert-columns)))

(defn- insert-direct-log-batch! [connection state records]
  (execute-closed-compact-rows!
    connection "otel_logs" direct-log-columns (map #(log-row % nil true) records)
    (fn []
      (let [snapshot @state]
        (when-not (direct-log-layout? snapshot)
          (throw (ex-info "Direct compact log plan changed"
                          {:type ::invalid-direct-log-plan})))
        snapshot))))

(defn- export-metric-groups!
  ([connection state groups] (export-metric-groups! connection state groups false))
  ([connection state groups direct?]
  (if (:durable? @state)
    (doseq [type [:gauge :sum :histogram]
            :let [selected (get groups type)]
            :when (seq selected)]
      (let [columns (metric-insert-columns state type)]
        (if direct?
          (insert-direct-metric-batch! connection state type columns selected)
          (insert-batch! connection state (get schema/metric-table-names type)
                         columns (metric-insert-query type columns) selected))))
    ;; Eagerly validate and encode every physical batch before the first driver
    ;; call. Native execution failures can still partially apply a logical
    ;; batch: this transport does not promise an atomic transaction/rollback.
    (let [format (:insert-format @state :json-each-row)
          prepared
          (vec (for [type [:gauge :sum :histogram]
                     :let [selected (get groups type)]
                     :when (seq selected)
                     :let [columns (metric-insert-columns state type)
                           payload (ordinary-payload columns selected format)]]
                 [(get schema/metric-table-names type) columns payload]))]
      (when (= :json-compact-each-row format)
        (doseq [[table columns _] prepared]
          (compact-format/require-order! (:compact-plans @state) connection table columns)))
      (context/with-instrumentation-suppressed
        (doseq [[table columns payload] prepared]
          (insert-ordinary-payload! connection table columns format payload)))))))

(defn- export-metric-rows! [connection state rows]
  ;; Compatibility for existing private fixture callers. SDK ingestion uses
  ;; metric-row-groups directly and never adds/removes this temporary field.
  (export-metric-groups!
    connection state
    (reduce (fn [groups row]
              (update groups (:_type row) (fnil conj []) (dissoc row :_type)))
            {} rows)))

(defn- admit-signal! [owned? expected-signals state signal]
  ;; Admission and shutdown's signal fence must linearize on this same atom.
  ;; A prior open? read cannot protect an in-progress native writer request.
  (let [undeclared? (and owned? (not (contains? expected-signals signal)))
        [old new]
        (swap-vals! state
                    (fn [snapshot]
                      (cond
                        undeclared?
                        (assoc snapshot :last-error
                               (ex-info (str "OTel signal is not enabled for this exporter: "
                                             (name signal))
                                        {:signal signal :expected-signals expected-signals}))
                        (contains? (:closed-signals snapshot) signal) snapshot
                        :else (-> snapshot
                                  (update :in-flight (fnil inc 0))
                                  (update-in [:in-flight-by-signal signal] (fnil inc 0))))))]
    (and (not undeclared?)
         (= (inc (or (:in-flight old) 0)) (:in-flight new)))))

(def ^:private confirmed-durable-statuses #{:committed :reconciled})

(def ^:private durable-phase-names
  [:payload-built :atomic-execute-and-flush-returned])

(defn durable-phase-receipts
  "Create an opt-in, caller-owned aggregate sink for the acknowledged untyped
  Durable span path. Supply the returned atom as :durable-phase-receipts to
  `exporter`. `:atomic-execute-and-flush-returned` is one combined chDB writer
  boundary, not invented per-native-execute and per-publication timings. Each
  phase contains only a completed count, total elapsed nanoseconds, and span
  count; payloads, rows, and attribute values are never retained. Omit the
  option (the default) to avoid timing and aggregation."
  []
  (atom (zipmap durable-phase-names
                (repeat {:count 0 :nanos 0 :spans 0}))))

(defn- record-durable-phase! [receipts phase elapsed-nanos span-count]
  ;; Diagnostics must not change the acknowledgement result, including when a
  ;; caller installs a problematic atom watch. The sink receives aggregates
  ;; only, never payload text or SDK values.
  (when receipts
    (try
      (swap! receipts
             (fn [snapshot]
               (update snapshot phase
                       (fn [current]
                         (let [current (or current {:count 0 :nanos 0 :spans 0})]
                           {:count (inc (:count current))
                            :nanos (+ (:nanos current) elapsed-nanos)
                            :spans (+ (:spans current) span-count)})))))
      (catch Throwable _ nil))))

(defn- execute-durable-sql! [connection sql]
  (let [result
        (context/with-instrumentation-suppressed
          (if (string? sql)
            (durable/execute-and-flush! connection sql)
            (durable/execute-owned-and-flush! connection sql)))]
    (when-not (contains? confirmed-durable-statuses (:status result))
      (throw (ex-info "Durable atomic execution did not confirm publication"
                      {:type ::durable-atomic-execution-unconfirmed
                       :status (:status result)})))
    result))

(defn- persistence-barrier! [connection state publication-required?]
  (when-let [barrier (:persistence-barrier @state)]
    (let [result
          (context/with-instrumentation-suppressed
            (barrier connection))]
      (when-not result
        (throw (ex-info "Persistence barrier did not confirm completion"
                        {:type ::persistence-barrier-unconfirmed})))
      (when (and publication-required?
                 (:durable? @state)
                 (not (contains? confirmed-durable-statuses (:status result))))
        (throw (ex-info "Durable barrier did not confirm publication"
                        {:type ::durable-barrier-unconfirmed
                         :status (:status result)})))
      result)))

(defn- complete-batch!
  ([connection state wrote?]
   (when (and wrote? (not (:durable? @state)))
     (persistence-barrier! connection state true))
   true)
  ([connection state wrote? receipts span-count]
   (when (and wrote? (not (:durable? @state)))
     (let [started (System/nanoTime)]
       (persistence-barrier! connection state true)
       (record-durable-phase! receipts :atomic-execute-and-flush-returned
                              (- (System/nanoTime) started) span-count)))
   true))

(defn- complete-owned-close! [connection state]
  (let [result
        (try
          (.close connection)
          (swap! state assoc
                 :connection-closed? true
                 :connection-close-status :closed
                 :connection-close-result true)
          true
          (catch Throwable error
            ;; A partly closed native handle is terminal; never retry it.
            (swap! state assoc
                   :connection-closed? false
                   :connection-close-status :failed
                   :connection-close-result false
                   :connection-close-error error
                   :last-error error)
            false))]
    (deliver (:connection-close-completion @state) result)
    result))

(defn- release-signal! [connection owned? state signal]
  (let [[old new]
        (swap-vals! state
                    (fn [snapshot]
                      (let [next (-> snapshot
                                     (update :in-flight dec)
                                     (update-in [:in-flight-by-signal signal] dec))]
                        (if (and owned?
                                 (zero? (:in-flight next))
                                 (:connection-close-claimed? next)
                                 (not (:connection-close-started? next)))
                          (assoc next :connection-close-started? true)
                          next))))]
    (when (and (contains? (:closed-signals new) signal)
               (pos? (get-in old [:in-flight-by-signal signal] 0))
               (zero? (get-in new [:in-flight-by-signal signal] 0)))
      (deliver (get-in new [:signal-drained signal]) true))
    (when (and (not (:connection-close-started? old))
               (:connection-close-started? new))
      (complete-owned-close! connection state))))

(defn- await-shutdown-completion! [state completion]
  (try
    @completion
    (catch java.lang.InterruptedException error
      ;; The final admitted caller still owns drain and terminal close.
      (swap! state assoc :last-error error)
      ;; Promise deref clears Java's interrupted status when throwing. Keep
      ;; the caller's cancellation signal intact even though close proceeds.
      (.interrupt (Thread/currentThread))
      false)))

(defn- close-signal! [connection owned? expected-signals state signal]
  ;; The last expected signal fences new admissions atomically. An admitted
  ;; caller owns its connection use through its finally/release transition.
  (let [[old new]
        (swap-vals! state
                    (fn [snapshot]
                      (let [next (if (contains? (:closed-signals snapshot) signal)
                                   snapshot
                                   (-> snapshot
                                       (update :closed-signals conj signal)
                                       (assoc-in [:signal-drained signal] (promise))))]
                        (if (and owned?
                                 (not (:connection-close-claimed? next))
                                 (every? (:closed-signals next) expected-signals))
                          (cond-> (assoc next
                                         :connection-close-claimed? true
                                         :connection-close-status :closing
                                         :connection-close-completion (promise))
                            (zero? (or (:in-flight next) 0))
                            (assoc :connection-close-started? true))
                          next))))
        start-close? (and (not (:connection-close-started? old))
                          (:connection-close-started? new))]
    (when start-close?
      (complete-owned-close! connection state))
    (when (zero? (get-in new [:in-flight-by-signal signal] 0))
      (deliver (get-in new [:signal-drained signal]) true))
    (and (await-shutdown-completion! state (get-in new [:signal-drained signal]))
         (if-let [completion (:connection-close-completion new)]
           (await-shutdown-completion! state completion)
           true))))

(defrecord ChdbExporter [connection owned? expected-signals state]
  export/SpanExporter
  (export-spans! [_ spans]
    (if-not (admit-signal! owned? expected-signals state :spans)
      false
      (try
        (binding [*insert-byte-limit* (:max-insert-bytes @state)
                  *experimental-attribute-transform* (get-in @state [:experimental-span-codec :attributes])
                  *json-backend* (:json-backend @state :configured)
                  *timestamp-wire* (:timestamp-wire @state :unix-nanos)]
        (let [receipts
              (when (seq spans)
                (let [snapshot @state
                      encoder (when (and (:durable? snapshot)
                                         (= :json-each-row (:insert-format snapshot :json-each-row))
                                         (or (nil? (:typed-span-projector snapshot))
                                             (:typed-span-encoder snapshot)))
                                (or (:typed-span-encoder snapshot)
                                    @untyped-span-encoder))
                      receipts (when encoder (:durable-phase-receipts snapshot))]
                  (if encoder
                    (if receipts
                      (let [span-count (count spans)
                            payload-start (System/nanoTime)
                            payload (untyped-span-payload encoder spans)]
                        (record-durable-phase! receipts :payload-built
                                               (- (System/nanoTime) payload-start) span-count)
                        (let [execute-start (System/nanoTime)]
                          (execute-durable-sql!
                           connection
                           (str (timestamp-insert-query "insert into otel_traces") " FORMAT JSONEachRow\n" payload))
                          ;; chDB performs native execution and persistence
                          ;; inside one writer request.  Report that combined
                          ;; boundary; separate timings would be invented.
                          (record-durable-phase!
                           receipts :atomic-execute-and-flush-returned
                           (- (System/nanoTime) execute-start) span-count))
                        receipts)
                      ;; Keep the disabled default path free of clocks,
                      ;; aggregation, or diagnostic callbacks.
                      (let [payload (untyped-span-payload encoder spans)]
                        (execute-durable-sql!
                         connection
                         (str (timestamp-insert-query "insert into otel_traces") " FORMAT JSONEachRow\n" payload))
                        nil))
                    (do
                      (cond
                        (direct-span-layout? snapshot)
                        (insert-direct-span-batch! connection state spans)
                        (direct-typed-span-layout? snapshot)
                        (insert-direct-typed-span-batch! connection state spans)
                        :else
                        (insert-batch! connection state "otel_traces"
                                       (:span-insert-columns @state) "insert into otel_traces"
                                       (map #(span-row % (:typed-span-projector @state)) spans)))
                      nil))))]
          (if receipts
            (complete-batch! connection state true receipts (count spans))
            (complete-batch! connection state (boolean (seq spans))))))
        (catch Throwable e
          (swap! state assoc :last-error e)
          false)
        (finally (release-signal! connection owned? state :spans)))))
  (flush-exporter! [_]
    (if-not (admit-signal! owned? expected-signals state :spans)
      false
      (try
        (persistence-barrier! connection state false)
        true
        (catch Throwable e
          (swap! state assoc :last-error e)
          false)
        (finally (release-signal! connection owned? state :spans)))))
  (shutdown-exporter! [_]
    (close-signal! connection owned? expected-signals state :spans))

  export/MetricExporter
  (export-metrics! [_ resource collected]
    (if-not (admit-signal! owned? expected-signals state :metrics)
      false
      (try
        (binding [*insert-byte-limit* (:max-insert-bytes @state)
                  *json-backend* (:json-backend @state :configured)
                  *timestamp-wire* (:timestamp-wire @state :unix-nanos)]
        (let [snapshot @state
              direct? (direct-metric-layout? snapshot)
              groups (if direct?
                       (metric-row-groups resource collected nil true)
                       (metric-row-groups resource collected (:typed-metric-projectors snapshot)))]
          (if direct?
            (export-metric-groups! connection state groups true)
            (export-metric-groups! connection state groups))
          (complete-batch! connection state (boolean (seq groups)))))
        (catch Throwable e
          (swap! state assoc :last-error e)
          false)
        (finally (release-signal! connection owned? state :metrics)))))
  (shutdown-metric-exporter! [_]
    (close-signal! connection owned? expected-signals state :metrics))

  logs/LogRecordExporter
  (export-logs! [_ records]
    (if-not (admit-signal! owned? expected-signals state :logs)
      false
      (try
        (binding [*insert-byte-limit* (:max-insert-bytes @state)
                  *json-backend* (:json-backend @state :configured)
                  *timestamp-wire* (:timestamp-wire @state :unix-nanos)]
        (when (seq records)
          (insert-untyped-log-records! connection state records))
        (complete-batch! connection state (boolean (seq records))))
        (catch Throwable e
          (swap! state assoc :last-error e)
          false)
        (finally (release-signal! connection owned? state :logs)))))
  (shutdown-log-exporter! [_]
    (close-signal! connection owned? expected-signals state :logs)))

(defn exporter
  "Create a span+log+metric exporter. Supply :connection to share ownership
  with an application, or :db-spec for an exporter-owned one. A chDB map dbspec
  may include :database to create/select an isolated logical database. A shared
  :connection is used in its already selected database; the exporter never
  issues USE or qualifies table names. :signals declares enabled SDK signals so
  an owned connection closes after every pipeline; it defaults to the SDK
  defaults, spans+metrics. Export calls for an undeclared signal fail visibly
  through a false result and last-error. Unless :create-schema? is false,
  startup applies and validates the ordered schema migration registry in that
  selected database. With :durable? true, startup requires a Durable writer and,
  when this exporter creates the schema, checkpoints it; every non-empty
  physical signal insert uses one `execute-and-flush!` writer request and
  returns true only after its WAL publication commits or reconciles.
  :persistence-barrier supplies the same post-batch contract for another
  persistence implementation and is mutually exclusive with :durable?.
  Ordinary connections must expose the chDB driver context; startup rejects
  other drivers before schema mutation. Durable connections must explicitly
  opt into :durable? true; they never fall back from the ordinary row-data API.
  :typed-span-descriptors, :typed-log-descriptors, :typed-gauge-descriptors,
  :typed-sum-descriptors, and :typed-histogram-descriptors accept only opaque
  capabilities returned in active `install-approved!` results. They project
  their respective attributes while the compatible generic maps remain
  unchanged. Gauge descriptors cover point, resource, and scope attributes on
  `otel_metrics_gauge`; sum descriptors cover point, resource, and scope
  attributes on `otel_metrics_sum`; histogram descriptors cover the same three
  locations on `otel_metrics_histogram`. :json-backend defaults to :configured;
  :native-guarded opts only the general JSONEachRow fallback into the qualified
  source-run native writer. It preserves the configured byte bound and persistence
  acknowledgement; specialized codecs are unchanged. An unavailable native
  backend fails before database acquisition, without silent fallback.
  :native-guarded-string-cache and :native-guarded-byte-batch select the
  corresponding qualified row encoder for that same general fallback only.
  Contexts are per payload, never shared by concurrent SDK signals.
  :insert-format defaults to :json-each-row. :json-compact-each-row uses
  schema-ordered arrays and explicit columns with the same serial UTF-8 bound
  and persistence acknowledgement. It rejects missing/extra physical fields;
  custom JSONWriter methods see arrays instead of the former row maps.
  :max-insert-bytes optionally selects a positive integer byte bound up to
  64 MiB per encoded physical insert payload, including row newlines but not
  the SQL prefix. Omitted/nil retains the 8 MiB default.
  Larger limits allow larger batches but consume more memory; they are not
  heap/queue budgets. Other driver/protocol limits still apply independently.
  :owned-statement-output? defaults false. Experimental true requires explicit
  native-byte compact Durable export and the qualified owned snapshot runtime.
  Direct compact routes can avoid whole-SQL String construction; unsupported
  output decodes already encoded bytes without re-running input producers.
  Generic fallback routes retain text. WAL and acknowledgement are unchanged.
  :datetime64-wire defaults :auto: integer nanos on 26.7.3, UTC ISO on 26.9.0.
  Explicit :raw-ticks requires 26.9.0 and Durable; each INSERT carries its
  fixed raw-tick input setting into WAL without a connection/session toggle.
  Explicit :iso-utc selects the 26.9.0 alternative. Unknown choices fail closed.
  :experimental-span-codec? defaults false. True requires the source-only
  :experimental-span-codec alias, spans-only native-byte compact Durable export
  and owned output. Selects an explicit built-in-value codec, NOT JSONWriter
  extensions. Persistence and default encoding are unchanged."
  ([] (exporter {}))
  ([{:keys [connection db-spec create-schema? signals durable?
            persistence-barrier typed-span-descriptors typed-log-descriptors
            typed-gauge-descriptors typed-sum-descriptors typed-histogram-descriptors
            durable-phase-receipts json-backend insert-format owned-statement-output? datetime64-wire
            experimental-span-codec?]
     limit-override :max-insert-bytes
     :or {db-spec "chdb::memory:" create-schema? true
          signals #{:spans :metrics} durable? false json-backend :configured datetime64-wire :auto
          insert-format :json-each-row owned-statement-output? false experimental-span-codec? false}}]
   (when-not (and (boolean? experimental-span-codec?)
                  (or (not experimental-span-codec?)
                      (and durable? owned-statement-output? (= #{:spans} (set signals))
                           (= :native-guarded-byte-batch json-backend)
                           (= :json-compact-each-row insert-format))))
     (throw (ex-info "Experimental span codec requires spans-only owned compact Durable export"
                     {:type ::invalid-experimental-span-codec})))
   (when-not (or (nil? limit-override)
                 (and (integer? limit-override) (pos? limit-override)
                      (<= limit-override max-configured-insert-bytes)))
     (throw (ex-info "Invalid telemetry insert byte limit"
                     {:type ::invalid-insert-byte-limit
                      :minimum 1 :maximum max-configured-insert-bytes})))
   (when-not (and (boolean? owned-statement-output?)
                  (or (not owned-statement-output?)
                      (and durable? (= :native-guarded-byte-batch json-backend)
                           (= :json-compact-each-row insert-format))))
     (throw (ex-info "Owned statement output requires native-byte compact Durable export"
                     {:type ::invalid-owned-statement-output})))
   (when-not (#{:json-each-row :json-compact-each-row} insert-format)
     (throw (ex-info "Unsupported exporter insert format"
                     {:type ::invalid-insert-format})))
   (when-not (#{:configured :native-guarded :native-guarded-string-cache
               :native-guarded-byte-batch} json-backend)
     (throw (ex-info "Unsupported exporter JSON backend"
                     {:type ::invalid-json-backend})))
   ;; Resolve before database acquisition or DDL. Ordinary consumers never load
   ;; the incubating namespace/resources; missing support is not a downgrade.
   (let [experimental-span-codec (when experimental-span-codec? (load-experimental-span-codec!))]
   ;; Resolve an explicitly selected backend before opening a database or DDL.
   ;; Never silently downgrade an unavailable native backend.
   (when (and experimental-span-codec
              (not (try
                     (and (ifn? durable/execute-owned-and-flush!)
                          (owned-statement/statement?
                            (owned-statement/try-snapshot (byte-array 0))))
                     (catch Throwable _ false))))
     (throw (ex-info "Owned statement stack unavailable"
                     {:type ::owned-statement-unavailable})))
   (when (and (not experimental-span-codec) (not= :configured json-backend))
     (let [encoder (row-encoder/open-encoder {:parallelism 1 :json-backend json-backend})]
       (try
         (when (and owned-statement-output?
                    (not (try
                           (and (ifn? (:native-prefixed-byte-writer encoder))
                                (ifn? row-encoder/encode-limited-prefixed-statement!)
                                (ifn? durable/execute-owned-and-flush!)
                                ;; Probe the owned snapshot ABI before storage
                                ;; acquisition, with no SQL or caller payload.
                                (owned-statement/statement?
                                  (owned-statement/try-snapshot (byte-array 0))))
                           (catch Throwable _ false))))
           (throw (ex-info "Owned statement stack unavailable"
                           {:type ::owned-statement-unavailable})))
         (when (= :native-guarded-byte-batch json-backend)
           @small-attribute-transform)
         (finally (row-encoder/close! encoder)))))
   (when (and persistence-barrier (not (ifn? persistence-barrier)))
     (throw (ex-info ":persistence-barrier must be callable"
                     {:type ::invalid-persistence-barrier})))
   (when (and durable? persistence-barrier)
     (throw (ex-info "Choose :durable? or :persistence-barrier, not both"
                     {:type ::ambiguous-persistence-barrier})))
   (when (and durable-phase-receipts (not (atom? durable-phase-receipts)))
     (throw (ex-info ":durable-phase-receipts must be an atom"
                     {:type ::invalid-durable-phase-receipts})))
   (when (and (or typed-span-descriptors typed-log-descriptors typed-gauge-descriptors
                  typed-sum-descriptors typed-histogram-descriptors)
              (nil? connection))
     (throw (ex-info "Typed descriptors require their explicit install connection"
                     {:type ::typed-descriptors-require-connection})))
   (when-not (contains? #{:auto :iso-utc :raw-ticks} datetime64-wire)
     (throw (ex-info "Unsupported DateTime64 wire selection" {:type ::unqualified-timestamp-wire})))
   (let [owned? (nil? connection)
         conn (or connection (jdbc/connection db-spec))
         typed-span-projector (when typed-span-descriptors
                                (attribute-projection/trace-projector
                                 typed-span-descriptors conn))
         typed-span-encoder (when typed-span-projector
                              (compile-typed-span-encoder typed-span-projector))
         typed-log-projector (when typed-log-descriptors
                               (attribute-projection/log-projector
                                typed-log-descriptors conn))
         typed-gauge-projector (when typed-gauge-descriptors
                                 (attribute-projection/gauge-projector
                                  typed-gauge-descriptors conn))
         typed-sum-projector (when typed-sum-descriptors
                               (attribute-projection/sum-projector
                                  typed-sum-descriptors conn))
         typed-histogram-projector (when typed-histogram-descriptors
                                     (attribute-projection/histogram-projector
                                      typed-histogram-descriptors conn))
         span-fields (when typed-span-descriptors
                       (attribute-projection/confirmed-span-fields typed-span-descriptors conn))
         log-fields (when typed-log-descriptors
                      (attribute-projection/confirmed-log-fields typed-log-descriptors conn))
         gauge-fields (when typed-gauge-descriptors
                        (attribute-projection/confirmed-gauge-fields typed-gauge-descriptors conn))
         sum-fields (when typed-sum-descriptors
                      (attribute-projection/confirmed-sum-fields typed-sum-descriptors conn))
         histogram-fields (when typed-histogram-descriptors
                            (attribute-projection/confirmed-histogram-fields typed-histogram-descriptors conn))
         span-columns (into (into schema/clickstack-trace-insert-columns
                                  ["EventsJSON" "LinksJSON"])
                            (typed-columns span-fields))
         log-columns (into schema/clickstack-log-insert-columns
                           (typed-columns log-fields))
         gauge-columns (typed-columns gauge-fields)
         sum-columns (typed-columns sum-fields)
         histogram-columns (typed-columns histogram-fields)
         barrier (if durable? durable/flush! persistence-barrier)]
     (try
       (if durable?
         (when-not (= :writer (durable/connection-role conn))
           (throw (ex-info "Durable telemetry export requires a writer connection"
                           {:type ::durable-writer-required})))
         ;; The ordinary transport is chDB-specific. Validate its public driver
         ;; context before schema mutation; do not infer Durable by catching
         ;; failed probes or retry SQL after a rejected ordinary insertion.
         (jdbc-shim/driver-context (jdbc-proto/connection conn) :chdb))
       ;; Library overrides may satisfy the driver's compatibility minimum yet
       ;; change integer DateTime64 semantics. Fence the actual package before
       ;; any exporter DDL/checkpoint; neither ordinary nor Durable can bypass.
       (native/ensure-loaded!)
       (let [package (native/chdb-version)
             timestamp-wire (case package
                              "26.7.3" (when (= :auto datetime64-wire) :unix-nanos)
                              "26.9.0" (case datetime64-wire
                                         :auto :iso-utc :iso-utc :iso-utc
                                         :raw-ticks (when durable? :raw-ticks))
                              nil)]
       (when-not timestamp-wire
         (throw (ex-info "Unqualified chDB telemetry timestamp wire"
                         {:type ::unqualified-timestamp-wire})))
       (when create-schema? (schema/ensure-schema! conn))
       ;; Confirm before the schema checkpoint/first telemetry acknowledgement.
       ;; Explicit compact selection fails closed, not a silent named fallback.
       ;; Source/descriptor types and the exact explicit SQL order are bound at
       ;; startup under the existing single-schema-owner contract.
       (let [compact-plans
             (when (= :json-compact-each-row insert-format)
               (let [enabled (set signals)
                     targets
                     (cond-> []
                       (contains? enabled :spans)
                       (conj ["otel_traces" span-columns
                              (merge schema/clickstack-trace-insert-types
                                     (compact-format/typed-types span-fields))])
                       (contains? enabled :logs)
                       (conj ["otel_logs" log-columns
                              (merge schema/clickstack-log-insert-types
                                     (compact-format/typed-types log-fields))])
                       (contains? enabled :metrics)
                       (into (mapv (fn [[kind extra fields]]
                                      [(get schema/metric-table-names kind)
                                       (into (get schema/clickstack-metric-insert-columns kind) extra)
                                       (merge (get schema/clickstack-metric-insert-types kind)
                                              (compact-format/typed-types fields))])
                                    [[:gauge gauge-columns gauge-fields]
                                     [:sum sum-columns sum-fields]
                                     [:histogram histogram-columns histogram-fields]])))]
                 (into {} (map (fn [[table columns types]]
                                 [table (compact-format/confirm! conn table columns types)])) targets)))]
       ;; A full checkpoint makes the schema independently recoverable before
       ;; the exporter can acknowledge its first telemetry batch.
       (when (and durable? create-schema?)
         (let [result
               (context/with-instrumentation-suppressed
                 (durable/checkpoint! conn))]
           (when-not (contains? confirmed-durable-statuses (:status result))
             (throw (ex-info "Durable schema checkpoint was not confirmed"
                             {:type ::durable-checkpoint-unconfirmed
                              :status (:status result)})))))
       (->ChdbExporter conn owned? (set signals)
                       (atom {:closed-signals #{}
                              :in-flight 0
                              :in-flight-by-signal {}
                              :signal-drained {}
                              :connection-close-claimed? false
                              :connection-close-status :open
                              :connection-closed? false
                              :persistence-barrier barrier
                              :typed-span-projector typed-span-projector
                              :typed-span-vector-plan
                              (when (and durable? typed-span-descriptors
                                         (= :native-guarded-byte-batch json-backend)
                                         (= :json-compact-each-row insert-format))
                                {:map-projector typed-span-projector
                                 :vector-projector
                                 (if experimental-span-codec
                                   (attribute-projection/trace-vector-projector
                                     typed-span-descriptors conn true (:folder experimental-span-codec))
                                   (attribute-projection/trace-vector-projector
                                     typed-span-descriptors conn true))
                                 :columns span-columns})
                              :typed-span-encoder typed-span-encoder
                              :typed-log-projector typed-log-projector
                              :typed-metric-projectors
                              (cond-> {}
                                typed-gauge-projector (assoc :gauge typed-gauge-projector)
                                typed-sum-projector (assoc :sum typed-sum-projector)
                                typed-histogram-projector (assoc :histogram typed-histogram-projector))
                              :span-insert-columns span-columns
                              :log-insert-columns log-columns
                              :typed-metric-columns {:gauge gauge-columns
                                                     :sum sum-columns
                                                     :histogram histogram-columns}
                              :durable? durable?
                              :json-backend json-backend
                              :experimental-span-codec experimental-span-codec
                              :max-insert-bytes limit-override
                              :timestamp-wire timestamp-wire
                              :owned-statement-output? owned-statement-output?
                              :insert-format insert-format
                              :compact-plans compact-plans
                              :durable-phase-receipts durable-phase-receipts
                              :last-error nil}))))
       (catch Throwable t
         ;; A failed ownership cleanup must not replace the startup failure.
         (when owned? (try (.close conn) (catch Throwable _ nil)))
         (throw t)))))))

(defn last-error [exporter] (:last-error @(:state exporter)))
