(ns jdbc.chdb.json-each-row
  "Caller-owned, bounded JSONEachRow encoding. This is only an encoder: it does
  not admit a Durable mutation, write a WAL, or confirm persistence."
  (:require #?(:bb [cheshire.core :as bb-json]
               :jolt [clojure.data.json :as json]
               :clj [clojure.data.json :as json])
            #?(:jolt [jolt.fibers :as fibers])))

(def ^:private pending :pending)

(defn- fail! [kind message]
  (throw (ex-info message {:type kind})))

#?(:jolt
   (defn- load-native-writer! []
     (try
       (require 'clojure.data.json.jolt-native)
       (let [writer ((ns-resolve 'clojure.data.json.jolt-native 'load-writer!))]
         (when-not (ifn? writer)
           (fail! ::native-unavailable "Native JSON writer was not initialized"))
         writer)
       (catch Throwable _
         ;; Do not expose paths, input values, or arbitrary loader diagnostics.
         (fail! ::native-unavailable
                "Native JSON encoding requires the qualified compiler-bearing Jolt runtime")))))

#?(:jolt (def ^:dynamic ^:private *selected-writer* nil))

#?(:jolt
   (defn- with-selected-writer [writer operation]
     (if writer
       (binding [json/*experimental-native-writer* writer] (operation))
       (operation))))

(defn open-encoder
  "Create one caller-owned encoder. `:parallelism` defaults to 1; 4 is an
  explicit Jolt-only optimization. JVM and Babashka always encode serially.

  A context admits one batch at a time and rejects a concurrent call. Share
  one context across application callers to keep that bound meaningful;
  separate contexts can oversubscribe the machine. No global carrier setting
  is read or modified.

  `:json-backend` defaults to `:configured`, preserving the host writer and
  caller's data.json bindings. `:native-guarded` explicitly selects the guarded
  Jolt writer, overriding that caller binding without changing JSONWriter
  extensions. It requires a compiler-bearing qualified runtime; standalone/AOT
  is not qualified. Unsupported hosts/loading fail rather than silently falling
  back. This option does not change admission or persistence guarantees."
  ([] (open-encoder {}))
  ([options]
   (when-not (and (map? options)
                  (every? #{:parallelism :json-backend} (keys options))
                  (contains? #{1 4} (get options :parallelism 1))
                  (contains? #{:configured :native-guarded}
                             (get options :json-backend :configured)))
     (fail! ::invalid-options "Invalid JSONEachRow encoder options"))
   (let [backend (get options :json-backend :configured)
         writer (when (= :native-guarded backend)
                  #?(:jolt (load-native-writer!)
                     :bb (fail! ::native-unavailable "Native guarded JSON encoding requires Jolt")
                     :clj (fail! ::native-unavailable "Native guarded JSON encoding requires Jolt")))]
     {:state (atom {:phase :open :active nil})
      :json-backend backend
      :native-writer writer
      :requested-parallelism (get options :parallelism 1)
      :effective-parallelism #?(:jolt (get options :parallelism 1)
                                :bb 1
                                :clj 1)})))

(defn encoder-info
  "Bounded configuration diagnostics; excludes callbacks, rows and handles.
  :configured describes delegation, not the caller's effective writer binding."
  [encoder]
  {:json-backend (:json-backend encoder)
   :requested-parallelism (:requested-parallelism encoder)
   :effective-parallelism (:effective-parallelism encoder)
   :source-only? (= :native-guarded (:json-backend encoder))})

(defn- admit! [encoder]
  (let [batch {:done (promise)}]
    (loop []
      (let [{:keys [phase active] :as before} @(:state encoder)]
        (when-not (= :open phase)
          (fail! ::closed "JSONEachRow encoder is closing or closed"))
        (when active
          (fail! ::busy "JSONEachRow encoder already has an active batch"))
        (if (compare-and-set! (:state encoder) before
                              (assoc before :active batch))
          batch
          (recur))))))

(defn- release! [encoder batch]
  (loop []
    (let [before @(:state encoder)]
      (when (identical? batch (:active before))
        (if (compare-and-set!
             (:state encoder) before
             (assoc before :active nil
                    :phase (if (= :closing (:phase before)) :closed :open)))
          (deliver (:done batch) :closed)
          (recur))))))

(defn close!
  "Stop admission before waiting for the active batch. Without a timeout,
  wait for settlement; with `timeout-ms`, return `:pending` if it is still
  running. A pending context remains closed to new work, and close! can be
  called again to drain it. This does not forcibly cancel user serialization."
  ([encoder] (close! encoder nil))
  ([encoder timeout-ms]
   (when-not (or (nil? timeout-ms)
                 (and (integer? timeout-ms) (not (neg? timeout-ms))))
     (fail! ::invalid-timeout "close! timeout must be a nonnegative integer"))
   (let [active
         (loop []
           (let [before @(:state encoder)
                 after (assoc before :phase (if (:active before)
                                              :closing :closed))]
             (if (compare-and-set! (:state encoder) before after)
               (:active after)
               (recur))))]
     (if-not active
       :closed
       (if (nil? timeout-ms)
         @(:done active)
         (deref (:done active) timeout-ms pending))))))

#?(:bb
   (defn- check-native-value! [value]
     ;; Cheshire otherwise stringifies non-finite floats and may silently
     ;; encode arbitrary objects as {}. Neither is an admitted JSON value.
     (cond
       (or (nil? value) (string? value) (boolean? value)) nil
       (number? value)
       (when (and (or (instance? Double value) (instance? Float value))
                  (not (Double/isFinite (double value))))
         (fail! ::unsupported-value "JSONEachRow values must be finite"))
       (map? value)
       (doseq [[key child] value]
         (when-not (or (string? key) (keyword? key))
           (fail! ::unsupported-value "JSONEachRow object key is unsupported"))
         (check-native-value! child))
       (sequential? value) (doseq [child value] (check-native-value! child))
       :else (fail! ::unsupported-value "JSONEachRow value is unsupported"))))

(defn- row-text [row]
  #?(:bb (do (check-native-value! row)
             (str (bb-json/generate-string row) "\n"))
     :jolt (let [^java.io.StringWriter sink (java.io.StringWriter.)]
             (json/write row sink)
             (.append sink "\n")
             (.toString sink))
     :clj (str (json/write-str row) "\n")))

(defn- serial-payload [rows]
  ;; Materialize rows before apply-str to avoid forcing that lazy map in apply.
  ;; Jolt may still hold a counted lock during mapv's internal seq realization:
  ;; parallel row serialization must be CPU-only and nonparking throughout.
  (apply str (mapv row-text rows)))

#?(:jolt
   (defn- chunk-texts [rows]
     ;; Keep each completed row immutable; assemble the batch after settlement.
     (mapv row-text rows)))

#?(:jolt
   (defn- spawn-chunk [rows]
     ;; Worker exceptions are values, so join exceptions identify interruption
     ;; of the waiting caller rather than an InterruptedException from a row.
     (let [writer *selected-writer*]
       (fibers/spawn
        (fn []
          (try {:value (with-selected-writer writer #(chunk-texts rows))}
               (catch Throwable error {:error error})))))))

#?(:jolt
   (defn- join-uninterruptibly [job]
     (loop [interruption nil]
       (let [outcome (try {:value (fibers/join job)}
                          (catch InterruptedException error
                            {:interruption error}))]
         (if-let [error (:interruption outcome)]
           (recur (or interruption error))
           {:value (:value outcome) :interruption interruption})))))

#?(:jolt
   (defn- join-all [jobs]
     (reduce (fn [acc job]
               (let [result (join-uninterruptibly job)]
                 (-> acc
                     (update :outcomes conj (:value result))
                     (update :interruption #(or % (:interruption result))))))
             {:outcomes [] :interruption nil} jobs)))

#?(:jolt
   (defn- parallel-payload [rows settled?]
     (let [jobs (atom [])
           spawn-error
           (try
             (dotimes [index 4]
               (let [chunk (subvec rows
                                   (quot (* index (count rows)) 4)
                                   (quot (* (inc index) (count rows)) 4))
                     job (spawn-chunk chunk)]
                 (swap! jobs conj job)))
             nil
             (catch Throwable error error))
           joined (join-all @jobs)
           _ (reset! settled? true)
           interruption (:interruption joined)
           worker-error (some :error (:outcomes joined))]
       (when interruption
         (when-not (fibers/in-fiber?)
           (.interrupt (Thread/currentThread))))
       (cond
         spawn-error (throw spawn-error)
         interruption (throw interruption)
         worker-error (throw worker-error)
         ;; Concatenate only row-string references here, not chunk payloads.
         ;; No row writer runs after the workers have settled.
         :else (apply str (reduce into [] (mapv :value (:outcomes joined))))))))

(defn- materialize-utf8 [payload]
  (let [bytes (.getBytes payload "UTF-8")]
    {:payload payload :utf8 bytes :byte-count (alength bytes)}))

(defn- encode-with! [encoder rows materialize]
  (let [batch (admit! encoder)
        settled? (atom false)]
    (try
      (when-not (vector? rows)
        (reset! settled? true)
        (fail! ::invalid-rows "JSONEachRow encoder requires a rows vector"))
      (let [payload #?(:jolt (binding [*selected-writer* (:native-writer encoder)]
                              (if (= 4 (:effective-parallelism encoder))
                                (parallel-payload rows settled?)
                                (with-selected-writer *selected-writer*
                                  #(serial-payload rows))))
                       :bb (serial-payload rows)
                       :clj (serial-payload rows))
            _ (reset! settled? true)]
        ;; Result materialization remains part of the admitted operation.
        (materialize payload))
      (finally
        ;; An unexpected fiber join failure leaves the context busy rather
        ;; than admitting another batch while worker liveness is unknown.
        #?(:jolt (when (or (= 1 (:effective-parallelism encoder)) @settled?)
                   (release! encoder batch))
           :bb (release! encoder batch)
           :clj (release! encoder batch))))))

(defn encode-rows!
  "Encode an ordered vector as JSONEachRow: one JSON value and newline per
  row. Jolt/JVM use data.json; Babashka uses native Cheshire, preserving row
  order and decoded values but not cross-host byte identity. Returns `:payload` (immutable
  text), `:utf8` (caller-owned byte array), and `:byte-count`. No SQL prefix is
  added, and no mutable byte array is retained by the context.

  Input values must be finite and supported by the host writer. On Jolt,
  parallel row serialization (including lazy values and custom writers) must
  not park, block, or do I/O. If an encoding worker fails, all started workers
  finish before the error is rethrown. An
  interrupted waiting caller likewise waits for worker settlement first."
  [encoder rows]
  (encode-with! encoder rows materialize-utf8))

(defn encode-text!
  "Encode an ordered vector as immutable JSONEachRow text, without creating
  the UTF-8 result of encode-rows!. Uses the same host writer, admission,
  errors, and worker-settlement lifetime as encode-rows!. Its finite-value
  and CPU-only, nonparking parallel-serializer requirements also apply.
  No SQL prefix is added."
  [encoder rows]
  (encode-with! encoder rows identity))

(defn encode-limited-text!
  "Encode sequential rows with a UTF-8 output budget including each newline.
  Always serial: requires effective parallelism 1. Check every completed row
  before appending, and do not request the next row after exceeding the budget.
  Lazy input may itself realize chunks; arbitrary user serialization remains a
  trusted operation, and a single row may allocate before its size is known.
  The context retains no payload after return/error. This is encoding only,
  not admission or persistence."
  [encoder rows max-bytes]
  (let [batch (admit! encoder)]
    (try
      (when-not (= 1 (:effective-parallelism encoder))
        (fail! ::serial-required "Bounded JSONEachRow encoding requires serial parallelism"))
      (when-not (and (integer? max-bytes) (not (neg? max-bytes)))
        (fail! ::invalid-limit "JSONEachRow byte limit must be a nonnegative integer"))
      (when-not (or (nil? rows) (sequential? rows))
        (fail! ::invalid-rows "Bounded JSONEachRow encoding requires sequential rows"))
      (let [operation
            (fn []
              (loop [remaining max-bytes rows (seq rows) out (StringBuilder.)]
                (if (seq rows)
                  (let [encoded (row-text (first rows))
                        size (alength (.getBytes encoded "UTF-8"))]
                    (when (> size remaining)
                      (fail! ::output-limit "JSONEachRow output exceeds its byte limit"))
                    (.append out encoded)
                    (recur (- remaining size) (next rows) out))
                  (.toString out))))]
        #?(:jolt (with-selected-writer (:native-writer encoder) operation)
           :bb (operation)
           :clj (operation)))
      (finally (release! encoder batch)))))
