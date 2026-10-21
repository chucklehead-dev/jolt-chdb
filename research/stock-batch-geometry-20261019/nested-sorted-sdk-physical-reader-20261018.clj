;; Event timestamp arrays must be compared as exact stored nanosecond ticks,
;; not timezone-formatted query strings against the integer insertion wire.
(require '[db.jdbc] '[jdbc.core :as jdbc] '[clojure.string :as str]
         '[jdbc.chdb.durable :as durable] '[jdbc.chdb.native :as native]
         '[clojure.data.json :as json]
         '[otel.exporter.chdb :as e]
         '[otel.exporter.chdb-ordinary-transport-benchmark :as fixture]
         '[jdbc.chdb-typed-storage-support :as storage])
(load-file "/home/chuck/ai-src/evidence/trace-affinity-helpers-20261018.clj")
(defn ensure! [condition label]
  (when-not condition (throw (ex-info "Trace-affinity recovery failed" {:label label}))))

(let [[root] *command-line-args* n 5000 rounds 32
      _ (ensure! (nil? json/*experimental-native-reader*) :stock-json-reader)
      _ (native/ensure-loaded!)
      _ (ensure! (= "26.9.0" (native/chdb-version)) :native-version)
      spans (otel.research.trace-affinity/group-traces
              (mapv (fn [span]
                      (update span :attributes into
                              (map (fn [i] [(str "extra-" i)
                                            (case (mod i 3) 0 "value" 1 false i)])) (range 12)))
                    (with-redefs [fixture/batch-size n] (fixture/spans "typed-compact-prefix"))))
      fields (:fields (fixture/approved))
      cols (remove #{"Timestamp"} (fixture/columns fields))
      expected (sort-by (juxt :traceid :spanid)
                       (mapv (fn [span]
                               (let [row (merge (#'e/span-row span nil)
                                                 (into {} (mapcat (fn [f]
                                                                    [[(get-in f [:physical :value-column])
                                                                      (get-in span [:attributes (:key f)])]
                                                                     [(get-in f [:physical :status-column]) 3]]) fields)))]
                                 (assoc (into {} (map (fn [col] [(keyword (str/lower-case col)) (get row col)]) cols))
                                        :ticks (str (:start-time-unix-nano span)) :copies (+ 3 rounds)))) spans))]
  (with-open [conn (jdbc/connection (durable/snapshot-dbspec (storage/options root "reader")))]
    (let [query (str "SELECT " (str/join ", " (map (fn [column]
                                          (if (= column "Events.Timestamp")
                                            "arrayMap(x -> toUnixTimestamp64Nano(x), `Events.Timestamp`) AS `Events.Timestamp`"
                                            (fixture/sql-column column))) cols))
                     ", toString(toUnixTimestamp64Nano(Timestamp)) AS ticks, count() AS copies"
                     " FROM otel_traces GROUP BY ALL ORDER BY TraceId, SpanId")
          actual (jdbc/fetch conn query {:max-rows (inc n)})]
      (when-not (= expected actual)
        (let [pair (first (filter (fn [[a b]] (not= a b)) (map vector expected actual)))
              [a b] pair
              differences (vec (for [key (distinct (concat (keys a) (keys b)))
                                     :when (not= (get a key) (get b key))]
                                 {:field key
                                  :expected (subs (pr-str (get a key)) 0 (min 1024 (count (pr-str (get a key)))))
                                  :actual (subs (pr-str (get b key)) 0 (min 1024 (count (pr-str (get b key)))))}))]
          (prn {:expected-groups (count expected) :actual-groups (count actual)
                :mismatch-fields differences})))
      (ensure! (= expected actual) :full-physical-row-and-multiplicity)
      (ensure! (= (* n (+ 3 rounds)) (:n (jdbc/fetch-one conn "SELECT count() AS n FROM otel_traces"))) :row-count)
      (doseq [f fields]
        (ensure! (= (if (= :boolean (:type f)) "Bool" "Int64")
                     (:t (jdbc/fetch-one conn (str "SELECT toTypeName("
                                                  (fixture/sql-column (get-in f [:physical :value-column]))
                                                  ") AS t FROM otel_traces LIMIT 1")))) :physical-type))
      (spit (str root "/reader.edn")
            (pr-str {:passed true :rows 175000 :groups 5000 :trace-count 625
                     :full-physical-rows-equal true :typed-values-status true
                     :per-record-multiplicity 35 :deterministic-tie-order true}))
      (println :fresh-affinity-reader-green :rows 175000))))


