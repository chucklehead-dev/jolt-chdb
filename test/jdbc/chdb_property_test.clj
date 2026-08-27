(ns jdbc.chdb-property-test
  (:require [clojure.data.json :as json]
            [hegel.core :as h]
            [hegel.generator :as g]
            [hegel.stateful :as hs]
            [jdbc.chdb :as chdb]
            [jdbc.core :as jdbc]))

(def ^:private base-options
  {:database ""
   :derandomize? true
   :verbosity :quiet})

(defn- fail! [origin message data]
  (throw (ex-info message (assoc data :hegel/origin origin))))

(defn- require! [origin expected actual]
  (when-not (= expected actual)
    (fail! origin "property assertion failed"
           {:expected expected :actual actual})))

(defn- throws? [f]
  (try (f) false (catch Throwable _ true)))

(defn- run-property! [name options body]
  (let [seed (System/getenv "HEGEL_SEED")
        options (cond-> (merge base-options {:name name} options)
                  (seq seed) (assoc :seed (parse-long seed)))
        result (h/run-test! options body)]
    (println "  hegel" name "seed" (:seed result)
             "valid" (:valid-test-cases result))
    (when-not (and (:passed? result) (not (:flaky? result)))
      (throw (ex-info (str "Hegel property failed: " name) result)))
    result))

(defn- bytes->hex [value]
  (let [digits "0123456789ABCDEF"]
    (apply str
           (mapcat (fn [b]
                     (let [n (bit-and (int b) 255)]
                       [(nth digits (quot n 16)) (nth digits (mod n 16))]))
                   value))))

(defn- scalar-roundtrips! []
  (run-property!
   "chdb/scalar-roundtrips"
   {:test-cases 24}
   (fn [_]
     (let [i (h/draw! (g/integer -1000000000000 1000000000000))
           b (h/draw! (g/boolean))
           s (h/draw! (g/string {:max-size 48
                                 :alphabet "abcXYZ09 ?'\\\"-_/lambda-λ😀"}))
           payload (h/draw! (g/bytes {:max-size 64}))]
       (with-open [conn (jdbc/connection "chdb::memory:")]
         (let [row (jdbc/fetch-one
                    conn
                    ["select ? as i, ? as b, ? as s, hex(?) as h, isNull(?) as n"
                     i b s payload (chdb/typed-param "Nullable(String)" nil)])]
           (require! "chdb/scalar/integer" i (:i row))
           (require! "chdb/scalar/boolean" b (:b row))
           (require! "chdb/scalar/string" s (:s row))
           (require! "chdb/scalar/binary" (bytes->hex payload) (:h row))
           (require! "chdb/scalar/null" 1 (:n row))))))))

(def ^:private lexical-queries
  ["select ? as value, '?' as literal"
   "select ? as value /* ? */"
   "select ? as value -- ?\n"
   "select /* ? */ ? as value /* ? */"
   "select 'it''s ?' as literal, ? as value"
   "select ? as value, 1 as `?`"
   "select ? as value, 1 as \"?\""])

(defn- placeholder-lexing! []
  (run-property!
   "chdb/placeholder-lexical-contexts"
   {:test-cases 20}
   (fn [_]
     (let [query (h/draw! (g/sampled-from lexical-queries))
           value (h/draw! (g/string {:max-size 32
                                     :alphabet "abcXYZ09 ?'\\\"-_λ😀"}))]
       (with-open [conn (jdbc/connection "chdb::memory:")]
         (require! "chdb/placeholders/value" value
                   (:value (jdbc/fetch-one conn [query value]))))))))

(defn- stream-chunkings! []
  (run-property!
   "chdb/stream-chunkings"
   {:test-cases 14}
   (fn [_]
     (let [ids (h/draw! (g/vector {:max-size 20} (g/integer -100000 100000)))
           payload (apply str (map #(str (json/write-str {:id %}) "\n") ids))
           octets (vec (.getBytes payload "UTF-8"))
           chunks (h/draw! (g/chunkings octets))]
       (with-open [conn (jdbc/connection "chdb::memory:")]
         (jdbc/execute! conn "create table streamed_property (id Int64) engine=Memory")
         (chdb/stream-insert! conn "insert into streamed_property"
                              (mapv #(byte-array (map int %)) chunks))
         (require! "chdb/stream/chunk-boundaries" (sort ids)
                   (mapv :id (jdbc/fetch conn
                                         "select id from streamed_property order by id"))))))))

(defn- lifecycle-swarm! []
  (run-property!
   "chdb/lifecycle-swarm"
   {:test-cases 10 :stateful-step-count 12}
   (fn [_]
     (let [conn (jdbc/connection "chdb::memory:")]
       (try
         (hs/run!
          {:initial-state {:conn conn :closed? false}
           :rules
           [(hs/rule
             :query
             (fn [{:keys [conn closed?] :as state}]
               (if closed?
                 (require! "chdb/lifecycle/query-after-close" true
                           (throws? #(jdbc/fetch conn "select 1")))
                 (require! "chdb/lifecycle/query-open" 1
                           (:n (jdbc/fetch-one conn "select 1 n"))))
               state))
            (hs/rule
             :close
             (fn [{:keys [conn] :as state}]
               (.close conn)
               (assoc state :closed? true)))
            (hs/rule
             :idempotent-close
             (fn [{:keys [conn] :as state}]
               (.close conn)
               (.close conn)
               (assoc state :closed? true)))]
           :invariants
           [(hs/invariant :state-shape #(boolean? (:closed? %)))]})
         (.close conn)
         (require! "chdb/lifecycle/mandatory-use-after-close" true
                   (throws? #(jdbc/fetch conn "select 1")))
         (finally
           (.close conn)))))))

(defn run-properties! []
  (println "chDB Hegel properties")
  (scalar-roundtrips!)
  (placeholder-lexing!)
  (stream-chunkings!)
  (lifecycle-swarm!))
