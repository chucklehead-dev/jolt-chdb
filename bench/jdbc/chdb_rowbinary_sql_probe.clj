;; Bounded format feasibility, not a generic encoder or performance result.
(require '[db.jdbc] '[jdbc.core :as jdbc]
         '[jdbc.chdb.durable :as durable]
         '[jdbc.chdb.durable.control :as control]
         '[jdbc.chdb.durable.backend :as backend]
         '[jdbc.chdb.durable.local-posix :as local])
(import '(java.nio ByteBuffer ByteOrder) '(java.util Base64))

(def expected
  [{:u "1" :i "-9223372036854775808" :b false :t "0"
    :s "" :a [] :m {} :d "0" :n nil}
   {:u "18446744073709551615" :i "9223372036854775807" :b true
    :t "1700000000123456789" :s "quote\"slash\\nul\u0000漢字😀"
    :a ["-1" "0" "9223372036854775807"] :m {"flag" "true"} :d "1.5" :n "7"}])
(def schema
  "u UInt64, i Int64, b Bool, t DateTime64(9), s String, a Array(Int64), m Map(String, String), d Float64, n Nullable(Int64)")
(def read-query
  (str "SELECT toString(u) AS u, toString(i) AS i, b, "
       "toString(toUnixTimestamp64Nano(t)) AS t, s, "
       "arrayMap(x -> toString(x), a) AS a, m, toString(d) AS d, "
       "if(isNull(n), NULL, toString(n)) AS n FROM binary_probe ORDER BY u"))
(defn short-string! [buffer s]
  (let [bytes (.getBytes s "UTF-8")]
    ;; This probe intentionally uses only one-byte VarUInt lengths.
    (assert (< (alength bytes) 128))
    (.put buffer (byte (alength bytes)))
    (.put buffer bytes)))
(defn payload []
  (let [buffer (.order (ByteBuffer/allocate 1024) ByteOrder/LITTLE_ENDIAN)]
    (doseq [[index row] (map-indexed vector expected)]
      (.putLong buffer (if (zero? index) 1 -1))
      (.putLong buffer (if (zero? index) Long/MIN_VALUE Long/MAX_VALUE))
      (.put buffer (byte (if (:b row) 1 0)))
      (.putLong buffer (if (zero? index) 0 1700000000123456789))
      (short-string! buffer (:s row))
      (.put buffer (byte (count (:a row))))
      (doseq [value (if (zero? index) [] [-1 0 Long/MAX_VALUE])]
        (.putLong buffer value))
      (.put buffer (byte (count (:m row))))
      (doseq [[key value] (:m row)]
        (short-string! buffer key) (short-string! buffer value))
      (.putDouble buffer (if (zero? index) 0.0 1.5))
      (.put buffer (byte (if (nil? (:n row)) 1 0)))
      (when (:n row) (.putLong buffer 7)))
    (let [bytes (byte-array (.position buffer))]
      (.flip buffer) (.get buffer bytes) bytes)))

(let [[role root] *command-line-args*
      _ (assert (contains? #{"writer" "reader"} role))
      scratch (str root "/scratch-" role)
      _ (.mkdirs (java.io.File. scratch))
      options {:namespace-backend (local/local-backend (str root "/objects"))
               :object-id "rowbinary-probe" :scratch-parent scratch}
      ;; Options backend is a namespace root; the actual head lives under the
      ;; object ID. Read the same scoped provider as Durable uses.
      scoped (when (= role "reader")
               (backend/object-backend (:namespace-backend options) "rowbinary-probe"))
      wal-count (when scoped
                  (count (get-in (control/read-head-read-only! scoped)
                                 [:head "manifest" "wal"])))]
  (when scoped (assert (pos? wal-count)))
  (with-open [connection (jdbc/connection
                          (if (= role "writer")
                            (durable/writer-dbspec (assoc options :owner "rowbinary-probe"
                                                         :database "default"))
                            (durable/snapshot-dbspec options)))]
    (when (= role "writer")
      (jdbc/execute! connection
        (str "CREATE TABLE binary_probe (" schema ") ENGINE=MergeTree ORDER BY u"))
      (durable/flush! connection)
      (let [bytes (payload)
            encoded (.encodeToString (Base64/getEncoder) bytes)
            sql (str "INSERT INTO binary_probe SELECT * FROM format('RowBinary', '"
                     schema "', base64Decode('" encoded "'))")
            result (durable/execute-and-flush! connection sql)]
        (assert (= :committed (:status result)))
        (assert (= :wal (:confirmed-boundary (durable/persistence-observation connection))))
        (prn {:role role :scope :rowbinary-sql-durable-feasibility
              :payload-bytes (alength bytes) :sql-characters (count sql)
              :committed true})))
    (let [actual (jdbc/fetch connection read-query)
          result {:role role :exact-rows (= expected actual) :rows (count actual)
                  :referenced-wal-objects wal-count
                  :scope :rowbinary-sql-durable-feasibility}]
      (assert (= {:d "Float64" :u "UInt64"}
                 (jdbc/fetch-one connection
                   "SELECT toTypeName(d) AS d, toTypeName(u) AS u FROM binary_probe LIMIT 1")))
      (when-not (:exact-rows result)
        (prn {:mismatched-columns
              (mapv (fn [wanted received]
                      (vec (filter #(not= (get wanted %) (get received %)) (keys wanted))))
                    expected actual)}))
      (assert (:exact-rows result))
      (spit (str root "/" role ".edn") (pr-str result))
      (prn result))))
