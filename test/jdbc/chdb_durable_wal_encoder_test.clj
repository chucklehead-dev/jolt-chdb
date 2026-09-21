(ns jdbc.chdb-durable-wal-encoder-test
  (:require [clojure.data.json :as json]
            [jdbc.chdb.durable.wal :as wal]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println (str "  PASS " label))
    (do
      (swap! failures inc)
      (println (str "  FAIL " label " expected=" (pr-str expected)
                    " actual=" (pr-str actual))))))

(defn- portable-oracle [sql]
  ;; Match the original writer implementation, not json/write-str.  The
  ;; writer's pre-existing ranged-append capability gate rejects old Jolt
  ;; before a WAL can be accepted, while this encoder keeps its fallback
  ;; byte-compatible with the supported streaming path.
  (let [output (java.io.ByteArrayOutputStream.)
        text-output (java.io.OutputStreamWriter. output "UTF-8")]
    (json/write {"sql" sql} text-output)
    (.append text-output "\n")
    (.flush text-output)
    (vec (.toByteArray output))))

(defn -main [& _]
  (println "Durable V1 WAL byte encoder")
  (let [corpus [["empty" ""]
                ["ASCII" "SELECT 1"]
                ["double quote slash and backslash"
                 "SELECT \"quote\" / slash \\ backslash"]
                ["all named controls"
                 (str "controls" (char 0) (char 8) (char 9)
                      (char 10) (char 12) (char 13))]
                ["BMP" "β€"]
                ["astral" "😀"]]
        native-var (ns-resolve 'jdbc.chdb.durable.wal 'native-line-bytes)
        native-corpus?
        (and native-var
             (every? (fn [[_ sql]]
                       (try
                         (= (portable-oracle sql) (vec (@native-var sql)))
                         (catch Throwable _ false)))
                     corpus))]
    (doseq [[label sql] corpus]
      (let [portable (portable-oracle sql)]
        (check (str "portable " label " preserves existing JSONL bytes")
               portable (vec (wal/portable-line-bytes sql)))
        (check (str "selected " label " preserves existing JSONL bytes")
               portable (vec (wal/line sql)))
        (check (str "forced portable " label " preserves existing JSONL bytes")
               portable
               (vec (with-redefs [wal/native-line-enabled? (constantly false)]
                      (wal/line sql))))
        ;; Old Jolt intentionally has no primitive. If a candidate exists, it
        ;; must prove every record shape against the portable oracle rather
        ;; than relying on a version/pin assertion.
        (when native-var
          (let [candidate (try (@native-var sql)
                               (catch Throwable _ ::unavailable))]
            (when-not (= ::unavailable candidate)
              (check (str "native candidate " label " matches portable bytes")
                     portable (vec candidate)))))))
    (check "selection reflects the complete native behavior probe"
           (boolean native-corpus?) (wal/native-line-enabled?))
    (when-not (zero? @failures)
      (throw (ex-info (str @failures " Durable WAL encoder checks failed")
                      {:failures @failures}))))
  (println "Durable WAL encoder checks passed"))
