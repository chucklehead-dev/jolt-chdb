(ns jdbc.chdb-prepared-record-test
  (:require [clojure.test :as t :refer [deftest is]]
            [jdbc.chdb.owned-statement :as owned]
            [jdbc.chdb.byte-record :as record]
            [jdbc.chdb.byte-output :as output]
            [jdbc.chdb.durable.wal :as wal]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb-owned-writer-test :as support])
  (:import [java.io ByteArrayOutputStream OutputStream FileOutputStream]
           [java.nio.file Files] [java.nio.file.attribute FileAttribute]))

(defn bytes-from [line]
  (let [out (ByteArrayOutputStream.)]
    (wal/write-prepared! out line)
    (.toByteArray out)))

(deftest precomputed-record-is-opaque-and-public-chunks-stay-independent
  (doseq [sql ["" "SELECT 1" (apply str (map char (range 128)))
               (str (apply str (repeat 131071 "a")) "\"/\\\n")]]
    (let [source (.getBytes sql "UTF-8") original (owned/try-snapshot source)
          cached (owned/precompute-wal original)
          line (owned/prepared-wal-for-write cached)
          expected (wal/portable-line-bytes sql)
          public (owned/prepared-wal cached)]
      (is (owned/statement? cached))
      (is (identical? cached (owned/precompute-wal cached)))
      (is (satisfies? record/ByteRecord line))
      (is (not (map? line)))
      (is (= "#<prepared WAL record>" (str line)))
      (is (= (alength expected) (wal/prepared-size line)))
      (when (pos? (alength source)) (aset-byte source 0 88))
      (aset-byte (first (first (:chunks public))) 0 0)
      (is (= sql (owned/text cached) (owned/text original)))
      (is (java.util.Arrays/equals expected (bytes-from line)))
      (is (java.util.Arrays/equals expected (bytes-from (owned/prepared-wal cached))))
      (let [path (Files/createTempFile "jchdb-cached-wal-" ".bin" (into-array FileAttribute []))]
        (try
          (with-open [out (FileOutputStream. (.toFile path))] (wal/write-prepared! out line))
          (is (java.util.Arrays/equals expected (Files/readAllBytes path)))
          (finally (Files/deleteIfExists path)))))))

(deftest custom-stream-mutation-and-retention-cannot-poison-private-record
  (let [sql "SELECT 'private'" cached (owned/precompute-wal (support/snapshot sql))
        line (owned/prepared-wal-for-write cached) retained (atom [])
        sink (proxy [OutputStream] []
               (write
                 ([byte] (throw (ex-info "unexpected scalar overload" {})))
                 ([bytes _ _] (swap! retained conj bytes) (aset-byte bytes 0 0))))]
    (wal/write-prepared! sink line)
    (is (seq @retained))
    (doseq [bytes @retained] (aset-byte bytes 0 99))
    (is (java.util.Arrays/equals (wal/portable-line-bytes sql) (bytes-from line)))
    (is (= sql (owned/text cached)))))

(deftest prepare-once-before-worker-and-decline-oversized-cache
  (let [statement (support/snapshot "SELECT 1") calls (atom 0)
        kernel (deref (deref #'owned/wal-kernel))]
    (with-redefs [owned/wal-kernel (delay (fn [bytes] (swap! calls inc) (kernel bytes)))]
      (let [cached (owned/precompute-wal statement)]
        (is (= 1 @calls))
        (owned/prepared-wal cached)
        (owned/prepared-wal-for-write cached)
        (is (= 1 @calls))
        (is (identical? cached (owned/precompute-wal cached)))))
    (with-redefs [owned/max-precomputed-input-bytes 2
                  owned/wal-kernel (delay (fn [_] (throw (ex-info "must not encode" {}))))]
      (is (identical? statement (owned/precompute-wal statement)))))
  (is (thrown? clojure.lang.ExceptionInfo (owned/precompute-wal {}))))

(deftest cached-record-keeps-writer-policy-and-partial-append-boundaries
  (doseq [mode [:success :policy :partial-append]]
    (let [sql "INSERT INTO t VALUES ('private')"
          cached (owned/precompute-wal (support/snapshot sql)) native-calls (atom 0)]
      (support/fixture
        (fn [base _]
          (assoc base :with-native-owned-admitted-buffer!
                 (fn [_ statement _ admitted!]
                   (is (identical? cached statement))
                   (admitted! (cond-> support/accepted (= mode :policy) (assoc :has-secrets true))
                              #(do (swap! native-calls inc) {:count 1})))))
        (fn [instance store _ _]
          (with-redefs [owned/wal-kernel (delay (fn [_] (throw (ex-info "worker re-encoded WAL" {}))))]
            (case mode
              :success (do (writer/execute-owned-and-flush! instance cached)
                           (is (= [sql] (support/published-sql store))))
              :policy (do (is (thrown? clojure.lang.ExceptionInfo
                                      (writer/execute-owned-and-flush! instance cached)))
                          (is (zero? @native-calls))
                          (is (empty? (support/published-sql store))))
              :partial-append
              (with-redefs [output/write-private-prefix!
                            (fn [out bytes _]
                              (.write out bytes 0 1)
                              (throw (ex-info "injected partial append" {:type ::partial})))]
                (is (thrown? clojure.lang.ExceptionInfo
                             (writer/execute-owned-and-flush! instance cached)))
                (is (= 1 @native-calls))
                (is (:checkpoint-required? (writer/status instance)))
                (is (zero? (:pending-statements (writer/status instance))))
                (is (empty? (support/published-sql store)))))))))))

(deftest cached-record-keeps-existing-sealed-and-budget-rejection-controls
  ;; Run the same established assertions with cached statements. Invoke the
  ;; test bodies directly so this namespace keeps one predictable test count.
  (let [snapshot support/snapshot]
    (with-redefs [support/snapshot (fn [sql] (owned/precompute-wal (snapshot sql)))]
      ((:test (meta #'support/owned-policy-and-budget-rejections-do-not-mutate-or-publish)))
      ((:test (meta #'support/retained-sealed-wal-rejects-next-owned-request-before-preparation))))))

(defn -main [& _]
  (let [r (t/run-tests 'jdbc.chdb-prepared-record-test)]
    (assert (= 5 (:test r)))
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
