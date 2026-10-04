(ns jdbc.chdb-durable-wal-chunks-test
  (:require [clojure.test :refer [deftest is]]
            [jdbc.chdb.durable.wal :as wal]
            #?(:jolt [jdbc.chdb.durable.writer :as writer])
            #?(:jolt [jdbc.chdb.durable.backend :as backend])
            #?(:jolt [jdbc.chdb.durable.control :as control])
            #?(:jolt [jdbc.chdb-durable-writer-test-support :as support])
            #?(:jolt [jolt.scheme :as scheme])
            #?(:jolt [jdbc.chdb.durable.wal-chunks :as chunks])))

#?(:jolt
   (deftest optimized-kernel-has-checked-input-and-local-optimization
     ;; Raw encode requires the optional owned-byte runtime binding. The required
     ;; native CI lanes assert selection in -main before running this test; the
     ;; ordinary compatibility lane must exercise its declared portable fallback.
     (if (wal/native-prepared-enabled?)
       (let [level (scheme/eval-string "(optimize-level)")]
         (chunks/encode "checked")
         (is (= level (scheme/eval-string "(optimize-level)")))
         (doseq [invalid [nil 1 [] {}]]
           (is (thrown? Throwable (chunks/encode invalid))))
         (is (= level (scheme/eval-string "(optimize-level)"))))
       (is (bytes? (wal/prepared-line "checked"))))))

#?(:jolt
   (deftest absent-owned-byte-binding-does-not-invoke-raw-kernel
     ;; A live negative control for the compatibility lane: make any attempt to
     ;; call the raw kernel fail, rather than requiring an obsolete local Jolt.
     (with-redefs [wal/native-prepared-enabled? (constantly false)
                   chunks/encode (fn [_] (throw (ex-info "absent raw kernel invoked" {})))]
       (optimized-kernel-has-checked-input-and-local-optimization))))

(declare written-bytes)

(deftest maximum-escape-and-suffix-boundaries
  ;; Twelve-byte astral escapes reach/cross the minimum and full chunk ends.
  ;; ASCII lengths exercise suffix-only flush and the inclusive reserve edge.
  (doseq [s (concat (for [n [19 20 21 22 5459 5460 5461 10922]]
                     (apply str (repeat n "😀")))
                   (for [n [233 234 235 236 237 65512 65513 65514 65524 65525]]
                     (str (apply str (repeat n "a")) "😀")))]
    (let [prepared (wal/prepared-line s) expected (wal/portable-line-bytes s)]
      (is (= (alength expected) (wal/prepared-size prepared)))
      (is (java.util.Arrays/equals expected (written-bytes prepared))))))

(defn- written-bytes [prepared]
  (let [out (java.io.ByteArrayOutputStream.)]
    (wal/write-prepared! out prepared)
    (.toByteArray out)))

#?(:jolt
   (defn- start-writer [calls published]
     (let [store (backend/memory-backend)
           acquired (control/acquire! store support/base-options)]
       (writer/start!
        {:store store :token (:token acquired) :handle :fake :database "default"
         :engine-metadata {:version "26.7.3" :backup-format 1 :min-reader "26.7.3"}
         :operations (assoc (support/fake-operations calls (atom 0))
                            :publish-wal-file!
                            (fn [& _] (swap! published inc)
                              (throw (ex-info "unexpected publication" {}))))}))))

(deftest prepared-bytes-and-accounting-remain-exact
  (doseq [s ["" "plain" "\"/\\\b\f\n\r\tβ😀"
             (str (apply str (repeat 65524 "a")) "😀\"/\\\nβ")
             (str (apply str (repeat 131071 "a")) "😀\"/\\\nβ")]]
    (let [prepared (wal/prepared-line s) expected (wal/portable-line-bytes s)]
      (is (= (alength expected) (wal/prepared-size prepared)))
      (is (java.util.Arrays/equals expected (written-bytes prepared)))
      (is (java.util.Arrays/equals expected (wal/line s))))))

(deftest unavailable-codec-retains-existing-byte-api
  (with-redefs [wal/native-prepared-enabled? (constantly false)]
    (let [prepared (wal/prepared-line "fallbackβ😀")]
      (is (not (map? prepared)))
      (is (java.util.Arrays/equals prepared (wal/line "fallbackβ😀")))
      (is (= (alength prepared) (wal/prepared-size prepared))))))

(deftest independent-owned-chunk-results
  (when (wal/native-prepared-enabled?)
    (let [a (wal/prepared-line "owned") b (wal/prepared-line "owned")
          expected (written-bytes b)]
      (is (map? a))
      (aset-byte (first (first (:chunks a))) 0 0)
      (is (java.util.Arrays/equals expected (written-bytes b)))
      (is (java.util.Arrays/equals expected (written-bytes (wal/prepared-line "owned")))))))

(deftest small-records-do-not-allocate-full-chunks
  (when (wal/native-prepared-enabled?)
    (doseq [s ["" "SELECT 1" (apply str (repeat 260 "😀"))]]
      (let [prepared (wal/prepared-line s)]
        (is (every? #(<= (alength (first %)) 512) (:chunks prepared)))
        (is (java.util.Arrays/equals (wal/portable-line-bytes s)
                                    (written-bytes prepared)))))))

#?(:jolt
   (deftest partial-chunk-append-requires-checkpoint-before-publication
     (when (wal/native-prepared-enabled?)
       (let [calls (atom []) published (atom 0) partial-written (atom 0)
             w (start-writer calls published)
             sql (str "INSERT INTO t VALUES ('" (apply str (repeat 131071 "a")) "')")]
         (try
           (with-redefs [wal/write-prepared!
                         (fn [output prepared]
                           (is (> (count (:chunks prepared)) 1))
                           (let [[bytes used] (first (:chunks prepared))]
                             (.write ^java.io.OutputStream output bytes 0 used)
                             (reset! partial-written used))
                           (throw (ex-info "injected partial append" {:type ::partial-append})))]
             (is (= ::partial-append (support/error-type #(writer/execute! w sql)))))
           (is (pos? @partial-written))
           (is (= [[:analyze-execute sql "default"] [:execute sql]] @calls))
           (is (:checkpoint-required? (writer/status w)))
           (is (= 0 (:pending-statements (writer/status w))))
           (is (= 0 (:pending-wal-bytes (writer/status w))))
           (is (= :jdbc.chdb.durable.writer/checkpoint-unavailable
                  (support/error-type #(writer/flush! w))))
           (is (= 0 @published))
           (finally (try (writer/close! w) (catch Throwable _))))))))

#?(:jolt
   (deftest invalid-native-descriptor-is-rejected-before-engine-side-effects
     (when (wal/native-prepared-enabled?)
       (let [calls (atom []) published (atom 0) w (start-writer calls published)]
         (try
           (with-redefs [chunks/encode (constantly [0 []])]
             (is (thrown? clojure.lang.ExceptionInfo
                          (writer/execute! w "INSERT INTO t VALUES (1)"))))
           (is (empty? @calls))
           (is (= 0 (:pending-statements (writer/status w))))
           (is (false? (:checkpoint-required? (writer/status w))))
           (finally (try (writer/close! w) (catch Throwable _))))))))

(defn -main [& _]
  (println "Native owned WAL chunks enabled:" (wal/native-prepared-enabled?))
  (when (= "true" (System/getenv "JOLT_CHDB_REQUIRE_WAL_CHUNKS"))
    (assert (wal/native-prepared-enabled?)))
  (let [result (clojure.test/run-tests 'jdbc.chdb-durable-wal-chunks-test)]
    (when-not (zero? (+ (:fail result) (:error result)))
      (throw (ex-info "Owned WAL chunk tests failed" result)))))
