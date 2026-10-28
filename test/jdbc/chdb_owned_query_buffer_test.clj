(ns jdbc.chdb-owned-query-buffer-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.owned-statement :as owned]
            [jdbc.chdb.byte-copy :as copy]
            [jolt.ffi :as ffi]))

(deftest full-copy-does-not-allocate-a-managed-zero-template
  (doseq [n [0 1 17 65537]]
    (let [input (byte-array (map #(byte (mod % 128)) (range n)))
          statement (owned/try-snapshot input)
          ordinary-alloc ffi/alloc original-free ffi/free
          zeroed (atom 0) freed (atom 0)]
      (with-redefs [ffi/alloc (fn [size] (swap! zeroed inc) (ordinary-alloc size))
                    ffi/free (fn [pointer] (swap! freed inc) (original-free pointer))]
        (is (= :observed
               (owned/with-query-buffer statement
                 (fn [{:keys [pointer length]}]
                   (is (= n length))
                   (is (java.util.Arrays/equals input (ffi/read-array pointer :byte length)))
                   (when (zero? n) (is (= 0 (ffi/read pointer :uint8))))
                   :observed))))
        (is (zero? @zeroed) "fully overwritten private native storage needs no zero template")
        (is (= 1 @freed)))
      (is (= (String. input "UTF-8") (owned/text statement))))))

(deftest allocation-is-released-if-copy-fails-before-the-consumer
  (doseq [partial? [false true]]
   (let [statement (owned/try-snapshot (.getBytes "SELECT 1" "UTF-8"))
        failure (ex-info "controlled copy failure" {:fixture true})
        original-copy copy/write-prefix!
        original-free ffi/free freed (atom 0) called (atom false)]
    (with-redefs [copy/write-prefix! (fn [pointer bytes _]
                                     (when partial?
                                       (original-copy pointer bytes 1)
                                       (is (= 83 (ffi/read pointer :uint8))))
                                     (throw failure))
                  ffi/free (fn [pointer] (swap! freed inc) (original-free pointer))]
      (is (identical? failure
            (try (owned/with-query-buffer statement (fn [_] (reset! called true)))
                 (catch Throwable error error))))
      (is (false? @called))
      (is (= 1 @freed)))
    (is (= "SELECT 1" (owned/text statement))))))

(deftest incomplete-copy-never-exposes-the-uninitialized-buffer
  (doseq [reported [0 nil -1]]
    (let [statement (owned/try-snapshot (.getBytes "SELECT 1" "UTF-8"))
          original-free ffi/free freed (atom 0) called (atom false)]
      (with-redefs [copy/write-prefix! (fn [& _] reported)
                    ffi/free (fn [pointer] (swap! freed inc) (original-free pointer))]
        (is (thrown? clojure.lang.ExceptionInfo
              (owned/with-query-buffer statement (fn [_] (reset! called true)))))
        (is (false? @called))
        (is (= 1 @freed))))))

(defn -main [& _]
  (let [r (run-tests 'jdbc.chdb-owned-query-buffer-test)]
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
