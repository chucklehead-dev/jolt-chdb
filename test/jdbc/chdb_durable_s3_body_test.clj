(ns jdbc.chdb-durable-s3-body-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.durable.s3-body :as body])
  (:import [java.io ByteArrayInputStream]))

(deftest exact-partial-copies-and-request-local-reuse
  (let [input (ByteArrayInputStream. (byte-array (range 10)))
        observed (atom []) buffers (atom [])
        read! (body/chunk-reader input 10
                (fn [destination buffer n]
                  (swap! buffers conj buffer)
                  (swap! observed conj [destination (vec (take n buffer))])))]
    (is (= 4 (read! :first 4)))
    (is (= 2 (read! :second 2)))
    (is (= 4 (read! :third 4)))
    (is (zero? (read! :end 4)))
    (is (= [[:first [0 1 2 3]] [:second [4 5]] [:third [6 7 8 9]]] @observed))
    (is (= 3 (count @buffers)))
    (is (every? #(identical? (first @buffers) %) @buffers))))

(deftest growth-and-bounded-scratch
  (let [data (byte-array 150000) buffers (atom [])
        read! (body/chunk-reader (ByteArrayInputStream. data) 150000
                (fn [_ buffer _] (swap! buffers conj buffer)))]
    (is (= 2 (read! :a 2)))
    (is (= 65536 (read! :b 100000)))
    (is (= 1 (read! :c 1)))
    (is (= [2 65536 65536] (mapv #(alength %) @buffers)))
    (is (not (identical? (first @buffers) (second @buffers))))
    (is (identical? (second @buffers) (nth @buffers 2)))))

(deftest small-and-empty-body-bounds
  (let [buffers (atom [])
        read! (body/chunk-reader (ByteArrayInputStream. (byte-array [1 2 3])) 3
                (fn [_ buffer _] (swap! buffers conj (alength buffer))))]
    (is (= 3 (read! :a 65536)))
    (is (= [3] @buffers))
    (is (zero? (read! :b 65536))))
  (let [read! (body/chunk-reader (ByteArrayInputStream. (byte-array 0)) 0 (fn [& _]))]
    (is (zero? (read! :empty 65536)))))

(deftest signed-byte-values-cross-reused-buffer-unchanged
  (let [snapshots (atom [])
        read! (body/chunk-reader (ByteArrayInputStream. (byte-array [-128 -1 0 127 -64])) 5
                (fn [_ buffer n] (swap! snapshots conj (vec (take n buffer)))))]
    (is (= 2 (read! :a 2)))
    (is (= 2 (read! :b 2)))
    (is (= 1 (read! :c 2)))
    (is (= [[-128 -1] [0 127] [-64]] @snapshots))))

(deftest readers-do-not-share-scratch-and-preserve-copy-errors
  (let [buffers (atom [])
        copy! (fn [_ buffer _] (swap! buffers conj buffer))
        a (body/chunk-reader (ByteArrayInputStream. (byte-array [1])) 1 copy!)
        b (body/chunk-reader (ByteArrayInputStream. (byte-array [2])) 1 copy!)]
    (a :a 1) (b :b 1)
    (is (not (identical? (first @buffers) (second @buffers)))))
  (let [failure (ex-info "fixture copy failure" {})
        read! (body/chunk-reader (ByteArrayInputStream. (byte-array [1])) 1
                (fn [& _] (throw failure)))]
    (is (identical? failure (try (read! :a 1) (catch Throwable e e))))))

(defn run-checks! []
  (let [result (run-tests 'jdbc.chdb-durable-s3-body-test)]
    (when (pos? (+ (:fail result) (:error result)))
      (throw (ex-info "S3 request scratch checks failed" {})))
    result))

(defn -main [& _] (run-checks!))
