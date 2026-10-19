(ns jdbc.chdb-byte-output-test
  (:require [clojure.test :as t :refer [deftest is]]
            [jdbc.chdb.byte-output :as output]
            #?(:jolt [jolt.scheme :as scheme]))
  (:import [java.io ByteArrayOutputStream FileOutputStream OutputStream IOException
            PipedInputStream PipedOutputStream]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn caught [f] (try (f) nil (catch Throwable error error)))

(deftest builtin-prefixes-preserve-exact-bytes-and-borrowed-ownership
  (doseq [n [0 1 63 64 65535 65536 65537]]
    (let [source (byte-array (map #(unchecked-byte (mod % 256)) (range (inc n))))
          original (vec source) out (ByteArrayOutputStream.)]
      (is (nil? (output/write-prefix! out source n)))
      (is (= (vec (take n original)) (vec (.toByteArray out))))
      (is (= original (vec source)))
      (when (pos? n) (aset-byte source 0 (byte 77)))
      (is (= (vec (take n original)) (vec (.toByteArray out))))
      (.write out 90)
      (is (= (conj (vec (take n original)) 90) (vec (.toByteArray out)))))))

(deftest custom-stream-keeps-zero-and-invalid-call-dispatch
  (let [calls (atom []) flushes (atom 0) closes (atom 0)
        out (proxy [OutputStream] []
              (write
                ([value] (swap! calls conj [:scalar value]))
                ([bytes start length] (swap! calls conj [:slice start length])))
              (flush [] (swap! flushes inc))
              (close [] (swap! closes inc)))]
    (doseq [n [0 2 5 -1]] (output/write-prefix! out (byte-array 2) n))
    (is (= [[:slice 0 0] [:slice 0 2] [:slice 0 5] [:slice 0 -1]] @calls))
    (is (zero? @flushes))
    (is (zero? @closes))))

(deftest closed-file-and-invalid-range-retain-ordinary-error-order
  (let [path (Files/createTempFile "jchdb-output-" ".bin" (into-array FileAttribute []))
        out (FileOutputStream. (.toFile path)) bytes (byte-array [1 2])]
    (try
      (.close out)
      (doseq [n [0 1 3 -1]]
        (let [old (caught #(.write out bytes 0 n))
              new (caught #(output/write-prefix! out bytes n))]
          (if old
            (do (is (some? new))
                (is (= (class old) (class new)))
                (is (= (.getMessage old) (.getMessage new))))
            (is (nil? new)))))
      (finally (.close out) (Files/deleteIfExists path)))))

(deftest file-output-is-borrowed-and-prefix-is-byte-exact
  (let [path (Files/createTempFile "jchdb-output-" ".bin" (into-array FileAttribute []))
        source (byte-array (map #(unchecked-byte (mod % 256)) (range 65538)))]
    (try
      (with-open [out (FileOutputStream. (.toFile path))]
        (is (nil? (output/write-prefix! out source 65537)))
        ;; FileOutputStream, unlike BAOS, would reject this if helper closed it.
        (.write out 90)
        (.flush out))
      (is (= (conj (vec (take 65537 source)) 90) (vec (Files/readAllBytes path))))
      (finally (Files/deleteIfExists path)))))

#?(:jolt
   (deftest native-admission-and-no-retry-after-partial-write
     (let [bytes (byte-array [1 2 3 4]) out (ByteArrayOutputStream.)
           kernel (deref (deref #'output/output-prefix))
           path (Files/createTempFile "jchdb-output-admission-" ".bin" (into-array FileAttribute []))]
       (is (false? (kernel out bytes 3)))
       (try
         (with-open [file (FileOutputStream. (.toFile path))]
           (is (true? (kernel file bytes 3N))))
         (is (= [1 2 3] (vec (Files/readAllBytes path))))
         (finally (Files/deleteIfExists path)))
       (let [boxed ((scheme/eval-string "(lambda () (make-jolt-array (vector 0 -1 127 -128) 'byte))"))]
         (is (false? ((deref (deref #'output/output-prefix)) out boxed 4)))
         (output/write-prefix! out boxed 4)
         (is (= [0 -1 127 -128] (vec (.toByteArray out)))))
       (let [out (ByteArrayOutputStream.) calls (atom 0)]
         (with-redefs-fn
           {#'output/output-prefix
            (delay (fn [stream bytes _]
                     (swap! calls inc)
                     (.write stream bytes 0 1)
                     (throw (ex-info "partial write" {:type ::partial}))))}
           (fn []
             (is (= ::partial (:type (ex-data (caught #(output/write-prefix! out bytes 4))))))
             (is (= [1] (vec (.toByteArray out))))
             (is (= 1 @calls))))))))

#?(:jolt
   (deftest pipe-keeps-ordinary-implicit-flush
     (with-open [input (PipedInputStream.) out (PipedOutputStream. input)]
       (let [bytes (byte-array [1 2 3 4])]
         (is (false? ((deref (deref #'output/output-prefix)) out bytes 4)))
         (output/write-prefix! out bytes 4)
         (is (= 4 (.available input)))
         (is (= [1 2 3 4] (vec (.readNBytes input 4))))))))

#?(:jolt
   (deftest custom-native-port-declines-before-observable-writer
     (let [[out writes] ((scheme/eval-string
                          "(lambda () (let ((writes 0)) (let ((p (make-custom-binary-output-port \"test\" (lambda (bv at n) (set! writes (+ writes 1)) n) #f #f #f))) (jolt-vector (make-out-stream p) (lambda () writes)))))"))
           bytes (byte-array [1 2 3])]
       (try
         (is (false? ((deref (deref #'output/output-prefix)) out bytes 3)))
         (is (zero? (writes)))
         (output/write-prefix! out bytes 3)
         (.flush out)
         (is (pos? (writes)))
         (finally (.close out))))))

(defn -main [& _]
  (let [result (t/run-tests 'jdbc.chdb-byte-output-test)]
    (assert (pos? (:test result)))
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
