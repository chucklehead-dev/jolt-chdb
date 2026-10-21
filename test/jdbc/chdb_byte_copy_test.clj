(ns jdbc.chdb-byte-copy-test
  (:require [clojure.test :refer [deftest is]] [jolt.ffi :as ffi]
            [jolt.scheme :as scheme] [jdbc.chdb.byte-copy :as copy]))

(defn contents [pointer n] (vec (ffi/read-array pointer :byte n)))

(deftest copy-failure-is-not-retried-through-fallback
  (let [attempts (atom 0) fallbacks (atom 0)]
    (ffi/with-alloc [pointer 1]
      (with-redefs-fn
        {#'copy/prefix-copy (delay (fn [& _] (swap! attempts inc)
                                    (throw (ex-info "copy failure" {:type ::copy-failed}))))
         #'ffi/write-array (fn [& _] (swap! fallbacks inc))}
        (fn []
          (is (= ::copy-failed
                 (try (copy/write-prefix! pointer (byte-array [1]) 1) nil
                      (catch Throwable error (:type (ex-data error))))))
          (is (= 1 @attempts))
          (is (zero? @fallbacks)))))))

(deftest admitted-prefix-does-not-route-through-the-temporary-array-path
  (let [input (byte-array [(byte 1) (byte 2) (byte 3) (byte 4)])]
    (ffi/with-alloc [pointer 3]
      (with-redefs [ffi/write-array (fn [& _] (throw (ex-info "fallback mutant" {})))]
        (is (= 3 (copy/write-prefix! pointer input 3N)))
        (is (= [1 2 3] (contents pointer 3))))
      (aset-byte input 0 (byte 9))
      (is (= [1 2 3] (contents pointer 3)))
      (ffi/write pointer :byte 8 1)
      (is (= [9 2 3 4] (vec input))))))

(deftest prefix-copy-retains-guards-and-source-through-gc
  (doseq [n [0 1 2 7 8 9 31 32 33 64 65 65535 65536 65537]]
    (let [input (byte-array (map #(unchecked-byte (mod % 256)) (range (inc n))))
          before (vec input)]
      (ffi/with-alloc [pointer (+ n 6)]
        (ffi/write-array pointer (byte-array (repeat (+ n 6) (byte 91))))
        (is (= n (copy/write-prefix! (+ pointer 3) input n)))
        (System/gc)
        (is (= (vec (concat [91 91 91] (take n before) [91 91 91]))
               (contents pointer (+ n 6))))
        (is (= before (vec input)))))))

(deftest invalid-range-declines-before-mutating-destination
  (ffi/with-alloc [pointer 8]
    (ffi/write-array pointer (byte-array (repeat 8 (byte 91))))
    (doseq [length [-1 5 1.5 nil]]
      (is (thrown? clojure.lang.ExceptionInfo
                   (copy/write-prefix! pointer (byte-array 4) length)))
      (is (= (vec (repeat 8 91)) (contents pointer 8)))))
  (is (= 0 (copy/write-prefix! ffi/null (byte-array 0) 0))))

(deftest fallback-and-boxed-byte-backing-are-correct
  (let [input (byte-array [(byte 0) (byte -1) (byte 127) (byte -128) (byte 65)])
        boxed ((scheme/eval-string "(lambda () (make-jolt-array (vector 0 -1 127 -128 65) 'byte))"))
        ordinary ffi/write-array calls (atom 0)]
    (ffi/with-alloc [pointer 5]
      (with-redefs [ffi/write-array (fn [& args] (swap! calls inc) (apply ordinary args))]
        (is (= 5 (copy/write-prefix! pointer boxed 5)))
        (is (= (vec input) (contents pointer 5)))
        (is (= 1 @calls))
        (with-redefs-fn {#'copy/prefix-copy (delay (fn [& _] false))}
          (fn []
            (is (= 5 (copy/write-prefix! pointer input 5N)))
            (is (= (vec input) (contents pointer 5)))
            (is (= 2 @calls))))))))
