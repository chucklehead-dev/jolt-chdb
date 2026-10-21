(ns perf.wal-clean-runs
  (:require [clojure.test :refer [deftest is run-tests]]
            [clojure.java.io :as io] [clojure.data.json :as json]
            [jolt.scheme :as scheme] [jolt.host :as host]))

(def baseline-path
  "/home/chuck/ai-src/worktrees/chdb-confirmed-owned-request-ticket-20261021/resources/jdbc/chdb/owned_ascii_wal.ss")
(def baseline (scheme/eval-string (slurp baseline-path)))
(def candidate (scheme/eval-string (slurp (io/resource "jdbc/chdb/owned_ascii_wal.ss"))))
(defn text [[_ chunks]]
  (apply str (map (fn [[bytes used]] (String. bytes 0 used "UTF-8")) chunks)))
(defn encode [encoder s] (encoder (.getBytes s "US-ASCII")))

(deftest byte-and-geometry-parity
  (doseq [n [0 1 243 244 245 255 256 257 65523 65524 65525 65535 65536 65537 131080]
          pattern ["a" "abc\"/\\\n" (apply str (map char (range 128)))]]
    (let [s (subs (apply str (repeat (inc (quot n (count pattern))) pattern)) 0 n)
          [old-size old-chunks :as old] (encode baseline s)
          [new-size new-chunks :as new] (encode candidate s)]
      (is (= old-size new-size))
      (is (= (mapv (fn [[a used]] [(alength a) used]) old-chunks)
             (mapv (fn [[a used]] [(alength a) used]) new-chunks)))
      (is (= (text old) (text new)))
      (is (= s (get (json/read-str (text new)) "sql"))))))

(defn snapshot []
  {:wall (System/nanoTime) :cpu (host/cpu-nanos) :gc (host/gc-count)})
(defn quantiles [xs]
  (let [xs (vec (sort xs)) n (count xs)]
    (mapv #(nth xs (dec (long (Math/ceil (* % n))))) [0.5 0.9 0.95 0.99])))
(defn -main [& [out]]
  (let [r (run-tests 'perf.wal-clean-runs)]
    (when-not (zero? (+ (:fail r) (:error r))) (System/exit 1)))
  ;; SQL-shaped ASCII with frequent JSON quotes/escapes and a long clean field.
  ;; A component screen only, not native persistence or real SDK throughput.
  (let [row "[\"0123456789abcdef0123456789abcdef\",\"a sufficiently long span name\",1234567890,{\"key\":\"value\\nquoted\"}]\n"
        sql (.getBytes (apply str (repeat 80000 row)) "US-ASCII")
        expected (first (baseline sql))
        results (atom [])]
    (doseq [[arm encoder] [[:baseline baseline] [:candidate candidate]]]
      (dotimes [_ 3] (encoder sql)))
    (doseq [[arm encoder] [[:baseline baseline] [:candidate candidate]
                          [:candidate candidate] [:baseline baseline]]]
      (dotimes [_ 8]
        (let [before (snapshot) result (encoder sql) after (snapshot)]
          (when-not (= expected (first result)) (throw (ex-info "size mismatch" {})))
          (swap! results conj {:arm arm :nanos (- (:wall after) (:wall before))
                               :cpu-nanos (- (:cpu after) (:cpu before))
                               :gc-count (- (:gc after) (:gc before))}))))
    (let [report {:scope :synthetic-wal-component :input-bytes (alength sql)
                  :output-bytes expected :samples @results
                  :quantiles-nanos (into {} (for [arm [:baseline :candidate]]
                                             [arm (quantiles (map :nanos (filter #(= arm (:arm %)) @results)))]))}]
      (spit out (pr-str report))
      (prn (dissoc report :samples)))))

(apply -main *command-line-args*)
