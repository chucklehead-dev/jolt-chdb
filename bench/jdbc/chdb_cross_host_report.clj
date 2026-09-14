(ns jdbc.chdb-cross-host-report)

(def schema-version 3)
(def max-samples 10000)

(defn- fail! [message data]
  (throw (ex-info message (assoc data :type ::invalid-report))))

(defn percentile
  "Nearest-rank percentile for a non-empty collection of nonnegative nanos."
  [samples fraction]
  (when-not (and (sequential? samples) (seq samples)
                 (<= (count samples) max-samples)
                 (every? #(and (integer? %) (not (neg? %))) samples)
                 (number? fraction) (<= 0.0 fraction 1.0))
    (fail! "invalid benchmark samples or percentile" {}))
  (let [ordered (vec (sort samples))
        rank (max 1 (long (Math/ceil (* (double fraction)
                                        (count ordered)))))]
    (nth ordered (dec rank))))

(defn distribution [samples]
  (let [samples (vec samples)
        n (count samples)]
    {:count n
     :total-ns (reduce + 0 samples)
     :p50-ns (percentile samples 0.50)
     :p95-ns (percentile samples 0.95)
     :p99-ns (percentile samples 0.99)
     :max-ns (reduce max samples)
     :p50-supported? (>= n 2)
     :p95-supported? (>= n 20)
     :p99-supported? (>= n 100)
     :samples-ns samples}))

(declare canonical)

(defn canonical [value]
  (cond
    (map? value) (into (sorted-map)
                       (map (fn [[key item]] [key (canonical item)]))
                       value)
    (vector? value) (mapv canonical value)
    (sequential? value) (mapv canonical value)
    :else value))

(defn render [report]
  (str (pr-str (canonical report)) "\n"))
