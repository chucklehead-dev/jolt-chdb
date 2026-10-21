(require '[clojure.edn :as edn])
(let [[prefix lanes-text] *command-line-args*
      lanes (parse-long lanes-text)
      w (edn/read-string (slurp (str prefix ".writer.edn")))
      r (edn/read-string (slurp (str prefix ".reader.edn")))
      batches (:batches w) stats (:stats w)
      n (count batches)]
  (assert (contains? #{1 2 4} lanes))
  (assert (= [160000 5000 32 lanes lanes 1 20000 625 8 "saturated"]
             (mapv w [:rows :batch-size :rounds :lanes :distinct-exporter-count
                      :shared-connection-owner-count :total-queue-capacity
                      :trace-count :siblings-per-trace :arrival])))
  (assert (= lanes (count stats)))
  (assert (= 20000 (* lanes (:queue-capacity-per-lane w))))
  (assert (<= 5000 (:queue-capacity-per-lane w)))
  (let [cpu (:os-process-window w)]
    (assert (pos? (:ticks-per-second cpu)))
    (assert (pos? (+ (:user-ticks-delta cpu) (:system-ticks-delta cpu))))
    (assert (every? pos? (map #(get-in cpu [:after %]) [:rss-kib :highwater-kib :thread-count]))))
  (assert (= 160000 (reduce + (map :attempted-span-count stats))
                   (reduce + (map :exported-span-count stats))
                   (reduce + (map :rows batches))))
  (assert (every? #(= [0 0 0] (mapv % [:queue-size :failed-span-count :dropped-count])) stats))
  (assert (every? pos? (:expected-lane-counts w)))
  (assert (= (:expected-lane-counts w) (mapv :exported-span-count stats)))
  (assert (and (:per-trace-counts-checked w) (:per-trace-callback-order-checked w)))
  (assert (= (+ 3 n) (:stream-calls w)))
  (assert (every? #(and (<= 1 (:rows %) 5000) (:ok %)) batches))
  (assert (= (set (range lanes)) (set (map :lane batches))))
  (doseq [[_ events] (group-by :lane batches)]
    (assert (every? (fn [[a b]] (<= (:end-nanos a) (:start-nanos b)))
                    (partition 2 1 (sort-by :start-nanos events)))))
  (when (= 1 lanes)
    (assert (= 32 n))
    (assert (every? #(= 5000 (:rows %)) batches)))
  (assert (and (false? (:force-flush-before-measurement w)) (false? (:tail-qualified w))))
  (assert (and (:passed r) (:full-physical-rows-equal r) (:typed-values-status r)
               (= [175000 5000 625 35] (mapv r [:rows :groups :trace-count :per-record-multiplicity]))))
  (prn {:verified true :scope :rich-local-sdk-fixed-trace-affinity-research
        :lanes lanes :callbacks n :recovered-rows 175000
        :source-overlay false :old-stack-independent-reader true
        :confirmed-window-rows-per-second (:confirmed-rows-per-second w)
        :tail-qualified false :end-to-end-span-latency-measured false}))
