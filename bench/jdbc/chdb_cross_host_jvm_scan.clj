(ns jdbc.chdb-cross-host-jvm-scan)

(defn scan-record-boundaries
  "JVM primitive-loop control for the shared portable WAL boundary scanner."
  [^bytes bytes ^long ordinal]
  (let [length (long (alength bytes))]
    (loop [index (long 0)
           start (long 0)
           current (long 1)
           record-count (long 0)
           target-start (long -1)
           target-end (long -1)]
      (if (= index length)
        {:record-count record-count
         :record-start target-start
         :record-end-exclusive target-end
         :bytes-visited index}
        (if (= (byte 10) (aget bytes index))
          (let [target? (= current ordinal)]
            (recur (unchecked-inc index)
                   (unchecked-inc index)
                   (unchecked-inc current)
                   (unchecked-inc record-count)
                   (if target? start target-start)
                   (if target? index target-end)))
          (recur (unchecked-inc index) start current record-count
                 target-start target-end))))))
