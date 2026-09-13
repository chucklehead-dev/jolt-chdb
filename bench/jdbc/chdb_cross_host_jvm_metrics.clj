(ns jdbc.chdb-cross-host-jvm-metrics
  (:import [java.lang.management ManagementFactory]
           [com.sun.management ThreadMXBean]))

(defn- gc-snapshot []
  (reduce
   (fn [result bean]
     (-> result
         (update :gc-count + (max 0 (.getCollectionCount bean)))
         (update :gc-time-ms + (max 0 (.getCollectionTime bean)))))
   {:gc-count 0 :gc-time-ms 0}
   (ManagementFactory/getGarbageCollectorMXBeans)))

(defn provider []
  (let [thread-bean (ManagementFactory/getThreadMXBean)
        cpu? (.isCurrentThreadCpuTimeSupported thread-bean)
        allocation? (instance? ThreadMXBean thread-bean)
        allocation-bean (when allocation? ^ThreadMXBean thread-bean)]
    (when (and cpu? (not (.isThreadCpuTimeEnabled thread-bean)))
      (.setThreadCpuTimeEnabled thread-bean true))
    (when (and allocation? (not (.isThreadAllocatedMemoryEnabled allocation-bean)))
      (.setThreadAllocatedMemoryEnabled allocation-bean true))
    {:support {:calling-thread-cpu (if cpu? :supported :unsupported)
               :managed-allocation (if allocation?
                                     :jvm-thread-allocated-bytes
                                     :unsupported)
               :gc :process-global-jvm-counters}
     :snapshot
     (fn []
       (cond-> (gc-snapshot)
         cpu? (assoc :calling-thread-cpu-ns
                     (.getCurrentThreadCpuTime thread-bean))
         allocation? (assoc :managed-allocation-bytes
                            (.getThreadAllocatedBytes
                             allocation-bean (.getId (Thread/currentThread))))))}))
