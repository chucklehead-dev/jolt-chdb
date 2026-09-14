(ns jdbc.chdb-cross-host-jvm-metrics
  (:import [java.lang.management ManagementFactory]
           [com.sun.management ThreadMXBean]))

(defn- heap-snapshot []
  (let [usage (.getHeapMemoryUsage (ManagementFactory/getMemoryMXBean))]
    {:heap-used-bytes (.getUsed usage)
     :heap-committed-bytes (.getCommitted usage)}))

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
               :heap :jvm-process-heap
               :gc :process-global-jvm-counters}
     :metadata
     {:vm-name (System/getProperty "java.vm.name")
      :vm-vendor (System/getProperty "java.vm.vendor")
      :vm-version (System/getProperty "java.vm.version")
      :gc-beans (mapv #(.getName %) (ManagementFactory/getGarbageCollectorMXBeans))}
     :snapshot
     (fn []
       (cond-> (merge (gc-snapshot) (heap-snapshot))
         cpu? (assoc :calling-thread-cpu-ns
                     (.getCurrentThreadCpuTime thread-bean))
         allocation? (assoc :managed-allocation-bytes
                            (.getThreadAllocatedBytes
                             allocation-bean (.getId (Thread/currentThread))))))}))
