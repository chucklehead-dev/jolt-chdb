(ns jdbc.chdb-cross-host-jolt-metrics
  (:require [jolt.host :as host]))

(defn provider []
  {:support {:calling-thread-cpu :supported
             :managed-allocation :jolt-scheme-allocation-counter
             :gc :supported}
   :snapshot
   (fn []
     {:calling-thread-cpu-ns (host/cpu-nanos)
      :gc-count (host/gc-count)
      :gc-cpu-ns (host/gc-cpu-nanos)
      :gc-real-ns (host/gc-real-nanos)
      ;; live heap + cumulative collected bytes is Jolt's allocation counter.
      :managed-allocation-bytes (+ (host/bytes-allocated) (host/gc-bytes))})})
