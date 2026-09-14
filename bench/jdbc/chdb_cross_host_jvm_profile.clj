(ns jdbc.chdb-cross-host-jvm-profile
  (:import [java.nio.file Paths]
           [jdk.jfr Configuration Recording]))

(defn start! [path]
  (when path
    (doto (Recording. (Configuration/getConfiguration "profile"))
      (.setToDisk true)
      (.setDestination (Paths/get path (make-array String 0)))
      ;; Default JFR profiles retain ambient environment values and process
      ;; command lines. They are irrelevant to this benchmark and may contain
      ;; credentials or unrelated payloads, so fail closed by disabling them
      ;; before the recording starts.
      (.disable "jdk.InitialEnvironmentVariable")
      (.disable "jdk.InitialSystemProperty")
      (.disable "jdk.SystemProcess")
      (.start))))

(defn stop! [recording]
  (when recording
    (try
      (.stop recording)
      (finally (.close recording)))))
