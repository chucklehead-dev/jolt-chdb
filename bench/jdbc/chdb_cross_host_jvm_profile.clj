(ns jdbc.chdb-cross-host-jvm-profile
  (:import [java.nio.file Paths]
           [jdk.jfr Configuration Recording]))

(defn start! [path]
  (when path
    (doto (Recording. (Configuration/getConfiguration "profile"))
      (.setToDisk true)
      (.setDestination (Paths/get path (make-array String 0)))
      (.start))))

(defn stop! [recording]
  (when recording
    (try
      (.stop recording)
      (finally (.close recording)))))
