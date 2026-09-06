(ns jdbc.chdb-durable-file-allocation
  "Manual Jolt allocation probe for payload-size scaling of Durable file I/O."
  (:require [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.local-posix :as local]
            [jolt.host :as host])
  (:import [java.nio.file Files Path Paths]
           [java.nio.file.attribute FileAttribute]))

(defn- allocation-total []
  ;; Live heap plus cumulative reclaimed bytes is a cumulative-allocation proxy
  ;; in this isolated process. This is evidence, not a stable CI assertion.
  (+ (host/bytes-allocated) (host/gc-bytes)))

(defn- measure [f]
  (System/gc)
  (let [before (allocation-total)
        result (f)]
    (System/gc)
    {:result result
     :allocation-delta (- (allocation-total) before)}))

(def ^:private target-max-growth (* 2 1024 1024))

(defn- allocation-growth [small large]
  (let [growth (- (:allocation-delta large) (:allocation-delta small))]
    {:bytes growth
     :target-less-than target-max-growth
     :passes? (< growth target-max-growth)}))

(defn -main [root-text output-text small-text large-text]
  (let [root (Paths/get root-text (into-array String []))
        output (Paths/get output-text (into-array String []))
        small (Paths/get small-text (into-array String []))
        large (Paths/get large-text (into-array String []))
        suffix (str (random-uuid))
        small-key (str "bench/small-" suffix ".bin")
        large-key (str "bench/large-" suffix ".bin")
        store (local/local-backend root)]
    (Files/createDirectories output (into-array FileAttribute []))
    ;; Warm namespace, stream, FFI, lock, and envelope paths before sampling.
    (backend/put-file-if-absent!
     store (str "bench/warmup-" suffix ".bin") small)
    (let [small-upload
          (measure #(backend/put-file-if-absent! store small-key small))
          large-upload
          (measure #(backend/put-file-if-absent! store large-key large))
          small-target (.resolve ^Path output (str "small-" suffix ".bin"))
          large-target (.resolve ^Path output (str "large-" suffix ".bin"))
          small-download
          (measure #(backend/download-to-file!
                     store small-key small-target))
          large-download
          (measure #(backend/download-to-file!
                     store large-key large-target))]
      (println
       (pr-str
        {:source-bytes {:small (Files/size small) :large (Files/size large)}
         :upload {:small small-upload :large large-upload}
         :download {:small small-download :large large-download}
         :allocation-growth
         {:upload (allocation-growth small-upload large-upload)
          :download (allocation-growth small-download large-download)}})))))
