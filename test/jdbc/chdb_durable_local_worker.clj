(ns jdbc.chdb-durable-local-worker
  (:require [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.local-posix :as local]
            [jolt.ffi :as ffi])
  (:import [java.nio.file Paths]))

(defn- one-byte [text]
  (byte-array [(parse-long text)]))

(defn -main [root operation & arguments]
  (let [store (local/local-backend root)]
    (case operation
      "create"
      (println (:etag (backend/put-bytes-if-absent!
                       store "head.json" (one-byte (first arguments)))))

      "replace"
      (println (:status (backend/replace-if-match!
                         store "head.json" (one-byte (second arguments))
                         (first arguments))))

      "put-file"
      (println (:status (backend/put-file-if-absent!
                         store "checkpoints/one.tar" (first arguments))))

      "download"
      (println (pr-str (backend/download-to-file!
                        store "checkpoints/one.tar" (first arguments))))

      "read"
      (println (first (backend/get-bytes store "head.json")))

      "read-error-type"
      (println
       (try
         (backend/get-bytes store (first arguments))
         nil
         (catch Throwable error (:type (ex-data error)))))

      "partial-write-probe"
      (let [payload (byte-array [10 11 12 13 14 15 16 17 18])
            outcomes (atom [[-1 4] [2 0] [3 0] [4 0]])
            requests (atom [])
            emitted (atom [])
            write-all-with! @(ns-resolve
                              'jdbc.chdb.durable.local-posix
                              'write-all-with!)]
        (ffi/with-alloc [buffer (alength payload)]
          (ffi/write-array buffer payload)
          (write-all-with!
           (fn [_ pointer requested]
             (swap! requests conj requested)
             (let [[n errno] (first @outcomes)]
               (swap! outcomes next)
               (when (pos? n)
                 (swap! emitted into (vec (ffi/read-array pointer n))))
               [n errno]))
           0 buffer (alength payload)))
        (if (and (= [9 9 7 4] @requests)
                 (= (vec payload) @emitted)
                 (empty? @outcomes))
          (println :ok)
          (throw (ex-info "Partial-write probe failed"
                          {:requests @requests :emitted @emitted}))))

      "hold-lock"
      (let [root-path (Paths/get root (into-array String []))
            lock-path (.resolve (.toRealPath root-path
                                             (make-array java.nio.file.LinkOption 0))
                                ".jchdb.lock")]
        (backend/with-exclusive-lock
         (local/posix-durability) lock-path
         #(do (println :locked)
              (flush)
              (Thread/sleep (parse-long (first arguments))))))

      (throw (ex-info "Unknown local backend worker operation"
                      {:operation operation})))))
