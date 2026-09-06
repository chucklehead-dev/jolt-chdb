(ns jdbc.chdb-durable-local-worker
  (:require [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.local-posix :as local])
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

      "read"
      (println (first (backend/get-bytes store "head.json")))

      "read-error-type"
      (println
       (try
         (backend/get-bytes store (first arguments))
         nil
         (catch Throwable error (:type (ex-data error)))))

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
