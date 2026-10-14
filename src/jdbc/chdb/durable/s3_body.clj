(ns jdbc.chdb.durable.s3-body
  "Internal managed scratch for one synchronous libcurl upload request."
  (:import [java.io InputStream]))

(def ^:private max-chunk-bytes 65536)

(defn chunk-reader
  "Return [destination capacity] -> copied byte count (zero at EOF).

  This reader belongs to one request and is invoked serially. copy! receives
  destination, managed buffer and count, consumes the bytes synchronously, and
  must not retain that buffer or the borrowed native destination. Input closing
  remains the request owner's responsibility. No zero-copy loan is introduced."
  [^InputStream input byte-count copy!]
  (let [scratch (volatile! nil)]
    (fn [destination capacity]
      (let [limit (min capacity max-chunk-bytes byte-count)
            previous @scratch
            buffer (if (and previous (>= (alength previous) limit))
                     previous
                     (let [created (byte-array limit)]
                       (vreset! scratch created)
                       created))
            n (.read input buffer 0 limit)]
        (if (neg? n)
          0
          (do (copy! destination buffer n) n))))))
