(ns jdbc.chdb.byte-record
  "Internal opaque byte record. Implementations retain their backing storage;
  writing must not expose that storage to a caller-supplied stream.")

(defprotocol ByteRecord
  (byte-count [record] "Exact number of encoded bytes.")
  (write-to! [record output] "Synchronously append bytes; never flush or close."))
