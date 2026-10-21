(ns jdbc.chdb.byte-copy
  "Internal synchronous byte-prefix copies into caller-owned foreign storage."
  (:require [clojure.java.io :as io] [jolt.scheme :as scheme] [jolt.ffi :as ffi]))

(defmacro ^:private kernel-source []
  (if-let [resource (io/resource "jdbc/chdb/byte_prefix_copy.ss")]
    (slurp resource)
    (throw (ex-info "Missing byte-copy resource" {}))))
(def ^:private source (kernel-source))
(def ^:private prefix-copy
  ;; An unavailable private runtime adapter keeps the ordinary FFI path.
  ;; Catch only loading/qualification, NEVER a copy that may already have run.
  (delay (try (scheme/eval-string source) (catch Throwable _ (fn [& _] false)))))

(defn write-prefix!
  "Copy bytes[0,length) into valid caller-owned foreign storage. Return length.
  No destination allocation/lifetime checks or source ownership transfer are
  implied. Source must not race with this call. Unsupported runtime/backing
  uses ordinary ffi/write-array."
  [pointer bytes length]
  (when-not (and (bytes? bytes) (integer? length) (<= 0 length (alength bytes)))
    (throw (ex-info "Invalid byte-copy range" {:type ::invalid-range})))
  (if (zero? length) 0
    (let [result (@prefix-copy pointer bytes length)]
      (if (false? result) (ffi/write-array pointer bytes 0 length) result))))
