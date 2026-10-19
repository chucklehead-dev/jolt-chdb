(ns jdbc.chdb.byte-output
  "Internal synchronous byte-prefix output; borrowed stream stays caller-owned."
  #?(:jolt (:require [clojure.java.io :as io] [jolt.scheme :as scheme]))
  (:import [java.io OutputStream]))

#?(:jolt
   (do
     (defmacro ^:private kernel-source []
       (if-let [resource (io/resource "jdbc/chdb/output_prefix_write.ss")]
         (slurp resource)
         (throw (ex-info "Missing output-prefix resource" {}))))
     (def ^:private source (kernel-source))
     (def ^:private output-prefix
       ;; Only loading may decline. Never retry an attempted/partial write.
       (delay (try (scheme/eval-string source) (catch Throwable _ (fn [& _] false)))))
     (defn- try-write! [output bytes length]
       ;; Invalid arguments retain the ordinary method's error/closed ordering.
       (and (bytes? bytes) (integer? length) (<= 0 length (alength bytes))
            (@output-prefix output bytes length)))))

(defn write-prefix!
  "Write bytes[0,length), borrowing source and stream only during the call.
  Never flush or close. Only qualified native file streams accelerate;
  custom streams, memory ports and unsupported layouts keep ordinary
  three-argument write dispatch, including errors and zero-length effects."
  [output bytes length]
  #?(:jolt (when-not (try-write! output bytes length)
             (.write ^OutputStream output bytes 0 length))
     :clj (.write ^OutputStream output bytes 0 length))
  nil)
