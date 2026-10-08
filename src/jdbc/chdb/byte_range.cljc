(ns jdbc.chdb.byte-range
  "Internal generic byte-range search with a portable compatibility oracle."
  #?(:jolt (:require [clojure.java.io :as io] [jolt.scheme :as scheme])))

(defn- portable-index [^bytes bytes target ^long start ^long end]
  (loop [index start]
    (if (or (= index end) (= target (bit-and 255 (aget bytes index))))
      index
      (recur (unchecked-inc index)))))

#?(:jolt
   (do
     (defmacro ^:private native-source []
       ;; Missing resources/support select the portable path, not startup failure.
       (when-let [resource (io/resource "jdbc/chdb/byte_range.ss")]
         (slurp resource)))
     (def ^:private source (native-source))
     (defn- qualified-search [search]
       (let [bytes (byte-array (map unchecked-byte [0 10 127 128 255 10]))]
         (when (every? (fn [[target start end]]
                         (= (portable-index bytes target start end)
                            (search bytes target start end)))
                       [[0 0 6] [10 0 6] [10 2 6] [127 0 6]
                        [128 0 6] [255 0 6] [1 0 6] [10 2 2]
                        [10 6 6]])
           search)))
     (def ^:private native-search
       (delay
         (try
           (when (string? source)
             ;; Record accessors are Chez syntax, not scheme/proc bindings.
             ;; Compile the whole resource and qualify actual calls instead.
             (qualified-search (scheme/eval-string source)))
           (catch Throwable _ nil))))))

(defn native-enabled?
  "Successful source-mode behavioral qualification only; not an AOT guarantee."
  [] #?(:jolt (some? @native-search) :clj false))

(defn index-of-byte
  "Find unsigned target in [start,end), or return end. Internal valid-range API.
  Unsupported native backings use portable reads. Input is borrowed read-only
  during the call; no mutable aliases or scan results are retained."
  [bytes target start end]
  (when-not (and (bytes? bytes) (integer? target) (<= 0 target 255)
                (integer? start) (integer? end) (<= 0 start end (alength bytes)))
    (throw (ex-info "Invalid byte search range or target" {:type ::invalid-range})))
  #?(:jolt
     (if-let [search @native-search]
       (let [result (search bytes target start end)]
         (if (false? result) (portable-index bytes target start end) result))
       (portable-index bytes target start end))
     :clj (portable-index bytes target start end)))
