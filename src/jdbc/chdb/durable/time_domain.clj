(ns jdbc.chdb.durable.time-domain
  "Validated numeric domain for Durable lease times at adapter boundaries.")

(def max-safe-epoch-milliseconds
  "Largest epoch-millisecond magnitude represented exactly as a JSON integer
  across the supported runtimes. This is a local adapter safety policy, not a
  Protocol V1 limit."
  9007199254740991)

(def max-wire-epoch-seconds
  "Largest active V1 wire expiry whose millisecond magnitude remains in the
  cross-runtime safe-integer domain. Fractional epoch seconds remain valid."
  9007199254740.991M)

(defn finite-number? [value]
  (and (number? value)
       (= value value)
       (not= value ##Inf)
       (not= value ##-Inf)))

(defn supported-millisecond-magnitude? [value]
  (and (finite-number? value)
       (not (neg? value))
       (<= value max-safe-epoch-milliseconds)))

(defn supported-nonnegative-milliseconds? [value]
  (and (supported-millisecond-magnitude? value)
       (zero? (rem value 1))))

(defn supported-positive-milliseconds? [value]
  (and (supported-nonnegative-milliseconds? value)
       (pos? value)))

(defn supported-wire-epoch-seconds? [value]
  (and (finite-number? value)
       (not (neg? value))
       (<= value max-wire-epoch-seconds)))
