(ns jdbc.chdb-durable-time-domain-test
  (:require [clojure.data.json :as json]
            [jdbc.chdb.durable.head :as head]
            [jdbc.chdb.durable.time-domain :as time-domain]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- error-type [f]
  (try (f) nil (catch Throwable error (:type (ex-data error)))))

(def ^:private valid-head
  {"protocol" {"version" 1 "reader_features" [] "writer_features" []}
   "engine" {"name" "chdb" "version" "26.7.2-rc.2"
             "backup_format" 1 "min_reader" "26.7.2-rc.2"}
   "lease" {"generation" 1 "owner" "writer" "instance" "attempt"
            "expires_at" 1788230400.1255M}
   "manifest" {"db" "default" "base" nil "wal" [] "seq" 0}})

(defn run-checks! []
  (reset! failures 0)
  (println "Durable cross-runtime time domain")
  (check "whole public milliseconds include the exact safe boundary"
         [true true true]
         [(time-domain/supported-positive-milliseconds? 1M)
          (time-domain/supported-nonnegative-milliseconds? 0M)
          (time-domain/supported-positive-milliseconds?
           time-domain/max-safe-epoch-milliseconds)])
  (check "fractional and one-over public millisecond mutants are rejected"
         [false false]
         [(time-domain/supported-positive-milliseconds? 1.5M)
          (time-domain/supported-positive-milliseconds?
           (inc time-domain/max-safe-epoch-milliseconds))])
  (let [boundary (assoc-in valid-head ["lease" "expires_at"]
                           time-domain/max-wire-epoch-seconds)
        one-over (assoc-in boundary ["lease" "expires_at"]
                           (+ time-domain/max-wire-epoch-seconds 0.001M))
        extreme (assoc-in boundary ["lease" "expires_at"]
                          (bigdec "1e308"))]
    (check "ordinary fractional seconds remain non-vacuously supported"
           valid-head (head/decode (head/encode valid-head) :writer))
    (check "the exact wire ceiling round-trips"
           boundary (head/decode (head/encode boundary) :writer))
    (check "the finite-only mutant accepts the extreme witness"
           true (time-domain/finite-number? (bigdec "1e308")))
    (check "raw decode rejects one millisecond over the ceiling"
           ::head/corrupt
           (error-type #(head/decode (json/write-str one-over) :writer)))
    (check "raw decode rejects the finite extreme mutant"
           ::head/corrupt
           (error-type #(head/decode (json/write-str extreme) :writer))))
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable time-domain checks failed")
                    {:failures @failures})))
  (println "all Durable time-domain checks passed")
  true)

(defn -main [& _]
  (run-checks!))
