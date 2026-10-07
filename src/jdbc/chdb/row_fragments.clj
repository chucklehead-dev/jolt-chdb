(ns jdbc.chdb.row-fragments
  "Source-only, behavior-qualified fragment transfer. No shared payload state."
  (:require [clojure.java.io :as io] [jolt.scheme :as scheme]))

(defmacro ^:private source []
  (when-let [resource (io/resource "jdbc/chdb/row_fragments.ss")]
    (slurp resource)))

(def ^:private code (source))

(def ^:private transfer
  (delay
    (try
      (when (string? code)
        (let [f (scheme/eval-string code)
              row (java.io.StringWriter.) batch (StringBuilder.)]
          (.append batch "prefix:")
          (.append row "é")
          (.toString row) ;; exercise materialized base plus pending fragments
          (.append row "😀\n")
          (when (and (= 7 (f row batch 6))
                     (= "prefix:" (.toString batch))
                     (= 7 (f row batch 7))
                     (= "prefix:é😀\n" (.toString batch))
                     (= "é😀\n" (.toString row)))
            (.append row "late")
            (when (= "prefix:é😀\n" (.toString batch)) f))))
      (catch Throwable _ nil))))

(defn append-row!
  "Return exact bytes, or false to decline. Overflow does not append anything.
  Row and batch are caller-owned; no callbacks run in this operation."
  [row batch remaining]
  (if-let [f @transfer] (f row batch remaining) false))
