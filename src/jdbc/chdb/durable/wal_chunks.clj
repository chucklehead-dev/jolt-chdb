(ns jdbc.chdb.durable.wal-chunks
  "Internal source-only qualification loader for exact owned WAL chunks."
  (:require [clojure.java.io :as io]
            [jolt.scheme :as scheme]))

(defmacro ^:private codec-source []
  (if-let [url (io/resource "jdbc/chdb/wal_chunks.ss")]
    (slurp url)
    (throw (ex-info "Missing owned WAL chunk codec resource" {}))))

(def ^:private source (codec-source))
(defonce ^:private codec
  (delay
    ;; No copied byte-array representation. Missing host/compiler support is
    ;; caught by the behavioral selector before choosing any prepared output.
    (scheme/proc "na-owned-bv->bytearray")
    (scheme/eval-string source)))

(defn encode [sql]
  (@codec sql))
