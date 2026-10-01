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
    ;; Compile only the pure, internally bounded kernel with primitive checks
    ;; elided. The outer input guard is compiled at checked level 2 even if
    ;; the caller uses level 3; parameterize restores that level after eval.
    ;; At most twelve bytes per Scheme character plus eleven framing bytes
    ;; also bounds every fx+ used for indices and accumulated output length.
    (scheme/eval-string
     (str "(parameterize ((optimize-level 2)) (eval '"
          "(let ((kernel (parameterize ((optimize-level 3)) (eval '"
          source " (interaction-environment))))"
          "      (max-input (quotient (- (greatest-fixnum) 11) 12)))"
          "  (lambda (s)"
          "    (if (and (string? s) (<= (string-length s) max-input))"
          "        (kernel s)"
          "        (error 'durable-wal-chunks \"Invalid WAL input\"))))"
          " (interaction-environment)))"))))

(defn encode [sql]
  (@codec sql))
