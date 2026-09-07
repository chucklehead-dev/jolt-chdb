(ns jdbc.chdb.durable.policy
  "Fail-closed Durable V1 admission policy over chdb-core query analysis."
  (:require [jdbc.chdb.native :as native]))

(def ^:private query-classes
  #{:read-only :mutating :mutating-global :control :unknown})

(defn- reject! [operation reason analysis]
  (throw (ex-info "SQL is not admissible for this Durable operation"
                  {:type ::rejected
                   :operation operation
                   :reason reason
                   :analysis analysis})))

(defn- validate-analysis! [operation analysis]
  (when-not (and (map? analysis)
                 (contains? query-classes (:query-class analysis))
                 (integer? (:statement-count analysis))
                 (not (neg? (:statement-count analysis)))
                 (boolean? (:has-secrets analysis))
                 (boolean? (:writes-only-target-database analysis))
                 (boolean? (:changes-database-lifecycle analysis)))
    (reject! operation :invalid-analysis analysis))
  analysis)

(defn authorize-query!
  "Accept only one read-only statement. Secret-bearing reads are admissible
  because query text is not written to the statement WAL; telemetry/logging
  layers remain responsible for redaction."
  [analysis]
  (validate-analysis! :query analysis)
  (when-not (= 1 (:statement-count analysis))
    (reject! :query :statement-count analysis))
  (when-not (= :read-only (:query-class analysis))
    (reject! :query :query-class analysis))
  analysis)

(defn authorize-execute!
  "Accept exactly one persistent mutation wholly contained in the managed
  database, with no database-lifecycle change and no secret-bearing text."
  [analysis]
  (validate-analysis! :execute analysis)
  (when-not (= 1 (:statement-count analysis))
    (reject! :execute :statement-count analysis))
  (when-not (= :mutating (:query-class analysis))
    (reject! :execute :query-class analysis))
  (when-not (:writes-only-target-database analysis)
    (reject! :execute :target-database analysis))
  (when (:changes-database-lifecycle analysis)
    (reject! :execute :database-lifecycle analysis))
  (when (:has-secrets analysis)
    (reject! :execute :secrets analysis))
  analysis)

(defn analyze-query!
  "Classify SQL through chdb-core and apply the Durable query gate."
  [handle sql target-database]
  (authorize-query! (native/classify-query! handle sql target-database)))

(defn analyze-execute!
  "Classify SQL through chdb-core and apply the Durable execute/WAL gate."
  [handle sql target-database]
  (authorize-execute! (native/classify-query! handle sql target-database)))
