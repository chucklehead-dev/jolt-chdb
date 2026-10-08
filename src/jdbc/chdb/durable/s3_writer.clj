(ns jdbc.chdb.durable.s3-writer
  "Experimental lexical S3 writer with independently owned renewal transport."
  (:require [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb.durable.s3-curl :as curl]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.core :as jdbc]))

(defn- invalid! [message]
  (throw (ex-info message {:type ::invalid-options})))

(defn- with-owned! [s3-options writer-options f open! close!]
  (when-not (and (map? s3-options) (map? writer-options) (fn? f))
    (invalid! "S3 options, writer options and callback are required"))
  (when (and (some? (:operations writer-options))
             (not (map? (:operations writer-options))))
    (invalid! "Writer operations must be a map"))
  (when (contains? s3-options :request!)
    (invalid! "The scoped writer owns its transport request functions"))
  (when (or (contains? writer-options :store)
            (contains? writer-options :namespace-backend)
            (contains? (:operations writer-options) :renew-control!))
    (invalid! "The scoped writer owns its namespace and renewal routing"))
  (curl/with-reused-transport!
    (fn [send-data]
      (curl/with-reused-transport!
        (fn [send-renewal]
          (let [namespace (s3/s3-backend (assoc s3-options :request! send-data))
                renewal-store (backend/object-backend
                                (s3/s3-backend (assoc s3-options :request! send-renewal))
                                (:object-id writer-options))
                w (open!
                    (-> writer-options
                        (assoc :namespace-backend namespace)
                        (update :operations assoc :renew-control!
                                (fn [_ token expiry-seconds retry-options]
                                  (control/renew! renewal-store token expiry-seconds
                                                  retry-options)))))
                outcome (try {:value (f w)} (catch Throwable error {:error error}))
                closed (try (close! w) nil (catch Throwable error error))]
            (if-let [error (:error outcome)]
              (throw error)
              (if closed (throw closed) (:value outcome)))))))))

(defn with-writer!
  "EXPERIMENTAL: call f with a Durable writer, then close/join it before return.

  s3-options describes one S3 namespace. writer-options supplies object-id,
  owner/instance/database and ordinary Durable options. Two serial curl owners
  share that namespace: data/publication and independent lease renewal.
  Do not retain the writer or launch unjoined work outside f. Credential changes
  require a new scope; no automatic rotation or general shared pool is provided.
  f's error remains primary if writer close also fails. A close failure otherwise
  propagates. Defaults outside this explicit helper remain unchanged."
  [s3-options writer-options f]
  (with-owned! s3-options writer-options f durable/open-writer! writer/close!))

(defn with-connection!
  "EXPERIMENTAL: call f with a normal Durable JDBC writer connection.

  The outer scope owns the connection and both transports. Pass the connection
  as borrowed to consumers such as the chDB exporter, and flush/shut them down
  before f returns. Do not return a live connection or unjoined producers.
  Connection close joins the writer before either curl owner retires. Other
  options/error/credential contracts are the same as with-writer!."
  [s3-options writer-options f]
  (with-owned! s3-options writer-options f
               #(jdbc/connection (durable/writer-dbspec %))
               #(.close %)))
