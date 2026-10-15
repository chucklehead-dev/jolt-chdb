(ns jdbc.chdb-typed-storage-support
  "Benchmark-only storage selection; no application defaults are changed."
  (:require [jdbc.core :as jdbc]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.local-posix :as local]
            [jdbc.chdb.durable.s3-curl :as curl]
            [jdbc.chdb.durable.s3-writer :as s3-writer]
            [jdbc.chdb-typed-storage-profile :as profile]))

(defn- invalid! [field]
  (throw (ex-info "Invalid typed storage benchmark configuration"
                  {:type ::configuration :field field})))

(defn arm []
  (case (or (System/getenv "SDK_STORAGE_ARM") "local")
    "local" :local
    "s3" :s3
    (invalid! :storage-arm)))

(defn- required-env [name field]
  (let [value (System/getenv name)]
    (when (or (nil? value) (= "" value)) (invalid! field))
    value))

(defn selected-profile []
  (profile/select-profile (arm) (System/getenv "SDK_S3_EXPECT_WAIT_MS")))

(defn report-profile []
  (select-keys (selected-profile) [:profile :expect-100-timeout-ms]))

(defn s3-options []
  ;; CI may write only inside its unique benchmark run namespace. Credentials
  ;; stay inside the backend/transport; none enter reports or error data.
  (let [prefix (required-env "JOLT_CHDB_S3_PREFIX" :prefix)]
    (when-not (re-matches #"ci/jolt-chdb/[0-9]+-[0-9]+/typed-window" prefix)
      (invalid! :prefix))
    (merge (select-keys (selected-profile) [:expect-100-timeout-ms])
           {:endpoint (required-env "JOLT_CHDB_S3_ENDPOINT" :endpoint)
            :bucket (required-env "JOLT_CHDB_S3_BUCKET" :bucket)
            :prefix prefix
            :region (required-env "JOLT_CHDB_S3_REGION" :region)
            :access-key (required-env "JOLT_CHDB_S3_ACCESS_KEY" :access-key)
            :secret-key (required-env "JOLT_CHDB_S3_SECRET_KEY" :secret-key)
            :session-token (required-env "JOLT_CHDB_S3_SESSION_TOKEN" :session-token)})))

(defn options [root phase]
  (let [scratch (str root "/scratch-" phase)
        _ (.mkdirs (java.io.File. scratch))
        basic {:object-id (:object-id (selected-profile)) :scratch-parent scratch}]
    (case (arm)
      :local (assoc basic :namespace-backend (local/local-backend (str root "/objects")))
      :s3 (if (= phase "writer") basic
              (assoc basic :namespace-backend (curl/s3-backend (s3-options)))))))

(defn with-writer! [options on-renew! f]
  (case (arm)
    :local
    (with-open [connection (jdbc/connection (durable/writer-dbspec options))]
      (f connection))
    :s3
    (let [renew! control/renew!
          ;; The production scope alone routes renewal over its second owner.
          ;; Retain checkpoint/JSON observers, but never replace that routing
          ;; with the local benchmark's default-store renewal operation.
          selected (-> options
                       (dissoc :namespace-backend)
                       (update :operations dissoc :renew-control!))]
      ;; This isolated benchmark process observes completed real renewals.
      ;; It neither fabricates a control result nor extends the handle scope.
      (with-redefs [control/renew! (fn [& args]
                                   (let [result (apply renew! args)]
                                     (when on-renew! (on-renew!))
                                     result))]
        (s3-writer/with-connection! (s3-options) selected f)))))
