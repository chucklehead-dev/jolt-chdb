(ns jdbc.chdb-stream-storage
  "Storage selection for the bounded confirmed-export qualification only.
  Never print returned S3 options or exception causes; credentials stay in memory."
  (:require [clojure.string :as str]
            [jdbc.chdb.durable.local-posix :as local]
            [jdbc.chdb.durable.s3-curl :as s3]))

(defn- invalid! []
  (throw (ex-info "Invalid storage qualification configuration"
                  {:type ::invalid-configuration})))

(defn kind [lookup]
  (case (lookup "CHDB_BENCH_STORAGE")
    nil :local
    "local" :local
    "s3" :s3
    (invalid!)))

(defn configuration
  "Pure validation. S3 is explicit, scoped to an isolated GitHub CI run prefix,
  and never falls back to local storage. No offending values enter errors."
  [selected lookup]
  (case selected
    :local nil
    :s3
    (let [required (fn [name]
                     (let [value (lookup name)]
                       (when (or (not (string? value)) (str/blank? value)) (invalid!))
                       value))
          region (required "JOLT_CHDB_S3_REGION")
          bucket (required "JOLT_CHDB_S3_BUCKET")
          prefix (required "JOLT_CHDB_S3_PREFIX")
          endpoint (required "JOLT_CHDB_S3_ENDPOINT")]
      (when-not (and (re-matches #"[a-z]+(?:-[a-z]+)+-[0-9]+" region)
                     (= endpoint (str "https://s3." region ".amazonaws.com"))
                     (re-matches #"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]" bucket)
                     (re-matches #"ci/jolt-chdb/[0-9]+-[0-9]+/stream-26-9(?:/[a-z0-9-]+)?" prefix))
        (invalid!))
      {:endpoint endpoint :bucket bucket :prefix prefix :region region
       :access-key (required "JOLT_CHDB_S3_ACCESS_KEY")
       :secret-key (required "JOLT_CHDB_S3_SECRET_KEY")
       :session-token (required "JOLT_CHDB_S3_SESSION_TOKEN")})
    (invalid!)))

(defn options [root phase lookup]
  (let [selected (kind lookup)
        config (configuration selected lookup)
        scratch (str root "/scratch-" phase)]
    (when-not (.mkdirs (java.io.File. scratch))
      (when-not (.isDirectory (java.io.File. scratch)) (invalid!)))
    {:namespace-backend (case selected
                          :local (local/local-backend (str root "/objects"))
                          :s3 (s3/s3-backend config))
     :object-id "typed-compact-prefix" :scratch-parent scratch}))
