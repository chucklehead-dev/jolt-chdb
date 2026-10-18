(ns s3-checksum-fault-config)

(def env-names ["AWS_REGION" "AWS_ACCESS_KEY_ID" "AWS_SECRET_ACCESS_KEY" "AWS_SESSION_TOKEN"
                "TASK_BUCKET" "TASK_CHECKSUM_PREFIX" "GITHUB_RUN_ID" "GITHUB_RUN_ATTEMPT"])

(defn options [env]
  (let [region (get env "AWS_REGION") run (get env "GITHUB_RUN_ID")
        attempt (get env "GITHUB_RUN_ATTEMPT")
        prefix (get env "TASK_CHECKSUM_PREFIX")]
    (when-not (and (every? #(let [s (get env %)] (and (string? s) (not= "" s))) env-names)
                   (re-matches #"[a-z]{2}-[a-z]+-[0-9]+" region)
                   (re-matches #"[0-9]+" run) (re-matches #"[1-9][0-9]*" attempt)
                   (= prefix (str "ci/jolt-chdb/" run "-" attempt "/typed-window/checksum-faults")))
      ;; Never expose malformed input, identifiers or credentials in error data.
      (throw (ex-info "Invalid checksum control configuration" {:type ::invalid})))
    {:endpoint (str "https://s3." region ".amazonaws.com")
     :bucket (get env "TASK_BUCKET") :prefix prefix :region region
     :access-key (get env "AWS_ACCESS_KEY_ID") :secret-key (get env "AWS_SECRET_ACCESS_KEY")
     :session-token (get env "AWS_SESSION_TOKEN")
     :connect-timeout-ms 10000 :timeout-ms 30000 :max-response-bytes 65536}))
