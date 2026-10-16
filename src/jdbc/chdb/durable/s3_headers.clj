(ns jdbc.chdb.durable.s3-headers
  "Bounded allowlisted final-response headers from libcurl's header callback.

  A callback receives status lines, interim response blocks, and trailers too.
  No provider text is included in errors. This parser establishes response
  boundaries, not checksum validity or upload integrity."
  (:require [clojure.string :as str]))

(def initial-state
  {:status nil :collecting? false :headers {} :invalid? false
   :last-selected? false})

(def ^:private limits
  {"etag" 1024 "x-amz-checksum-sha256" 256 "x-amz-checksum-type" 64})

(defn accept-line
  "Consume one complete ISO-8859-1-decoded libcurl header-callback line.
  Retain only bounded selected fields from the latest response header block.
  Duplicate selected headers, even equal ones, invalidate that block."
  [state raw-line]
  (let [line (str/replace raw-line #"\r?\n$" "")]
    (cond
      (str/starts-with? line "HTTP/")
      (if-let [[_ code] (re-matches #"HTTP/(?:1\.[01]|2|3) ([0-9]{3})(?:[ \t].*)?" line)]
        (assoc initial-state :status (Long/parseLong code) :collecting? true)
        (assoc initial-state :invalid? true))

      (= "" line) (assoc state :collecting? false :last-selected? false)

      ;; Trailers and headers arriving without a status line are not evidence.
      (not (:collecting? state)) state

      (or (str/includes? line "\r") (str/includes? line "\n"))
      (assoc state :invalid? true :last-selected? false)

      ;; Old curl versions may pass obsolete continuation lines separately.
      ;; Never accept a selected field's prefix while ignoring its continuation.
      (or (str/starts-with? line " ") (str/starts-with? line "\t"))
      (cond-> state (:last-selected? state) (assoc :invalid? true))

      :else
      (let [colon (.indexOf line ":")
            name (when (pos? colon) (str/lower-case (subs line 0 colon)))
            limit (get limits name)]
        (if limit
          (let [value (str/replace (subs line (inc colon)) #"^[ \t]+|[ \t]+$" "")]
            (if (or (contains? (:headers state) name) (> (count value) limit))
              (assoc state :invalid? true :last-selected? true)
              (-> state
                  (assoc-in [:headers name] value)
                  (assoc :last-selected? true))))
          (assoc state :last-selected? false))))))

(defn final-headers
  "Return only a complete final block matching CURLINFO_RESPONSE_CODE.
  Invalid final headers fail closed with no retained provider text in ex-data."
  [state expected-status]
  (when (or (:invalid? state) (:collecting? state)
            (not= expected-status (:status state)))
    (throw (ex-info "S3 response headers are invalid"
                    {:category :transport :definitely-not-sent? false})))
  (:headers state))
