(ns jdbc.chdb.durable.s3-checksum
  "Single-PUT SHA256 request/response comparison; no persistence or trust proof."
  (:import [java.util Base64]))

(defn- invalid! []
  (throw (ex-info "S3 upload checksum confirmation is invalid"
                  {:type ::invalid-confirmation})))

(defn request-checksum
  "Encode an already selected V1 lowercase-hex SHA256 as canonical base64.
  Never recompute a possibly changed source and substitute a different digest."
  [sha256]
  (when-not (and (string? sha256) (re-matches #"[0-9a-f]{64}" sha256))
    (invalid!))
  (let [bytes (byte-array
               (map (fn [offset]
                      (unchecked-byte (Integer/parseInt
                                        (subs sha256 offset (+ offset 2)) 16)))
                    (range 0 64 2)))]
    (.encodeToString (Base64/getEncoder) bytes)))

(defn confirmed?
  "True only for the matching full-object response of a new single PUT.
  Missing/composite evidence declines; malformed/conflicting evidence fails
  closed. Callers must separately establish qualified provider/native transport,
  unambiguous creation, exact transmitted size, and backend/reference binding."
  [sha256 response-headers]
  (let [actual (get response-headers "x-amz-checksum-sha256")
        kind (get response-headers "x-amz-checksum-type")]
    (cond
      (nil? actual) false
      (= "COMPOSITE" kind) false
      (and (some? kind) (not= "FULL_OBJECT" kind)) (invalid!)
      (= (request-checksum sha256) actual) true
      :else (invalid!))))
