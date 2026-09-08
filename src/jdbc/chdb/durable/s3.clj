(ns jdbc.chdb.durable.s3
  "S3-compatible Durable backend semantics over a streaming HTTP transport."
  (:require [clojure.string :as str]
            [jdbc.chdb.durable.backend :as backend])
  (:import [java.net URI]
           [java.nio.file Files Paths]))

(def ^:private retryable-statuses #{409 429 500 502 503 504})
(def ^:private unreserved
  (set (map int
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")))

(defn- fail!
  ([type message] (fail! type message nil))
  ([type message data]
   ;; Never attach the request, URL, provider body, credentials, or cause.
   (throw (ex-info message (assoc (or data {}) :type type)))))

(defn- nonblank! [value label]
  (when-not (and (string? value) (not (str/blank? value)))
    (fail! ::invalid-options (str label " must be a nonblank string")))
  value)

(defn- positive-option! [value label]
  (when-not (and (integer? value) (pos? value))
    (fail! ::invalid-options (str label " must be a positive integer")))
  value)

(defn- safe-key! [key]
  (let [parts (when (string? key) (str/split key #"/" -1))]
    (when-not (and (seq parts)
                   (not (str/starts-with? key "/"))
                   (not (str/includes? key "\\"))
                   (every? #(and (not (str/blank? %))
                                 (not (contains? #{"." ".."} %)))
                           parts))
      (fail! ::invalid-key "S3 object key must be a safe relative key"))
    key))

(defn- owned-bytes [value]
  (when-not (bytes? value)
    (fail! ::invalid-response "S3 byte response did not contain owned bytes"))
  (java.util.Arrays/copyOf value (alength value)))

(defn- hex-byte [value]
  (format "%02X" (bit-and value 255)))

(defn- encode-component [value]
  (apply str
         (mapcat (fn [octet]
                   (let [unsigned (bit-and (int octet) 255)]
                     (if (contains? unreserved unsigned)
                       [(char unsigned)]
                       [\% (hex-byte unsigned)])))
                 (.getBytes (str value) "UTF-8"))))

(defn- encode-key [key]
  (str/join "/" (map encode-component (str/split (safe-key! key) #"/"))))

(defn- normalized-prefix [prefix]
  (if (or (nil? prefix) (= "" prefix))
    ""
    (str (encode-key (str/replace prefix #"/+$" "")) "/")))

(defn- endpoint! [endpoint]
  (let [endpoint (nonblank! endpoint "endpoint")
        uri (try (URI. endpoint)
                 (catch Throwable _
                   (fail! ::invalid-options "endpoint must be a valid URL")))]
    (when-not (and (contains? #{"http" "https"} (.getScheme uri))
                   (string? (.getHost uri))
                   (nil? (.getUserInfo uri))
                   (nil? (.getQuery uri))
                   (nil? (.getFragment uri)))
      (fail! ::invalid-options
             "endpoint must be an HTTP(S) URL without credentials, query, or fragment"))
    (str/replace endpoint #"/+$" "")))

(defn- bucket! [bucket]
  (let [bucket (nonblank! bucket "bucket")]
    (when (or (str/includes? bucket "/")
              (str/includes? bucket "\\")
              (contains? #{"." ".."} bucket))
      (fail! ::invalid-options "bucket must be one safe path component"))
    bucket))

(defn- object-url [{:keys [endpoint bucket prefix]} key]
  (str endpoint "/" (encode-component bucket) "/" prefix (encode-key key)))

(defn- etag! [response]
  (let [etag (or (get-in response [:headers "etag"])
                 (get-in response [:headers "ETag"]))]
    (when-not (and (string? etag) (not (str/blank? etag)))
      (fail! ::invalid-response "S3 success response omitted its ETag"))
    etag))

(defn- retryable-error? [error]
  (contains? #{:transport :throttled} (:category (ex-data error))))

(defn- request!
  [state operation request write?]
  (loop [attempt 1]
    (let [outcome (try
                    {:response ((:request! state)
                                (assoc request
                                       :operation operation
                                       :auth (:auth state)
                                       :region (:region state)))}
                    (catch Throwable error {:error error}))]
      (if-let [error (:error outcome)]
        (let [category (:category (ex-data error))
              definitely-not-sent? (true? (:definitely-not-sent?
                                            (ex-data error)))]
          (cond
            (= :authentication category)
            (fail! ::authentication "S3 authentication failed")

            (= :permission category)
            (fail! ::permission "S3 permission was denied")

            (and (< attempt (:max-attempts state))
                 (or (not write?) definitely-not-sent?)
                 (retryable-error? error))
            (recur (inc attempt))

            (and write? (not definitely-not-sent?))
            {:ambiguous? true}

            (= :throttled category)
            (fail! ::throttled "S3 request was throttled")

            :else
            (fail! ::transport "S3 transport failed")))
        (let [response (:response outcome)
              status (:status response)]
          (when-not (integer? status)
            (fail! ::invalid-response "S3 transport returned no HTTP status"))
          (if (contains? retryable-statuses status)
            (if (< attempt (:max-attempts state))
              (recur (inc attempt))
              (if write?
                {:ambiguous? true}
                (if (= 429 status)
                  (fail! ::throttled "S3 request was throttled")
                  (fail! ::transport "S3 service remained unavailable"))))
            {:response response}))))))

(defn- response-or-error! [operation {:keys [response ambiguous?]}]
  (if ambiguous?
    {:status :ambiguous}
    (let [status (:status response)]
      (cond
        (= 401 status) (fail! ::authentication "S3 authentication failed")
        (= 403 status) (fail! ::permission "S3 permission was denied")
        (= 429 status) (fail! ::throttled "S3 request was throttled")
        :else (fail! ::provider "S3 request failed"
                     {:operation operation :status status})))))

(deftype ^:private S3Backend [state]
  backend/ObjectBackend
  (get-bytes [_ key]
    (let [outcome (request! state :get
                            {:method :get :url (object-url state key)
                             :response-body :bytes}
                            false)
          response (:response outcome)]
      (case (:status response)
        200 (owned-bytes (:body response))
        404 nil
        (response-or-error! :get outcome))))

  (get-with-etag [_ key]
    (let [outcome (request! state :get-with-etag
                            {:method :get :url (object-url state key)
                             :response-body :bytes}
                            false)
          response (:response outcome)]
      (case (:status response)
        200 {:bytes (owned-bytes (:body response)) :etag (etag! response)}
        404 nil
        (response-or-error! :get-with-etag outcome))))

  (put-file-if-absent! [_ key local-path]
    (let [path (Paths/get (str local-path) (into-array String []))]
      (when-not (Files/isRegularFile path (make-array java.nio.file.LinkOption 0))
        (fail! ::invalid-file "S3 upload source must be a regular file"))
      (let [outcome (request! state :put-file-if-absent
                              {:method :put :url (object-url state key)
                               :headers {"if-none-match" "*"}
                               :request-body {:file path
                                              :byte-count (Files/size path)}}
                              true)
            response (:response outcome)]
        (cond
          (:ambiguous? outcome) {:status :ambiguous}
          (= 200 (:status response)) {:status :created :etag (etag! response)}
          (= 412 (:status response)) {:status :precondition-failed}
          :else (response-or-error! :put-file-if-absent outcome)))))

  (put-bytes-if-absent! [_ key bytes]
    (when-not (bytes? bytes)
      (fail! ::invalid-bytes "S3 object value must be a byte array"))
    (let [outcome (request! state :put-bytes-if-absent
                            {:method :put :url (object-url state key)
                             :headers {"if-none-match" "*"}
                             :request-body {:bytes bytes
                                            :byte-count (alength bytes)}}
                            true)
          response (:response outcome)]
      (cond
        (:ambiguous? outcome) {:status :ambiguous}
        (= 200 (:status response)) {:status :created :etag (etag! response)}
        (= 412 (:status response)) {:status :precondition-failed}
        :else (response-or-error! :put-bytes-if-absent outcome))))

  (replace-if-match! [_ key bytes etag]
    (when-not (bytes? bytes)
      (fail! ::invalid-bytes "S3 object value must be a byte array"))
    (nonblank! etag "etag")
    (let [outcome (request! state :replace-if-match
                            {:method :put :url (object-url state key)
                             :headers {"if-match" etag}
                             :request-body {:bytes bytes
                                            :byte-count (alength bytes)}}
                            true)
          response (:response outcome)]
      (cond
        (:ambiguous? outcome) {:status :ambiguous}
        (= 200 (:status response)) {:status :replaced :etag (etag! response)}
        (= 412 (:status response)) {:status :precondition-failed}
        :else (response-or-error! :replace-if-match outcome))))

  (download-to-file! [_ key local-path]
    (let [path (Paths/get (str local-path) (into-array String []))
          outcome (request! state :download-to-file
                            {:method :get :url (object-url state key)
                             :response-body {:file path :create-new? true}}
                            false)
          response (:response outcome)]
      (case (:status response)
        200 (let [byte-count (:byte-count response)]
              (when-not (and (integer? byte-count) (not (neg? byte-count)))
                (fail! ::invalid-response
                       "S3 streamed response omitted its byte count"))
              {:status :downloaded :byte-count byte-count})
        404 {:status :not-found}
        (response-or-error! :download-to-file outcome)))))

(defn s3-backend
  "Create an S3-compatible namespace backend.

  `request!` is the streaming SigV4 transport boundary. It receives no SQL or
  Durable manifest data beyond the exact object request and must honor file
  bodies/destinations without buffering them."
  [{:keys [endpoint bucket prefix region access-key secret-key session-token
           request! max-attempts connect-timeout-ms timeout-ms
           max-response-bytes]
    :or {prefix "" max-attempts 3}}]
  (when-not (and (integer? max-attempts) (pos? max-attempts) (<= max-attempts 8))
    (fail! ::invalid-options "max-attempts must be between one and eight"))
  (when connect-timeout-ms
    (positive-option! connect-timeout-ms "connect-timeout-ms"))
  (when timeout-ms
    (positive-option! timeout-ms "timeout-ms"))
  (when max-response-bytes
    (positive-option! max-response-bytes "max-response-bytes"))
  (let [state
        {:endpoint (endpoint! endpoint)
         :bucket (bucket! bucket)
         :prefix (normalized-prefix prefix)
         :region (nonblank! region "region")
         :auth {:access-key (nonblank! access-key "access-key")
                :secret-key (nonblank! secret-key "secret-key")
                :session-token (when session-token
                                 (nonblank! session-token "session-token"))}
         :max-attempts max-attempts}
        transport-options
        (cond-> {}
          connect-timeout-ms (assoc :connect-timeout-ms connect-timeout-ms)
          timeout-ms (assoc :timeout-ms timeout-ms)
          max-response-bytes (assoc :max-response-bytes max-response-bytes))
        request! (or request!
                     ((requiring-resolve
                       'jdbc.chdb.durable.s3-curl/request-function)
                      transport-options))]
    (when-not (fn? request!)
      (fail! ::invalid-options "request! must be a function"))
    (S3Backend. (assoc state :request! request!))))
