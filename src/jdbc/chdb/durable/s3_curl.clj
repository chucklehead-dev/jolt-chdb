(ns jdbc.chdb.durable.s3-curl
  "Jolt-native libcurl transport for the Durable S3 backend."
  (:require [clojure.string :as str]
            [jdbc.chdb.durable.s3 :as s3]
            [jolt.ffi :as ffi])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.nio.file Files]))

;; These values are the public ABI from curl 7.75+ (the first release with
;; CURLOPT_AWS_SIGV4). Keep the option kind in the binding used below: libcurl's
;; curl_easy_setopt is variadic, so LONG, pointer, string, and curl_off_t are not
;; interchangeable at the call boundary.
(def ^:private curlopt-writedata 10001)
(def ^:private curlopt-url 10002)
(def ^:private curlopt-userpwd 10005)
(def ^:private curlopt-readdata 10009)
(def ^:private curlopt-writefunction 20011)
(def ^:private curlopt-readfunction 20012)
(def ^:private curlopt-httpheader 10023)
(def ^:private curlopt-headerdata 10029)
(def ^:private curlopt-customrequest 10036)
(def ^:private curlopt-upload 46)
(def ^:private curlopt-headerfunction 20079)
(def ^:private curlopt-nosignal 99)
(def ^:private curlopt-infilesize-large 30115)
(def ^:private curlopt-timeout-ms 155)
(def ^:private curlopt-connecttimeout-ms 156)
(def ^:private curlopt-aws-sigv4 10305)
(def ^:private curlinfo-response-code 0x200002)
(def ^:private curl-readfunc-abort 0x10000000)
(def ^:private curl-global-default 3)

(ffi/load-system-library "curl")

(ffi/defcfn curl-global-init "curl_global_init" [:long] :int)
(ffi/defcfn curl-easy-init "curl_easy_init" [] :pointer)
(ffi/defcfn curl-easy-cleanup "curl_easy_cleanup" [:pointer] :void)
(ffi/defcfn curl-easy-perform "curl_easy_perform" [:pointer] :int :blocking)
(ffi/defcfn curl-easy-setopt-long
  "curl_easy_setopt" [:pointer :int :& :long] :int)
(ffi/defcfn curl-easy-setopt-off-t
  "curl_easy_setopt" [:pointer :int :& :int64] :int)
(ffi/defcfn curl-easy-setopt-pointer
  "curl_easy_setopt" [:pointer :int :& :pointer] :int)
(ffi/defcfn curl-easy-setopt-string
  "curl_easy_setopt" [:pointer :int :& :string] :int)
(ffi/defcfn curl-easy-getinfo-pointer
  "curl_easy_getinfo" [:pointer :int :& :pointer] :int)
(ffi/defcfn curl-slist-append
  "curl_slist_append" [:pointer :string] :pointer)
(ffi/defcfn curl-slist-free-all
  "curl_slist_free_all" [:pointer] :void)
(ffi/defcfn c-fopen "fopen" [:string :string] :pointer)
(ffi/defcfn c-fclose "fclose" [:pointer] :int)

(def ^:private initialized
  (delay
    (let [code (curl-global-init curl-global-default)]
      (when-not (zero? code)
        (throw (ex-info "libcurl initialization failed"
                        {:category :transport
                         :definitely-not-sent? true})))
      true)))

(defn- transport-failure!
  ([message definitely-not-sent?]
   (transport-failure! message definitely-not-sent? nil))
  ([message definitely-not-sent? data]
   ;; Do not retain a libcurl error string, URL, header, credential, body, or
   ;; cause. The semantic S3 layer deliberately exposes only stable categories.
   (throw (ex-info message
                   (merge {:category :transport
                           :definitely-not-sent? definitely-not-sent?}
                          data)))))

(defn- check-code!
  ([code stage] (check-code! code stage true))
  ([code stage definitely-not-sent?]
   (when-not (zero? code)
     (transport-failure! "libcurl request failed"
                         definitely-not-sent? {:stage stage}))))

(defn- set-long! [handle option value stage]
  (check-code! (curl-easy-setopt-long handle option value) stage))

(defn- set-off-t! [handle option value stage]
  (check-code! (curl-easy-setopt-off-t handle option value) stage))

(defn- set-pointer! [handle option value stage]
  (check-code! (curl-easy-setopt-pointer handle option value) stage))

(defn- set-string! [handle option value stage]
  (check-code! (curl-easy-setopt-string handle option value) stage))

(defn- append-header! [headers line]
  (let [next (curl-slist-append headers line)]
    (when (ffi/null? next)
      (transport-failure! "libcurl header allocation failed" true))
    next))

(defn- header-list [headers session-token]
  (reduce (fn [head [name value]]
            (append-header! head (str name ": " value)))
          ffi/null
          (cond-> (vec headers)
            session-token
            (conj ["x-amz-security-token" session-token]))))

(defn- callback-size [size nitems]
  (let [n (* size nitems)]
    (if (> n Integer/MAX_VALUE) Integer/MAX_VALUE (int n))))

(defn- response-writer [response-body max-response-bytes]
  (cond
    (nil? response-body)
    {:write-pointer! (fn [_ _] nil)
     :result (fn [] {})
     :close! (fn [] nil)
     :cleanup! (fn [] nil)}

    (= :bytes response-body)
    (let [output (ByteArrayOutputStream.)
          count (atom 0)]
      {:output output
       :write-pointer! (fn [pointer n]
                 (let [new-count (+ @count n)]
                   (when (> new-count max-response-bytes)
                     (throw (ex-info "S3 byte response exceeded its bound" {})))
                   (let [bytes (ffi/read-array pointer n)]
                     (.write output bytes 0 (alength bytes))
                     (reset! count new-count))))
       :result (fn [] {:body (.toByteArray output)})
       :close! (fn [] nil)
       :cleanup! (fn [] nil)})

    :else
    (let [{:keys [file create-new?]} response-body]
      (when-not (and file create-new?)
        (transport-failure! "invalid libcurl response destination" true))
      (let [stream
            (try
              ;; Preserve CREATE_NEW before handing the already-private scratch
              ;; path to stdio. Libcurl's default write callback is fwrite, so
              ;; checkpoint bytes never cross into a managed callback buffer.
              (Files/createFile file
                                (make-array java.nio.file.attribute.FileAttribute 0))
              (let [stream (c-fopen (str file) "wb")]
                (when (ffi/null? stream)
                  (transport-failure!
                   "S3 response destination could not be opened" true))
                stream)
              (catch Throwable error
                (Files/deleteIfExists file)
                (if (:category (ex-data error))
                  (throw error)
                  (transport-failure!
                   "S3 response destination could not be created" true))))
            closed? (atom false)]
        {:write-data stream
         :result (fn [] {:byte-count (Files/size file)})
         :close! (fn []
                   (when (compare-and-set! closed? false true)
                     (when-not (zero? (c-fclose stream))
                       (transport-failure!
                        "S3 response destination could not be closed" false))))
         :cleanup! (fn [] (Files/deleteIfExists file))}))))

(defn- request-reader [request-body]
  (when request-body
    (let [{:keys [bytes file byte-count]} request-body]
      (when-not (and (integer? byte-count) (not (neg? byte-count)))
        (transport-failure! "invalid libcurl request body length" true))
      (try
        (cond
          (bytes? bytes)
          (let [input (ByteArrayInputStream. bytes)]
            {:input input :byte-count byte-count
             :close! (fn [] (.close input))})

          file
          (let [stream (c-fopen (str file) "rb")
                closed? (atom false)]
            (when (ffi/null? stream)
              (transport-failure! "S3 request source could not be opened" true))
            ;; Libcurl's default read callback is fread. Keeping the FILE* on
            ;; the native side avoids one managed allocation per upload chunk.
            {:read-data stream :byte-count byte-count
             :close! (fn []
                       (when (compare-and-set! closed? false true)
                         (c-fclose stream)))})

          :else
          (transport-failure! "invalid libcurl request body" true))
        (catch Throwable error
          (if (:category (ex-data error))
            (throw error)
            (transport-failure!
             "S3 request source could not be opened" true)))))))

(defn- parse-header! [headers bytes]
  (let [line (str/trim (String. bytes "ISO-8859-1"))
        colon (.indexOf line ":")]
    (when (pos? colon)
      (let [name (str/lower-case (subs line 0 colon))]
        ;; ETag is the only response header in the ObjectBackend contract. Not
        ;; retaining arbitrary provider headers also avoids reflecting secrets.
        (when (= "etag" name)
          (swap! headers assoc name (str/trim (subs line (inc colon)))))))))

(defn request!
  "Execute one request emitted by jdbc.chdb.durable.s3 using libcurl SigV4.

  Byte responses are bounded; checkpoint downloads stream to a CREATE_NEW file.
  Transport errors are sanitized and write failures remain ambiguous unless the
  request was proven not to have started."
  [{:keys [method url headers request-body response-body auth region
           connect-timeout-ms timeout-ms max-response-bytes]
    :or {connect-timeout-ms 10000
         timeout-ms 300000
         max-response-bytes (* 128 1024 1024)
         headers {}}}]
  @initialized
  (when-not (contains? #{:get :put} method)
    (transport-failure! "unsupported libcurl request method" true))
  (let [{:keys [access-key secret-key session-token]} auth]
    (when-not (and (string? access-key) (string? secret-key)
                   (string? region) (string? url)
                   (map? headers)
                   (integer? connect-timeout-ms) (pos? connect-timeout-ms)
                   (integer? timeout-ms) (pos? timeout-ms)
                   (integer? max-response-bytes) (pos? max-response-bytes))
      (transport-failure! "invalid libcurl request options" true))
    (let [handle (curl-easy-init)]
      (when (ffi/null? handle)
        (transport-failure! "libcurl request allocation failed" true))
      (let [request-source
            (try
              (request-reader request-body)
              (catch Throwable error
                (curl-easy-cleanup handle)
                (throw error)))
            response-sink
            (try
              (response-writer response-body max-response-bytes)
              (catch Throwable error
                (when request-source
                  (try ((:close! request-source)) (catch Throwable _ nil)))
                (curl-easy-cleanup handle)
                (throw error)))
            response-headers (atom {})
            callback-error (atom nil)
            slist (atom ffi/null)]
        (try
          (with-open [arena (ffi/shared-arena)]
            (let [read-callback
                  (ffi/callback
                   arena
                   (fn [pointer size nitems _]
                     (try
                       (let [capacity (callback-size size nitems)
                             buffer (byte-array (min capacity 65536))
                             n (.read (:input request-source) buffer 0
                                      (alength buffer))]
                         (if (neg? n)
                           0
                           (do (ffi/write-array pointer buffer 0 n) n)))
                       (catch Throwable error
                         (reset! callback-error error)
                         curl-readfunc-abort)))
                   [:pointer :size_t :size_t :pointer] :size_t :collect-safe)
                  write-callback
                  (ffi/callback
                   arena
                   (fn [pointer size nitems _]
                     (let [n (callback-size size nitems)]
                       (try
                         ((:write-pointer! response-sink) pointer n)
                         n
                         (catch Throwable error
                           (reset! callback-error error)
                           0))))
                   [:pointer :size_t :size_t :pointer] :size_t :collect-safe)
                  header-callback
                  (ffi/callback
                   arena
                   (fn [pointer size nitems _]
                     (let [n (callback-size size nitems)]
                       (try
                         (parse-header! response-headers
                                        (ffi/read-array pointer n))
                         n
                         (catch Throwable error
                           (reset! callback-error error)
                           0))))
                   [:pointer :size_t :size_t :pointer] :size_t :collect-safe)]
              (set-string! handle curlopt-url url :url)
              (set-string! handle curlopt-userpwd
                           (str access-key ":" secret-key) :credentials)
              (set-string! handle curlopt-aws-sigv4
                           (str "aws:amz:" region ":s3") :sigv4)
              (set-long! handle curlopt-nosignal 1 :nosignal)
              (set-long! handle curlopt-connecttimeout-ms
                         connect-timeout-ms :connect-timeout)
              (set-long! handle curlopt-timeout-ms timeout-ms :timeout)
              (if-let [write-data (:write-data response-sink)]
                (set-pointer! handle curlopt-writedata write-data :write-data)
                (do
                  (set-pointer! handle curlopt-writedata ffi/null :write-data)
                  (set-pointer! handle curlopt-writefunction
                                write-callback :write-callback)))
              (set-pointer! handle curlopt-headerdata ffi/null :header-data)
              (set-pointer! handle curlopt-headerfunction
                            header-callback :header-callback)
              (reset! slist (header-list headers session-token))
              (when-not (ffi/null? @slist)
                (set-pointer! handle curlopt-httpheader @slist :headers))
              (when (= :put method)
                (when-not request-source
                  (transport-failure! "PUT request omitted its body" true))
                (set-long! handle curlopt-upload 1 :upload)
                (set-string! handle curlopt-customrequest "PUT" :method)
                (if-let [read-data (:read-data request-source)]
                  (set-pointer! handle curlopt-readdata read-data :read-data)
                  (do
                    (set-pointer! handle curlopt-readdata ffi/null :read-data)
                    (set-pointer! handle curlopt-readfunction
                                  read-callback :read-callback)))
                (set-off-t! handle curlopt-infilesize-large
                            (:byte-count request-source) :content-length))
              (let [perform-code (curl-easy-perform handle)]
                ((:close! response-sink))
                (when-not (zero? perform-code)
                  (transport-failure! "libcurl transfer failed" false
                                      (cond-> {:curl-code perform-code}
                                        @callback-error
                                        (assoc :callback-failed? true))))
                (ffi/with-out [status-pointer :long]
                  (check-code!
                   (curl-easy-getinfo-pointer
                    handle curlinfo-response-code status-pointer)
                   :response-code false)
                  (let [status (ffi/read status-pointer :long)
                        result (merge {:status status
                                       :headers @response-headers}
                                      ((:result response-sink)))]
                    (when-not (= 200 status)
                      ((:cleanup! response-sink)))
                    result)))))
          (catch Throwable error
            (try ((:close! response-sink)) (catch Throwable _ nil))
            (try ((:cleanup! response-sink)) (catch Throwable _ nil))
            (throw error))
          (finally
            (when request-source
              (try ((:close! request-source)) (catch Throwable _ nil)))
            (when-not (ffi/null? @slist)
              (curl-slist-free-all @slist))
            (curl-easy-cleanup handle)))))))

(defn request-function
  "Create a transport function with optional timeout and byte-response bounds."
  [options]
  (let [selected (select-keys options
                              [:connect-timeout-ms :timeout-ms
                               :max-response-bytes])]
    (fn [request] (request! (merge selected request)))))

(defn s3-backend
  "Create the S3 ObjectBackend with a statically reachable libcurl transport.

  Compiled Jolt applications should call this entry point so the native
  transport namespace is retained by AOT reachability analysis."
  [options]
  (s3/s3-backend
   (assoc options :request! (request-function options))))
