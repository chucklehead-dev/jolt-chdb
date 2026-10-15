(ns typed-storage-curl-profile
  "Benchmark-only aggregate native transfer accounting. Never retain requests."
  (:require [jdbc.chdb.durable.s3-curl :as curl]
            [jolt.ffi :as ffi]))

;; curl8.5.0 include/curl/curl.h: DOUBLE base0x300000, LONG base0x200000.
;; Read after perform, before its owning request resets/frees the easy handle.
;; All time values are cumulative milestones in seconds, NOT exclusive phases.
(def ^:private time-fields
  [[:curl-total-seconds 3145731] [:dns-complete-seconds 3145732]
   [:connect-complete-seconds 3145733] [:pretransfer-complete-seconds 3145734]
   [:first-byte-seconds 3145745] [:tls-complete-seconds 3145761]])

(def ^:dynamic *request-class* [:unattributed :none])

(defn request-class [request]
  (case (:method request)
    :get [:get :none]
    :put (let [body (:request-body request)
               n (:byte-count body)
               kind (cond (contains? body :bytes) :bytes
                          (contains? body :file) :file :else :other)
               size (cond (not (and (integer? n) (<= 0 n))) :unknown
                          (<= n 4096) :small
                          (<= n 1048576) :medium :else :large)]
           [:put kind size])
    [:other :none]))

(defn- double-info! [handle option]
  (ffi/with-out [p :double]
    (when-not (zero? (curl/curl-easy-getinfo-pointer handle option p))
      (throw (ex-info "Native timing unavailable" {:type ::unavailable})))
    (let [n (ffi/read p :double)]
      (when-not (and (<= 0.0 n) (< n Double/POSITIVE_INFINITY))
        (throw (ex-info "Native timing unavailable" {:type ::unavailable})))
      n)))

(defn read-info! [handle]
  (let [times (into {} (map (fn [[key option]] [key (double-info! handle option)]) time-fields))]
    (ffi/with-out [p :long]
      (when-not (zero? (curl/curl-easy-getinfo-pointer handle 2097178 p))
        (throw (ex-info "Native timing unavailable" {:type ::unavailable})))
      (let [n (ffi/read p :long)]
        (when (neg? n)
          (throw (ex-info "Native timing unavailable" {:type ::unavailable})))
        (assoc times :new-connections n)))))

(defn- add! [counts class deltas]
  (swap! counts update class #(merge-with + (or % {}) deltas)))

(defn wrap-request [request! counts]
  (fn [request]
    (binding [*request-class* (request-class request)]
      (let [start (System/nanoTime) success (volatile! false)]
        (try
          (let [value (request! request)] (vreset! success true) value)
          (finally
            (add! counts *request-class*
                  {:requests 1 :request-failures (if @success 0 1)
                   :request-wall-nanos (- (System/nanoTime) start)})))))))

(defn wrap-perform [perform! counts sample!]
  (fn [handle]
    (let [start (System/nanoTime)]
      (add! counts *request-class* {:performs-started 1})
      (try
        (let [code (perform! handle)
              elapsed (- (System/nanoTime) start)]
          ;; Diagnostic read failure never changes the original transfer result.
          ;; Its explicit count makes the profile incomplete, not valid evidence.
          (try (add! counts *request-class*
                     (merge (sample! handle)
                            {:samples 1 :perform-failures (if (zero? code) 0 1)}))
               (catch Throwable _ (add! counts *request-class* {:sample-errors 1})))
          (add! counts *request-class* {:performs-returned 1 :perform-wall-nanos elapsed})
          code)
        (catch Throwable error
          (add! counts *request-class* {:perform-throws 1})
          (throw error))))))

(defn with-profile! [counts f]
  ;; f must join/close all request owners before returning, as the benchmark
  ;; already requires. Global Var wrappers are scoped to this isolated process.
  (let [request! curl/request! perform! curl/curl-easy-perform]
    (with-redefs [curl/request! (wrap-request request! counts)
                  curl/curl-easy-perform (wrap-perform perform! counts read-info!)]
      (f))))

(defn report! [counts]
  (let [classes @counts]
    (when-not (and (seq classes) (not (contains? classes [:unattributed :none]))
                  (every? (fn [[_ x]]
                            (and (zero? (get x :sample-errors 0))
                                 (zero? (get x :perform-throws 0))
                                 (= (get x :performs-started 0) (get x :performs-returned 0)
                                    (get x :samples 0))
                                 (<= (get x :samples 0) (get x :requests 0)))) classes))
      (throw (ex-info "Incomplete native transfer profile" {:type ::incomplete})))
    {:scope :aggregate-native-transfer-diagnostic :complete true
     :milestones-are-cumulative true :classes classes}))
