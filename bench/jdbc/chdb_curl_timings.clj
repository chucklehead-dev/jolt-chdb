(ns jdbc.chdb-curl-timings
  "Manual-profile-only scalar libcurl milestones, with no provider identity.
  This namespace neither reuses handles nor changes requests or persistence."
  (:require [jolt.ffi :as ffi]
            [jdbc.chdb.durable.s3-curl :as curl]))

;; curl7.75 public header: CURLINFO_OFF_T=0x600000, LONG=0x200000.
;; *_TIME_T outputs are curl_off_t (int64) microseconds, not doubles.
;; https://github.com/curl/curl/blob/curl-7_75_0/include/curl/curl.h
(def ^:private fields
  {:total 0x600032 :dns 0x600033 :connect 0x600034
   :pretransfer 0x600035 :first-byte 0x600036 :tls 0x600038})
(def ^:private operations
  #{:get :get-with-etag :put-file-if-absent :put-bytes-if-absent
    :replace-if-match :download-to-file})
(def ^:dynamic ^:private *operation* :unknown)
(def ^:private sample-limit 2048)

(defn recorder [] {:entries (atom {}) :declined (atom 0)})

(defn- scalar [handle option]
  (ffi/with-out [out :int64]
    (when (zero? (#'curl/curl-easy-getinfo-pointer handle option out))
      (let [value (ffi/read out :int64)] (when (not (neg? value)) value)))))

(defn- connections [handle]
  (ffi/with-out [out :long]
    (when (zero? (#'curl/curl-easy-getinfo-pointer handle 0x20001a out))
      (let [value (ffi/read out :long)] (when (not (neg? value)) value)))))

(defn- snapshot [handle]
  (let [timings (into {} (map (fn [[label option]] [label (scalar handle option)]) fields))
        n (connections handle)]
    (when (and (every? integer? (vals timings)) (integer? n))
      (assoc timings :new-connections n))))

(defn- record! [stats values]
  (swap! (:entries stats) update *operation*
    (fn [entry]
      (let [entry (or entry {:calls 0 :new-connections 0 :samples []})]
        (-> entry
            (update :calls inc)
            (update :new-connections + (:new-connections values))
            (update :samples #(if (< (count %) sample-limit)
                               (conj % (dissoc values :new-connections)) %)))))))

(defn instrument-request [stats request!]
  (if-not stats request!
    (fn [request]
      (binding [*operation* (if (contains? operations (:operation request))
                             (:operation request) :unknown)]
        (request! request)))))

(defn observe!
  "Temporarily interpose the perform Var in ONE owned profile process.
  Diagnostic failures do not alter its code/exception. No pointers retained.
  Not a production/global observer or concurrent-profile API."
  [stats f]
  (if-not stats (f)
    (let [target #'curl/curl-easy-perform original @target]
      (with-redefs-fn
        {target (fn [handle]
                  (let [code (original handle)]
                    (when (zero? code)
                      (try
                        (if-let [values (snapshot handle)]
                          (record! stats values)
                          (swap! (:declined stats) inc))
                        (catch Throwable _ (swap! (:declined stats) inc))))
                    code))}
        f))))

(defn- summary [values]
  (let [ordered (vec (sort values)) n (count ordered)
        pct (fn [fraction] (nth ordered (max 0 (dec (long (Math/ceil (* n fraction)))))))]
    {:samples n :p50-us (pct 0.50) :p90-us (pct 0.90)
     :p95-us (pct 0.95) :p99-us (pct 0.99) :max-us (peek ordered)
     :p99-supported? (>= n 100)}))

(defn report [stats]
  {:schema-version 1 :scope :whole-process-cumulative-curl-milestones
   :units :microseconds :sample-limit-per-operation sample-limit
   :diagnostic-declines @(:declined stats)
   :operations
   (into {} (map (fn [[operation entry]]
                  [operation
                   {:calls (:calls entry) :new-connections (:new-connections entry)
                    :samples-truncated? (> (:calls entry) (count (:samples entry)))
                    :milestones (into {} (map (fn [field]
                                               [field (summary (map field (:samples entry)))])
                                             (keys fields)))}])
                @(:entries stats)))})

(defn assert-coverage!
  "Require successful HTTP-response attempts to reach the actual perform Var.
  A compiled bypass or unavailable scalar ABI must not yield green empty data."
  [curl-report provider-report]
  (let [expected (reduce + 0
                         (for [[_ phase] (:transport provider-report)
                               [_ entry] phase
                               [status n] (:results entry)
                               :when (and (integer? status) (<= 100 status 599))]
                           n))
        observed (reduce + 0 (map :calls (vals (:operations curl-report))))]
    (when-not (and (pos? expected) (= expected observed)
                   (zero? (:diagnostic-declines curl-report)))
      (throw (ex-info "Curl timing coverage is incomplete"
                      {:type ::incomplete-coverage}))))
  nil)
