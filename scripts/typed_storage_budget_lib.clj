(ns typed-storage-budget-lib)

(defn requester [request! counts request-limit upload-limit allowed-methods]
  (fn [request]
    (let [method (:method request)
          body (:request-body request)
          bytes (if body (:byte-count body) 0)]
      (when-not (and (contains? allowed-methods method)
                     (contains? #{:get :put} method)
                     (if (= method :get) (nil? body) (map? body))
                     (integer? bytes) (<= 0 bytes upload-limit))
        (throw (ex-info "Cloud benchmark request shape exceeds budget"
                        {:type :typed-storage/request-budget})))
      (swap! counts
        (fn [before]
          (let [after (-> before (update :requests inc) (update :upload-bytes + bytes))]
            (when (or (> (:requests after) request-limit)
                      (> (:upload-bytes after) upload-limit))
              (throw (ex-info "Cloud benchmark request budget exhausted"
                              {:type :typed-storage/request-budget})))
            after)))
      (let [result (request! request)]
        (swap! counts update :completed inc)
        result))))
