(require '[otel.exporter.chdb-ordinary-transport-benchmark :as fixture])
(defn nested-sorted-sdk-shape [spans]
  ;; Deterministic SDK-shaped ended records for the queue/Durable test. Real
  ;; tracer-produced representation was measured separately; this is not OTLP.
  (let [attrs (into (sorted-map "service.name" "producer-shape" "flag" false "count" 42)
                    (map (fn [i] [(str "extra-" i)
                                  (case (mod i 3) 0 "value-λ" 1 false i)]) (range 14)))]
    (mapv #(-> % (assoc-in [:resource :attributes] attrs)
               (assoc-in [:scope :attributes] attrs)
               (assoc :events [{:name "scenario-event"
                                :timestamp-unix-nano (:start-time-unix-nano %)
                                :attributes attrs}])) spans)))
