(ns jdbc.chdb-durable-log-fixture
  "The deterministic ClickStack/OTel log rows used by the Durable 512 probe.")

(def ^:private base-nanos 1700000000000000000)

(defn- padded-hex [width n]
  (let [value (format "%x" n)]
    (str (apply str (repeat (- width (count value)) "0")) value)))

(defn log-row [index question-mark?]
  {"Timestamp" (format "%d.%09d"
                       (quot (+ base-nanos (* index 1000000)) 1000000000)
                       (mod (+ base-nanos (* index 1000000)) 1000000000))
   "TraceId" (padded-hex 32 (inc index))
   "SpanId" (padded-hex 16 (+ 1000000 index))
   "TraceFlags" (mod index 2)
   "SeverityText" (if (zero? (mod index 20)) "ERROR" "INFO")
   "SeverityNumber" (if (zero? (mod index 20)) 17 9)
   "ServiceName" "oscope.benchmark"
   "Body" (str "request completed route=/api/items/" (mod index 64)
               " status=" (if (zero? (mod index 20)) 500 200)
               (if question-mark? " query=ready?" ""))
   "ResourceSchemaUrl" "https://opentelemetry.io/schemas/1.27.0"
   "ResourceAttributes" {"service.name" "oscope.benchmark"
                         "deployment.environment.name" "benchmark"}
   "ScopeSchemaUrl" ""
   "ScopeName" "oscope.benchmark"
   "ScopeVersion" "1.0"
   "ScopeAttributes" {"library.language" "clojure"}
   "LogAttributes" {"http.request.method" "GET"
                    "http.response.status_code"
                    (str (if (zero? (mod index 20)) 500 200))
                    "benchmark.bucket" (str (mod index 16))}
   "EventName" "benchmark.request"})

(defn rows-512 []
  (mapv #(log-row % false) (range 512)))
