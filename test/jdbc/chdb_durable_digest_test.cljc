(ns jdbc.chdb-durable-digest-test
  (:require [jdbc.chdb.durable.digest :as digest]
            [jdbc.chdb-durable-local-test :as local]
            #?(:jolt [jolt.ffi :as ffi]))
  (:import [java.io ByteArrayInputStream InputStream]
           [java.nio.file Files OpenOption]
           [java.nio.file.attribute FileAttribute]))

(def failures (atom 0))
(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do (swap! failures inc)
        (println "  FAIL" label "expected" expected "actual" actual))))
(defn- caught [f]
  (try (f) nil (catch Throwable error error)))

(defn- scripted-input [payload mode closes reads]
  (let [offset (atom 0)
        read! (fn [buffer start length]
                (swap! reads inc)
                (cond
                  (= mode :zero) 0
                  (= mode :error) (throw (ex-info "injected read" {:type ::read-failed}))
                  (= @offset (alength payload)) -1
                  :else (let [n (min 7 length (- (alength payload) @offset))]
                          (System/arraycopy payload @offset buffer start n)
                          (swap! offset + n)
                          n)))]
    (proxy [InputStream] []
      (read
        ([] (throw (ex-info "unexpected scalar read" {})))
        ([buffer] (read! buffer 0 (alength buffer)))
        ([buffer start length] (read! buffer start length)))
      (close [] (swap! closes inc)))))

(defn- run-stream-checks! []
  (doseq [n [0 1 65535 65536 65537 131089]]
    (let [payload (byte-array (map #(unchecked-byte (mod % 256)) (range n)))
          expected (if (zero? n)
                     "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                     (digest/sha256-bytes payload))
          input (ByteArrayInputStream. payload)
          path (Files/createTempFile "jchdb-digest-" ".bin"
                                     (into-array FileAttribute []))]
      (try
        (Files/write path payload (into-array OpenOption []))
        (check "stream digest/count matches independent byte hashing"
               {:byte-count n :sha256 expected}
               (digest/hash+count-input-stream input))
        (check "file API still returns the same lowercase digest"
               expected (digest/sha256-file path))
        (finally (.close input) (Files/deleteIfExists path)))))
  (let [payload (.getBytes "partial reads consume the complete body" "UTF-8")
        closes (atom 0) reads (atom 0)
        input (scripted-input payload :normal closes reads)]
    (check "partial reads are counted and hashed through EOF"
           {:byte-count (alength payload) :sha256 (digest/sha256-bytes payload)}
           (digest/hash+count-input-stream input))
    (check "partial-read control is nonvacuous" true (> @reads 2))
    (check "borrowed input remains caller-owned" 0 @closes)
    (.close input)
    (check "caller closes its own input exactly once" 1 @closes))
  (doseq [[mode expected] [[:zero ::digest/zero-read] [:error ::read-failed]]]
    (let [closes (atom 0) reads (atom 0)
          input (scripted-input (byte-array [1]) mode closes reads)]
      (check "zero/error read cannot yield a successful partial digest" expected
             (:type (ex-data (caught #(digest/hash+count-input-stream input)))))
      (check "failing borrowed input was actually read" 1 @reads)
      (check "failing input still belongs to caller" 0 @closes)
      (.close input))))

#?(:jolt
   (defn- run-native-cleanup-checks! []
     (let [new-var (ns-resolve 'jdbc.chdb.durable.digest 'evp-md-ctx-new)
           free-var (ns-resolve 'jdbc.chdb.durable.digest 'evp-md-ctx-free)
           new-context @new-var free-context @free-var
           new-arena ffi/confined-arena
           created (atom []) freed (atom []) arenas (atom [])]
       (with-redefs-fn
         {new-var (fn [] (let [p (new-context)] (swap! created conj p) p))
          free-var (fn [p] (swap! freed conj p) (free-context p))
          #'ffi/confined-arena (fn [] (let [a (new-arena)] (swap! arenas conj a) a))}
         (fn []
           (doseq [mode [:normal :zero :error]]
             (with-open [input (scripted-input (byte-array [7]) mode (atom 0) (atom 0))]
               (caught #(digest/hash+count-input-stream input))))))
       (check "native contexts really allocated for all three routes" 3 (count @created))
       (check "native context freed on success/zero/error" @created @freed)
       (check "all three confined arenas were opened" 3 (count @arenas))
       (check "native arenas closed on success/zero/error" [false false false]
              (mapv ffi/arena-open? @arenas)))))

(defn run-checks! []
  (reset! failures 0)
  (run-stream-checks!)
  #?(:jolt (run-native-cleanup-checks!))
  (local/run-digest-checks!)
  (when-not (zero? @failures)
    (throw (ex-info "Durable digest checks failed" {:failures @failures})))
  (println "all Durable digest checks passed")
  true)

(defn -main [& _]
  (run-checks!))
