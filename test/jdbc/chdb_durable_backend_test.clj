(ns jdbc.chdb-durable-backend-test
  (:require [hegel.core :as h]
            [hegel.generator :as g]
            [jdbc.chdb.durable.backend :as backend]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- error-type [f]
  (try (f) nil (catch Throwable error (:type (ex-data error)))))

(defn- octets [value]
  (vec (backend/get-bytes value "head.json")))

(defn- run-deterministic-checks []
  (println "Durable V1 object-backend contract")
  (check "replace cannot create an absent object" :precondition-failed
         (:status (backend/replace-if-match!
                   (backend/memory-backend) "head.json"
                   (byte-array [1]) nil)))
  (let [store (backend/memory-backend)
        input (byte-array [1 2 3])
        created (backend/put-bytes-if-absent! store "head.json" input)]
    (System/arraycopy (byte-array [99]) 0 input 0 1)
    (check "conditional create succeeds once" :created (:status created))
    (check "backend owns created bytes" [1 2 3] (octets store))
    (check "second conditional create loses" :precondition-failed
           (:status (backend/put-bytes-if-absent!
                     store "head.json" (byte-array [4]))))

    (let [{first-etag :etag} (backend/get-with-etag store "head.json")
          winner (backend/replace-if-match!
                  store "head.json" (byte-array [4]) first-etag)
          stale (backend/replace-if-match!
                 store "head.json" (byte-array [5]) first-etag)]
      (check "first same-snapshot replacement wins" :replaced
             (:status winner))
      (check "second same-snapshot replacement is stale"
             :precondition-failed (:status stale))
      (check "stale writer cannot overwrite the winner" [4] (octets store))
      (check "replacement advances the opaque token" false
             (= first-etag (:etag winner))))

    (let [returned (backend/get-bytes store "head.json")]
      (System/arraycopy (byte-array [88]) 0 returned 0 1)
      (check "backend owns returned bytes" [4] (octets store))))

  ;; Semantic SAT control corresponding to the Chiasmus HEAD-then-PUT mutant:
  ;; both writers authorize against one snapshot and then write unconditionally.
  (let [live (atom {:etag ::old :bytes [0]})
        snapshot @live
        non-atomic-replace!
        (fn [expected-etag value]
          (if (= expected-etag (:etag snapshot))
            (do (reset! live {:etag (gensym "mutant-etag-") :bytes [value]})
                {:status :replaced})
            {:status :precondition-failed}))
        a-success (= :replaced (:status (non-atomic-replace! ::old 1)))
        b-success (= :replaced (:status (non-atomic-replace! ::old 2)))]
    (check "non-atomic mutant exposes double success"
           true (and a-success b-success))
    (check "non-atomic mutant overwrites the first winner"
           [2] (:bytes @live)))

  (doseq [key ["" "/head.json" "wal//x" "../head.json"
               "wal/../head.json" "wal\\escape"]]
    (check "unsafe relative key fails closed" ::backend/invalid-key
           (error-type #(backend/get-bytes (backend/memory-backend) key))))
  (check "non-byte value fails closed" ::backend/invalid-bytes
         (error-type #(backend/put-bytes-if-absent!
                       (backend/memory-backend) "head.json" [1 2 3]))))

(defn- run-cas-property! []
  (println "Durable backend Hegel CAS property")
  (let [result
        (h/run-test!
         {:name "chdb/durable-backend-stale-etag"
          :database ""
          :derandomize? true
          :verbosity :quiet
          :test-cases 60}
         (fn [_]
           (let [steps (h/draw!
                        (g/vector
                         {:max-size 24}
                         (g/tuple (g/boolean)
                                  (g/integer -128 127))))
                 store (backend/memory-backend)
                 created (backend/put-bytes-if-absent!
                          store "head.json" (byte-array [0]))]
             (loop [remaining steps
                    current-etag (:etag created)
                    stale-etag nil
                    expected 0]
               (when-let [[[use-stale? value] & more] (seq remaining)]
                 (let [candidate (if (and use-stale? stale-etag)
                                   stale-etag current-etag)
                       result (backend/replace-if-match!
                               store "head.json" (byte-array [value]) candidate)
                       succeeds? (= candidate current-etag)
                       actual (first (octets store))]
                   (when-not (= succeeds? (= :replaced (:status result)))
                     (throw (ex-info "CAS acceptance diverged from the model"
                                     {:hegel/origin
                                      "chdb/durable-backend/acceptance"})))
                   (when-not (= (if succeeds? value expected) actual)
                     (throw (ex-info "stale CAS changed stored bytes"
                                     {:hegel/origin
                                      "chdb/durable-backend/value"})))
                   (recur more
                          (if succeeds? (:etag result) current-etag)
                          (if succeeds? current-etag stale-etag)
                          (if succeeds? value expected))))))))]
    (println "  hegel stale-etag seed" (:seed result)
             "valid" (:valid-test-cases result))
    (when-not (and (:passed? result) (not (:flaky? result)))
      (swap! failures inc)
      (println "  FAIL stale-etag" (pr-str result)))))

(defn run-checks! []
  (reset! failures 0)
  (run-deterministic-checks)
  (run-cas-property!)
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable backend checks failed")
                    {:failures @failures})))
  (println "all Durable backend checks passed")
  true)

(defn -main [& _]
  (run-checks!))
