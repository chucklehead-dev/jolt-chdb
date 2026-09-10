(ns jdbc.chdb-durable-head-depth-test
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [jdbc.chdb.durable.head :as head]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- thrown-data [f]
  (try (f) nil (catch Throwable error (ex-data error))))

(def base-head
  {"protocol" {"version" 1 "reader_features" [] "writer_features" []}
   "engine" {"name" "chdb" "version" "1.0.0"
             "backup_format" 1 "min_reader" "1.0.0"}
   "lease" {"generation" 1 "owner" nil "instance" nil
            "expires_at" nil}
   "manifest" {"db" "default" "base" nil "wal" [] "seq" 0}})

(defn- nest-value [container depth]
  (nth (iterate (case container
                  "array" #(vector %)
                  "object" #(hash-map "nested" %))
                0)
       depth))

(defn- nested-head [container total-depth]
  ;; The head's required root object consumes one container level.
  (assoc base-head "private-extension"
         (nest-value container (dec total-depth))))

(declare unbounded-array-mutant unbounded-object-mutant)

(defn- unbounded-value-mutant [text index]
  (case (.charAt text index)
    \[ (unbounded-array-mutant text index)
    \{ (unbounded-object-mutant text index)
    (inc index)))

(defn- unbounded-array-mutant [text index]
  ;; Causal negative control: the prior scanner recursively descended without
  ;; checking depth. This intentionally supports only the nested-array fixture.
  (let [end (unbounded-value-mutant text (inc index))]
    (when-not (= \] (.charAt text end))
      (throw (ex-info "invalid mutant fixture" {})))
    (inc end)))

(defn- unbounded-object-mutant [text index]
  ;; The fixture uses one fixed key so the mutant isolates container recursion.
  (when-not (= "{\"nested\":" (subs text index (+ index 10)))
    (throw (ex-info "invalid mutant fixture" {})))
  (let [end (unbounded-value-mutant text (+ index 10))]
    (when-not (= \} (.charAt text end))
      (throw (ex-info "invalid mutant fixture" {})))
    (inc end)))

(defn- nested-array-text [depth]
  (str (apply str (repeat depth "[")) "0"
       (apply str (repeat depth "]"))))

(defn- nested-object-text [depth]
  (str (apply str (repeat depth "{\"nested\":")) "0"
       (apply str (repeat depth "}"))))

(defn run-checks! []
  (reset! failures 0)
  (println "Durable V1 JSON nesting bound")
  (let [{:keys [source maximum-container-depth causal-mutant-depth cases]}
        (edn/read-string
         (slurp "test/fixtures/durable/head-json-depth.edn"))]
    (check "fixture pins the normative upstream protocol"
           (select-keys head/protocol-source [:repository :commit :document])
           {:repository (:repository source)
            :commit (:commit source)
            :document (:protocol source)})
    (check "fixture independently pins the implementation depth bound"
           maximum-container-depth head/max-json-depth)
    (doseq [{:keys [id container]} cases]
      (let [exact (nested-head container maximum-container-depth)
            over (nested-head container (inc maximum-container-depth))
            encoded (head/encode exact)
            raw-exact (.getBytes (json/write-str exact) "UTF-8")
            raw-over (.getBytes (json/write-str over) "UTF-8")
            over-data (thrown-data #(head/encode over))
            raw-over-data (thrown-data #(head/decode raw-over :writer))]
        (check (str (name id) " at the exact depth encodes")
               true (bytes? encoded))
        (check (str (name id) " raw exact-depth input decodes")
               exact (head/decode raw-exact :writer))
        (check (str (name id) " one level over is rejected before encoding")
               ::head/corrupt (:type over-data))
        (check (str (name id) " raw input one level over is rejected")
               ::head/corrupt (:type raw-over-data))
        (check (str (name id) " depth diagnostics retain no extension name")
               false (.contains (str (pr-str over-data)
                                     (pr-str raw-over-data))
                                "private-extension"))))
    (check "causal mutant exercises materially greater recursion"
           true (>= causal-mutant-depth (* 4 maximum-container-depth)))
    (doseq [[id over-text]
            [[:array (nested-array-text causal-mutant-depth)]
             [:object (nested-object-text causal-mutant-depth)]]]
      (check (str "unbounded " (name id)
                  " scanner mutant accepts the over-depth value")
             (count over-text) (unbounded-value-mutant over-text 0))
      (check (str "production scanner rejects the same over-depth "
                  (name id))
             ::head/corrupt
             (:type (thrown-data
                     #(#'head/single-json-value-bounds over-text))))))
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable JSON depth checks failed")
                    {:failures @failures})))
  (println "all Durable JSON depth checks passed")
  true)

(defn -main [& _]
  (run-checks!))
