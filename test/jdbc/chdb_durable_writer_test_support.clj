(ns jdbc.chdb-durable-writer-test-support
  (:require [clojure.string :as str]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]))

(def base-options
  {:owner "writer-1"
   :instance "instance-1"
   :expires-at 1000M
   :now 0M
   :clock-skew 0M
   :database "default"
   :engine-version "26.7.2-rc.2"
   :backup-format 1
   :min-reader "26.7.2-rc.2"})

(defn check [failures label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn error-type [f]
  (try (f) nil (catch Throwable error (:type (ex-data error)))))

(defn fake-operations [calls close-count]
  {:classification-sql! (fn [sql _] sql)
   :classify! (fn [_ sql _]
                {:query-class (if (str/starts-with? sql "SELECT")
                                :read-only :mutating)
                 :statement-count 1 :has-secrets false
                 :writes-only-target-database true
                 :changes-database-lifecycle false})
   :analyze-query! (fn [_ sql database]
                     (swap! calls conj [:analyze-query sql database]))
   :analyze-execute! (fn [_ sql database]
                       (swap! calls conj [:analyze-execute sql database]))
   :execute-native! (fn [_ sql _]
                      (swap! calls conj [:execute sql])
                      {:sql sql})
   :query-native! (fn [_ sql _]
                    (swap! calls conj [:execute sql])
                    {:sql sql})
   :close-native! (fn [_] (swap! close-count inc))
   :cleanup-scratch! (fn [] nil)})

(defn model-checkpoint-operations [calls close-count]
  (let [bytes (.getBytes "abc" "UTF-8")]
    (assoc (fake-operations calls close-count)
           :create-checkpoint! (fn [_ _] :model-checkpoint)
           :delete-checkpoint! (fn [_] nil)
           :publish-checkpoint!
           (fn [store token _]
             (let [sequence (inc (get-in (:head (control/read-head! store))
                                         ["manifest" "seq"]))
                   reference
                   {"key" (str "checkpoints/" (:generation token) "-"
                               sequence "-" (format "%08x" sequence)
                               ".tar.gz")
                    "size" 3
                    "sha256" "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}]
               (backend/put-bytes-if-absent!
                store (get reference "key") bytes)
               {:status :published :reference reference}))
           :verify-checkpoint-reference! control/verify-byte-reference!)))
