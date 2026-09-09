(ns jdbc.chdb-durable-open-test-support
  (:require [jdbc.chdb.durable.writer :as writer])
  (:import [java.io File]
           [java.nio.file Files OpenOption Path]
           [java.nio.file.attribute FileAttribute]))

(defn delete-tree! [path]
  (let [file (.toFile ^Path path)]
    (when (.exists file)
      (doseq [child (or (.listFiles file) (make-array File 0))]
        (delete-tree! (.toPath child)))
      (.delete file)))
  nil)

(defn fake-open-operations [calls clocks close-count cleanup-count]
  {:classification-sql! (fn [sql _] sql)
   :now-ms (fn []
             (let [value (first @clocks)]
               (swap! clocks #(if (next %) (vec (next %)) %))
               value))
   :await-heartbeat! (fn [stop _] @stop :stop)
   :durable-capability (fn [] {:status :supported
                               :native-version "26.7.2-rc.2"})
   :create-scratch! (fn [_]
                      (let [path (Files/createTempDirectory
                                  "jolt-chdb-open-test-"
                                  (make-array FileAttribute 0))]
                        (swap! calls conj [:scratch])
                        path))
   :cleanup-scratch! (fn [path]
                       (swap! cleanup-count inc)
                       (delete-tree! path))
   :open-native! (fn [_] (swap! calls conj [:open]) :fake-handle)
   :close-native! (fn [_] (swap! close-count inc))
   :restore-database! (fn [_ database _]
                        (swap! calls conj [:restore database]))
   :create-checkpoint! (fn [_ database scratch]
                         (let [path (.resolve ^Path scratch
                                              "new-checkpoint.tar.gz")]
                           (swap! calls conj [:backup database])
                           (Files/write path (.getBytes "checkpoint" "UTF-8")
                                        (make-array OpenOption 0))
                           path))
   :delete-checkpoint! (fn [path] (Files/deleteIfExists ^Path path))
   :create-database! (fn [_ database]
                       (swap! calls conj [:create database]))
   :use-database! (fn [_ database]
                    (swap! calls conj [:use database]))
   :analyze-query! (fn [_ sql database]
                     (swap! calls conj [:analyze-query sql database]))
   :classify! (fn [_ sql _]
                {:query-class (if (= "SELECT ?" sql) :read-only :mutating)
                 :statement-count 1 :has-secrets false
                 :writes-only-target-database true
                 :changes-database-lifecycle false})
   :query-native! (fn [_ sql params]
                    (swap! calls conj [:query sql (vec params)])
                    {:labels ["value"] :rows [[(first params)]] :count 1})
   :query-bytes-native! (fn [_ sql params options]
                          (swap! calls conj [:query-bytes sql (vec params)
                                             (:format options)])
                          {:byte-count 3 :bytes (byte-array [1 2 3])})
   :analyze-execute! (fn [_ sql database]
                       (swap! calls conj [:analyze-execute sql database]))
   :execute-native! (fn [_ sql _]
                      (swap! calls conj [:execute sql])
                      {:sql sql})})
