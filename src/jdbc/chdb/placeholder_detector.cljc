(ns jdbc.chdb.placeholder-detector
  "Internal behavior-qualified lexical detector. Nil means portable fallback."
  #?(:jolt (:require [clojure.java.io :as io] [jolt.scheme :as scheme])))

#?(:jolt
   (do
     (defmacro ^:private native-source []
       (when-let [resource (io/resource "jdbc/chdb/placeholder_detector.ss")]
         (slurp resource)))
     (def ^:private source (native-source))
     (defn- qualified-detector [detect]
       (when (every? (fn [[sql expected]] (= expected (detect sql)))
                     [["" false] ["SELECT ?" true] ["SELECT '?'" false]
                      ["SELECT \"?\", `?`" false]
                      ["SELECT 'it\\'?', ?" true]
                      ["SELECT 'it''s ?'" false]
                      ["SELECT -- ?\n?" true] ["SELECT -- ?\r?" false]
                      ["SELECT /* ? /* ? */ ? */ ?" true]
                      ["SELECT /* ? /* ? */ ? */ 1" false]
                      ["SELECT 'unterminated ?" false]
                      ["SELECT /* unterminated ?" false]
                      ["SELECT 'λ😀\u0000?', ?" true]])
         detect))
     (def ^:private native-detector
       (delay
         (try
           (when (string? source)
             (qualified-detector (scheme/eval-string source)))
           (catch Throwable _ nil))))))

(defn native-enabled?
  "Successful source-mode qualification only; not an AOT guarantee."
  [] #?(:jolt (some? @native-detector) :clj false))

(defn try-code-placeholder?
  "Detect an executable positional question mark, or nil if unsupported.
  False is a successful result, NOT a declined call. Only immutable Strings
  are admitted. The procedure owns no shared state and retains no query data."
  [sql]
  #?(:jolt (when (string? sql)
             (when-let [detect @native-detector]
               (detect sql)))
     :clj nil))
