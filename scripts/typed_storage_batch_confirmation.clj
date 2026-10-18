(ns typed-storage-batch-confirmation
  (:require [typed-storage-confirmation-receipt :as selection]
            [typed-storage-batch-lib :as batch]))

(def schedule [[1 "baseline"] [2 "candidate"] [3 "candidate"] [4 "baseline"]])

(defn selection!
  ([report arm phase] (selection! report arm phase 33))
  ([report arm phase publications]
  (when-not (and (contains? #{33 97} publications)
                 (or (= 33 publications) (= "candidate" arm)))
    (batch/fail!))
  (selection/validate! report arm phase)
  ;;35 warmup/measured exports, with2 checkpoint substitutions: exactly33 WALs.
  ;; A positive but incomplete observer is not a selected current workload.
  ;; The single 100-sample follow-up has103 exports and6 substitutions:97 WALs.
  (when (and (= phase "window") (not= publications (:file-publications report)))
    (batch/fail!))
  report))
