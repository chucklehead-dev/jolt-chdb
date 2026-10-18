(ns typed-storage-batch-confirmation
  (:require [typed-storage-confirmation-receipt :as selection]
            [typed-storage-batch-lib :as batch]))

(def schedule [[1 "baseline"] [2 "candidate"] [3 "candidate"] [4 "baseline"]])

(defn selection! [report arm phase]
  (selection/validate! report arm phase)
  ;;35 warmup/measured exports, with2 checkpoint substitutions: exactly33 WALs.
  ;; A positive but incomplete observer is not a selected current workload.
  (when (and (= phase "window") (not= 33 (:file-publications report)))
    (batch/fail!))
  report)
