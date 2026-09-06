(ns jdbc.chdb.typed.controls.malformed-abi-descriptor-lookup
  "Mutation control: an unregistered ABI function id must be rejected."
  (:require [jdbc.chdb.abi :as abi]
            [jdbc.chdb.typed.abi :as typed-abi]
            [typed.clojure :as t]))

(t/ann malformed-lookup [-> typed-abi/FunctionSpec])
(defn malformed-lookup []
  (abi/function-spec :not-a-chdb-function))
