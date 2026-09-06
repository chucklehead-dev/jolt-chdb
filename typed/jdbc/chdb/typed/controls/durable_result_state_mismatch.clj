(ns jdbc.chdb.typed.controls.durable-result-state-mismatch
  "Mutation control: unsupported Durable data cannot be a supported result."
  (:require [jdbc.chdb.typed.abi :as typed-abi]
            [jdbc.chdb.typed.driver :as driver]
            [typed.clojure :as t]))

(t/ann impossible-supported-result typed-abi/SupportedCapability)
(def impossible-supported-result driver/stable-unsupported)
