(let [[mode & args] *command-line-args*
      namespace (case mode
                  "option" 'jdbc.chdb-owned-stream-option-native-test
                  "uncertainty" 'jdbc.chdb-owned-compact-stream-native-test
                  "bad-option-read" 'jdbc.chdb-owned-stream-option-native-test
                  (throw (ex-info "Unknown stream test mode" {})))]
  ;; These are acceptance proofs, not optional assertion instrumentation.
  (binding [*assert* false] (require namespace :reload))
  (let [run (ns-resolve namespace '-main)]
    (if (= mode "bad-option-read")
      (let [[outcome root] args
            failure (with-redefs-fn {(ns-resolve namespace 'expected) (constantly [])}
                      #(try (run "reader" outcome root) nil (catch Throwable e e)))]
        (when-not (= "Owned streaming native check failed" (some-> failure .getMessage))
          (throw (ex-info "Changed physical expectation was not rejected correctly" {})))
        (println "Changed physical-row expectation rejected"))
      (apply run args))))
