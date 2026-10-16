;; Exercise the real nested benchmark entry point, not the small TLS fixture.
;; Inject a failure before connection/native work; forbid all HTTP requests.
(require '[jdbc.chdb.native :as native]
         '[jdbc.chdb.durable.s3-curl :as curl]
         '[jdbc.chdb-typed-storage-support :as storage])
(with-redefs [native/ensure-loaded! (fn [] nil)
              native/chdb-version (constantly "26.9.0")
              curl/request! (fn [& _] (throw (ex-info "Unexpected test HTTP" {})))
              storage/with-writer!
              (fn [& _]
                (throw (ex-info "PRIVATE-ENTRYPOINT-CANARY"
                               {:type :jdbc.chdb.durable.s3/permission})))]
  (load-file (System/getenv "TEST_CONFIRMATION_DRIVER")))
