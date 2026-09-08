(ns jdbc.chdb.durable.digest
  "Incremental file digests for Durable checkpoint publication and recovery."
  #?(:jolt (:require [jolt.ffi :as ffi]))
  (:import [java.io FileInputStream]
           [java.nio.file Path]
           [java.security MessageDigest]
           [java.util Arrays]))

(def ^:private chunk-bytes 65536)

(defn- byte->hex [value]
  (format "%02x" (bit-and 255 value)))

#?(:jolt
   (do
     (ffi/defcfn evp-md-ctx-new "EVP_MD_CTX_new" [] :pointer)
     (ffi/defcfn evp-md-ctx-free "EVP_MD_CTX_free" [:pointer] :void)
     (ffi/defcfn evp-sha256 "EVP_sha256" [] :pointer)
     (ffi/defcfn evp-digest-init
       "EVP_DigestInit_ex" [:pointer :pointer :pointer] :int)
     (ffi/defcfn evp-digest-update
       "EVP_DigestUpdate" [:pointer :pointer :size_t] :int)
     (ffi/defcfn evp-digest-final
       "EVP_DigestFinal_ex" [:pointer :pointer :pointer] :int)

     (defn- openssl-failure! []
       ;; Do not retain native error strings or paths in Durable diagnostics.
       (throw (ex-info "Incremental checkpoint digest failed"
                       {:type ::digest-failed})))

     (defn- sha256-file-jolt [path]
       (let [context (evp-md-ctx-new)]
         (when (ffi/null? context)
           (openssl-failure!))
         (try
           (with-open [arena (ffi/confined-arena)]
             (let [input-buffer (ffi/alloc arena chunk-bytes)
                   output-buffer (ffi/alloc arena 32)
                   output-length (ffi/alloc arena :uint)
                   managed-buffer (byte-array chunk-bytes)]
               (when-not (= 1 (evp-digest-init context (evp-sha256) ffi/null))
                 (openssl-failure!))
               (with-open [input (FileInputStream. (.toFile ^Path path))]
                 (loop []
                   (let [n (.read input managed-buffer)]
                     (when (pos? n)
                       (ffi/write-array input-buffer managed-buffer 0 n)
                       (when-not (= 1 (evp-digest-update context input-buffer n))
                         (openssl-failure!))
                       (recur)))))
               (when-not (= 1 (evp-digest-final
                               context output-buffer output-length))
                 (openssl-failure!))
               (when-not (= 32 (ffi/read output-length :uint))
                 (openssl-failure!))
               (apply str (map byte->hex (ffi/read-array output-buffer 32)))))
           (finally
             (evp-md-ctx-free context)))))))

#?(:clj
   (defn- sha256-file-jvm [path]
     (let [digest (MessageDigest/getInstance "SHA-256")
           buffer (byte-array chunk-bytes)]
       (with-open [input (FileInputStream. (.toFile ^Path path))]
         (loop []
           (let [n (.read input buffer)]
             (when (pos? n)
               (.update digest (if (= n (alength buffer))
                                 buffer
                                 (Arrays/copyOf buffer n)))
               (recur)))))
       (apply str (map byte->hex (.digest digest))))))

(defn sha256-file
  "Return a lowercase SHA-256 digest without retaining payload-sized state."
  [path]
  #?(:jolt (sha256-file-jolt path)
     :clj (sha256-file-jvm path)))
