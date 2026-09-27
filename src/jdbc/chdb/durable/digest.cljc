(ns jdbc.chdb.durable.digest
  "Incremental file digests for Durable checkpoint publication and recovery."
  #?(:jolt (:require [jolt.ffi :as ffi]))
  (:import [java.io FileInputStream InputStream]
           [java.nio.file Path]
           [java.security MessageDigest]
           [java.util Arrays]))

(def ^:private chunk-bytes 65536)

(defn- byte->hex [value]
  (format "%02x" (bit-and 255 value)))

(defn- zero-read! []
  (throw (ex-info "Incremental digest input returned no bytes before EOF"
                  {:type ::zero-read})))

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

     (defn- hash+count-input-stream-jolt [^InputStream input]
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
               (let [byte-count
                     (loop [total 0]
                       (let [n (.read input managed-buffer)]
                         (cond
                           (neg? n) total
                           (zero? n) (zero-read!)
                           :else
                           (do
                             (ffi/write-array input-buffer managed-buffer 0 n)
                             (when-not (= 1 (evp-digest-update context input-buffer n))
                               (openssl-failure!))
                             (recur (+ total n))))))]
                 (when-not (= 1 (evp-digest-final
                                 context output-buffer output-length))
                   (openssl-failure!))
                 (when-not (= 32 (ffi/read output-length :uint))
                   (openssl-failure!))
                 {:byte-count byte-count
                  :sha256 (apply str (map byte->hex
                                         (ffi/read-array output-buffer 32)))})))
           (finally
             (evp-md-ctx-free context)))))

     (defn- sha256-bytes-jolt [bytes]
       (let [context (evp-md-ctx-new)]
         (when (ffi/null? context)
           (openssl-failure!))
         (try
           (with-open [arena (ffi/confined-arena)]
             (let [input-buffer (ffi/alloc arena (alength bytes))
                   output-buffer (ffi/alloc arena 32)
                   output-length (ffi/alloc arena :uint)]
               (when-not (= 1 (evp-digest-init context (evp-sha256) ffi/null))
                 (openssl-failure!))
               (ffi/write-array input-buffer bytes 0 (alength bytes))
               (when-not (= 1 (evp-digest-update context input-buffer
                                                  (alength bytes)))
                 (openssl-failure!))
               (when-not (= 1 (evp-digest-final
                               context output-buffer output-length))
                 (openssl-failure!))
               (when-not (= 32 (ffi/read output-length :uint))
                 (openssl-failure!))
               (apply str (map byte->hex (ffi/read-array output-buffer 32)))))
           (finally
             (evp-md-ctx-free context)))))))

#?(:clj
   (do
     (defn- sha256-bytes-jvm [bytes]
       (apply str (map byte->hex
                       (.digest (MessageDigest/getInstance "SHA-256") bytes))))

     (defn- hash+count-input-stream-jvm [^InputStream input]
       (let [digest (MessageDigest/getInstance "SHA-256")
             buffer (byte-array chunk-bytes)
             byte-count
             (loop [total 0]
               (let [n (.read input buffer)]
                 (cond
                   (neg? n) total
                   (zero? n) (zero-read!)
                   :else
                   (do (.update digest (if (= n (alength buffer))
                                         buffer
                                         (Arrays/copyOf buffer n)))
                       (recur (+ total n))))))]
         {:byte-count byte-count
          :sha256 (apply str (map byte->hex (.digest digest)))}))))

(defn sha256-bytes
  "Return a lowercase SHA-256 digest for caller-supplied bytes."
  [bytes]
  #?(:jolt (sha256-bytes-jolt bytes)
     :clj (sha256-bytes-jvm bytes)))

(defn hash+count-input-stream
  "Hash from the current position through EOF with bounded memory. Return
  {:byte-count n :sha256 lowercase-hex}. Borrow input; never close it."
  [input]
  #?(:jolt (hash+count-input-stream-jolt input)
     :clj (hash+count-input-stream-jvm input)))

(defn sha256-file
  "Return a lowercase SHA-256 digest without retaining payload-sized state."
  [path]
  (with-open [input (FileInputStream. (.toFile ^Path path))]
    (:sha256 (hash+count-input-stream input))))
