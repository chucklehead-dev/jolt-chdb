(ns jdbc.chdb.install
  "Explicit, checksum-verified installer for the libchdb release pinned by
  jdbc.chdb.native. Requiring the driver never downloads native code."
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [jdbc.chdb.native :as native]
            [jolt.ffi :as ffi]))

(def ^:private release-base
  (str "https://github.com/chdb-io/chdb/releases/download/v" native/version))

(ffi/defcfn c-sha256 "SHA256" [:pointer :size_t :pointer] :pointer)

(defn- ensure-directory! [path]
  (let [file (java.io.File. path)]
    (when-not (or (.isDirectory file) (.mkdirs file))
      (throw (ex-info (str "could not create " path) {:path path}))))
  path)

(defn- delete-file! [path]
  (let [file (java.io.File. path)]
    (when (and (.exists file) (not (.delete file)))
      (throw (ex-info (str "could not remove " path) {:path path})))))

(defn- ensure-crypto! []
  (let [candidates (if (= :darwin (:os (native/platform)))
                     ["/opt/homebrew/opt/openssl@3/lib/libcrypto.dylib"
                      "/usr/local/opt/openssl@3/lib/libcrypto.dylib"
                      "libcrypto.dylib"]
                     ["libcrypto.so.3" "libcrypto.so.1.1" "libcrypto.so"])]
    (when-not (some (fn [candidate]
                      (try (ffi/load-library candidate) true
                           (catch Throwable _ false)))
                    candidates)
      (throw (ex-info "could not load OpenSSL libcrypto to verify libchdb"
                      {:type ::crypto-unavailable})))))

(defn- sha256 [path]
  (ensure-crypto!)
  (with-open [input (java.io.FileInputStream. path)]
    (let [data (.readAllBytes input)
          size (alength data)
          source (ffi/alloc (max 1 size))
          digest (ffi/alloc 32)]
      (try
        (ffi/write-array source data)
        (when (ffi/null? (c-sha256 source size digest))
          (throw (ex-info (str "SHA256 failed for " path) {:path path})))
        (apply str (map #(format "%02x" (bit-and % 0xff))
                        (seq (ffi/read-array digest 32))))
        (finally
          (ffi/free digest)
          (ffi/free source))))))

(defn- fetch! [url path]
  (delete-file! path)
  (println "libchdb: downloading" url)
  (let [fetch (requiring-resolve 'jolt.mvn-http/fetch)]
    (when-not (fetch url path)
      (throw (ex-info (str "failed to download " url) {:url url :path path}))))
  (when-not (.isFile (java.io.File. path))
    (throw (ex-info (str "download does not exist: " path) {:path path})))
  path)

(defn- extract! [archive directory]
  (let [{:keys [exit out err]} (shell/sh "tar" "-xzf" archive "-C" directory)]
    (when-not (zero? exit)
      (throw (ex-info "could not extract libchdb archive"
                      {:archive archive :exit exit :out out :err err}))))
  directory)

(defn- find-library [directory library-name]
  (first (filter #(and (.isFile %) (= library-name (.getName %)))
                 (file-seq (java.io.File. directory)))))

(defn- nonblank-env [name]
  (some-> (System/getenv name) str/trim not-empty))

(defn install!
  "Download, verify, and install the pinned libchdb for this platform."
  []
  (if-let [external (nonblank-env "JOLT_CHDB_LIB")]
    (do
      (when-not (.isFile (java.io.File. external))
        (throw (ex-info (str "JOLT_CHDB_LIB does not exist: " external)
                        {:path external})))
      (println "libchdb: using" external "(JOLT_CHDB_LIB)")
      external)
    (let [{:keys [os arch library-name]} (native/platform)
          {asset-name :name expected-sha :sha256} (get native/assets [os arch])
          directory (ensure-directory! (native/cache-directory))
          target (native/library-path)
          marker (str target ".archive-sha256")]
    (if (and (.isFile (java.io.File. target))
             (.isFile (java.io.File. marker))
             (= expected-sha (str/trim (slurp marker))))
      (do (println "libchdb: already installed from verified archive" target) target)
      (let [archive (str directory "/" asset-name ".download")
            staging (str directory "/extract-" (System/currentTimeMillis))
            url (str (or (some-> (System/getenv "JOLT_CHDB_RELEASE_BASE") str/trim not-empty)
                         release-base)
                     "/" asset-name)]
        (ensure-directory! staging)
        (try
          (fetch! url archive)
          (let [actual (sha256 archive)]
            (when-not (= expected-sha actual)
              (throw (ex-info "SHA-256 mismatch for libchdb archive"
                              {:expected expected-sha :actual actual :path archive}))))
          (extract! archive staging)
          (let [source (find-library staging library-name)]
            (when-not source
              (throw (ex-info (str library-name " is absent from libchdb archive")
                              {:archive archive})))
            (delete-file! target)
            (when-not (.renameTo source (java.io.File. target))
              (throw (ex-info (str "could not install " target) {:path target})))
            (delete-file! marker)
            (spit marker (str expected-sha "\n")))
          (println "libchdb: installed" target "from verified archive" asset-name)
          target
          (finally
            (delete-file! archive)
            ;; Extraction contains only a checksum-verified release archive.
            (doseq [file (reverse (file-seq (java.io.File. staging)))]
              (when (.exists file) (.delete file))))))))))

(defn -main [& _]
  (install!))
