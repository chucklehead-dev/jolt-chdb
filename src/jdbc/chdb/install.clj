(ns jdbc.chdb.install
  "Explicit, checksum-verified installer for the libchdb release pinned by
  jdbc.chdb.native. Requiring the driver never downloads native code."
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [jdbc.chdb.native :as native]))

(def ^:private release-base
  (str "https://github.com/chdb-io/chdb-core/releases/download/v" native/version))

(defn- ensure-directory! [path]
  (let [file (java.io.File. path)]
    (when-not (or (.isDirectory file) (.mkdirs file))
      (throw (ex-info (str "could not create " path) {:path path}))))
  path)

(defn- delete-file! [path]
  (let [file (java.io.File. path)]
    (when (and (.exists file) (not (.delete file)))
      (throw (ex-info (str "could not remove " path) {:path path})))))

(defn- sha256 [path]
  ;; Native release archives are hundreds of megabytes. Hash them with a
  ;; streaming platform tool instead of materializing both a host byte array
  ;; and an equally large FFI buffer. Linux ships sha256sum; macOS ships
  ;; shasum, and openssl is a final portable fallback.
  (let [commands (if (= :darwin (:os (native/platform)))
                   [["shasum" "-a" "256" path]
                    ["openssl" "dgst" "-sha256" path]]
                   [["sha256sum" path]
                    ["openssl" "dgst" "-sha256" path]])
        result (some (fn [command]
                       (let [{:keys [exit out] :as result}
                             (apply shell/sh command)]
                         (when (zero? exit) (assoc result :command command))))
                     commands)
        digest (some #(when (re-matches #"[0-9a-fA-F]{64}" %) %)
                     (some-> result :out str/trim (str/split #"\s+")))]
    (when-not (and digest (re-matches #"[0-9a-fA-F]{64}" digest))
      (throw (ex-info (str "could not compute SHA-256 for " path)
                      {:path path :commands commands})))
    (str/lower-case digest)))

(defn- fetch! [url path]
  (delete-file! path)
  (println "libchdb: downloading" url)
  ;; jolt.mvn-http intentionally buffers Maven-sized responses. libchdb's
  ;; release archive is much larger, so curl it directly to disk with bounded
  ;; memory and let the pinned digest below authenticate the completed file.
  (let [{:keys [exit out err]}
        (shell/sh "curl" "--fail" "--location" "--retry" "2"
                  "--retry-all-errors" "--silent" "--show-error"
                  "--output" path url)]
    (when-not (zero? exit)
      (throw (ex-info (str "failed to download " url)
                      {:url url :path path :exit exit :out out :err err}))))
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
