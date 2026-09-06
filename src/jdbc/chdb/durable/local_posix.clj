(ns jdbc.chdb.durable.local-posix
  "Jolt POSIX locking and durability edge for the shared local backend."
  (:require [clojure.string :as str]
            [jdbc.chdb.durable.backend :as durable]
            [jolt.ffi :as ffi]))

(ffi/load-library)

(ffi/defcfn c-open "open" [:string :int :&] :int
  {:capture-native-error true})
(ffi/defcfn c-flock "flock" [:int :int] :int
  {:blocking true :capture-native-error true})
(ffi/defcfn c-fsync "fsync" [:int] :int
  {:blocking true :capture-native-error true})
(ffi/defcfn c-mkdir "mkdir" [:string :int] :int
  {:capture-native-error true})
(ffi/defcfn c-lseek "lseek" [:int :int64 :int] :int64
  {:capture-native-error true})
(ffi/defcfn c-read "read" [:int :pointer :size_t] :ssize_t
  {:blocking true :capture-native-error true})
(ffi/defcfn c-write "write" [:int :pointer :size_t] :ssize_t
  {:blocking true :capture-native-error true})
(ffi/defcfn c-close "close" [:int] :int
  {:capture-native-error true})

(def ^:private o-rdonly 0)
(def ^:private o-wronly 1)
(def ^:private o-rdwr 2)
(def ^:private mode-0600 384)
(def ^:private mode-0700 448)
(def ^:private lock-exclusive 2)
(def ^:private eintr 4)
(def ^:private eexist 17)
(def ^:private seek-set 0)
(def ^:private seek-end 2)
(def ^:private copy-buffer-bytes 65536)

(defn- target []
  (let [os-name (str/lower-case (or (System/getProperty "os.name") ""))]
    (cond
      (str/includes? os-name "linux")
      {:os :linux :o-creat 64 :o-excl 128 :o-nofollow 131072}
      (or (str/includes? os-name "mac") (str/includes? os-name "darwin"))
      {:os :darwin :o-creat 512 :o-excl 2048 :o-nofollow 256}
      :else
      (throw (ex-info "Durable local POSIX backend is unsupported on this host"
                      {:type ::unsupported-target :os os-name})))))

(defn- io-error [operation errno]
  ;; Paths can contain tenant data, so only the operation and captured native
  ;; error cross this boundary.
  (ex-info (str "Durable local " (name operation) " failed")
           {:type ::io-failed :operation operation :errno errno}))

(defn- open! [path flags mode?]
  (let [[fd errno] (if mode?
                     (c-open (str path) flags mode-0600)
                     (c-open (str path) flags))]
    (when (neg? fd)
      (throw (io-error :open errno)))
    fd))

(defn- close-result [fd outcome]
  (let [[result errno] (c-close fd)]
    (cond
      (:error outcome) (throw (:error outcome))
      (neg? result) (throw (io-error :close errno))
      :else (:value outcome))))

(defn- with-fd [fd f]
  (let [outcome (try {:value (f fd)}
                     (catch Throwable error {:error error}))]
    (close-result fd outcome)))

(defn- zero-call! [operation call fd]
  (loop []
    (let [[result errno] (call fd)]
      (cond
        (zero? result) nil
        (= eintr errno) (recur)
        :else (throw (io-error operation errno))))))

(defn- seek! [fd offset]
  (loop []
    (let [[position errno] (c-lseek fd offset seek-set)]
      (cond
        (= position offset) position
        (= eintr errno) (recur)
        :else (throw (io-error :seek errno))))))

(defn- size! [fd]
  (loop []
    (let [[position errno] (c-lseek fd 0 seek-end)]
      (cond
        (not (neg? position)) position
        (= eintr errno) (recur)
        :else (throw (io-error :seek-end errno))))))

(defn- read-once! [fd buffer]
  (loop []
    (let [[n errno] (c-read fd buffer copy-buffer-bytes)]
      (if (neg? n)
        (if (= eintr errno) (recur) (throw (io-error :read errno)))
        n))))

(defn- write-all-with! [write-call fd buffer length]
  (loop [offset 0]
    (when (< offset length)
      ;; Jolt pointers are native addresses. Advance both the pointer and the
      ;; remaining count so a short write cannot replay the prefix.
      (let [[n errno] (write-call fd (+ buffer offset) (- length offset))]
        (cond
          (pos? n) (recur (+ offset n))
          (= eintr errno) (recur offset)
          :else (throw (io-error :write errno)))))))

(defn- write-all! [fd buffer length]
  (write-all-with! c-write fd buffer length))

(defn- native-copy-file-range! [source destination source-offset target-offset]
  (let [{:keys [o-nofollow]} (target)]
    (with-fd
      (open! source (bit-or o-rdonly o-nofollow) false)
      (fn [source-fd]
        (with-fd
          (open! destination (bit-or o-wronly o-nofollow) false)
          (fn [target-fd]
            (when-not (= target-offset (size! target-fd))
              (throw (ex-info "Durable local copy target has an unexpected size"
                              {:type ::invalid-copy-target})))
            (seek! source-fd source-offset)
            (seek! target-fd target-offset)
            (ffi/with-alloc [buffer copy-buffer-bytes]
              (loop [total 0]
                (let [n (read-once! source-fd buffer)]
                  (if (zero? n)
                    total
                    (do
                      (write-all! target-fd buffer n)
                      (recur (+ total n)))))))))))))

(deftype ^:private PosixDurability []
  durable/LocalDurability
  (with-exclusive-lock [_ lock-path f]
    (let [{:keys [o-creat o-nofollow]} (target)
          fd (open! lock-path (bit-or o-rdwr o-creat o-nofollow) true)]
      (with-fd fd
        (fn [fd]
          (zero-call! :lock #(c-flock % lock-exclusive) fd)
          (f)))))

  (sync-file! [_ path]
    (let [{:keys [o-nofollow]} (target)]
      (with-fd (open! path (bit-or o-rdonly o-nofollow) false)
        #(zero-call! :sync-file c-fsync %))))

  (sync-directory! [_ path]
    (let [{:keys [o-nofollow]} (target)]
      (with-fd (open! path (bit-or o-rdonly o-nofollow) false)
        #(zero-call! :sync-directory c-fsync %))))

  (create-private-directory! [_ path]
    (let [[result errno] (c-mkdir (str path) mode-0700)]
      (cond
        (zero? result) true
        (= eexist errno) false
        :else (throw (io-error :create-directory errno)))))

  (create-private-temp-file! [_ parent]
    (let [{:keys [o-creat o-excl]} (target)]
      (loop []
        (let [path (.resolve parent (str ".jchdb-" (random-uuid) ".tmp"))
              [fd errno] (c-open (str path)
                                 (bit-or o-rdwr o-creat o-excl)
                                 mode-0600)]
          (cond
            (not (neg? fd)) (do (with-fd fd (constantly nil)) path)
            (= eexist errno) (recur)
            :else (throw (io-error :create-temp-file errno)))))))

  (create-private-file! [_ path]
    (let [{:keys [o-creat o-excl o-nofollow]} (target)
          [fd errno] (c-open (str path)
                             (bit-or o-rdwr o-creat o-excl o-nofollow)
                             mode-0600)]
      (cond
        (not (neg? fd)) (do (with-fd fd (constantly nil)) true)
        (= eexist errno) false
        :else (throw (io-error :create-file errno)))))

  (copy-file-range! [_ source target source-offset target-offset]
    (native-copy-file-range! source target source-offset target-offset)))

(defn posix-durability
  "Return the Jolt POSIX lock/fsync adapter for focused conformance tests and
  advanced composition with the shared local provider."
  []
  (target)
  (PosixDurability.))

(defn local-backend
  "Create a crash-durable, process-safe local backend on Linux or macOS Jolt."
  [root]
  (durable/local-backend root (posix-durability)))
