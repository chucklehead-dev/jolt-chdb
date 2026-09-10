(ns jdbc.chdb.durable.compatibility
  "Pure chDB release precedence shared by compatibility and head transitions."
  (:require [clojure.string :as str]))

(defn- version-parts [version]
  (when (string? version)
    (when-let [[_ major minor patch prerelease]
               (re-matches
                #"(?i)^v?(\d+)\.(\d+)\.(\d+)(?:-([0-9a-z.-]+))?(?:\+[0-9a-z.-]+)?$"
                version)]
      {:core [(bigint major) (bigint minor) (bigint patch)]
       :prerelease (when prerelease (str/split prerelease #"\."))})))

(defn release-version? [version]
  (boolean (version-parts version)))

(defn- compare-prerelease-part [left right]
  (let [left-number? (boolean (re-matches #"\d+" left))
        right-number? (boolean (re-matches #"\d+" right))]
    (cond
      (and left-number? right-number?)
      (compare (bigint left) (bigint right))

      left-number? -1
      right-number? 1
      :else (compare left right))))

(defn- compare-prerelease [left right]
  (cond
    (and (nil? left) (nil? right)) 0
    (nil? left) 1
    (nil? right) -1
    :else
    (loop [left left right right]
      (cond
        (and (empty? left) (empty? right)) 0
        (empty? left) -1
        (empty? right) 1
        :else (let [comparison (compare-prerelease-part
                                (first left) (first right))]
                (if (zero? comparison)
                  (recur (next left) (next right))
                  comparison))))))

(defn compare-release-versions
  "Compare valid chDB releases by numeric release/prerelease precedence."
  [left right]
  (let [left (version-parts left)
        right (version-parts right)]
    (when (and left right)
      (let [core-comparison (compare (:core left) (:core right))]
        (if (zero? core-comparison)
          (compare-prerelease (:prerelease left) (:prerelease right))
          core-comparison)))))
