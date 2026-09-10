(ns jdbc.chdb.durable.compatibility
  "Pure chDB release precedence shared by compatibility and head transitions."
  (:require [clojure.string :as str]))

(def ^:private prerelease-ranks
  {"alpha" 0 "a" 0 "beta" 1 "b" 1 "pre" 2 "rc" 3})

(def ^:private release-rank (count prerelease-ranks))

;; Python str.strip() whitespace, made explicit so Jolt and the JVM do not
;; inherit a host trim definition that accepts a different version language.
(def ^:private python-whitespace
  (into #{133 160 5760 8232 8233 8239 8287 12288}
        (concat (range 9 14) (range 28 33) (range 8192 8203))))

(defn- python-strip [text]
  (let [length (count text)
        start (loop [index 0]
                (if (and (< index length)
                         (contains? python-whitespace (int (nth text index))))
                  (recur (inc index))
                  index))
        end (loop [index length]
              (if (and (> index start)
                       (contains? python-whitespace
                                  (int (nth text (dec index)))))
                (recur (dec index))
                index))]
    (subs text start end)))

(defn- version-parts [version]
  (when (string? version)
    (when-let [[_ release prerelease prerelease-number]
               (re-matches
                #"(?i)([0-9]+(?:\.[0-9]+)*)(?:[-.]([A-Za-z]+)\.?([0-9]+)?)?(?:[-.+].*)?"
                (python-strip version))]
      (let [rank (when prerelease
                   (get prerelease-ranks (str/lower-case prerelease)))]
        {:release (mapv bigint (str/split release #"\."))
         :prerelease-rank (or rank release-rank)
         :prerelease-number (if rank
                              (bigint (or prerelease-number "0"))
                              0)}))))

(defn release-version? [version]
  (boolean (version-parts version)))

(defn- pad-release [release width]
  (into release (repeat (- width (count release)) 0)))

(defn compare-release-versions
  "Compare chDB versions using Durable V1's pinned Python precedence.

  Returns nil when either input is unrecognized. Numeric release tuples are
  zero-padded to equal width. Only alpha/a, beta/b, pre, and rc are prerelease
  markers; every other suffix has stable-release precedence."
  [left right]
  (let [left (version-parts left)
        right (version-parts right)]
    (when (and left right)
      (let [width (max (count (:release left)) (count (:release right)))
            release-comparison
            (compare (pad-release (:release left) width)
                     (pad-release (:release right) width))]
        (if-not (zero? release-comparison)
          release-comparison
          (compare [(:prerelease-rank left) (:prerelease-number left)]
                   [(:prerelease-rank right) (:prerelease-number right)]))))))
