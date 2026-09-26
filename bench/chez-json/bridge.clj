(ns chez-json.bridge
  "Benchmark-only raw Jolt-to-Chez JSON bridge. No production encoder changes."
  (:require [jolt.scheme :as scheme]))

(defn load-encoder!
  "Load the trusted local prototype and its raw Jolt representation adapter.
  Run once before measurement; requires the normal, compiler-bearing Jolt CLI."
  [path]
  ((scheme/proc "load") path)
  (scheme/eval-string
   "(begin
      ;; pmap-view-seq fills backward through pmap-fold. Walk that inverse
      ;; order directly, including collision buckets, without entry vectors.
      ;; This matters when distinct named keys serialize to the same name.
      (define (chez-json-jolt-map-fold x step seed)
        (let ((root (pmap-root x)))
          (if (not (hnode? root))
              (pmap-fold-fwd x step seed)
              (let walk ((node root) (acc seed))
                (let ((entries (hnode-arr node)))
                  (let loop ((i 0) (a acc))
                    (if (fx=? i (vector-length entries)) a
                        (let* ((entry (vector-ref entries i))
                               (next
                                 (cond
                                   ((hnode? entry) (walk entry a))
                                   ((hcoll? entry)
                                    (let reverse-bucket ((pairs (hcoll-alist entry)))
                                      (if (null? pairs) a
                                          (step (caar pairs) (cdar pairs)
                                                (reverse-bucket (cdr pairs))))))
                                   (else (step (car entry) (cdr entry) a)))))
                          (loop (fx+ i 1) next)))))))))
      (define (chez-json-jolt-name x)
        (cond ((string? x) x)
              ((keyword? x) (keyword-t-name x))
              ((symbol-t? x) (symbol-t-name x))
              (else (error 'chez-json \"unsupported Jolt object key\" x))))
      (define (chez-json-jolt-write x out flags)
        (cond
          ((jolt-nil? x) (put-string out \"null\"))
          ((or (keyword? x) (symbol-t? x))
           (chez-json-write-string (chez-json-jolt-name x) out flags))
          ((pmap? x)
           (put-char out #\\{)
           (chez-json-jolt-map-fold x
             (lambda (key child first?)
               (unless first? (put-char out #\\,))
               (chez-json-write-string (chez-json-jolt-name key) out flags)
               (put-char out #\\:)
               (chez-json-write-value child out flags)
               #f) #t)
           (put-char out #\\}))
          ((pvec? x)
           (put-char out #\\[)
           (let ((n (pvec-count x)))
             (do ((i 0 (fx+ i 1))) ((fx=? i n))
               (unless (fx=? i 0) (put-char out #\\,))
               (chez-json-write-value (pvec-nth! x i) out flags)))
           (put-char out #\\]))
          ((jolt-sequential? x)
           (put-char out #\\[)
           (let loop ((s (jolt-seq x)) (first? #t))
             (unless (jolt-nil? s)
               (unless first? (put-char out #\\,))
               (chez-json-write-value (jolt-first s) out flags)
               (loop (jolt-next s) #f)))
           (put-char out #\\]))
          (else (error 'chez-json \"unsupported Jolt value\" x))))
      (set! chez-json-foreign-write chez-json-jolt-write)
      (define (chez-json-jolt-flags options)
        (unless (pmap? options)
          (error 'chez-json \"options must be a Jolt map\" options))
        (chez-json-options
          (pmap-fold-fwd options
            (lambda (k v a)
              (unless (and (keyword? k) (not (keyword-t-ns k)))
                (error 'chez-json \"option keys must be unqualified keywords\" k))
              (cons (cons (string->symbol (keyword-t-name k)) v) a)) '())))
      (define (chez-json-jolt-encode x options)
        (let ((flags (chez-json-jolt-flags options)))
          (call-with-string-output-port
            (lambda (out) (chez-json-write-value x out flags)))))
      (define (chez-json-jolt-encode-rows rows options)
        (unless (pvec? rows)
          (error 'chez-json \"rows must be a Jolt vector\" rows))
        (let ((flags (chez-json-jolt-flags options)) (n (pvec-count rows)))
          (call-with-string-output-port
            (lambda (out)
              (do ((i 0 (fx+ i 1))) ((fx=? i n))
                (chez-json-write-value (pvec-nth! rows i) out flags)
                (put-char out #\\newline))))))
      (define (chez-json-jolt-to-native x)
        (cond
          ((jolt-nil? x) 'null)
          ((or (string? x) (boolean? x)
               (and (number? x) (or (flonum? x) (integer? x)))) x)
          ((or (keyword? x) (symbol-t? x)) (chez-json-jolt-name x))
          ((pmap? x)
           (make-chez-json-object
             (reverse (chez-json-jolt-map-fold x
               (lambda (k v a)
                 (cons (cons (chez-json-jolt-name k)
                             (chez-json-jolt-to-native v)) a)) '()))))
          ((pvec? x)
           (let* ((n (pvec-count x)) (result (make-vector n)))
             (do ((i 0 (fx+ i 1))) ((fx=? i n) result)
               (vector-set! result i (chez-json-jolt-to-native (pvec-nth! x i))))))
          ((jolt-sequential? x)
           (let loop ((s (jolt-seq x)) (a '()))
             (if (jolt-nil? s) (reverse a)
                 (loop (jolt-next s)
                   (cons (chez-json-jolt-to-native (jolt-first s)) a)))))
          (else (error 'chez-json \"unsupported Jolt value\" x))))
      (define (chez-json-native-encode-rows rows options)
        (let ((flags (chez-json-jolt-flags options)))
          (call-with-string-output-port
            (lambda (out) (chez-json-write-rows-value rows out flags))))))")
  :loaded)

(defn encode-value
  ([value] (encode-value value {}))
  ([value options] ((scheme/proc "chez-json-jolt-encode") value options)))

(defn encode-rows
  ([rows] (encode-rows rows {}))
  ([rows options] ((scheme/proc "chez-json-jolt-encode-rows") rows options)))

(defn to-native
  "Convert actual Jolt values into opaque Scheme values. Measure separately."
  [value]
  ((scheme/proc "chez-json-jolt-to-native") value))

(defn encode-native-rows
  ([rows] (encode-native-rows rows {}))
  ([rows options] ((scheme/proc "chez-json-native-encode-rows") rows options)))
