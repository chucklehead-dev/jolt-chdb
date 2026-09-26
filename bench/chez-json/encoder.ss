;; Standalone Chez 10.4.1 JSONEachRow prototype. No encoded key/value cache.
;; See README.md for its intentionally bounded, benchmark-only contract.

(define-record-type chez-json-object (fields entries))
(define-record-type chez-json-keyword (fields name))

(define chez-json-default-options
  '((escape-unicode . #t) (escape-slash . #t) (escape-js-separators . #t)))

(define (chez-json-options options)
  (unless (list? options)
    (error 'chez-json "options must be an association list" options))
  (for-each
    (lambda (entry)
      (unless (and (pair? entry)
                   (memq (car entry)
                     '(escape-unicode escape-slash escape-js-separators))
                   (boolean? (cdr entry)))
        (error 'chez-json "unsupported option or non-boolean value" entry)))
    options)
  (list->vector
    (map (lambda (key) (let ((entry (assq key options)))
                        (if entry (cdr entry) #t)))
         '(escape-unicode escape-slash escape-js-separators))))

(define chez-json-default-flags (chez-json-options '()))
(define chez-json-hex "0123456789abcdef")

(define (chez-json-u16 cp out)
  (put-string out "\\u")
  (put-char out (string-ref chez-json-hex (fxand (fxsra cp 12) 15)))
  (put-char out (string-ref chez-json-hex (fxand (fxsra cp 8) 15)))
  (put-char out (string-ref chez-json-hex (fxand (fxsra cp 4) 15)))
  (put-char out (string-ref chez-json-hex (fxand cp 15))))

(define (chez-json-write-string value out flags)
  (let ((size (string-length value))
        (unicode? (vector-ref flags 0))
        (slash? (vector-ref flags 1))
        (js? (vector-ref flags 2)))
    (put-char out #\")
    ;; Copy unescaped spans directly into the port, without substring allocation.
    (let loop ((i 0) (start 0))
      (if (fx=? i size)
          (when (fx<? start size) (put-string out value start (fx- size start)))
          (let* ((c (string-ref value i))
                 (cp (char->integer c))
                 (escape? (or (fx<? cp 32) (fx=? cp 34) (fx=? cp 92)
                              (and slash? (fx=? cp 47))
                              (if (or (fx=? cp #x2028) (fx=? cp #x2029))
                                  js?
                                  (and unicode? (fx>? cp 127))))))
            (if escape?
                (begin
                  (when (fx<? start i) (put-string out value start (fx- i start)))
                  (case cp
                    ((34) (put-string out "\\\""))
                    ((92) (put-string out "\\\\"))
                    ((47) (put-string out "\\/"))
                    ((8) (put-string out "\\b"))
                    ((12) (put-string out "\\f"))
                    ((10) (put-string out "\\n"))
                    ((13) (put-string out "\\r"))
                    ((9) (put-string out "\\t"))
                    (else
                      (if (fx<=? cp #xffff)
                          (chez-json-u16 cp out)
                          (let ((n (fx- cp #x10000)))
                            (chez-json-u16 (fx+ #xd800 (fxsra n 10)) out)
                            (chez-json-u16 (fx+ #xdc00 (fxand n #x3ff)) out)))))
                  (loop (fx+ i 1) (fx+ i 1)))
                (loop (fx+ i 1) start)))))
    (put-char out #\")))

(define (chez-json-write-fixnum value out)
  ;; Keep negative values negative, including the minimum fixnum.
  (when (fx<? value 0) (put-char out #\-))
  (let digits ((n value))
    (let ((q (fxquotient n 10)) (r (fxremainder n 10)))
      (unless (fx=? q 0) (digits q))
      (put-char out (integer->char (fx+ 48 (if (fx<? r 0) (fx- 0 r) r)))))))

(define (chez-json-float-string value)
  ;; Match Jolt's Double.toString layout using Chez's round-trip digits.
  ;; Strip Chez's optional subnormal precision suffix; it is not JSON syntax.
  (let* ((raw (number->string value))
         (neg? (char=? (string-ref raw 0) #\-))
         (start (if neg? 1 0))
         (end (let loop ((i start))
                (if (or (= i (string-length raw))
                        (char=? (string-ref raw i) #\|)) i
                    (loop (+ i 1)))))
         (epos (let loop ((i start))
                 (cond ((= i end) end)
                       ((memv (string-ref raw i) '(#\e #\E)) i)
                       (else (loop (+ i 1))))))
         (exp (if (= epos end) 0
                  (string->number (substring raw (+ epos 1) end))))
         (dot (let loop ((i start))
                (if (or (= i epos) (char=? (string-ref raw i) #\.)) i
                    (loop (+ i 1)))))
         (digits0 (if (= dot epos) (substring raw start epos)
                      (string-append (substring raw start dot)
                                     (substring raw (+ dot 1) epos))))
         (lead (let loop ((i 0))
                 (if (and (< i (- (string-length digits0) 1))
                          (char=? (string-ref digits0 i) #\0))
                     (loop (+ i 1)) i)))
         (tail (let loop ((i (string-length digits0)))
                 (if (and (> i (+ lead 1))
                          (char=? (string-ref digits0 (- i 1)) #\0))
                     (loop (- i 1)) i)))
         (digits (substring digits0 lead tail))
         (len (string-length digits))
         (point (- (+ (- dot start) exp) lead))
         (body
           (cond
             ((string=? digits "0") "0.0")
             ((and (>= point -2) (<= point 7))
              (cond ((<= point 0)
                     (string-append "0." (make-string (- point) #\0) digits))
                    ((>= point len)
                     (string-append digits (make-string (- point len) #\0) ".0"))
                    (else (string-append (substring digits 0 point) "."
                                        (substring digits point len)))))
             (else (string-append (substring digits 0 1) "."
                      (if (= len 1) "0" (substring digits 1 len))
                      "E" (number->string (- point 1)))))))
    (if neg? (string-append "-" body) body)))

(define (chez-json-name value)
  (if (string? value) value
      (if (chez-json-keyword? value)
          (let* ((s (chez-json-keyword-name value)) (n (string-length s)))
            (let loop ((i 0))
              (cond ((fx=? i n) s)
                    ((and (char=? (string-ref s i) #\/) (fx>? i 0)
                          (fx<? i (fx- n 1)))
                     (substring s (fx+ i 1) n))
                    (else (loop (fx+ i 1))))))
          (error 'chez-json "object key must be a string or named value" value))))

;; The bridge installs a writer only for Jolt's opaque collection/named/nil
;; representations. Native values never cross this extension callback.
(define chez-json-foreign-write
  (lambda (value out flags)
    (error 'chez-json "unsupported value" value)))

(define (chez-json-write-value value out flags)
  (cond
    ((string? value) (chez-json-write-string value out flags))
    ((fixnum? value) (chez-json-write-fixnum value out))
    ((eq? value #t) (put-string out "true"))
    ((eq? value #f) (put-string out "false"))
    ((eq? value 'null) (put-string out "null"))
    ((flonum? value)
     (unless (finite? value) (error 'chez-json "non-finite number" value))
     (put-string out (chez-json-float-string value)))
    ((and (number? value) (exact? value) (integer? value))
     (put-string out (number->string value)))
    ((chez-json-keyword? value)
     (chez-json-write-string (chez-json-name value) out flags))
    ((chez-json-object? value)
     (put-char out #\{)
     (let loop ((entries (chez-json-object-entries value)) (first? #t))
       (unless (null? entries)
         (unless (and (pair? entries) (pair? (car entries)))
           (error 'chez-json "object requires a proper association list" entries))
         (unless first? (put-char out #\,))
         (chez-json-write-string (chez-json-name (caar entries)) out flags)
         (put-char out #\:)
         (chez-json-write-value (cdar entries) out flags)
         (loop (cdr entries) #f)))
     (put-char out #\}))
    ((vector? value)
     (put-char out #\[)
     (let ((n (vector-length value)))
       (do ((i 0 (fx+ i 1))) ((fx=? i n))
         (unless (fx=? i 0) (put-char out #\,))
         (chez-json-write-value (vector-ref value i) out flags)))
     (put-char out #\]))
    ((or (null? value) (pair? value))
     (put-char out #\[)
     (let loop ((values value) (first? #t))
       (unless (null? values)
         (unless (pair? values) (error 'chez-json "improper array list" value))
         (unless first? (put-char out #\,))
         (chez-json-write-value (car values) out flags)
         (loop (cdr values) #f)))
     (put-char out #\]))
    (else (chez-json-foreign-write value out flags))))

(define (chez-json-write-rows-value rows out flags)
  (cond
    ((vector? rows)
     (let ((n (vector-length rows)))
       (do ((i 0 (fx+ i 1))) ((fx=? i n))
         (chez-json-write-value (vector-ref rows i) out flags)
         (put-char out #\newline))))
    ((list? rows)
     (for-each (lambda (row) (chez-json-write-value row out flags)
                            (put-char out #\newline)) rows))
    (else (error 'chez-json "native rows must be a vector or proper list" rows))))

(define chez-json-write
  (case-lambda
    ((value out) (chez-json-write-value value out chez-json-default-flags))
    ((value out options) (chez-json-write-value value out (chez-json-options options)))))
(define chez-json-write-rows
  (case-lambda
    ((rows out) (chez-json-write-rows-value rows out chez-json-default-flags))
    ((rows out options) (chez-json-write-rows-value rows out (chez-json-options options)))))
(define chez-json-encode
  (case-lambda
    ((value) (call-with-string-output-port (lambda (out) (chez-json-write value out))))
    ((value options)
     (call-with-string-output-port (lambda (out) (chez-json-write value out options))))))
(define chez-json-encode-rows
  (case-lambda
    ((rows) (call-with-string-output-port (lambda (out) (chez-json-write-rows rows out))))
    ((rows options)
     (call-with-string-output-port (lambda (out) (chez-json-write-rows rows out options))))))
