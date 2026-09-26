#!r6rs
(import (chezscheme))
(load "bench/chez-json/encoder.ss")

(define args (cdr (command-line)))
(unless (= (length args) 1)
  (error 'parity "usage: parity.ss GENERATED_FIXTURE_DIRECTORY"))
(load (string-append (car args) "/fixture.ss"))

(define checks 0)
(define failures 0)
(define (check label ok?)
  (set! checks (+ checks 1))
  (unless ok?
    (set! failures (+ failures 1))
    (printf "FAIL ~a\n" label)))

(for-each
  (lambda (test)
    (let* ([label (car test)] [value (cadr test)]
           [options (caddr test)] [expected (cadddr test)]
           [actual (chez-json-encode value options)])
      (unless (string=? expected actual)
        (printf "Mismatch ~a expected=~s actual=~s\n" label expected actual))
      (check label (equal? (string->utf8 expected) (string->utf8 actual)))))
  parity-cases)

(define (raises? thunk)
  (guard (condition [else #t]) (thunk) #f))

(for-each
  (lambda (entry)
    (check (car entry) (raises? (lambda () (chez-json-encode (cdr entry))))))
  (list (cons "unsupported-symbol" 'arbitrary)
        (cons "unsupported-character" #\x)
        (cons "unsupported-bytevector" #vu8(1 2))
        (cons "unsupported-procedure" (lambda () 1))
        (cons "unsupported-ratio" 1/3)
        (cons "unsupported-complex" 1+2i)
        (cons "nan" +nan.0)
        (cons "positive-infinity" +inf.0)
        (cons "negative-infinity" -inf.0)))
(check "unsupported-option"
       (raises? (lambda () (chez-json-encode 1 '((unknown . #t))))))
(check "non-boolean-option"
       (raises? (lambda () (chez-json-encode 1 '((escape-slash . yes))))))
(check "unsupported-map-key"
       (raises? (lambda ()
                  (chez-json-encode (make-chez-json-object (list (cons 17 1)))))))
(check "empty-rows" (string=? "" (chez-json-encode-rows '#())))

(for-each
  (lambda (n)
    (let* ([rows (list->vector (list-head (vector->list telemetry-rows) n))]
           [actual (chez-json-encode-rows rows)]
           [expected (call-with-input-file
                       (string-append (car args) "/expected-" (number->string n) ".jsonl")
                       get-string-all)])
      (check (string-append "varying-telemetry-rows-" (number->string n))
             (equal? (string->utf8 expected) (string->utf8 actual)))))
  '(512 1024 5000 10000))
(printf "{:checks ~a :failures ~a :oracle :exact-data-json-utf8}\n" checks failures)
(exit (if (= failures 0) 0 1))
