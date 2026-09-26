#!r6rs
(import (chezscheme))
(load "bench/chez-json/encoder.ss")
(define args (cdr (command-line)))
(unless (= (length args) 6)
  (error 'bench "usage: bench.ss FIXTURE_DIR compatible|minimal ROWS WARMUPS SAMPLES OUTPUT_DIR"))
(load (string-append (car args) "/fixture.ss"))
(define mode (cadr args))
(define row-count (string->number (caddr args)))
(define warmups (string->number (cadddr args)))
(define samples (string->number (list-ref args 4)))
(define output (list-ref args 5))
(unless (and (member row-count '(512 1024 5000 10000))
             (integer? warmups) (<= 0 warmups 1000)
             (integer? samples) (<= 1 samples 10000))
  (error 'bench "invalid bounded counts"))
(define options
  (cond [(string=? mode "compatible") '()]
        [(string=? mode "minimal")
         '((escape-unicode . #f) (escape-slash . #f) (escape-js-separators . #f))]
        [else (error 'bench "invalid mode")]))
;; Native-only ceiling: the input representation is prepaid and this does not
;; stand in for the actual Jolt bridge, which traverses live Clojure maps.
(define rows (list->vector (list-head (vector->list telemetry-rows) row-count)))
(load-shared-object "libcrypto.so.3")
(define sha256-native (foreign-procedure "SHA256" (u8* size_t u8*) void*))
(define (sha256 bytes)
  ;; Linux benchmark-only digest; deliberately outside every stopwatch.
  (let ([result (make-bytevector 32)] [hex "0123456789abcdef"]
        [text (make-string 64)])
    (sha256-native bytes (bytevector-length bytes) result)
    (do ([i 0 (+ i 1)]) [(= i 32) text]
      (let ([b (bytevector-u8-ref result i)])
        (string-set! text (* 2 i) (string-ref hex (quotient b 16)))
        (string-set! text (+ (* 2 i) 1) (string-ref hex (modulo b 16)))))))
(define (nanotime)
  (let ([now (current-time 'time-monotonic)])
    (+ (* (time-second now) 1000000000) (time-nanosecond now))))
(define (sample)
  (let-values ([(port extract) (open-string-output-port)])
    (let* ([start (nanotime)]
           [_ (chez-json-write-rows rows port options)]
           [encoded (nanotime)] [text (extract)] [assembled (nanotime)]
           [bytes (string->utf8 text)] [end (nanotime)])
      (vector (- encoded start) (- assembled encoded) (- end assembled)
              (- end start) bytes))))
(define baseline (sample))
(do ([i 0 (+ i 1)]) [(= i warmups)] (sample))
(define timings
  (let loop ([i 0] [out '()])
    (if (= i samples) (reverse out)
        (let ([result (sample)])
          (unless (equal? (vector-ref baseline 4) (vector-ref result 4))
            (error 'bench "measured bytes changed"))
          (loop (+ i 1)
                (cons (vector (vector-ref result 0) (vector-ref result 1)
                              (vector-ref result 2) (vector-ref result 3))
                      out))))))
(define (summary index)
  (let* ([numbers (sort < (map (lambda (r) (vector-ref r index)) timings))]
         [percentile (lambda (q) (list-ref numbers (- (exact (ceiling (* samples q))) 1)))])
    (format "{:count ~a :total-ns ~a :p50-ns ~a :p95-ns ~a :p99-ns ~a :max-ns ~a}"
            samples (apply + numbers) (percentile 0.5) (percentile 0.95)
            (percentile 0.99) (apply max numbers))))
(define byte-count (bytevector-length (vector-ref baseline 4)))
(define total-median
  (list-ref (sort < (map (lambda (r) (vector-ref r 3)) timings))
            (- (exact (ceiling (* samples 0.5))) 1)))
(define basename (string-append output "/native-" mode "-" (number->string row-count)))
(when (equal? (getenv "BENCH_RETAIN_PAYLOAD") "1")
  (call-with-port
    (open-file-output-port (string-append basename ".jsonl") (file-options no-fail))
    (lambda (port) (put-bytevector port (vector-ref baseline 4)))))
(define report
  (format "{:scope :encode-only :runtime :chez :scheme-version ~s :machine-type ~s :mode :~a :input :preconverted-native-ceiling :rows ~a :utf8-bytes ~a :utf8-sha256 ~s :warmups ~a :samples ~a :encode ~a :assembly ~a :utf8 ~a :total ~a :rows-per-second ~a :bytes-per-second ~a :p99-qualified? ~a}"
          (scheme-version) (symbol->string (machine-type)) mode row-count byte-count
          (sha256 (vector-ref baseline 4)) warmups samples
          (summary 0) (summary 1) (summary 2) (summary 3)
          (/ (* 1.0e9 row-count) total-median) (/ (* 1.0e9 byte-count) total-median)
          (if (>= samples 100) "true" "false")))
(call-with-output-file (string-append basename ".edn")
  (lambda (port) (display report port) (newline port)) 'replace)
(display report)
(newline)
