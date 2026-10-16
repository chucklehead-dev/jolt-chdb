;; Library-owned exact Durable V1 JSONL codec; fresh output chunks per request.
;; Depends only on runtime-owned array adoption and ordinary Chez primitives.
(let ((durable-wal-prefix (bytevector 123 34 115 113 108 34 58 34))
      (durable-wal-suffix (bytevector 34 125 10))
      (durable-wal-ascii-escape
        (let ((table (make-bytevector 128 0)))
          (do ((cp 0 (fx+ cp 1))) ((fx= cp 32))
            (bytevector-u8-set! table cp 1))
          (for-each
            (lambda (pair) (bytevector-u8-set! table (car pair) (cdr pair)))
            '((34 . 34) (92 . 92) (47 . 47) (8 . 98) (12 . 102)
              (10 . 110) (13 . 114) (9 . 116)))
          table)))
;; All chunks are fully prepared before any native mutation.
;; Return [total-byte-count [[owned-byte-array used-length] ...]]. Padding is
;; never output. A sealed backing is never reused or mutated by this encoder.
;; Byte accesses alone use #3% primitives under explicit guards: i<n, ASCII
;; cp<128, and at<=capacity-12 before writes of at..at+11. Private snapshot
;; admission is capped at64MiB. See formal/smt/owned-wal-bounds for bounded
;; access/value safety, induction and violating/boundary controls. Global
;; compiler optimization and all arithmetic/copy operations remain checked.
(lambda (s)
  ;; Small records must not pay for a 64 KiB backing. ASCII-heavy records
  ;; normally fit this capacity; escaped records seal additional owned chunks.
  ;; The 256-byte minimum still fits prefix, largest escape and suffix safely.
  (let* ((input (jolt-array-vec s))
         (n (bytevector-length input)) (capacity (min 65536 (max 256 (+ n 11))))
         (buffer (make-bytevector capacity)) (chunks '()) (total 0))
    (define (seal! at)
      (set! chunks (cons (jolt-vector (na-owned-bv->bytearray buffer) at) chunks))
      (set! total (fx+ total at)))
    (define (flush! at)
      (seal! at)
      (set! buffer (make-bytevector capacity)))
    (define (finish! at)
      (let ((at (if (fx> at (fx- capacity 3)) (begin (flush! at) 0) at)))
        (bytevector-copy! durable-wal-suffix 0 buffer at 3)
        (seal! (fx+ at 3))
        (jolt-vector total (apply jolt-vector (reverse chunks)))))
    (define (hex! at cp)
      (#3%bytevector-u8-set! buffer at 92)
      (#3%bytevector-u8-set! buffer (fx+ at 1) 117)
      (do ((shift 12 (fx- shift 4)) (j (fx+ at 2) (fx+ j 1))) ((fx< shift 0))
        (let ((digit (fxand (fxarithmetic-shift-right cp shift) 15)))
          (#3%bytevector-u8-set! buffer j
            (if (fx< digit 10) (fx+ 48 digit) (fx+ 87 digit))))))
    (bytevector-copy! durable-wal-prefix 0 buffer 0 8)
    (let loop ((i 0) (at 8))
      (cond
        ((fx= i n) (finish! at))
        ((fx> at (fx- capacity 12)) (flush! at) (loop i 0))
        (else
         (let* ((cp (#3%bytevector-u8-ref input i))
                (escape (if (fx< cp 128)
                            (#3%bytevector-u8-ref durable-wal-ascii-escape cp) 1)))
           (cond
             ((fx= escape 0)
              (#3%bytevector-u8-set! buffer at cp)
              (loop (fx+ i 1) (fx+ at 1)))
             ((not (fx= escape 1))
              (#3%bytevector-u8-set! buffer at 92)
              (#3%bytevector-u8-set! buffer (fx+ at 1) escape)
              (loop (fx+ i 1) (fx+ at 2)))
             ((fx<= cp #xffff)
              (hex! at cp) (loop (fx+ i 1) (fx+ at 6)))
             (else
              (let ((rest (fx- cp #x10000)))
                (hex! at (fx+ #xd800 (fxquotient rest #x400)))
                (hex! (fx+ at 6) (fx+ #xdc00 (fxmodulo rest #x400)))
                (loop (fx+ i 1) (fx+ at 12)))))))))))

)
