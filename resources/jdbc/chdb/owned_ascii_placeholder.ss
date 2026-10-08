;; Same lexical contexts as the portable jdbc.chdb placeholder detector.
;; No SQL parsing, classification, payload rejection or persistence decision.
(lambda (s)
  (unless (and (jolt-array? s) (eq? (jolt-array-kind s) 'byte))
    (error 'placeholder-detector "owned ASCII snapshot required"))
  (let* ((input (jolt-array-vec s)) (n (bytevector-length input)))
    ;; mode 0 code; 1 line comment; 2 block comment; otherwise quote codepoint.
    (let loop ((i 0) (mode 0) (depth 0))
      (if (fx>=? i n) #f
        (let* ((c (bytevector-u8-ref input i))
               (next-i (fx+ i 1))
               (next (if (fx<? next-i n)
                         (bytevector-u8-ref input next-i) -1)))
          (cond
            ((fx=? mode 0)
             (cond
               ((fx=? c 63) #t)
               ((or (fx=? c 39) (fx=? c 34) (fx=? c 96))
                (loop next-i c 0))
               ((and (fx=? c 45) (fx=? next 45))
                (loop (fx+ i 2) 1 0))
               ((and (fx=? c 47) (fx=? next 42))
                (loop (fx+ i 2) 2 1))
               (else (loop next-i 0 0))))
            ((fx=? mode 1)
             (loop next-i (if (fx=? c 10) 0 1) 0))
            ((fx=? mode 2)
             (cond
               ((and (fx=? c 47) (fx=? next 42))
                (loop (fx+ i 2) 2 (fx+ depth 1)))
               ((and (fx=? c 42) (fx=? next 47))
                (let ((d (fx- depth 1)))
                  (loop (fx+ i 2) (if (fx=? d 0) 0 2) d)))
               (else (loop next-i 2 depth))))
            ;; Consume escaped or doubled delimiters before closing a quote.
            ((and (fx=? c 92) (fx<? next-i n)) (loop (fx+ i 2) mode depth))
            ((fx=? c mode)
             (if (fx=? next mode) (loop (fx+ i 2) mode depth)
                 (loop next-i 0 0)))
            (else (loop next-i mode depth))))))))
