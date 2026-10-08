;; Same lexical contexts as the portable jdbc.chdb placeholder detector.
;; No SQL parsing, classification, payload rejection or persistence decision.
(lambda (s)
  (unless (and (jolt-array? s) (eq? (jolt-array-kind s) 'byte))
    (error 'placeholder-detector "owned ASCII snapshot required"))
  (let* ((input (jolt-array-vec s)) (n (bytevector-length input)))
    ;; Conservative zero-byte lane detection. Cross-lane borrow may report
    ;; extra matches, never hide a matching byte; those words stay scalar.
    ;; ASCII snapshots and 64-bit fixnums keep all intermediate values unboxed.
    (define (word-has? word repeated-byte)
      (let ((x (fxxor word repeated-byte)))
        (not (fx=? (fxand (fx- x #x01010101) (fxnot x) #x80808080) 0))))
    (define (interesting? word mode)
      (cond
        ((fx=? mode 0)
         (or (word-has? word #x3f3f3f3f)  ; ?
             (word-has? word #x27272727)  ; single quote
             (word-has? word #x22222222)  ; double quote
             (word-has? word #x60606060)  ; backtick
             (word-has? word #x2d2d2d2d)  ; dash (line comment)
             (word-has? word #x2f2f2f2f))) ; slash (block comment)
        ((fx=? mode 1) (word-has? word #x0a0a0a0a))
        ((fx=? mode 2)
         (or (word-has? word #x2f2f2f2f) (word-has? word #x2a2a2a2a)))
        (else
         (or (word-has? word #x5c5c5c5c)
             (word-has? word (fx* mode #x01010101))))))
    ;; mode 0 code; 1 line comment; 2 block comment; otherwise quote codepoint.
    (let loop ((i 0) (mode 0) (depth 0))
      (if (fx>=? i n) #f
        (if (and (fixnum? #x80808080) (fx=? (fxand i 3) 0)
                 (fx<=? (fx+ i 4) n)
                 ;; Private <=64MiB snapshot; aligned, complete word only.
                 (not (interesting? (#3%bytevector-u32-native-ref input i) mode)))
            (loop (fx+ i 4) mode depth)
        (let* ((c (#3%bytevector-u8-ref input i))
               (next-i (fx+ i 1))
               (next (if (fx<? next-i n)
                         (#3%bytevector-u8-ref input next-i) -1)))
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
            (else (loop next-i mode depth)))))))))
