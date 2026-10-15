;; Internal wrapper for private ASCII snapshots. Absence of the marker proves
;; absence of an executable placeholder; a hit always uses the original lexer.
;; Resolve the optional C primitive once. Unsupported hosts keep the lexer.
(lambda (scan)
  (let ((find (guard (condition (else #f))
                (foreign-procedure "memchr" (u8* int size_t) void*))))
    (lambda (s)
      (unless (and (jolt-array? s) (eq? (jolt-array-kind s) 'byte))
        (error 'placeholder-detector "owned ASCII snapshot required"))
      (let* ((input (jolt-array-vec s)) (n (bytevector-length input)))
        ;; Ordinary, NOT __collect_safe, u8* borrows the managed backing only
        ;; for this synchronous bounded call. No collection, retained address,
        ;; native write, input copy or string conversion is introduced.
        (and (not (fx=? n 0))
             (or (not find) (not (= (find input 63 n) 0)))
             (scan s))))))
