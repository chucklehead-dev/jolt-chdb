;; Synchronous copy only. No managed address, owner or borrowed view escapes.
;; Stock ffi/write-array currently copies into a temporary bytevector first.
;; Reuse its SAME non-collect-safe adapter, but pass the existing byte backing.
(if (and (top-level-bound? 'sa-foreign-bytes-set!)
         (procedure? (top-level-value 'sa-foreign-bytes-set!))
         (bitwise-bit-set? (procedure-arity-mask (top-level-value 'sa-foreign-bytes-set!)) 3))
    (let ((copy! (top-level-value 'sa-foreign-bytes-set!)))
      (lambda (pointer bytes n)
        (and (jolt-array? bytes)
             (eq? (record-type-uid (record-rtd bytes)) 'jolt-array-v1)
             (eq? (jolt-array-kind bytes) 'byte)
             (bytevector? (jolt-array-vec bytes))
             (let ((backing (jolt-array-vec bytes)) (n (jnum->exact n)))
               (unless (and (fixnum? n) (fx>=? n 0)
                            (fx<=? n (bytevector-length backing)))
                 (error 'byte-prefix-copy "invalid byte range"))
               (unless (fx=? n 0) (copy! (jnum->exact pointer) backing n))
               n))))
    (lambda (pointer bytes n) #f))
