;; Copy first, then qualify the private snapshot. No caller backing is adopted.
(lambda (input limit)
  (and (jolt-array? input) (eq? (jolt-array-kind input) 'byte)
       (fx<=? (ja-len input) limit)
       (let* ((bytes (na-bytearray->bv input)) (n (bytevector-length bytes)))
         (let loop ((i 0))
           (cond ((fx=? i n) (na-owned-bv->bytearray bytes))
                 ((fx>? (bytevector-u8-ref bytes i) 127) #f)
                 (else (loop (fx+ i 1))))))))
