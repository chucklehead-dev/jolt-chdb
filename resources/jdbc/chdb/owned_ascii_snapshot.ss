;; Copy first, then qualify the private snapshot. No caller backing is adopted.
(lambda (input limit)
  (and (jolt-array? input) (eq? (jolt-array-kind input) 'byte)
       (fx<=? (ja-len input) limit)
       (let* ((bytes (na-bytearray->bv input)) (n (bytevector-length bytes)))
         (let loop ((i 0))
           ;; Endian-neutral high-bit mask. Read only aligned complete words;
           ;; the unsigned32 value/mask fit Chez's64-bit target fixnums. Do not
           ;; use u64 here: ordinary ASCII words could allocate bignums.
           (cond ((and (fixnum? #x80808080) (fx<=? (fx+ i 4) n))
                  (and (fx=? (fxand (bytevector-u32-native-ref bytes i) #x80808080) 0)
                       (loop (fx+ i 4))))
                 ((fx=? i n) (na-owned-bv->bytearray bytes))
                 ((fx>? (bytevector-u8-ref bytes i) 127) #f)
                 (else (loop (fx+ i 1))))))))
