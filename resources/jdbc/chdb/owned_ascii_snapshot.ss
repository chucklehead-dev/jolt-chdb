;; Copy first, then qualify the private snapshot. No caller backing is adopted.
(let ((adopt
       (if (and (top-level-bound? 'na-owned-bv->bytearray)
                (procedure? (top-level-value 'na-owned-bv->bytearray)))
           (top-level-value 'na-owned-bv->bytearray)
           ;; Stock Jolt: this wraps ONLY the fresh private copy below. Native
           ;; consumers still use scoped foreign copies, never a managed address.
           (lambda (bytes) (make-jolt-array bytes 'byte)))))
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
                 ((fx=? i n) (adopt bytes))
                 ((fx>? (bytevector-u8-ref bytes i) 127) #f)
                 (else (loop (fx+ i 1)))))))))
