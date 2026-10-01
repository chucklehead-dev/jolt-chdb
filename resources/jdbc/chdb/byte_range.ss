;; Generic unsigned-byte range search. No mutation, callback, storage or FFI.
;; The runtime owns array representation; never copy its record layout here.
;; False declines unsupported backings/arguments before inspecting any element.
(lambda (arr target start end)
  (if (not (and (jolt-array? arr) (eq? (jolt-array-kind arr) 'byte)
                (fixnum? target) (fx<= 0 target 255)
                (fixnum? start) (fixnum? end)))
      #f
      (let ((v (jolt-array-vec arr)))
        (if (not (and (bytevector? v)
                      (fx<= 0 start end (bytevector-length v))))
            #f
            (let loop ((i start))
              (if (or (fx= i end) (fx= target (bytevector-u8-ref v i)))
                  i
                  (loop (fx+ i 1))))))))
