;; Borrow managed byte storage only during this call. Reuse the runtime's
;; Java/Chez UTF-8 agreement guard; it rejects malformed input and a leading
;; BOM. No foreign pointer, mutable alias, or decoded buffer is retained.
;; An unsupported backing or a declined input returns #f to the original codec.
(lambda (bytes)
  (and (jolt-array? bytes)
       (eq? (jolt-array-kind bytes) 'byte)
       (let ((backing (jolt-array-vec bytes)))
         (and (bytevector? backing)
              (%utf8-java-plain? backing)
              (utf8->string backing)))))
