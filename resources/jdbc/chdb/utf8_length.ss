;; Exact scalar UTF-8 size, with no bytevector allocation. Immutable string in,
;; integer out; no runtime string/array layout duplication. Surrogates decline
;; to the host codec so its invalid-character behavior remains authoritative.
(lambda (text)
  (if (not (string? text)) #f
    (let ((n (string-length text)))
      (let loop ((i 0) (bytes 0))
        (if (fx= i n) bytes
          (let ((c (char->integer (string-ref text i))))
            (if (or (fx<= #xD800 c #xDFFF) (fx> c #x10FFFF)) #f
              (loop (fx+ i 1)
                    (fx+ bytes (cond ((fx<= c #x7F) 1)
                                    ((fx<= c #x7FF) 2)
                                    ((fx<= c #xFFFF) 3)
                                    (else 4)))))))))))
