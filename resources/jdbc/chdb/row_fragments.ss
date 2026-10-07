;; Source-only experiment over Jolt's concrete accumulator. Decline unknown
;; layouts/characters before touching either sink. Copy list cells, not string
;; contents, so a callback retaining its row sink cannot mutate batch state.
(lambda (row batch limit)
  (define (state x tag validate-row?)
    (and (jhost? x) (string=? (jhost-tag x) tag)
         (let ((s (jhost-state x)))
           (and (vector? s) (= (vector-length s) 3)
                (string? (vector-ref s 0))
                ;; The private batch was constructed here and mutated only by
                ;; append/this transfer. Do not rescan its growing chunk list
                ;; per row: that would make linear encoding quadratic.
                (if validate-row?
                    (and (list? (vector-ref s 1))
                         (for-all string? (vector-ref s 1)))
                    (or (null? (vector-ref s 1)) (pair? (vector-ref s 1))))
                (fixnum? (vector-ref s 2)) (>= (vector-ref s 2) 0) s))))
  (define (size s)
    (let loop ((i 0) (n 0))
      (if (= i (string-length s)) n
          (let ((c (char->integer (string-ref s i))))
            (and (not (<= #xD800 c #xDFFF)) (<= c #x10FFFF)
                 (loop (+ i 1) (+ n (cond ((<= c #x7F) 1)
                                         ((<= c #x7FF) 2)
                                         ((<= c #xFFFF) 3) (else 4)))))))))
  (let ((r (state row "writer" #t)) (b (state batch "string-builder" #f)))
    (and r b (not (eq? r b))
         (let* ((base (vector-ref r 0)) (chunks (vector-ref r 1))
                (bytes (let loop ((xs (cons base chunks)) (n 0))
                         (if (null? xs) n
                             (let ((v (size (car xs))))
                               (and v (loop (cdr xs) (+ n v))))))))
           (and bytes
                (begin
                  ;; Overflow is reported without appending. The caller owns
                  ;; the established output-limit exception and next-row rule.
                  (when (<= bytes limit)
                    (vector-set! b 1
                      (append chunks
                              (if (zero? (string-length base)) '() (list base))
                              (vector-ref b 1)))
                    (vector-set! b 2
                      (+ (vector-ref b 2) (string-length base)
                         (vector-ref r 2))))
                  bytes))))))
