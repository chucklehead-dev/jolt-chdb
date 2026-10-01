# Owned WAL kernel bounds

Source: `resources/jdbc/chdb/wal_chunks.ss`, checked loader
`src/jdbc/chdb/durable/wal_chunks.clj`. Only the unchanged pure kernel is
compiled at Chez level 3. The string/type guard is compiled at checked level 2.
Both compiler parameters restore the caller's setting. Missing support still
declines through the existing behavioral selector; no publication transition
or wire format changes.

The guard bounds Scheme string length by `(greatest-fixnum - 11) / 12`.
On the qualified ta6le Chez 10.4.1 runtime, greatest-fixnum is
1152921504606846975 and the inclusive length bound is 96076792050570580.
This is an arithmetic limit, not a practically allocatable fixture.

Capacity is `min(65536,max(256,n+11))`. The eight-byte prefix establishes
`0 <= i <= n`, `0 <= at <= capacity`, `total+at <= 12*i+8` with total=0,
i=0, at=8. Normal writes consume one character and write 1, 2, 6 or 12 bytes.
At offsets greater than capacity-12 the old backing is sealed and replaced
before any character access. Finish appends three bytes, flushing first when
at exceeds capacity-3. Sealing preserves total+at; consuming a character
increases it by at most 12. Final output is at most 12*n+11. These facts bound
every index and accumulated fixnum addition. Hex digits occupy offsets 0..5;
astral escapes use two adjacent hex writes, offsets 0..11. The ASCII table is
indexed only by character values below 128; Scheme characters are nonnegative.

Chiasmus/Z3 verification on 2026-09-30, after structural lint:

- `wal-chunks-production.smt2`: UNSAT for any modeled index/offset violation.
- `wal-chunks-small.smt2`: UNSAT at minimum capacity 256.
- `wal-chunks-mutant.smt2`: SAT when reserve is reduced to 11: cap=256,
  at=245, width=12, nextat=257. This is a symbolic transition witness, not
  a claim that its particular n/i combination is reachable from initialization.
- `wal-chunks-boundary.smt2`: SAT valid inclusive edge, at=244, width=12,
  nextat=256, violation=false.
- `wal-chunks-suffix.smt2`: SAT valid inclusive suffix edge, at=253,
  width=3, nextat=256, violation=false.
- `wal-chunks-accounting.smt2`: UNSAT for output accounting/fixnum overflow
  given the entry invariant and checked input bound.

The transition model overapproximates loop states; it is not a multistep
reachability model. The accounting model abstracts writes by their width.
Neither proves JSON semantics, compiler correctness, GC, allocation success,
runtime-owned array adoption, or concurrency of mutable raw Scheme strings.
The public edge supplies immutable Clojure strings. Runtime parity against the
portable encoder, Unicode/control corpus, ownership/fault tests and actual
chunk-boundary regressions remain mandatory companions, not replaced by SMT.

To run a saved query locally: `z3 -in` with the file contents followed by
`(check-sat)`. SAT controls use the same derived violation as production;
valid-edge queries assert its negation rather than injecting a violation.
