# Bounded private WAL byte-access checks

This model checks one narrow claim: unchecked byte accesses in the private
ASCII WAL encoder stay inside their backing buffers and write byte-sized values.
It does not prove SQL semantics, concurrency, crash recovery, durability, native
engine correctness or the absence of arbitrary unsafe-FFI/private-reflection
interference. Exact portable-byte tests and native writer/readback tests remain
necessary.

## Source facts and trusted premises

- `src/jdbc/chdb/owned_statement.clj`: maximum snapshot64MiB; require the sealed
  private statement before WAL preparation. Public input is copied before use.
- `resources/jdbc/chdb/owned_ascii_snapshot.ss`: accept only byte arrays, copy
  first, and reject non-ASCII snapshot bytes. These ownership/type premises are
  trusted by this arithmetic model rather than modeled as a complete runtime.
- `resources/jdbc/chdb/owned_ascii_wal.ss`: capacity is
  `min(65536,max(256,n+11))`; prefix8bytes; body starts i0/at8; finish when i=n;
  flush before emission if at>capacity-12. An ASCII emission advances input by1
  and output by1,2or6 bytes. The table is128bytes and its lookup is guarded by
  cp<128. Prefix/suffix copies remain checked. The non-ASCII surrogate branch
  is unreachable under the sealed ASCII premise, not a new accepted input.
- Every fresh output buffer is allocated with that capacity; input/table/output
  identities and non-mutation remain runtime ownership assumptions. No global
  optimization setting or unchecked arithmetic change is involved.

The source digest file pins these three exact files. It is a drift alarm, not a
semantic parser or a proof that all source behavior was extracted. A changed
digest requires revisiting the fact mapping, controls and tests before accepting
the new proof receipt.

## Queries and interpretation

`access-bounds.smt2` derives the accessed index/backing length and written byte
from source branch facts and asserts their out-of-bounds/out-of-byte violation.
The current guard is UNSAT: no such counterexample within the bounded model.
`loop-induction.smt2` checks initialization and preservation across emission,
flush and completion. It also bounds total output by6*n+11, below the conservative
536,870,911 fixnum maximum. It is UNSAT. Quantification is existential over an
arbitrary invariant-satisfying loop state; UNSAT gives the bounded inductive step.

`reservation-mutant.smt2` uses the same violation query with an insufficient
four-byte reserve: SAT at n101/i100/at252/capacity256/cp0, writing index257.
The companion runtime test reconstructs this reachable prefix and runs the
mutant ONLY after restoring checked byte primitives. Do not execute an
out-of-bounds unchecked-memory mutant.

`small-control.smt2` is UNSAT at a one-byte valid input. `inclusive-boundary.smt2`
is SAT with violation=false at the real inclusive at=capacity-12 boundary. The
runtime fixture reaches at244/capacity256 and verifies a253byte, single-chunk WAL.

Chiasmus lint and verification succeeded for the real checks and controls.
Initial symbolic-divisor syntax exceeded QF_LIA; it was rewritten to constant
division branches before the successful results. No failed verifier attempt is
counted as evidence. Named assertions produce interpretable UNSAT cores.

## Reproduce

From the repository root, run `bash scripts/check-owned-wal-bounds.sh` with Z3
installed. This source-pinned arithmetic check is fast and separate from the
Durable exhaustive Quint checker. Its checks do not justify rerunning unrelated
exhaustive models when no model-relevant source changed.
