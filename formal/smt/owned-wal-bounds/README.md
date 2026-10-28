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
  optimization setting changes. Guarded loop cursor advances alone use
  unchecked addition under the bounds described below.

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

### Confirmed-ticket integration requalification (2026-10-21)

The statement owner adds two read-only scalar observations: retained SQL/WAL
array capacity and the existing precompute input ceiling. Neither modifies or
creates a statement, invokes encoding, changes a limit, nor exposes backing
arrays. The encoder/snapshot resources and all modeled arithmetic are unchanged.
The owner-file digest was refreshed only after auditing that exact 19-line
delta and replaying the same five queries with lint/verification and controls;
see `ticket-integration-replay-20261021.json`.

The budget tests separately check no preparation/public-copy call, padding
capacity rather than used length, and invalid statements. The ticket queue,
publication order, interruption and persistence semantics are NOT proved by
these byte-access models; their runtime tests and the approved pipeline model
remain separate evidence. No SMT logic or exhaustive Quint campaign was added.

### Stock-buffer integration requalification (2026-10-21)

The main-integration CI correctly rejected the old three-file source digests
before running the solver. The source audit against main found these changes:

- Snapshot and WAL buffers now use the selected runtime's adoption helper, or
  wrap a **fresh private** backing on stock Jolt. Copy-before-ASCII-validation,
  capacity, escape table, unchecked accesses, indices and flush guards are unchanged.
- Owned statements can retain precomputed WAL chunks. The 64MiB snapshot limit,
  sealed-statement check and encoder invocation are unchanged; the optional
  cache's separate 128MiB worst-case-output guard only narrows cache eligibility.
- Scoped native query copying and private record egress use separate byte-copy/
  output helpers. Their aliasing, FFI lifetime and stream-dispatch behavior are
  **not** proved by this arithmetic model. Fresh private buffer identity and
  non-mutation remain explicit trusted premises, exercised by runtime controls.

The five SMT queries are unchanged. Chiasmus lint/verification replayed the
real access and induction queries, safe small control, violating reserve mutant
and valid inclusive boundary. The recorded results and concrete true/false
violation witnesses are in `stock-integration-replay-20261021.json`. The digest
update follows that audit/replay; it does not expand the proof into ownership,
cached-record lifetime, persistence or concurrency guarantees.

The companion `bounded-wal-byte-access-control-positions-and-capacity-witness`
test runs all ASCII bytes at eight padding positions, safely reconstructs the
four-byte-reserve mutant with CHECKED accesses, and reaches the valid253-byte
single-chunk boundary. The integration's full owned-statement gate passed
14 tests/15,563 assertions including real native SQL/WAL equality; prepared
record/output gates separately cover public-copy and custom-stream isolation.
No unrelated exhaustive Quint run is required by this bounded requalification.
The stock-byte-copy feature-branch workflow now replays this source-pinned gate
before downloading its runtime, and watches the two kernels, proof directory
and replay script as well as the statement owner. A green copy-only feature
gate must no longer conceal stale arithmetic provenance until main integration.

### Native query-buffer requalification (2026-10-27)

Against main `312812f`, runtime change `7cbf433` modifies only
`with-query-buffer`: raw private native allocation replaces zeroed allocation,
the full logical SQL range is copied before the consumer runs, an empty buffer
keeps its zero sentinel, and `finally` frees storage even after a partial copy
throws. A reported incomplete copy rejects before invoking the consumer.

The sealed managed snapshot and WAL buffers, 64MiB limit, encoder invocation,
capacity, table, indices, escape widths, flush guards and arithmetic are unchanged.
Both Scheme resource files and all five SMT queries are byte-for-byte unchanged.
The outgoing native buffer is separate from the backing arrays modeled here.
Its initialization and cleanup ordering are **outside this arithmetic theorem**;
the query-buffer runtime tests cover complete/partial/short copies, consumer
failure, empty input and release. Private identity and non-mutation remain trusted
premises rather than new conclusions of the solver.

The source digest was refreshed after this audit and successful Chiasmus
lint/verification of the five unchanged queries. Access/induction/small control
remain UNSAT; the four-byte-reserve mutant remains SAT with index257/length256
and violation=true; the inclusive boundary remains SAT with index249/length256
and violation=false. See `query-buffer-replay-20261027.json` for named cores and
witnesses. The full owned-statement gate passed 17 tests/15,606 assertions, and
the four-arm native comparison independently recovered 700,000 rows. Those are
separate runtime evidence, not an expansion of the bounded arithmetic claim.

### Guarded cursor arithmetic (2026-10-27)

The scalar algorithm and chunk boundaries are unchanged. Only the four emission
branches' `i+1` and `at+width` updates omit repeated overflow checks. The existing
loop theorem already derives these exact integer updates and rejects next-state
overflow or invalid positions: `i<n<=67108864`, `at<=capacity-12`, capacity at most
65536, public ASCII emission widths1/2/6. The unreachable surrogate branch's
width12 is also below the explicit reserve; no unchecked aggregate-size or
allocation arithmetic was added. Snapshot/alias/type premises remain trusted.

The five original queries are unchanged and replay successfully. To exercise
the induction query's own violation schema, the runner now also mutates the
emission's output advance to536870911. It is SAT at n1/i0/at8/capacity256,
next_at536870919 and violation=true. The unchanged one-byte induction step is
UNSAT; the real inclusive at244/capacity256, width6 step is SAT with
next_i101/next_at250 and violation=false. These controls supplement the existing
unsafe-reservation mutant; the oversized advance is never executed in memory.
See `guarded-arithmetic-replay-20261027.json` for named cores and witnesses.
This requalification proves only the bounded arithmetic, not wire semantics,
ownership, timing or persistence. Exact byte/chunk tests and native recovery
remain separate gates; no Quint logic or exhaustive campaign changed.

From the repository root, run `bash scripts/check-owned-wal-bounds.sh` with Z3
and Python3 installed. This source-pinned arithmetic check is fast and separate from the
Durable exhaustive Quint checker. Its checks do not justify rerunning unrelated
exhaustive models when no model-relevant source changed.
