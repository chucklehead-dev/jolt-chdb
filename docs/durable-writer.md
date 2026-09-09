# Durable V1 serialized writer operations

`jdbc.chdb.durable.writer` owns the operation queue after a writer lease has
been acquired and the native engine has been recovered. Public callers obtain
one through `jdbc.chdb.durable/open-writer!`; `start!` remains the lower-level
composition and deterministic-test seam.

Start a writer with `start!`, passing an object-scoped backend, its current
owner/instance/generation token, the recovered native handle, and logical
database name. After that point, callers must use the writer rather than the
handle directly.

The synchronous operations are:

- `query!`: classify exactly one read-only statement, then execute it without
  changing the WAL;
- `query-bytes!`: apply the same read-only gate, then return an owned bounded
  Arrow or Parquet result through the neutral `db.export` SPI;
- `execute!`: reject oversize or inadmissible SQL before execution, execute one
  contained non-secret mutation locally, then append its JSONL record;
- `flush!`: publish the complete pending WAL under a fresh UUIDv4 key, or a full
  checkpoint when a bound mutation requires it; commit its reference with head
  CAS, and clear pending recovery state only after confirmed or reconciled
  success; and
- `checkpoint!`: create a full backup, stream and verify its immutable
  publication without excluding heartbeat renewal, then replace the base and
  clear both the manifest WAL list and covered in-memory WAL after confirmed
  commit; and
- `close!`: stop admission, drain earlier operations, flush, attempt release,
  close the native engine, and remove an owned scratch directory exactly once.
  It is idempotent and returns the first persistence or release failure after
  attempting every cleanup step.

An `ArrayBlockingQueue` plus one owned OS thread provides the explicit bounded
FIFO. The worker is deliberately not a Jolt fiber: native chDB, filesystem, and
object-store calls may block in ways that pin a shared fiber carrier.
Admission and the transition to closing share one lock, so an operation cannot
pass the open check and enter behind the close request. `status` exposes only
the lifecycle, local writability, pending counts, and whether a checkpoint is
required. A second owned OS thread renews the lease independently of long
queued engine work. It remains active after close admission while earlier FIFO
work drains and while close flushes. Close then signals and positively joins
the heartbeat before lease release, native close, and scratch cleanup. This
ordering prevents renewal after release. Every mutation and flush also checks
the locally known expiry immediately before its side effects.
Immutable publication and verification likewise leave heartbeat renewal
unblocked. The manifest commit rereads the latest owned head afterward and
retries only definite same-owner heartbeat CAS collisions; takeover fences it,
and ambiguous outcomes retain exact reconciliation semantics. No writer-local
lock surrounds publication, verification, head CAS, or reconciliation: even a
blocked manifest CAS must not prevent the heartbeat's independent CAS. The two
transitions compose through backend CAS, bounded same-owner retry, and
operation-specific semantic reread proof.
Terminal worker-loop failures stop admission, attempt flush/release/native
cleanup, and fail every already queued request instead of leaving callers
blocked on unresolved promises.

The V1 limits are applied before a mutation reaches the engine: 64 MiB of SQL
UTF-8 per statement and 128 MiB for the uncompressed JSONL segment. A local
engine failure does not append a replay record. A failed or ambiguous flush
retains the complete buffer.

The frozen WAL stores replayable SQL text. Fully materialized mutations use
that path. Parameterized mutations retain their native bound values for local
execution, then mark the writer checkpoint-required; the next `flush!` or
successful close publishes a full checkpoint and never serializes or logs the
values. Streaming inserts remain outside the Durable writer contract. Read
queries and `query-bytes` also retain native bound parameters.

Run the focused gate with Jolt v0.8.3 and Chez 10.4.1 (the shared maintainer
workspace supplies its pinned wrapper through the parent `AGENTS.md`):

```sh
jolt -M:durable-thread-test
jolt -M:durable-writer-test
```

The isolated one-carrier gate proves native worker work cannot starve heartbeat
renewal and checks both writer and reader execution are off-fiber. The
deterministic suite covers queue ordering, admission-before-execution,
ordered WAL serialization, checkpoint fallback, empty and successful flush,
close, heartbeat failure identity, close-time renewal/flush/release ordering,
blocked publication, verification, and manifest-CAS renewal/takeover, bounded
same-owner CAS retry in both directions, ambiguous renewal followed by manifest
advance, limits, and ambiguous-commit retention. Checkpoint-fallback fault cuts
also cover backup failure, failed or ambiguous immutable upload, and ownership
takeover after publication: each failed boundary retains the checkpoint marker
and covered statement WAL, while a stale generation cannot make its published
checkpoint reachable. A Hegel state machine checks pending statements,
checkpoint requirements, and manifest sequence agreement over generated
materialized execute, parameterized execute, query, and flush traces.

Read-only open is implemented by `jdbc.chdb.durable/open-reader!` and shares
the verified recovery path without participating in lease state.
