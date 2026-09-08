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
- `flush!`: publish the complete pending WAL under a fresh UUIDv4 key, commit
  its reference with head CAS, and clear the buffer only after confirmed or
  reconciled success; and
- `checkpoint!`: create a full backup outside the head-CAS lock, stream and
  verify its immutable publication, then replace the base and clear both the
  manifest WAL list and covered in-memory WAL after confirmed commit; and
- `close!`: stop admission, drain earlier operations, flush, attempt release,
  close the native engine, and remove an owned scratch directory exactly once.
  It is idempotent and returns the first persistence or release failure after
  attempting every cleanup step.

An `ArrayBlockingQueue` plus one Jolt fiber provides the explicit bounded FIFO.
Admission and the transition to closing share one lock, so an operation cannot
pass the open check and enter behind the close request. `status` exposes only
the lifecycle, local writability, and pending counts. A second fiber renews the
lease independently of long queued engine work. Every mutation and flush also
checks the locally known expiry immediately before its side effects.
Terminal worker-loop failures stop admission, attempt flush/release/native
cleanup, and fail every already queued request instead of leaving callers
blocked on unresolved promises.

The V1 limits are applied before a mutation reaches the engine: 64 MiB of SQL
UTF-8 per statement and 128 MiB for the uncompressed JSONL segment. A local
engine failure does not append a replay record. A failed or ambiguous flush
retains the complete buffer.

Current mutation boundary: the frozen WAL stores replayable SQL text, so
mutations must be fully materialized strings rather than parameter vectors or
streaming inserts. Read queries and `query-bytes` retain native bound
parameters because they never enter the WAL.

Run the focused gate through the workspace's pinned Chez wrapper:

```sh
/home/chuck/ai-src/tools/jolt-with-chez-10.4.1 \
  jolt -M:durable-writer-test
```

The deterministic suite covers queue ordering, admission-before-execution,
ordered WAL serialization, empty and successful flush, close, limits, and
ambiguous-commit retention. A Hegel state machine checks pending statement and
manifest sequence agreement over generated execute/query/flush traces.

Read-only open is implemented by `jdbc.chdb.durable/open-reader!` and shares
the verified recovery path without participating in lease state.
