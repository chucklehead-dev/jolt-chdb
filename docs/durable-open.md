# Durable V1 open and recovery

`jdbc.chdb.durable/open-writer!` is the public composition point for the
implemented writer path. It checks the running Durable ABI and chDB release
precedence, rejects unsupported backup formats and minimum-reader versions,
then acquires the object lease.

Recovery creates a mode-0700 scratch directory, opens chDB on its private data
path, streams each referenced object to a unique temporary file, verifies size
and SHA-256, and atomically publishes the verified scratch file. A base archive
is restored through the separate native database/archive arguments. With no
base the logical database is created. WAL files are decoded and replayed in
manifest and line order through the internal engine path, never through the
public writer queue. A final lease renewal must succeed before the writer is
returned.

The active writer runs heartbeat renewal independently of its operation queue;
the configured interval cannot exceed one third of the lease TTL. Mutations
and flushes check the locally known expiry at execution time and fail with
`lease-fenced` once ownership can no longer be proved. Head CAS calls share one
writer lock. Ambiguous manifest commits reconcile by exact reference and
sequence while ownership remains intact, so a later heartbeat-only expiry
change cannot turn an already-landed WAL into a duplicate retry.

Failure after acquisition attempts native close, lease release, and scratch
cleanup without replacing the primary error. A restore failure after the
compatibility gate is reported as `engine-incompatible`, as required by the
full-archive promise. Successful `close!` stops heartbeat admission, drains and
flushes the operation queue, releases the lease, closes chDB, and removes the
scratch tree.

`jdbc.chdb.durable/open-reader!` reads and validates the head once in
read-only mode, returns `not-found` without creating a missing object, and
restores exactly that first immutable manifest snapshot. It never acquires,
renews, or releases a lease. Its own bounded FIFO admits read-only queries and
bounded Arrow/Parquet `query-bytes` calls, and serializes them with idempotent
native close and scratch cleanup. Unknown writer features remain readable;
unknown reader features still fail closed.

If either operation worker terminates outside ordinary request handling, it
stops admission, attempts full owned cleanup, and resolves every queued caller
and later `close!` with the same terminal error. A queue failure therefore
cannot strand an unbounded promise wait.

The `chdb-durable` JDBC adapter selects this path with `:read-only? true`.
Both reader and writer handles advertise the neutral `db.export/query-bytes`
capability, so oscope's existing export route uses the same serialized Durable
connection rather than bypassing its lifecycle or policy gate.
Parameterized reads are classified using the same value-free named-placeholder
SQL shape sent to native execution. Parameter types remain visible to the core
parser, but values never enter classifier input, errors, traces, or the WAL.

`jdbc.chdb.durable/flush!` and `checkpoint!` expose these persistence boundaries
for a Durable `jdbc.core` connection without exposing its native handle. Both
reject another driver type or a read-only Durable connection at the JDBC
extension boundary. Oscope uses `checkpoint!` after its sole schema owner has
applied migrations and before ingress, then uses `flush!` before acknowledging
each accepted OTLP batch.

Run the focused gate through the pinned Chez wrapper:

```sh
/home/chuck/ai-src/tools/jolt-with-chez-10.4.1 \
  jolt -M:durable-open-test
```

`checkpoint!` runs the full native backup outside the head-CAS lock so heartbeat
renewal remains live, then streams and verifies immutable publication before
one checkpoint head CAS. Only a proved commit clears covered in-memory WAL.

The current native production pin still reports the Durable ABI as
unsupported. Deterministic tests inject that ABI boundary and exercise real
backend bytes and CAS semantics. The sibling oscope development gate combines
that injected capability/classifier seam with real native recovery, ordinary
queries, and encoded export.
