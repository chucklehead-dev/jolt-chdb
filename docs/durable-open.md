# Durable V1 open and recovery

`jdbc.chdb.durable/open-writer!` is the public composition point for the
implemented writer path. It checks the running Durable ABI and chDB release
precedence, rejects unsupported backup formats and minimum-reader versions,
then acquires the object lease.

Callers select object identity explicitly. `:namespace-backend` names a shared
provider namespace and `:object-id` names one validated path component within
it; the same pair is accepted by the `chdb-durable` JDBC dbspec. Every backend
operation is scoped below `<object-id>/`, including `head.json`, WALs, and
checkpoints. Supplying only half of the pair, or combining it with the legacy
already-scoped `:store` option (`:backend` in a JDBC dbspec), fails closed.

Recovery creates a mode-0700 scratch directory, opens chDB on its private data
path, streams each referenced object to a unique temporary file, verifies size
and SHA-256, and atomically publishes the verified scratch file. A base archive
is restored through the separate native database/archive arguments. With no
base the logical database is created. WAL files are decoded and replayed in
manifest and line order through the internal engine path, never through the
public writer queue. A final lease renewal must succeed before the writer is
returned.

The focused public-open conformance corpus exercises missing, wrong-size, and
wrong-digest checkpoint and WAL references through both reader and writer open.
Valid controls reach restore or replay; malformed references return `corrupt`
before those stages. Writer failure still releases the acquired lease and both
open modes close native state and remove scratch without allowing secondary
cleanup errors to replace the verification result.

The active writer runs its operation queue and heartbeat on separate owned OS
threads; the read-only queue likewise owns an OS thread. These are deliberately
not Jolt fibers because native chDB and storage calls may block a shared fiber
carrier. The configured heartbeat interval cannot exceed one third of the
lease TTL. Mutations and flushes check the locally known expiry at execution
time and fail with `lease-fenced` once ownership can no longer be proved.
Immutable publication and verification do not exclude heartbeat renewal from
the head; the later manifest CAS retries a definite heartbeat collision from
the newest owned head. No writer-local lock surrounds publication, verification,
head CAS, or reconciliation, because a remote call can block beyond the TTL.
Manifest and renewal transitions coordinate through backend CAS plus bounded
same-owner retries. Ambiguous manifest commits reconcile by exact reference and
sequence while ownership remains intact; ambiguous renewal proves the same token
and an expiry at least as late as requested. Thus either order preserves both
effects without turning an already-landed WAL into a duplicate retry.

A failed heartbeat replacement does not fence the writer while the last proved
lease remains live. If renewal loss continues through that expiry, the writer
self-fences: public execute, flush, and checkpoint calls fail with
`lease-fenced` before native or persistence effects. The focused fake-operation
test proves that a public queued read still reaches the opened local handle and
returns the same injected result; it does not claim native restored-data
coverage. Closing still stops owned threads and attempts all local cleanup while
retaining the fencing error.

Failure after acquisition attempts native close, lease release, and scratch
cleanup without replacing the primary error. A restore failure after the
compatibility gate is reported as `engine-incompatible`, as required by the
full-archive promise. Successful `close!` stops operation admission, drains and
flushes the operation queue while heartbeat remains live, then stops and joins
heartbeat before it releases the lease, closes chDB, and removes the scratch
tree. It then joins the operation OS thread before returning. Reader close
likewise joins its operation thread after native close and scratch cleanup.
These positive termination handshakes prevent renewal after release and prevent
public close from leaving an owned operation worker live.
Once close is admitted, interruption does not weaken this ownership boundary:
an interrupt received while joining is retained, the join continues until the
owned thread exits, and only then is the interrupt restored and rethrown. An
earlier persistence or cleanup failure remains the primary error.

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
extension boundary. `connection-role` provides the corresponding non-publishing
preflight for integrations that must reject the wrong connection before schema
mutation. Oscope uses `checkpoint!` after its sole schema owner has
applied migrations and before ingress, then uses `flush!` before acknowledging
each accepted OTLP batch.

Applications should normally build JDBC maps with
`jdbc.chdb.durable/writer-dbspec` and `snapshot-dbspec`. These data-only
constructors select the existing driver and validate storage identity, role,
writer identity, and timing before `open-writer!` or `open-reader!` runs. The
driver applies the same validation to handwritten maps for fail-closed
compatibility. A generated writer instance is UUIDv4; protocol ordering comes
from the lease generation, not UUID sorting.

Run the focused gate with the pinned Jolt v0.8.6 aspect compiler and Chez
10.4.1 (the shared maintainer
workspace supplies its pinned wrapper through the parent `AGENTS.md`):

```sh
jolt -M:durable-open-test
```

`checkpoint!` runs the full native backup without excluding heartbeat renewal,
then streams and verifies immutable publication before one checkpoint head CAS.
No backend phase shares a writer-local head lock. Only a proved commit clears
covered in-memory WAL.

The current native production pin still reports the Durable ABI as
unsupported. Deterministic tests inject that ABI boundary and exercise real
backend bytes and CAS semantics. The sibling oscope development gate combines
that injected capability/classifier seam with real native recovery, ordinary
queries, and encoded export.
