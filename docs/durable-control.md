# Durable V1 lease and head-CAS control

`jdbc.chdb.durable.control` is the first executable Durable V1 state-machine
slice. It composes the strict head codec with the atomic backend seam and
implements:

- conditional generation-one creation;
- acquisition of released leases and normal or explicit-force takeover;
- owner, instance, and generation fencing;
- heartbeat renewal and fenced release without generation changes;
- writer-aware bounded WAL publication;
- already-published immutable WAL/checkpoint reference commits; and
- ambiguous head-CAS reconciliation.

The normative source is chDB commit
`66643e5030fb73c30ac5cdd31d4c7858ea040ed0`,
`docs/durable/protocol-v1.mdx#state-machine`. This control layer is composed by
the public Durable reader/writer API; its formal bounds are narrower than that
complete runtime path and are described below.

## Ownership and time

The application fencing token is the exact triple `owner`, `instance`, and
`generation`. Matching an owner name alone is never sufficient. Every
acquisition of an existing head increments the generation; heartbeat, ordinary
commit, and release preserve it. A stale writer is rejected before object
verification or head mutation.

Normal takeover becomes eligible only when
`now > expires_at + clock_skew`; equality remains held. Explicit force can
take over an unexpired lease. A new lease expiry must be later than the
acquisition time, and a heartbeat must strictly extend the existing expiry.
Scheduling heartbeats at no more than one third of the TTL is owned by the
public writer's independent heartbeat worker.

A successful forced takeover of a still-live lease returns exactly one
structured warning in the acquisition result's `:warnings` vector. The event
contains only its stable name, warning severity, protocol version, and new lease
generation; it never retains the prior or new owner/instance, object key,
credentials, SQL, paths, or backend response. Fresh, released, and normally
expired acquisitions return an empty vector. Landed ambiguous CAS results carry
the same single warning through reconciliation, so observation advice sees one
completed warning without changing fencing or acknowledgement semantics.

At this low-level protocol seam, `now`, `expires-at`, and `clock-skew` are all
epoch seconds, matching Python's `time.time()` and the frozen `expires_at`
field. The public Durable API remains millisecond-configured and converts
explicitly before calling this namespace. Its writer keeps local expiry and
heartbeat scheduling in milliseconds, while the separate retry budget uses a
monotonic millisecond clock. There is no magnitude-based unit detection.

All desired heads are encoded and decoded before CAS. That is a correctness
boundary, not cosmetic normalization: JSON may serialize an integral decimal
as an integer, and ambiguous reconciliation must compare the exact semantic
value a reread will produce.

## Engine compatibility metadata

An existing-head acquisition records the running chDB release in
`engine.version` in the same CAS that advances the fencing generation. It
preserves `backup_format`, `min_reader`, and unknown engine fields. The producer
version says who wrote the object most recently; readers do not require an
exact version match. Fresh acquisition validates both `engine.version` and
`engine.min_reader`, and every takeover validates the replacement producer
version before the first backend read or conditional write. These checks call
the same `compatibility/release-version?` parser used by the SHA-pinned release
comparison oracle; the control layer does not maintain a second version syntax.

A full-checkpoint commit atomically records its producer version and archive
format with the new base reference. Its `backup_format` and `min_reader`
requirements may advance but never decrease. WAL commits, heartbeat renewal,
and release preserve all engine metadata. Public writer open checks the stored
format and minimum-reader requirement before lease acquisition, scratch
creation, native recovery, or mutation.

Raw `commit-reference!` checkpoint callers must supply all three values in
`:engine-metadata`; omission or lowering is rejected before object verification
or head mutation. Producer `version` and `min-reader` are also rejected there
unless the canonical release parser accepts them. Validation errors are
redacted: neither the rejected text nor storage and lease identities enter the
exception. Writer composition preflights the same metadata against the owned
head before checkpoint creation or immutable upload, then `commit-reference!`
repeats the check before verification and head replacement. The public writer
supplies this metadata from the checked native capability, so ordinary callers
do not configure these fields separately.

## Immutable commits and ambiguity

`publish-wal-bytes!` is the semantic publication seam for bounded statement
WAL payloads. It derives the full reference from the writer token's remembered
generation, the current head's next manifest sequence, the byte count, and the
full SHA-256 digest. Every call creates a fresh key using the first eight
lowercase hexadecimal characters of a runtime-verified UUIDv4; callers cannot
supply a mismatched generation, sequence, or reuse token. The vanishingly rare
pre-existing-key collision is accepted only when full reference verification
proves the same object. A stale writer may leave an unreachable old-generation
immutable object, as the Quint model permits, but cannot commit it; a future
generation that no acquisition could have issued is rejected.

Remote conditional creates may return `:ambiguous` when the response is lost.
The publisher rereads that one unique immutable key: exact size and SHA-256
prove `:reconciled`, a known different object fails integrity verification, and
an absent object remains `commit-ambiguous`. It never treats a transport
failure as proof that the create did not land. Streaming checkpoint publication
uses the same reconciliation rule and also verifies a confirmed create before
allowing its reference to reach the head CAS.

This byte-materializing function is intentionally not a checkpoint API.
Checkpoint publication still requires the streaming file/hash slice.

`commit-reference!` accepts `:wal` or `:checkpoint` and requires a
`verify-reference!` function. The candidate head is schema-validated first;
the verifier must then prove that the immutable object already present in the
backend matches the reference's size and SHA-256; only then can the head CAS
run. WAL appends exactly one reference and advances the sequence once.
Checkpoint replaces the base, clears WAL, and advances the sequence once.
Ownership and reference currency are checked before verification. Because
verification may block, heartbeat renewal remains independent while it runs;
the commit rereads and rebuilds from the latest owned head afterward. A
definite same-owner CAS collision retries within the bounded commit attempt
limit, while changed ownership fences and an ambiguous CAS is never retried.
There is no writer-local head lock around a backend read, CAS, or reconciliation
read: any of those remote calls may outlive the lease TTL. Manifest commit and
heartbeat renewal instead race through backend CAS. A definite same-owner loser
rebuilds from the newest head; renewal reconciliation proves the same fencing
token plus an expiry at least as late as requested, while manifest reconciliation
proves the exact sequence/reference effect. Neither path can erase the other's
landed transition or duplicate manifest advancement.

An in-flight heartbeat transport request can still block until its configured
backend timeout. That is an availability limit, not a fencing exception:
mutations and flushes compare the local clock with the last proved expiry and
self-fence once it is reached, and nested S3 retries observe that predicate
before another request. Deployments should configure per-request backend
timeouts within their renewal slack when continued availability under a
stalled request matters.

That equality behavior is intentionally asymmetric. A competing writer must
wait until strictly after the skew-adjusted expiry before normal takeover, but
the current writer self-fences as soon as its locally proved expiry is reached.
The overlap-free interval is conservative under clock uncertainty; force
takeover remains an explicit operator override.

`verify-byte-reference!` is an in-memory verifier for bounded statement WAL
objects. It uses the pinned `jolt-lang/jolt-crypto` MessageDigest shim on Jolt
and the JDK implementation on JVM Clojure. Full checkpoint archives require a
streaming verifier; callers must not route them through this byte-materializing
helper.

Backends may report an ambiguous conditional write only as
`{:status :ambiguous}`. The control plane rereads `head.json`:

- the canonical intended head proves `:reconciled`;
- changed ownership is `lease-fenced`; and
- retained ownership without the intended head is `commit-ambiguous`.

Errors do not retain identities, object keys, paths, head bytes, or provider
results.

## Formal and executable evidence

Before implementation, the target was a two-writer, six-transition model in
which:

- only the current identity plus generation can commit;
- manifest sequence never regresses;
- the exact generation/sequence reference was already published;
- a stale writer cannot change the head; and
- an ambiguous acknowledgement requires the exact intended reread.

The model abstracts expiry/force as the choice of a successful acquisition
and each serialized step as one atomic backend or reconciliation point. An
immutable reference is modeled exactly as object identity plus lease
generation and manifest sequence; publication is set membership of that exact
reference rather than a content-only bit.

Verified through Chiasmus with Z3:

- `formal/durable-head-cas.smt2`: **UNSAT** for the safety violation;
- `formal/durable-head-cas-stale-mutant.smt2`: **SAT**, with takeover followed
  by release and a stale prior writer incorrectly committing; and
- `formal/durable-head-cas-boundary.smt2`: **SAT** for a reachable
  acquire, publish, commit path.

The literate executable companion in `formal/quint/` expresses the same bounded
corrected, mutant, and boundary shapes as a Quint state machine.
Its fast gate typechecks both modules, runs deterministic traces, and samples
10,000 six-step executions while requiring every major action witness to be
nonzero. The checked-in
[evidence record](../formal/quint/evidence/2026-09-06.edn) distinguishes those
sampled results from separate bounded Apalache results for each named invariant.
A solver `UNKNOWN`, interrupted run, or timeout is not recorded as proof.
It additionally checks that a committed reference never names a future lease
generation, exactly matches manifest sequence, and that publication satisfies
both full attempt-bearing and attempt-erased checks. Commit validity separately
checks its content projection. Independent generation- and sequence-mismatch
mutants are required to fail those checks.

The checked ADR-015 trace is replayed against this namespace with
`jolt -M:durable-itf-test`. The driver compares every completed outcome and
head projection, then validates the versioned Hegel operation-event contract
and its Durable domain model. See
`docs/durable-trace-validation.md` for the observation-only advice and
offline-validation boundary.

The focused executable suite retains the same red/green shape. Deterministic
cases cover fresh/live/expired leases, heartbeat, release, verified and missing
objects, stale fencing, landed ambiguity, and dropped ambiguity. A 40-case
Hegel state machine generates 18-step traces across two force-taking writers,
valid commits, and stale attempts. Its explicit event model checks monotonic
generation/sequence, publication before commit, and no head change after a
stale attempt.

`jdbc.chdb.durable.writer` supplies the serialized operation worker, statement
buffering and limits, confirmed WAL flush, checkpoint publication, heartbeat,
and ordered close cleanup. `jdbc.chdb.durable/open-writer!` composes that worker
with compatibility gates, scratch restore/replay, lease acquisition and
self-fencing, and open-failure cleanup. See [Durable storage](durable.md) for
the current end-to-end status and remaining qualification work.
