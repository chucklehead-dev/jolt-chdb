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
`db10b548a3e1e21e51c213baf863cb1050963d9c`,
`docs/durable/protocol-v1.mdx#state-machine`. This is one bounded control-plane
milestone, not a complete Durable-open API or a conformance claim.

## Ownership and time

The application fencing token is the exact triple `owner`, `instance`, and
`generation`. Matching an owner name alone is never sufficient. Every
acquisition of an existing head increments the generation; heartbeat, ordinary
commit, and release preserve it. A stale writer is rejected before object
verification or head mutation.

Normal takeover becomes eligible only when
`now >= expires_at + clock_skew`. Explicit force can take over an unexpired
lease. A new lease expiry must be later than the acquisition time, and a
heartbeat must strictly extend the existing expiry. Scheduling heartbeats at no
more than one third of the TTL belongs to the later operation-worker slice.

All desired heads are encoded and decoded before CAS. That is a correctness
boundary, not cosmetic normalization: JSON may serialize an integral decimal
as an integer, and ambiguous reconciliation must compare the exact semantic
value a reread will produce.

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

This byte-materializing function is intentionally not a checkpoint API.
Checkpoint publication still requires the streaming file/hash slice.

`commit-reference!` accepts `:wal` or `:checkpoint` and requires a
`verify-reference!` function. The candidate head is schema-validated first;
the verifier must then prove that the immutable object already present in the
backend matches the reference's size and SHA-256; only then can the head CAS
run. WAL appends exactly one reference and advances the sequence once.
Checkpoint replaces the base, clears WAL, and advances the sequence once.

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
nonzero. The checked-in evidence record distinguishes sampled results from the
critical invariant's bounded Apalache result and records a combined-invariant
`UNKNOWN` as tool-blocked rather than proof.
It additionally checks that a committed reference never names a future lease
generation, exactly matches manifest sequence, and that each exact transition
refines the content-level view. Independent generation- and sequence-mismatch
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

`jdbc.chdb.durable.writer` now supplies the serialized operation worker,
statement buffering and limits, confirmed WAL flush, and ordered close cleanup
for an already-acquired, already-recovered handle. Still outside the composed
Durable `open!`: compatibility gates, scratch restore/replay, heartbeat
scheduling and self-fencing, streaming checkpoint publication/hashing, bounded
persistence retry, and open-failure cleanup.
