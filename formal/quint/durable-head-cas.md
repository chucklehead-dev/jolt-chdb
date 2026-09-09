# Durable head-CAS Quint model

This document is the authoritative executable, literate companion to the bounded SMT
models in `formal/durable-head-cas*.smt2`. It models two writers coordinating
through one shared, linearizable `head.json` CAS register. It uses plain Quint,
not Choreo, because the writers do not exchange protocol messages.
The `lmt` tangler extracts its Quint fences into ignored files under
`target/formal/quint/`; generated `.qnt` files are never edited directly.

## Authority and scope

The normative source is chDB commit
`66643e5030fb73c30ac5cdd31d4c7858ea040ed0`,
`docs/durable/protocol-v1.mdx#state-machine`. The implementation correspondence
is:

| Model operation | Implementation |
| --- | --- |
| ownership and fencing | `src/jdbc/chdb/durable/control.clj`, `ownership`, `owns?`, and `assert-owned!` |
| acquire/takeover | `src/jdbc/chdb/durable/control.clj`, `acquire!` |
| bounded WAL publication | `publish-wal-bytes!` derives the exact reference; `verify-byte-reference!` proves idempotent reuse |
| confirmed or ambiguous commit | `replace-owned!`, `classify-after-cas`, and `commit-reference!` |
| release | `release!` |

The initial state is the SMT model's existing released generation-one head,
not missing-head creation. Expiry, force, and lease eligibility are abstracted
as the choice to take an `acquire` transition. Publication integrity is an
already-published set membership. Each action is one atomic provider or
reconciliation point.

`ObjectId` is a finite immutable-content identity. `AttemptId` is the bounded
identity of the fresh UUIDv4 nonce in every physical publication key. The
model retains that identity in exact references. One transition monitor checks
the full publication reference; a second checks the projection produced by
erasing only the attempt, leaving object, generation, and sequence. Commit
validity separately compares the content projection of the committed head. The
ITF driver maps every finite attempt to exactly one runtime UUID-bearing
reference.

The model deliberately excludes wall-clock time and heartbeat scheduling, but
includes an abstract lease revision so a same-owner renewal can occur between
an ambiguous CAS landing and its reconciliation read. It excludes retry/backoff,
generation exhaustion, full JSON/ETag encoding, WAL versus checkpoint manifest
shape, scratch recovery, close, and process lifecycle. It establishes
no liveness claim.

That abstraction does not make time units irrelevant at the implementation
boundary. Protocol and runtime traces tag every observed wire lease value as
`EpochSeconds`; the separate time-dimension model below checks the explicit
millisecond-to-seconds conversion and retains a wrong-unit mutant. Neither is a
proof of real-clock behavior.

## Executable structure

- The generated `durableHeadCas.qnt` owns the types, pure transition functions,
  the split ambiguous-land/renew/reconcile state machine, witnesses, and safety
  invariants.
- The generated `durableHeadCasTest.qnt` owns deterministic corrected boundaries
  and all mutation witnesses.
- The generated `durableWriterBoundary.qnt` separately models the writer's
  statement-WAL versus full-checkpoint choice for bound mutations.
- `durableHeadCasCorrected` binds the six-step bounds with correct ownership.
- `durableHeadCasStaleMutant` binds the same model with one semantic fault:
  any remembered generation is treated as authority.
- `durableHeadCasGenerationMutant` and `durableHeadCasSequenceMutant` corrupt one
  coordinate of the exact immutable reference written to the head.
- `durableHeadCasAttemptReuseMutant` permits a publication to reuse an attempt
  identity, and `durableHeadCasCommitAttemptMutant` accepts an unpublished
  attempt through an attempt-erased reference match.
- `durableHeadCasWholeHeadReconciliationMutant` incorrectly requires the whole
  desired head document to match after an ambiguous landing, so a harmless
  same-owner lease revision produces the red control.
- `durableHeadCasLandingReferenceMutant` corrupts only the reference installed
  by the ambiguous landing. The landing monitor must reject that intermediate
  state before a later reconciliation or takeover can hide it.

The event history records before/after head snapshots. This is intentional: a
later publication must not retroactively make an earlier invalid commit look
valid, and a stale attempt must be fenced before object verification.

The SMT filler actions `0` and `11` are omitted. `quint verify --max-steps 6`
checks every shorter prefix without inventing a protocol stutter. Six attempt
identities suffice because a six-transition trace cannot publish more than six
objects.

The `legacyStep` action retains the four-command MBT vocabulary while the normal
`step` expands ambiguous landing, renewal, and reconciliation. With Quint's
experimental `--mbt` flag, each legacy ITF state therefore names one command and
only the parameters relevant to that command.
Publication and commit carry writer, object, and attempt because all three
participate in the exact immutable reference.

## Red, green, and boundary controls

The corrected analysis checks that published references remain published,
sequence does not regress, stale attempts cannot mutate or pass verification,
acknowledged commits have the exact ownership and state transition, non-landed
failed commits leave the head unchanged, and an ambiguous landed commit is
reconciled from expected reference/sequence plus current ownership even when a
same-owner renewal changes the lease revision.

The deterministic valid boundary is:

```text
acquire Writer1 -> publish Object1 -> confirmed commit Writer1/Object1
generation 2, owner Writer1, sequence 1, reference Object1
```

The stale-ownership mutant witness is:

```text
publish Object2 -> acquire Writer1 -> acquire Writer2
  -> stale Writer1 confirmed commit Object2
```

Apalache also finds the shorter trace recorded under `traces/`: after takeover,
the stale writer reaches `ObjectUnverified`. Correct behavior is `LeaseFenced`
before verification, even when the requested object is absent.

The reference mutants demonstrate that content identity alone is insufficient:
changing generation or sequence makes a head reference non-canonical, reusing
an attempt breaks key freshness, and accepting an attempt-erased commit can
install a reference that was never published.

The pending-takeover boundary is:

```text
acquire Writer1 -> publish Object1 -> ambiguous CAS lands
  -> acquire Writer2 -> Writer1 reconciles as LeaseFenced
```

Writer2's takeover preserves the landed reference. Writer1 receives no success
acknowledgement, and the immutable object remains available to recovery.

The deterministic reconciliation boundary is:

```text
acquire Writer1 -> publish Object1 -> ambiguous CAS lands
  -> same-owner renewal changes only lease revision -> reconcile
```

The corrected model returns `Reconciled`; the whole-head equality mutant returns
`CommitAmbiguous` even though ownership and the expected reference/sequence are
still present.

## Commands

Fast deterministic and sampled gates:

```sh
scripts/check-durable-head-quint.sh
```

Generate one canonical ITF trace plus a compact JSON command projection with:

```sh
scripts/generate-durable-head-itf.sh
```

The generator uses a checked-in default seed and records it in the command
projection. Set `QUINT_SEED` to explore or replay another simulator trace.

The command projection is intentionally lossy and is only a driver input. The
ADR-015 ITF file remains the evidence artifact and contains the complete model
states, `mbt::actionTaken`, and `mbt::nondetPicks`. The jq adapter rejects
unknown actions, variants, or missing picks instead of silently skipping them.

The Hegel replay driver in `test/jdbc/chdb_durable_itf_test.clj` consumes the
full ITF artifact, invokes `acquire!`, immutable publication,
`commit-reference!`, and `release!`, and compares an implementation observation
projection after every step with the corresponding model state. Its command
decoder remains separate from the state oracle so the same ITF vocabulary can
later drive a real object backend and woven aspect lifecycle runs.

The deterministic 64-trace corpus uses a seed selected to reach every legacy
action and every modeled control outcome. Before replay, the corpus gate writes
`target/formal/quint/itf-corpus-coverage.json` and fails if confirmed or
reconciled commit, ambiguity, fencing, object rejection, or either release
outcome is absent. This coverage assertion does not add renewal or retry to the
legacy vocabulary; those require a separate lifecycle/concurrency adapter.

The script requires Quint 0.32.0 and `lmt` pinned at commit
`62fe18f2f6a6e11c158ff2b2209e1082a4fcd59c`. It first tangles this Markdown
source, then runs every Quint command against the generated files. Install the
extractor with:

```sh
go install github.com/driusan/lmt@62fe18f2f6a6e11c158ff2b2209e1082a4fcd59c
```

The wrapper uses Quint's TypeScript evaluator. Quint 0.32.0's separately
downloaded Rust evaluator 0.6.0 currently crashes while decoding this
sum-type-rich state (`Failed to parse data from Rust evaluator`); this is a
tool-backend limitation, not a model result.

Run the explicit bounded corrected and mutation-control checks with:

```sh
scripts/check-durable-head-quint.sh --verify
```

The explicit corrected gates check publication-attempt freshness, stale-writer
safety, exact reference canonicality, operation-specific ambiguous
reconciliation, and both publication transition monitors through six
transitions. They also check the five-step writer boundary: bound mutations
remain checkpoint-required across later writes and failed flushes, and only a
full checkpoint can acknowledge them.
Each mutation module must produce its expected
bounded counterexample. The refinement monitors are inductive and local, which
avoids a solver-expensive quantified scan of the complete history.

## Evidence discipline

Simulation is sampled evidence. Apalache results are bounded to the stated
six-transition horizon. A mutation control is valid only while the corrected
model passes the same property, the mutant produces the expected
counterexample, and the valid boundary remains reachable.

Seeds are replay conveniences tied to tool/backend versions. The normalized
operation trace and ITF output are the durable replay coordinates. The Hegel
adapter and future aspect-instrumented adapters use the same `Acquire`,
`Publish`, `CommitAttempt`, and `ReleaseAttempt` vocabulary. This pilot replays
the in-memory Durable implementation; real object-backend and woven-aspect
lanes remain follow-on work.

Update and recheck this model when the named state-machine functions or the
normative Durable V1 transitions change. Do not weaken the model merely to
match an implementation defect.

## Executable model

The following sections tangle, in order, to the generated model consumed by the
check script. Repeated `+=` fence targets append to the same file. This keeps the
explanation close to the code without creating separate semantic modules or
changing the generated Quint.

Do not edit the generated file. Edit these sections and rerun the script. For a
user-facing view of the runtime around this state machine, start with
[Durable storage](../../docs/durable.md).

### Types, bounds, and state

The model uses two symbolic writers, two symbolic object contents, and six
publication attempts. These are finite identities, not strings or UUID
implementations. The `Reference` type keeps all four facts that make an
immutable publication safe to commit: content, fresh attempt, lease generation,
and manifest sequence.

`Head` is the shared `head.json` register at the level needed for the safety
properties. `WriterState` remembers each writer's fencing generation.
`PendingReconciliation` represents the interval after an ambiguous CAS is known
to have landed but before its response is reconciled. `Event` retains complete
before/after data so properties are checked at the transition where they
happened, rather than inferred from a later state.

The boolean constants are mutation switches. Every concrete module below binds
exactly one switch, or none for the corrected model. `MAX_GENERATION` and
`MAX_SEQUENCE` close the state space at values reachable within the six-step
analysis; they are model bounds, not runtime limits.

```quint target/formal/quint/durableHeadCas.qnt +=
// Durable V1 lease/head-CAS model.
//
// This model is intentionally bounded to the same two writers, two immutable
// objects, and six-transition horizon as formal/durable-head-cas.smt2. The
// concrete analysis modules at the bottom initialize every module constant.

module durableHeadCas {
  type WriterId = Writer1 | Writer2
  type ObjectId = Object1 | Object2
  type AttemptId =
    | Attempt1
    | Attempt2
    | Attempt3
    | Attempt4
    | Attempt5
    | Attempt6

  type LeaseOwner =
    | Released
    | HeldBy(WriterId)

  type OptionalGeneration =
    | NoGeneration
    | SomeGeneration(int)

  type Reference = {
    objectId: ObjectId,
    attemptId: AttemptId,
    generation: int,
    sequence: int,
  }

  type ExactReference = {
    objectId: ObjectId,
    generation: int,
    sequence: int,
  }

  type OptionalReference =
    | NoReference
    | SomeReference(Reference)

  type ContentReference =
    | NoObject
    | SomeObject(ObjectId)

  type CommitMode =
    | Confirmed
    | AmbiguousLanded
    | AmbiguousDropped

  type CommitResult =
    | Committed
    | Reconciled
    | LeaseFenced
    | ObjectUnverified
    | CommitAmbiguous

  type Head = {
    generation: int,
    owner: LeaseOwner,
    leaseRevision: int,
    sequence: int,
    reference: OptionalReference,
  }

  type PendingReconciliationData = {
    writer: WriterId,
    objectId: ObjectId,
    attemptId: AttemptId,
    tokenGeneration: OptionalGeneration,
    expectedReference: Reference,
    desiredHead: Head,
    casBefore: Head,
  }

  type PendingReconciliation =
    | NoPendingReconciliation
    | AwaitingReconciliation(PendingReconciliationData)

  type WriterState = {
    rememberedGeneration: OptionalGeneration,
  }

  type AcquireEventData = {
    writer: WriterId,
    before: Head,
    after: Head,
  }

  type PublishEventData = {
    writer: WriterId,
    objectId: ObjectId,
    attemptId: AttemptId,
    tokenGeneration: OptionalGeneration,
    reference: Reference,
    before: Set[Reference],
    after: Set[Reference],
    usedBefore: Set[AttemptId],
    usedAfter: Set[AttemptId],
  }

  type CommitEventData = {
    writer: WriterId,
    objectId: ObjectId,
    attemptId: AttemptId,
    reference: Reference,
    tokenGeneration: OptionalGeneration,
    mode: CommitMode,
    referenceWasPublished: bool,
    exactReferenceWasPublished: bool,
    landed: bool,
    acknowledged: bool,
    result: CommitResult,
    desiredHead: Head,
    operationSpecificProofHeld: bool,
    wholeHeadProofHeld: bool,
    before: Head,
    after: Head,
  }

  type ReleaseEventData = {
    writer: WriterId,
    tokenGeneration: OptionalGeneration,
    accepted: bool,
    before: Head,
    after: Head,
  }

  type Event =
    | Acquired(AcquireEventData)
    | Published(PublishEventData)
    | CommitAttempted(CommitEventData)
    | ReleaseAttempted(ReleaseEventData)

  type State = {
    head: Head,
    writers: WriterId -> WriterState,
    published: Set[Reference],
    usedAttempts: Set[AttemptId],
    pendingReconciliation: PendingReconciliation,
    renewedWhilePending: bool,
    events: List[Event],
    lastTransitionRefinesExactView: bool,
    lastTransitionRefinesContentView: bool,
  }

  const MAX_GENERATION: int
  const MAX_SEQUENCE: int
  const USE_STALE_OWNERSHIP_MUTANT: bool
  const USE_ATTEMPT_REUSE_MUTANT: bool
  const USE_REFERENCE_GENERATION_MUTANT: bool
  const USE_REFERENCE_SEQUENCE_MUTANT: bool
  const USE_COMMIT_ATTEMPT_MUTANT: bool
  const USE_WHOLE_HEAD_RECONCILIATION_MUTANT: bool
  const USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT: bool

  pure val WRITERS: Set[WriterId] = Set(Writer1, Writer2)
  pure val OBJECTS: Set[ObjectId] = Set(Object1, Object2)
  pure val ATTEMPTS: Set[AttemptId] =
    Set(Attempt1, Attempt2, Attempt3, Attempt4, Attempt5, Attempt6)

  var state: State

```

### Acquisition and immutable publication

Acquisition increments the shared generation and stores the same generation in
the chosen writer's token. This is the abstract fencing event: another acquire
can make that remembered token stale without changing the older writer's local
state.

Publication creates an exact immutable reference and records the attempt as
used. It deliberately does not change `head`. Publication-before-commit is
therefore visible in the state, and attempt reuse can be isolated as its own
mutant. The runtime UUID is represented only by `AttemptId`; random generation,
key encoding, hashing, and provider I/O do not affect these invariants.

```quint target/formal/quint/durableHeadCas.qnt +=
  pure def applyAcquire(s: State, writer: WriterId): State = {
    val nextHead = {
      ...s.head,
      generation: s.head.generation + 1,
      owner: HeldBy(writer),
      leaseRevision: s.head.leaseRevision + 1,
    }
    val nextWriter = {
      rememberedGeneration: SomeGeneration(nextHead.generation),
    }
    val event = Acquired({ writer: writer, before: s.head, after: nextHead })
    {
      ...s,
      head: nextHead,
      writers: s.writers.put(writer, nextWriter),
      events: s.events.append(event),
      lastTransitionRefinesExactView: eventRefinesExactView(event),
      lastTransitionRefinesContentView: eventRefinesContentView(event),
    }
  }

  pure def generationValue(generation: OptionalGeneration): int =
    match generation {
      | NoGeneration => 0
      | SomeGeneration(value) => value
    }

  pure def referenceFor(
    s: State,
    writer: WriterId,
    objectId: ObjectId,
    attemptId: AttemptId
  ): Reference = {
    val remembered = generationValue(
      s.writers.get(writer).rememberedGeneration
    )
    {
      objectId: objectId,
      attemptId: attemptId,
      generation:
        if (USE_REFERENCE_GENERATION_MUTANT) remembered + 1 else remembered,
      sequence:
        if (USE_REFERENCE_SEQUENCE_MUTANT) s.head.sequence + 2
        else s.head.sequence + 1,
    }
  }

  pure def applyPublish(
    s: State,
    writer: WriterId,
    objectId: ObjectId,
    attemptId: AttemptId
  ): State = {
    val reference = referenceFor(s, writer, objectId, attemptId)
    val nextPublished = s.published.union(Set(reference))
    val nextUsedAttempts = s.usedAttempts.union(Set(attemptId))
    val event = Published({
      writer: writer,
      objectId: objectId,
      attemptId: attemptId,
      tokenGeneration: s.writers.get(writer).rememberedGeneration,
      reference: reference,
      before: s.published,
      after: nextPublished,
      usedBefore: s.usedAttempts,
      usedAfter: nextUsedAttempts,
    })
    {
      ...s,
      published: nextPublished,
      usedAttempts: nextUsedAttempts,
      events: s.events.append(event),
      lastTransitionRefinesExactView: eventRefinesExactView(event),
      lastTransitionRefinesContentView: eventRefinesContentView(event),
    }
  }

```

### Ownership and commit evaluation

`tokenOwns` is the corrected fencing rule: both writer identity and remembered
generation must match the current head. The stale-ownership mutant changes only
that decision, making it possible to distinguish a useful red control from an
unrelated broken model.

The model defines projections at two useful boundaries. Erasing the physical
publication attempt leaves object, generation, and sequence and is checked by a
publication transition monitor. Projecting a head reference to object content
is used later by acknowledged-commit validity; it is not a second general
transition refinement. The commit evaluator records whether ownership and
publication proofs held, whether the CAS applied, and the resulting head.
Keeping these facts in the event is what lets later invariants prove the
transition rather than merely inspect its final value.

```quint target/formal/quint/durableHeadCas.qnt +=
  pure def tokenOwns(
    snapshot: Head,
    writer: WriterId,
    tokenGeneration: OptionalGeneration
  ): bool =
    snapshot.owner == HeldBy(writer)
      and tokenGeneration == SomeGeneration(snapshot.generation)

  pure def correctlyOwns(s: State, writer: WriterId): bool =
    tokenOwns(
      s.head,
      writer,
      s.writers.get(writer).rememberedGeneration
    )

  // Exactly-one-fault control: the stale mutant changes only this ownership
  // decision. It treats any remembered generation as authority, reproducing
  // formal/durable-head-cas-stale-mutant.smt2.
  pure def ownershipForCommit(s: State, writer: WriterId): bool =
    if (USE_STALE_OWNERSHIP_MUTANT)
      s.writers.get(writer).rememberedGeneration != NoGeneration
    else
      correctlyOwns(s, writer)

  pure def eraseAttemptReference(reference: Reference): ExactReference = {
    objectId: reference.objectId,
    generation: reference.generation,
    sequence: reference.sequence,
  }

  pure def eraseAttemptReferences(
    references: Set[Reference]
  ): Set[ExactReference] =
    references.map(eraseAttemptReference)

  pure def attemptOrErasedReferenceWasPublished(
    s: State,
    reference: Reference
  ): bool =
    if (USE_COMMIT_ATTEMPT_MUTANT)
      s.published.exists(
        publishedReference =>
          eraseAttemptReference(publishedReference)
            == eraseAttemptReference(reference)
      )
    else
      s.published.contains(reference)

  pure def operationSpecificCommitProof(
    observed: Head,
    writer: WriterId,
    tokenGeneration: OptionalGeneration,
    expectedReference: Reference
  ): bool = and {
    tokenOwns(observed, writer, tokenGeneration),
    observed.sequence == expectedReference.sequence,
    observed.reference == SomeReference(expectedReference),
  }

  pure def evaluateCommit(
    s: State,
    writer: WriterId,
    objectId: ObjectId,
    attemptId: AttemptId,
    mode: CommitMode
  ): CommitEventData = {
    val tokenGeneration = s.writers.get(writer).rememberedGeneration
    val reference = referenceFor(s, writer, objectId, attemptId)
    val ownsForCommit = ownershipForCommit(s, writer)
    val referenceWasPublished =
      attemptOrErasedReferenceWasPublished(s, reference)
    val exactReferenceWasPublished = s.published.contains(reference)
    val landed = mode != AmbiguousDropped
    val applies = ownsForCommit and referenceWasPublished and landed
    val nextHead =
      if (applies) {
        ...s.head,
        sequence: s.head.sequence + 1,
        reference: SomeReference(reference),
      } else s.head
    val operationSpecificProofHeld =
      applies and operationSpecificCommitProof(
        nextHead, writer, tokenGeneration, reference
      )
    val result =
      if (not(ownsForCommit)) LeaseFenced
      else if (not(referenceWasPublished)) ObjectUnverified
      else match mode {
        | Confirmed => Committed
        | AmbiguousLanded => Reconciled
        | AmbiguousDropped => CommitAmbiguous
      }
    {
      writer: writer,
      objectId: objectId,
      attemptId: attemptId,
      reference: reference,
      tokenGeneration: tokenGeneration,
      mode: mode,
      referenceWasPublished: referenceWasPublished,
      exactReferenceWasPublished: exactReferenceWasPublished,
      landed: applies,
      acknowledged: applies,
      result: result,
      desiredHead: nextHead,
      operationSpecificProofHeld: operationSpecificProofHeld,
      wholeHeadProofHeld: applies,
      before: s.head,
      after: nextHead,
    }
  }

  pure def applyCommit(s: State, eventData: CommitEventData): State = {
    val event = CommitAttempted(eventData)
    {
      ...s,
      head: eventData.after,
      events: s.events.append(event),
      lastTransitionRefinesExactView: eventRefinesExactView(event),
      lastTransitionRefinesContentView: eventRefinesContentView(event),
    }
  }

```

### Ambiguous CAS, renewal, and release

A normal confirmed or dropped commit is evaluated in one model transition. A
landed ambiguous commit is split into begin, optional renewal or takeover, and
reconcile transitions. The landing transition checks the exact head written by
the CAS before reconciliation can report any result. Reconciliation then uses
the expected reference, sequence, and current ownership rather than exact
equality with an old whole-head snapshot whose lease revision may legitimately
have changed. If another writer takes over first, the original writer is fenced
without acknowledging the commit, while the landed immutable reference remains
recoverable from the shared head.

Release uses the same token rule and preserves generation. None of these
functions models retries or elapsed time; it models only the state visible at
the provider's serialized decision points.

```quint target/formal/quint/durableHeadCas.qnt +=
  pure def otherObjectId(objectId: ObjectId): ObjectId =
    match objectId {
      | Object1 => Object2
      | Object2 => Object1
    }

  pure def landedHeadFor(evaluated: CommitEventData): Head =
    if (USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT) {
      ...evaluated.after,
      reference: SomeReference({
        ...evaluated.reference,
        objectId: otherObjectId(evaluated.reference.objectId),
      }),
    } else evaluated.after

  pure def ambiguousLandingRefinesHead(
    s: State,
    evaluated: CommitEventData,
    landedHead: Head
  ): bool = and {
    evaluated.before == s.head,
    tokenOwns(s.head, evaluated.writer, evaluated.tokenGeneration),
    evaluated.referenceWasPublished,
    evaluated.exactReferenceWasPublished,
    evaluated.reference.objectId == evaluated.objectId,
    evaluated.reference.attemptId == evaluated.attemptId,
    evaluated.reference.generation == s.head.generation,
    evaluated.reference.sequence == s.head.sequence + 1,
    landedHead.generation == s.head.generation,
    landedHead.owner == s.head.owner,
    landedHead.leaseRevision == s.head.leaseRevision,
    landedHead.sequence == s.head.sequence + 1,
    landedHead.reference == SomeReference(evaluated.reference),
  }

  pure def applyBeginAmbiguousLanded(
    s: State,
    writer: WriterId,
    objectId: ObjectId,
    attemptId: AttemptId
  ): State = {
    val evaluated = evaluateCommit(
      s, writer, objectId, attemptId, AmbiguousLanded
    )
    val landedHead = landedHeadFor(evaluated)
    val landingRefines = ambiguousLandingRefinesHead(s, evaluated, landedHead)
    val pending = {
      writer: writer,
      objectId: objectId,
      attemptId: attemptId,
      tokenGeneration: evaluated.tokenGeneration,
      expectedReference: evaluated.reference,
      desiredHead: evaluated.after,
      casBefore: evaluated.before,
    }
    {
      ...s,
      head: landedHead,
      pendingReconciliation: AwaitingReconciliation(pending),
      lastTransitionRefinesExactView: landingRefines,
      lastTransitionRefinesContentView: landingRefines,
    }
  }

  pure def applyRenewPending(s: State): State = {
    ...s,
    head: { ...s.head, leaseRevision: s.head.leaseRevision + 1 },
    renewedWhilePending: true,
    lastTransitionRefinesExactView: true,
    lastTransitionRefinesContentView: true,
  }

  pure def evaluatePendingReconciliation(
    s: State,
    pending: PendingReconciliationData
  ): CommitEventData = {
    val operationProof = operationSpecificCommitProof(
      s.head,
      pending.writer,
      pending.tokenGeneration,
      pending.expectedReference
    )
    val wholeHeadProof = s.head == pending.desiredHead
    val accepted =
      if (USE_WHOLE_HEAD_RECONCILIATION_MUTANT) wholeHeadProof
      else operationProof
    val observedBefore = if (accepted) pending.casBefore else s.head
    val result =
      if (accepted) Reconciled
      else if (not(tokenOwns(
        s.head, pending.writer, pending.tokenGeneration
      ))) LeaseFenced
      else CommitAmbiguous
    {
      writer: pending.writer,
      objectId: pending.objectId,
      attemptId: pending.attemptId,
      reference: pending.expectedReference,
      tokenGeneration: pending.tokenGeneration,
      mode: AmbiguousLanded,
      referenceWasPublished: s.published.contains(pending.expectedReference),
      exactReferenceWasPublished: s.published.contains(pending.expectedReference),
      landed: accepted,
      acknowledged: accepted,
      result: result,
      desiredHead: pending.desiredHead,
      operationSpecificProofHeld: operationProof,
      wholeHeadProofHeld: wholeHeadProof,
      before: observedBefore,
      after: s.head,
    }
  }

  pure def applyPendingReconciliation(
    s: State,
    pending: PendingReconciliationData
  ): State = {
    val committed = applyCommit(s, evaluatePendingReconciliation(s, pending))
    { ...committed, pendingReconciliation: NoPendingReconciliation }
  }

  pure def evaluateRelease(s: State, writer: WriterId): ReleaseEventData = {
    val tokenGeneration = s.writers.get(writer).rememberedGeneration
    val accepted = tokenOwns(s.head, writer, tokenGeneration)
    val nextHead =
      if (accepted) { ...s.head, owner: Released }
      else s.head
    {
      writer: writer,
      tokenGeneration: tokenGeneration,
      accepted: accepted,
      before: s.head,
      after: nextHead,
    }
  }

  pure def applyRelease(s: State, eventData: ReleaseEventData): State = {
    val event = ReleaseAttempted(eventData)
    {
      ...s,
      head: eventData.after,
      events: s.events.append(event),
      lastTransitionRefinesExactView: eventRefinesExactView(event),
      lastTransitionRefinesContentView: eventRefinesContentView(event),
    }
  }

```

### Initial state and protocol actions

The initial head is released at generation one with no committed reference.
All writer tokens, publication attempts, and event history start empty. This is
the same initial boundary as the SMT model; missing-head creation and lease
expiry eligibility are intentionally outside this model.

The actions below are thin wrappers around the pure transition functions. Their
guards disable invalid actions: a publication attempt cannot be reused in the
corrected model and an ambiguous reconciliation must already be pending.
Acquisition remains enabled while reconciliation is pending because an expired
lease can be taken over while the original writer is waiting to reread the CAS.

```quint target/formal/quint/durableHeadCas.qnt +=
  action init: bool = all {
    state' = {
      head: {
        generation: 1,
        owner: Released,
        leaseRevision: 0,
        sequence: 0,
        reference: NoReference,
      },
      writers: WRITERS.mapBy(_ => { rememberedGeneration: NoGeneration }),
      published: Set(),
      usedAttempts: Set(),
      pendingReconciliation: NoPendingReconciliation,
      renewedWhilePending: false,
      events: List(),
      lastTransitionRefinesExactView: true,
      lastTransitionRefinesContentView: true,
    },
  }

  action acquire(writer: WriterId): bool = all {
    state.head.generation < MAX_GENERATION,
    state' = applyAcquire(state, writer),
  }

  action publish(
    writer: WriterId,
    objectId: ObjectId,
    attemptId: AttemptId
  ): bool = all {
    state.pendingReconciliation == NoPendingReconciliation,
    state.writers.get(writer).rememberedGeneration != NoGeneration,
    USE_ATTEMPT_REUSE_MUTANT or not(state.usedAttempts.contains(attemptId)),
    not(state.published.contains(referenceFor(state, writer, objectId, attemptId))),
    state' = applyPublish(state, writer, objectId, attemptId),
  }

  action commitAttempt(
    writer: WriterId,
    objectId: ObjectId,
    attemptId: AttemptId,
    mode: CommitMode
  ): bool = all {
    state.pendingReconciliation == NoPendingReconciliation,
    state.head.sequence < MAX_SEQUENCE,
    state' = applyCommit(
      state,
      evaluateCommit(state, writer, objectId, attemptId, mode)
    ),
  }

  action beginAmbiguousLanded(
    writer: WriterId,
    objectId: ObjectId,
    attemptId: AttemptId
  ): bool = all {
    state.pendingReconciliation == NoPendingReconciliation,
    state.head.sequence < MAX_SEQUENCE,
    ownershipForCommit(state, writer),
    attemptOrErasedReferenceWasPublished(
      state, referenceFor(state, writer, objectId, attemptId)
    ),
    state' = applyBeginAmbiguousLanded(state, writer, objectId, attemptId),
  }

  action renewPending: bool =
    match state.pendingReconciliation {
      | NoPendingReconciliation => all { false, state' = state }
      | AwaitingReconciliation(pending) => all {
          tokenOwns(state.head, pending.writer, pending.tokenGeneration),
          state' = applyRenewPending(state),
        }
    }

  action reconcilePending: bool =
    match state.pendingReconciliation {
      | NoPendingReconciliation => all { false, state' = state }
      | AwaitingReconciliation(pending) =>
          state' = applyPendingReconciliation(state, pending)
    }

  action releaseAttempt(writer: WriterId): bool = all {
    state.pendingReconciliation == NoPendingReconciliation,
    state' = applyRelease(state, evaluateRelease(state, writer)),
  }

```

### Event predicates and per-transition safety

These pure predicates explain what a single recorded event is allowed to do.
They check acquisition, publication freshness, generation and sequence changes,
stale rejection, acknowledged and failed commits, release, exact publication,
and attempt-erased publication.

Because every event contains its own before/after snapshots, a later
publication cannot retroactively make an earlier invalid commit appear safe.
The predicates are also reusable by deterministic tests and reachability
witnesses; they do not mutate model state.

```quint target/formal/quint/durableHeadCas.qnt +=
  pure def isAcquisition(event: Event): bool =
    match event {
      | Acquired(_) => true
      | _ => false
    }

  pure def isPublication(event: Event): bool =
    match event {
      | Published(_) => true
      | _ => false
    }

  pure def isCommitResult(event: Event, expected: CommitResult): bool =
    match event {
      | CommitAttempted(data) => data.result == expected
      | _ => false
    }

  pure def isRejectedStaleCommit(event: Event): bool =
    match event {
      | CommitAttempted(data) =>
          not(tokenOwns(data.before, data.writer, data.tokenGeneration))
            and data.result == LeaseFenced
      | _ => false
    }

  pure def isRelease(event: Event): bool =
    match event {
      | ReleaseAttempted(data) => data.accepted
      | _ => false
    }

  pure def publicationIsExact(event: Event): bool =
    match event {
      | Published(data) => and {
          data.tokenGeneration != NoGeneration,
          data.reference.objectId == data.objectId,
          data.reference.attemptId == data.attemptId,
          data.reference.generation == generationValue(data.tokenGeneration),
          data.reference.sequence >= 1,
          data.after == data.before.union(Set(data.reference)),
          data.usedAfter == data.usedBefore.union(Set(data.attemptId)),
        }
      | _ => true
    }

  pure def publicationAttemptIsFreshForEvent(event: Event): bool =
    match event {
      | Published(data) => and {
          not(data.usedBefore.contains(data.attemptId)),
          data.usedAfter.contains(data.attemptId),
        }
      | _ => true
    }

  pure def publicationRefinesExactView(event: Event): bool =
    match event {
      | Published(data) =>
          eraseAttemptReferences(data.after)
            == eraseAttemptReferences(data.before)
                 .union(Set(eraseAttemptReference(data.reference)))
      | _ => true
    }

  pure def projectReference(reference: OptionalReference): ContentReference =
    match reference {
      | NoReference => NoObject
      | SomeReference(value) => SomeObject(value.objectId)
    }

  pure def generationStepIsValid(event: Event): bool =
    match event {
      | Acquired(data) => and {
          data.after.generation == data.before.generation + 1,
          data.after.owner == HeldBy(data.writer),
          data.after.leaseRevision == data.before.leaseRevision + 1,
          data.after.sequence == data.before.sequence,
          data.after.reference == data.before.reference,
        }
      | CommitAttempted(data) =>
          data.after.generation == data.before.generation
      | ReleaseAttempted(data) =>
          data.after.generation == data.before.generation
      | Published(_) => true
    }

  pure def sequenceStepIsValid(event: Event): bool =
    match event {
      | Acquired(data) => data.after.sequence == data.before.sequence
      | CommitAttempted(data) =>
          if (data.landed)
            data.after.sequence == data.before.sequence + 1
          else
            data.after.sequence == data.before.sequence
      | ReleaseAttempted(data) => data.after.sequence == data.before.sequence
      | Published(_) => true
    }

  pure def staleCommitIsSafe(event: Event): bool =
    match event {
      | CommitAttempted(data) =>
          if (not(tokenOwns(data.before, data.writer, data.tokenGeneration)))
            and {
              data.after == data.before,
              not(data.acknowledged),
              not(data.landed),
              data.result == LeaseFenced,
            }
          else true
      | _ => true
    }

  pure def acknowledgedCommitIsValid(event: Event): bool =
    match event {
      | CommitAttempted(data) =>
          if (data.acknowledged)
            and {
              tokenOwns(data.before, data.writer, data.tokenGeneration),
              data.exactReferenceWasPublished,
              data.landed,
              data.after.generation == data.before.generation,
              data.after.owner == data.before.owner,
              data.after.sequence == data.before.sequence + 1,
              data.reference.objectId == data.objectId,
              data.reference.attemptId == data.attemptId,
              data.reference.generation == data.before.generation,
              data.reference.sequence == data.before.sequence + 1,
              data.after.reference == SomeReference(data.reference),
              projectReference(data.after.reference) == SomeObject(data.objectId),
              or {
                data.mode == Confirmed and data.result == Committed,
                data.mode == AmbiguousLanded and data.result == Reconciled,
              },
            }
          else true
      | _ => true
    }

  pure def failedCommitIsSafe(event: Event): bool =
    match event {
      | CommitAttempted(data) =>
          if (not(data.acknowledged) and not(data.landed))
            data.after == data.before
          else true
      | _ => true
    }

  pure def ambiguousReconciliationUsesOperationProof(event: Event): bool =
    match event {
      | CommitAttempted(data) =>
          if (data.mode == AmbiguousLanded
              and data.operationSpecificProofHeld)
            and {
              data.acknowledged,
              data.result == Reconciled,
            }
          else true
      | _ => true
    }

  pure def releaseIsValid(event: Event): bool =
    match event {
      | ReleaseAttempted(data) => and {
          data.after.generation == data.before.generation,
          data.after.sequence == data.before.sequence,
          data.after.reference == data.before.reference,
          data.after.leaseRevision == data.before.leaseRevision,
          if (data.accepted)
            and {
              tokenOwns(data.before, data.writer, data.tokenGeneration),
              data.after.owner == Released,
            }
          else
            data.after == data.before,
        }
      | _ => true
    }

  pure def eventRefinesContentView(event: Event): bool =
    match event {
      | Acquired(_) => generationStepIsValid(event)
      | Published(_) => publicationIsExact(event)
      | CommitAttempted(_) => and {
          generationStepIsValid(event),
          sequenceStepIsValid(event),
          staleCommitIsSafe(event),
          acknowledgedCommitIsValid(event),
          failedCommitIsSafe(event),
          ambiguousReconciliationUsesOperationProof(event),
        }
      | ReleaseAttempted(_) => and {
          generationStepIsValid(event),
          sequenceStepIsValid(event),
          releaseIsValid(event),
        }
    }

  pure def eventRefinesExactView(event: Event): bool =
    match event {
      | Acquired(_) => generationStepIsValid(event)
      | Published(_) => publicationRefinesExactView(event)
      | CommitAttempted(_) => and {
          generationStepIsValid(event),
          sequenceStepIsValid(event),
          staleCommitIsSafe(event),
          acknowledgedCommitIsValid(event),
          failedCommitIsSafe(event),
          ambiguousReconciliationUsesOperationProof(event),
        }
      | ReleaseAttempted(_) => and {
          generationStepIsValid(event),
          sequenceStepIsValid(event),
          releaseIsValid(event),
        }
    }

```

### State invariants and refinement monitors

The `val` definitions lift the per-event checks over the reachable history and
check the current head. The central safety statements are that stale writers
cannot change the head, every committed reference names a prior exact
publication, sequences do not regress, and an acknowledged commit has the
operation-specific result it claims.

`lastTransitionRefinesContentView` requires full attempt-bearing publication
identity. `lastTransitionRefinesExactView` weakens publication identity by
erasing the attempt while retaining object, generation, and sequence. The names
are historical and can be surprising, so rely on these definitions rather than
reading an erasure order from the names. Both are local inductive monitors;
passing them means their checks held for every transition in the bounded
reachable state graph, not that the runtime has an unbounded refinement proof.

```quint target/formal/quint/durableHeadCas.qnt +=
  val generationWithinBound: bool =
    state.head.generation >= 1 and state.head.generation <= MAX_GENERATION

  val sequenceWithinBound: bool =
    state.head.sequence >= 0 and state.head.sequence <= MAX_SEQUENCE

  val headReferenceWasPublished: bool =
    match state.head.reference {
      | NoReference => true
      | SomeReference(reference) => state.published.contains(reference)
    }

  val headReferenceIsCanonical: bool =
    match state.head.reference {
      | NoReference => state.head.sequence == 0
      | SomeReference(reference) => and {
          // Lease takeover advances fencing generation without rewriting the
          // manifest. The committed reference generation may therefore be
          // older, but can never come from a future lease generation.
          reference.generation <= state.head.generation,
          reference.sequence == state.head.sequence,
        }
    }

  val publicationAttemptIsFresh: bool =
    state.events.indices().forall(
      i => publicationAttemptIsFreshForEvent(state.events.nth(i))
    )

  val attemptIdentityIsUnique: bool =
    state.published.forall(
      left => state.published.forall(
        right => left.attemptId != right.attemptId or left == right
      )
    )

  val attemptTransitionsRefineExactView: bool =
    state.lastTransitionRefinesExactView

  val exactTransitionsRefineContentView: bool =
    state.lastTransitionRefinesContentView

  val acquisitionIncrementsGeneration: bool =
    state.events.indices().forall(i => generationStepIsValid(state.events.nth(i)))

  val sequenceNeverRegresses: bool =
    state.events.indices().forall(i => sequenceStepIsValid(state.events.nth(i)))

  val staleWriterCannotChangeHead: bool =
    state.events.indices().forall(i => staleCommitIsSafe(state.events.nth(i)))

  val acknowledgedCommitIsExact: bool =
    state.events.indices().forall(i => acknowledgedCommitIsValid(state.events.nth(i)))

  val failedCommitLeavesHeadUnchanged: bool =
    state.events.indices().forall(i => failedCommitIsSafe(state.events.nth(i)))

  val ambiguousLandedUsesOperationSpecificReconciliation: bool =
    state.events.indices().forall(
      i => ambiguousReconciliationUsesOperationProof(state.events.nth(i))
    )

  val releasePreservesGeneration: bool =
    state.events.indices().forall(i => releaseIsValid(state.events.nth(i)))

  val activeOwnerHasCurrentToken: bool =
    match state.head.owner {
      | Released => true
      | HeldBy(writer) =>
          state.writers.get(writer).rememberedGeneration
            == SomeGeneration(state.head.generation)
    }

```

### Reachability and the ITF command boundary

Safety checks are useful only when the important actions can actually happen.
The reachability values require acquisition, publication, both confirmed and
reconciled commit, an unprovable ambiguous drop, stale rejection, release, and
renewal during reconciliation to appear in sampled traces.

The `choose...` actions supply finite nondeterministic parameters. `step` is the
full analysis transition relation, including the multi-step ambiguous path.
`legacyStep` keeps the four-command model-based-testing vocabulary consumed by
the ADR-015 ITF adapter. It intentionally collapses provider internals so each
ITF state maps to one implementation command and its nondeterministic picks.

```quint target/formal/quint/durableHeadCas.qnt +=
  val acquisitionReached: bool =
    state.events.indices().exists(i => isAcquisition(state.events.nth(i)))

  val publicationReached: bool =
    state.events.indices().exists(i => isPublication(state.events.nth(i)))

  val confirmedCommitReached: bool =
    state.events.indices().exists(i => isCommitResult(state.events.nth(i), Committed))

  val reconciledCommitReached: bool =
    state.events.indices().exists(i => isCommitResult(state.events.nth(i), Reconciled))

  val ambiguousDropReached: bool =
    state.events.indices().exists(i => isCommitResult(state.events.nth(i), CommitAmbiguous))

  val staleCommitRejectedReached: bool =
    state.events.indices().exists(i => isRejectedStaleCommit(state.events.nth(i)))

  val releaseReached: bool =
    state.events.indices().exists(i => isRelease(state.events.nth(i)))

  val renewalDuringReconciliationReached: bool = state.renewedWhilePending

  val postRenewalReconciliationReached: bool =
    state.renewedWhilePending and reconciledCommitReached

  val takeoverFencedReconciliationReached: bool =
    state.events.indices().exists(
      i => match state.events.nth(i) {
        | CommitAttempted(data) => and {
            data.mode == AmbiguousLanded,
            data.result == LeaseFenced,
            not(data.acknowledged),
            not(data.landed),
            data.exactReferenceWasPublished,
            data.after == data.before,
          }
        | _ => false
      }
    )

  action chooseAcquire: bool = {
    nondet writer = WRITERS.oneOf()
    acquire(writer)
  }

  action choosePublish: bool = {
    nondet writer = WRITERS.oneOf()
    nondet objectId = OBJECTS.oneOf()
    nondet attemptId = ATTEMPTS.oneOf()
    publish(writer, objectId, attemptId)
  }

  action chooseCommit: bool = {
    nondet writer = WRITERS.oneOf()
    nondet objectId = OBJECTS.oneOf()
    nondet attemptId = ATTEMPTS.oneOf()
    nondet mode = Set(Confirmed, AmbiguousLanded, AmbiguousDropped).oneOf()
    commitAttempt(writer, objectId, attemptId, mode)
  }

  action chooseImmediateCommit: bool = {
    nondet writer = WRITERS.oneOf()
    nondet objectId = OBJECTS.oneOf()
    nondet attemptId = ATTEMPTS.oneOf()
    nondet mode = Set(Confirmed, AmbiguousDropped).oneOf()
    commitAttempt(writer, objectId, attemptId, mode)
  }

  action chooseBeginAmbiguousLanded: bool = {
    nondet writer = WRITERS.oneOf()
    nondet objectId = OBJECTS.oneOf()
    nondet attemptId = ATTEMPTS.oneOf()
    beginAmbiguousLanded(writer, objectId, attemptId)
  }

  action chooseRelease: bool = {
    nondet writer = WRITERS.oneOf()
    releaseAttempt(writer)
  }

  // The four-command step preserves the checked-in Hegel ITF vocabulary. The
  // ordinary analysis step below expands ambiguous landing and reconciliation.
  action legacyStep: bool = any {
    chooseAcquire,
    choosePublish,
    chooseCommit,
    chooseRelease,
  }

  action step: bool = any {
    chooseAcquire,
    choosePublish,
    chooseImmediateCommit,
    chooseBeginAmbiguousLanded,
    renewPending,
    reconcilePending,
    chooseRelease,
  }
}

```

### Corrected and single-fault modules

The generic module cannot run until all bounds and mutation switches are bound.
`durableHeadCasCorrected` disables every fault. Each remaining module enables
exactly one fault while keeping the same bounds and transition engine. The test
and model-checking commands therefore compare like with like: a corrected
property must pass while its matching mutant produces a concrete witness or
counterexample.

```quint target/formal/quint/durableHeadCas.qnt +=
module durableHeadCasCorrected {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = false,
    USE_ATTEMPT_REUSE_MUTANT = false,
    USE_REFERENCE_GENERATION_MUTANT = false,
    USE_REFERENCE_SEQUENCE_MUTANT = false,
    USE_COMMIT_ATTEMPT_MUTANT = false,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false,
    USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT = false
  ).*
}

module durableHeadCasStaleMutant {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = true,
    USE_ATTEMPT_REUSE_MUTANT = false,
    USE_REFERENCE_GENERATION_MUTANT = false,
    USE_REFERENCE_SEQUENCE_MUTANT = false,
    USE_COMMIT_ATTEMPT_MUTANT = false,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false,
    USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT = false
  ).*
}

module durableHeadCasGenerationMutant {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = false,
    USE_ATTEMPT_REUSE_MUTANT = false,
    USE_REFERENCE_GENERATION_MUTANT = true,
    USE_REFERENCE_SEQUENCE_MUTANT = false,
    USE_COMMIT_ATTEMPT_MUTANT = false,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false,
    USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT = false
  ).*
}

module durableHeadCasSequenceMutant {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = false,
    USE_ATTEMPT_REUSE_MUTANT = false,
    USE_REFERENCE_GENERATION_MUTANT = false,
    USE_REFERENCE_SEQUENCE_MUTANT = true,
    USE_COMMIT_ATTEMPT_MUTANT = false,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false,
    USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT = false
  ).*
}

module durableHeadCasAttemptReuseMutant {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = false,
    USE_ATTEMPT_REUSE_MUTANT = true,
    USE_REFERENCE_GENERATION_MUTANT = false,
    USE_REFERENCE_SEQUENCE_MUTANT = false,
    USE_COMMIT_ATTEMPT_MUTANT = false,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false,
    USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT = false
  ).*
}

module durableHeadCasCommitAttemptMutant {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = false,
    USE_ATTEMPT_REUSE_MUTANT = false,
    USE_REFERENCE_GENERATION_MUTANT = false,
    USE_REFERENCE_SEQUENCE_MUTANT = false,
    USE_COMMIT_ATTEMPT_MUTANT = true,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false,
    USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT = false
  ).*
}

module durableHeadCasWholeHeadReconciliationMutant {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = false,
    USE_ATTEMPT_REUSE_MUTANT = false,
    USE_REFERENCE_GENERATION_MUTANT = false,
    USE_REFERENCE_SEQUENCE_MUTANT = false,
    USE_COMMIT_ATTEMPT_MUTANT = false,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = true,
    USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT = false
  ).*
}

module durableHeadCasLandingReferenceMutant {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = false,
    USE_ATTEMPT_REUSE_MUTANT = false,
    USE_REFERENCE_GENERATION_MUTANT = false,
    USE_REFERENCE_SEQUENCE_MUTANT = false,
    USE_COMMIT_ATTEMPT_MUTANT = false,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false,
    USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT = true
  ).*
}
```

## Executable boundary and mutation tests

These deterministic traces tangle beside the generated model so relative
imports remain stable. The corrected examples make the expected boundary
behavior concrete: acquire/publish/commit, stale rejection, ambiguous landing
with an intervening renewal, ambiguous drop, and release. Each example checks
both the visible head and the relevant safety predicate, so it is more than a
reachability demonstration.

```quint target/formal/quint/durableHeadCasTest.qnt +=
// Deterministic boundaries and mutation witnesses for durableHeadCas.qnt.

module durableHeadCasCorrectedTest {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = false,
    USE_ATTEMPT_REUSE_MUTANT = false,
    USE_REFERENCE_GENERATION_MUTANT = false,
    USE_REFERENCE_SEQUENCE_MUTANT = false,
    USE_COMMIT_ATTEMPT_MUTANT = false,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false,
    USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT = false
  ).* from "./durableHeadCas"

  run boundaryAcquirePublishCommitTest =
    init
      .then(acquire(Writer1))
      .then(publish(Writer1, Object1, Attempt1))
      .then(commitAttempt(Writer1, Object1, Attempt1, Confirmed))
      .expect(and {
        state.head.generation == 2,
        state.head.owner == HeldBy(Writer1),
        state.head.sequence == 1,
        state.head.reference == SomeReference({
          objectId: Object1, attemptId: Attempt1,
          generation: 2, sequence: 1,
        }),
        state.published.contains({
          objectId: Object1, attemptId: Attempt1,
          generation: 2, sequence: 1,
        }),
        confirmedCommitReached,
        staleWriterCannotChangeHead,
        headReferenceIsCanonical,
        publicationAttemptIsFresh,
        attemptIdentityIsUnique,
        attemptTransitionsRefineExactView,
        exactTransitionsRefineContentView,
      })

  run staleWriterRejectedTest =
    init
      .then(acquire(Writer1))
      .then(acquire(Writer2))
      .then(publish(Writer1, Object2, Attempt1))
      .then(commitAttempt(Writer1, Object2, Attempt1, Confirmed))
      .expect(and {
        state.head.generation == 3,
        state.head.owner == HeldBy(Writer2),
        state.head.sequence == 0,
        state.head.reference == NoReference,
        staleCommitRejectedReached,
        staleWriterCannotChangeHead,
      })

  run ambiguousLandedReconcilesTest =
    init
      .then(acquire(Writer1))
      .then(publish(Writer1, Object1, Attempt1))
      .then(beginAmbiguousLanded(Writer1, Object1, Attempt1))
      .then(renewPending)
      .then(reconcilePending)
      .expect(and {
        state.head.leaseRevision == 2,
        state.head.sequence == 1,
        state.head.reference == SomeReference({
          objectId: Object1, attemptId: Attempt1,
          generation: 2, sequence: 1,
        }),
        reconciledCommitReached,
        renewalDuringReconciliationReached,
        postRenewalReconciliationReached,
        ambiguousLandedUsesOperationSpecificReconciliation,
        acknowledgedCommitIsExact,
      })

  run takeoverFencesPendingReconciliationTest =
    init
      .then(acquire(Writer1))
      .then(publish(Writer1, Object1, Attempt1))
      .then(beginAmbiguousLanded(Writer1, Object1, Attempt1))
      .then(acquire(Writer2))
      .then(reconcilePending)
      .expect(and {
        state.head.generation == 3,
        state.head.owner == HeldBy(Writer2),
        state.head.sequence == 1,
        state.head.reference == SomeReference({
          objectId: Object1, attemptId: Attempt1,
          generation: 2, sequence: 1,
        }),
        state.published.contains({
          objectId: Object1, attemptId: Attempt1,
          generation: 2, sequence: 1,
        }),
        takeoverFencedReconciliationReached,
        staleWriterCannotChangeHead,
        headReferenceWasPublished,
        attemptTransitionsRefineExactView,
        exactTransitionsRefineContentView,
      })

  run ambiguousDroppedDoesNotCommitTest =
    init
      .then(acquire(Writer1))
      .then(publish(Writer1, Object1, Attempt1))
      .then(commitAttempt(Writer1, Object1, Attempt1, AmbiguousDropped))
      .expect(and {
        state.head.sequence == 0,
        state.head.reference == NoReference,
        ambiguousDropReached,
        failedCommitLeavesHeadUnchanged,
      })

  run releasePreservesGenerationTest =
    init
      .then(acquire(Writer1))
      .then(releaseAttempt(Writer1))
      .expect(and {
        state.head.generation == 2,
        state.head.owner == Released,
        state.head.sequence == 0,
        releaseReached,
        releasePreservesGeneration,
      })
}

```

### Deterministic mutation witnesses

The modules below are deliberately expected to exhibit bad behavior. Each uses
the same transition engine and switches on one fault: stale ownership, wrong
reference generation, wrong sequence, attempt reuse, committing a different
attempt, or reconciling by whole-head equality. A witness test passes only when
that named fault is reachable. The bounded verification gate separately
requires the corresponding invariant to produce a counterexample.

These red controls matter because a corrected invariant that also passes after
its fault is injected is probably too weak or unreachable.

```quint target/formal/quint/durableHeadCasTest.qnt +=
module durableHeadCasMutantTest {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = true,
    USE_ATTEMPT_REUSE_MUTANT = false,
    USE_REFERENCE_GENERATION_MUTANT = false,
    USE_REFERENCE_SEQUENCE_MUTANT = false,
    USE_COMMIT_ATTEMPT_MUTANT = false,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false,
    USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT = false
  ).* from "./durableHeadCas"

  // This is a positive witness for the deliberately faulty control: the test
  // passes only when a token made stale by takeover can mutate the head.
  run staleOwnershipMutantWitnessTest =
    init
      .then(acquire(Writer1))
      .then(acquire(Writer2))
      .then(publish(Writer1, Object2, Attempt1))
      .then(commitAttempt(Writer1, Object2, Attempt1, Confirmed))
      .expect(and {
        state.head.generation == 3,
        state.head.owner == HeldBy(Writer2),
        state.head.sequence == 1,
        state.head.reference == SomeReference({
          objectId: Object2, attemptId: Attempt1,
          generation: 2, sequence: 1,
        }),
        not(staleWriterCannotChangeHead),
        headReferenceIsCanonical,
        not(exactTransitionsRefineContentView),
      })
}

module durableHeadCasReferenceMutantTest {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = false,
    USE_ATTEMPT_REUSE_MUTANT = false,
    USE_REFERENCE_GENERATION_MUTANT = true,
    USE_REFERENCE_SEQUENCE_MUTANT = false,
    USE_COMMIT_ATTEMPT_MUTANT = false,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false,
    USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT = false
  ).* from "./durableHeadCas"

  run generationMismatchMutantWitnessTest =
    init
      .then(acquire(Writer1))
      .then(publish(Writer1, Object1, Attempt1))
      .then(commitAttempt(Writer1, Object1, Attempt1, Confirmed))
      .expect(and {
        confirmedCommitReached,
        not(headReferenceIsCanonical),
        not(exactTransitionsRefineContentView),
      })
}

module durableHeadCasSequenceMutantTest {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = false,
    USE_ATTEMPT_REUSE_MUTANT = false,
    USE_REFERENCE_GENERATION_MUTANT = false,
    USE_REFERENCE_SEQUENCE_MUTANT = true,
    USE_COMMIT_ATTEMPT_MUTANT = false,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false,
    USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT = false
  ).* from "./durableHeadCas"

  run sequenceMismatchMutantWitnessTest =
    init
      .then(acquire(Writer1))
      .then(publish(Writer1, Object1, Attempt1))
      .then(commitAttempt(Writer1, Object1, Attempt1, Confirmed))
      .expect(and {
        confirmedCommitReached,
        not(headReferenceIsCanonical),
        not(exactTransitionsRefineContentView),
      })
}

module durableHeadCasAttemptReuseMutantTest {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = false,
    USE_ATTEMPT_REUSE_MUTANT = true,
    USE_REFERENCE_GENERATION_MUTANT = false,
    USE_REFERENCE_SEQUENCE_MUTANT = false,
    USE_COMMIT_ATTEMPT_MUTANT = false,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false,
    USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT = false
  ).* from "./durableHeadCas"

  run attemptReuseMutantWitnessTest =
    init
      .then(acquire(Writer1))
      .then(publish(Writer1, Object1, Attempt1))
      .then(publish(Writer1, Object2, Attempt1))
      .expect(and {
        not(publicationAttemptIsFresh),
        not(attemptIdentityIsUnique),
      })
}

module durableHeadCasCommitAttemptMutantTest {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = false,
    USE_ATTEMPT_REUSE_MUTANT = false,
    USE_REFERENCE_GENERATION_MUTANT = false,
    USE_REFERENCE_SEQUENCE_MUTANT = false,
    USE_COMMIT_ATTEMPT_MUTANT = true,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false,
    USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT = false
  ).* from "./durableHeadCas"

  run wrongCommitAttemptMutantWitnessTest =
    init
      .then(acquire(Writer1))
      .then(publish(Writer1, Object1, Attempt1))
      .then(commitAttempt(Writer1, Object1, Attempt2, Confirmed))
      .expect(and {
        confirmedCommitReached,
        not(headReferenceWasPublished),
        not(acknowledgedCommitIsExact),
        not(attemptTransitionsRefineExactView),
      })
}

module durableHeadCasWholeHeadReconciliationMutantTest {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = false,
    USE_ATTEMPT_REUSE_MUTANT = false,
    USE_REFERENCE_GENERATION_MUTANT = false,
    USE_REFERENCE_SEQUENCE_MUTANT = false,
    USE_COMMIT_ATTEMPT_MUTANT = false,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = true,
    USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT = false
  ).* from "./durableHeadCas"

  run wholeHeadReconciliationMutantWitnessTest =
    init
      .then(acquire(Writer1))
      .then(publish(Writer1, Object1, Attempt1))
      .then(beginAmbiguousLanded(Writer1, Object1, Attempt1))
      .then(renewPending)
      .then(reconcilePending)
      .expect(and {
        state.head.leaseRevision == 2,
        state.head.owner == HeldBy(Writer1),
        state.head.sequence == 1,
        state.head.reference == SomeReference({
          objectId: Object1, attemptId: Attempt1,
          generation: 2, sequence: 1,
        }),
        ambiguousDropReached,
        renewalDuringReconciliationReached,
        not(ambiguousLandedUsesOperationSpecificReconciliation),
      })
}

module durableHeadCasLandingReferenceMutantTest {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = false,
    USE_ATTEMPT_REUSE_MUTANT = false,
    USE_REFERENCE_GENERATION_MUTANT = false,
    USE_REFERENCE_SEQUENCE_MUTANT = false,
    USE_COMMIT_ATTEMPT_MUTANT = false,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false,
    USE_AMBIGUOUS_LANDING_REFERENCE_MUTANT = true
  ).* from "./durableHeadCas"

  run ambiguousLandingReferenceMutantWitnessTest =
    init
      .then(acquire(Writer1))
      .then(publish(Writer1, Object1, Attempt1))
      .then(beginAmbiguousLanded(Writer1, Object1, Attempt1))
      .expect(and {
        state.head.sequence == 1,
        state.head.reference == SomeReference({
          objectId: Object2, attemptId: Attempt1,
          generation: 2, sequence: 1,
        }),
        not(headReferenceWasPublished),
        not(attemptTransitionsRefineExactView),
        not(exactTransitionsRefineContentView),
      })
}
```

## Immutable-create acknowledgement refinement

Remote providers can lose a conditional-create response after the immutable
object landed. This small refinement keeps that provider acknowledgement
separate from head CAS: exact reread evidence is the only path from an
ambiguous acknowledgement to success. A dropped request remains ambiguous,
and a known conflicting object is an integrity failure.

This smaller model has one state variable because it describes a single object
publication attempt, not the lease/head protocol. `PublicationMode` is the
provider outcome, `StoredObject` is what a reread can prove, and
`PublicationResult` is the result visible to the control plane. The mutation
switch changes only the ambiguous-dropped result.

```quint target/formal/quint/durablePublicationAck.qnt +=
module durablePublicationAck {
  type PublicationMode =
    | ConfirmedCreate
    | MatchingPrecondition
    | ConflictingPrecondition
    | AmbiguousLanded
    | AmbiguousDropped

  type StoredObject = Missing | ExactObject | DifferentObject

  type PublicationResult =
    | PublishedResult
    | AlreadyPublishedResult
    | ReconciledResult
    | ObjectUnverifiedResult
    | CommitAmbiguousResult

  type PublicationState = {
    mode: PublicationMode,
    before: StoredObject,
    after: StoredObject,
    result: PublicationResult,
  }

  const USE_DROPPED_AS_RECONCILED_MUTANT: bool

  var publicationState: PublicationState

```

### Publication transition and safety property

`transitionFor` spells out all five provider outcomes. In particular,
`AmbiguousLanded` succeeds only because reread finds the exact object, while
`AmbiguousDropped` remains unprovable. `acknowledgementIsSound` restates that
case split independently as the invariant checked by simulation and Apalache.
The two witnesses ensure both ambiguous paths are reachable.

```quint target/formal/quint/durablePublicationAck.qnt +=
  pure def transitionFor(mode: PublicationMode): PublicationState =
    match mode {
      | ConfirmedCreate => {
          mode: mode, before: Missing, after: ExactObject,
          result: PublishedResult,
        }
      | MatchingPrecondition => {
          mode: mode, before: ExactObject, after: ExactObject,
          result: AlreadyPublishedResult,
        }
      | ConflictingPrecondition => {
          mode: mode, before: DifferentObject, after: DifferentObject,
          result: ObjectUnverifiedResult,
        }
      | AmbiguousLanded => {
          mode: mode, before: Missing, after: ExactObject,
          result: ReconciledResult,
        }
      | AmbiguousDropped => {
          mode: mode, before: Missing, after: Missing,
          result:
            if (USE_DROPPED_AS_RECONCILED_MUTANT) ReconciledResult
            else CommitAmbiguousResult,
        }
    }

  action init: bool =
    publicationState' = transitionFor(ConfirmedCreate)

  action attempt(mode: PublicationMode): bool =
    publicationState' = transitionFor(mode)

  action step: bool = {
    nondet mode = Set(
      ConfirmedCreate,
      MatchingPrecondition,
      ConflictingPrecondition,
      AmbiguousLanded,
      AmbiguousDropped
    ).oneOf()
    attempt(mode)
  }

  pure def acknowledgementIsSound(s: PublicationState): bool =
    match s.mode {
      | ConfirmedCreate =>
          s.before == Missing and s.after == ExactObject
            and s.result == PublishedResult
      | MatchingPrecondition =>
          s.before == ExactObject and s.after == ExactObject
            and s.result == AlreadyPublishedResult
      | ConflictingPrecondition =>
          s.before == DifferentObject and s.after == DifferentObject
            and s.result == ObjectUnverifiedResult
      | AmbiguousLanded =>
          s.before == Missing and s.after == ExactObject
            and s.result == ReconciledResult
      | AmbiguousDropped =>
          s.before == Missing and s.after == Missing
            and s.result == CommitAmbiguousResult
    }

  val publicationAcknowledgementIsSound: bool =
    acknowledgementIsSound(publicationState)

  val ambiguousLandedReached: bool =
    publicationState.mode == AmbiguousLanded

  val ambiguousDroppedReached: bool =
    publicationState.mode == AmbiguousDropped
}

```

### Corrected and acknowledgement-mutant instances

As in the main model, the corrected and mutant modules share all state and
transition definitions. The mutant incorrectly reports a dropped request as
reconciled, which must violate `publicationAcknowledgementIsSound`.

```quint target/formal/quint/durablePublicationAck.qnt +=
module durablePublicationAckCorrected {
  import durablePublicationAck(
    USE_DROPPED_AS_RECONCILED_MUTANT = false
  ).* from "./durablePublicationAck"
}

module durablePublicationAckMutant {
  import durablePublicationAck(
    USE_DROPPED_AS_RECONCILED_MUTANT = true
  ).* from "./durablePublicationAck"
}
```

The corrected tests exercise landed, dropped, matching-existing, and
conflicting-existing objects. The final test is a positive witness for the
single fault: it succeeds only when the mutant violates the soundness property.

```quint target/formal/quint/durablePublicationAckTest.qnt +=
module durablePublicationAckCorrectedTest {
  import durablePublicationAck(
    USE_DROPPED_AS_RECONCILED_MUTANT = false
  ).* from "./durablePublicationAck"

  run ambiguousLandedRequiresExactObjectTest =
    init
      .then(attempt(AmbiguousLanded))
      .expect(and {
        publicationState.after == ExactObject,
        publicationState.result == ReconciledResult,
        publicationAcknowledgementIsSound,
      })

  run ambiguousDroppedStaysUnprovableTest =
    init
      .then(attempt(AmbiguousDropped))
      .expect(and {
        publicationState.after == Missing,
        publicationState.result == CommitAmbiguousResult,
        publicationAcknowledgementIsSound,
      })

  run matchingAndConflictingObjectsStayDistinctTest =
    init
      .then(attempt(MatchingPrecondition))
      .expect(publicationState.result == AlreadyPublishedResult)
      .then(attempt(ConflictingPrecondition))
      .expect(and {
        publicationState.result == ObjectUnverifiedResult,
        publicationAcknowledgementIsSound,
      })
}

```

### Publication mutation witness

Keeping this witness in its own module prevents the intentionally invalid
result from being confused with the corrected examples or selected by their
test command.

```quint target/formal/quint/durablePublicationAckTest.qnt +=
module durablePublicationAckMutantTest {
  import durablePublicationAck(
    USE_DROPPED_AS_RECONCILED_MUTANT = true
  ).* from "./durablePublicationAck"

  run droppedAsReconciledMutantWitnessTest =
    init
      .then(attempt(AmbiguousDropped))
      .expect(not(publicationAcknowledgementIsSound))
}
```

## Parameterized-mutation checkpoint boundary

The head-CAS protocol above does not model the writer's in-process buffering
choice. V1 WAL records contain replayable SQL text, so a mutation with native
bound values cannot safely enter that format. This small companion model makes
the implementation boundary explicit: a bound mutation creates a checkpoint
obligation, later materialized mutations cannot weaken it, a failed flush
retains it, and a successful durability acknowledgement must publish a full
checkpoint covering the current local version.

`boundPending` is a ghost variable: the runtime deliberately retains no bound
values, while the model remembers only that at least one such mutation remains
uncommitted. The mutation control incorrectly routes the obligation through
statement WAL. It must violate the acknowledgement property after one bound
execute and one successful flush.

```quint target/formal/quint/durableWriterBoundary.qnt +=
module durableWriterBoundary {
  type PendingRecovery = Clean | StatementWal | FullCheckpoint
  type Publication = NoPublication | WalPublication | CheckpointPublication
  type Outcome = Idle | Executed | CommitAcknowledged | CommitFailed | Crashed

  type WriterState = {
    localVersion: int,
    committedVersion: int,
    pending: PendingRecovery,
    boundPending: bool,
    priorPending: PendingRecovery,
    priorBoundPending: bool,
    publication: Publication,
    outcome: Outcome,
  }

  const BOUND_MUTATION_USES_WAL_MUTANT: bool
  var writerState: WriterState

  action init: bool =
    writerState' = {
      localVersion: 0,
      committedVersion: 0,
      pending: Clean,
      boundPending: false,
      priorPending: Clean,
      priorBoundPending: false,
      publication: NoPublication,
      outcome: Idle,
    }

  action executeMaterialized: bool = {
    val nextPending =
      if (writerState.pending == FullCheckpoint) FullCheckpoint
      else StatementWal
    writerState' = {
      localVersion: writerState.localVersion + 1,
      committedVersion: writerState.committedVersion,
      pending: nextPending,
      boundPending: writerState.boundPending,
      priorPending: writerState.pending,
      priorBoundPending: writerState.boundPending,
      publication: NoPublication,
      outcome: Executed,
    }
  }

  action executeBound: bool = {
    val nextPending =
      if (BOUND_MUTATION_USES_WAL_MUTANT) StatementWal else FullCheckpoint
    writerState' = {
      localVersion: writerState.localVersion + 1,
      committedVersion: writerState.committedVersion,
      pending: nextPending,
      boundPending: true,
      priorPending: writerState.pending,
      priorBoundPending: writerState.boundPending,
      publication: NoPublication,
      outcome: Executed,
    }
  }

  action flushSuccess: bool = {
    val publication =
      if (writerState.pending == FullCheckpoint) CheckpointPublication
      else if (writerState.pending == StatementWal) WalPublication
      else NoPublication
    writerState' = {
      localVersion: writerState.localVersion,
      committedVersion: writerState.localVersion,
      pending: Clean,
      boundPending: false,
      priorPending: writerState.pending,
      priorBoundPending: writerState.boundPending,
      publication: publication,
      outcome: CommitAcknowledged,
    }
  }

  action flushFailure: bool =
    writerState' = {
      localVersion: writerState.localVersion,
      committedVersion: writerState.committedVersion,
      pending: writerState.pending,
      boundPending: writerState.boundPending,
      priorPending: writerState.pending,
      priorBoundPending: writerState.boundPending,
      publication: NoPublication,
      outcome: CommitFailed,
    }

  action crashAndRecover: bool =
    writerState' = {
      localVersion: writerState.committedVersion,
      committedVersion: writerState.committedVersion,
      pending: Clean,
      boundPending: false,
      priorPending: writerState.pending,
      priorBoundPending: writerState.boundPending,
      publication: NoPublication,
      outcome: Crashed,
    }

  action step: bool = any {
    executeMaterialized,
    executeBound,
    flushSuccess,
    flushFailure,
    crashAndRecover,
  }

  val versionsAreOrdered: bool =
    writerState.committedVersion <= writerState.localVersion

  val checkpointFallbackIsSound: bool =
    if (writerState.outcome == CommitAcknowledged
        and writerState.priorBoundPending)
      writerState.publication == CheckpointPublication
        and writerState.committedVersion == writerState.localVersion
    else true

  val failedFlushRetainsRecoveryObligation: bool =
    if (writerState.outcome == CommitFailed)
      writerState.pending == writerState.priorPending
        and writerState.boundPending == writerState.priorBoundPending
    else true

  val boundExecuteReached: bool = writerState.boundPending
  val checkpointCommitReached: bool =
    writerState.outcome == CommitAcknowledged
      and writerState.publication == CheckpointPublication
}

module durableWriterBoundaryCorrected {
  import durableWriterBoundary(
    BOUND_MUTATION_USES_WAL_MUTANT = false
  ).* from "./durableWriterBoundary"
}

module durableWriterBoundaryMutant {
  import durableWriterBoundary(
    BOUND_MUTATION_USES_WAL_MUTANT = true
  ).* from "./durableWriterBoundary"
}
```

The executable examples cover the mixed mutation order, retained obligation on
failure, crash-before-ack semantics, and the mutation witness.

```quint target/formal/quint/durableWriterBoundaryTest.qnt +=
module durableWriterBoundaryCorrectedTest {
  import durableWriterBoundary(
    BOUND_MUTATION_USES_WAL_MUTANT = false
  ).* from "./durableWriterBoundary"

  run mixedMutationsCommitThroughCheckpointTest =
    init
      .then(executeMaterialized)
      .then(executeBound)
      .then(executeMaterialized)
      .then(flushSuccess)
      .expect(and {
        writerState.committedVersion == 3,
        writerState.pending == Clean,
        writerState.publication == CheckpointPublication,
        checkpointFallbackIsSound,
      })

  run failedFlushRetainsCheckpointRequirementTest =
    init
      .then(executeBound)
      .then(flushFailure)
      .expect(and {
        writerState.pending == FullCheckpoint,
        writerState.boundPending,
        failedFlushRetainsRecoveryObligation,
      })

  run crashBeforeAcknowledgementRecoversCommittedVersionTest =
    init
      .then(executeBound)
      .then(crashAndRecover)
      .expect(and {
        writerState.localVersion == 0,
        writerState.committedVersion == 0,
        writerState.pending == Clean,
        versionsAreOrdered,
      })
}

module durableWriterBoundaryMutantTest {
  import durableWriterBoundary(
    BOUND_MUTATION_USES_WAL_MUTANT = true
  ).* from "./durableWriterBoundary"

  run boundMutationThroughWalWitnessTest =
    init
      .then(executeBound)
      .then(flushSuccess)
      .expect(not(checkpointFallbackIsSound))
}
```

## Lease time dimension boundary

The head-CAS state machine intentionally abstracts elapsed time, so it cannot
justify a wire-unit claim. This small, separate functional model makes the
binding conversion explicit. Public wall-clock values are integer epoch
milliseconds; the V1 wire value is epoch seconds represented here as whole
seconds plus a millisecond fraction. Nominal unit tags prevent a value from
silently crossing the boundary unchanged. The wrong-unit function is retained
only as a red control.

```quint target/formal/quint/durableLeaseTime.qnt +=
module durableLeaseTime {
  type EpochUnit = EpochSeconds | EpochMilliseconds

  type WireEpoch = {
    wholeSeconds: int,
    millisecondFraction: int,
    unit: EpochUnit,
  }

  pure def epochMillisToWire(epochMillis: int): WireEpoch = {
    wholeSeconds: epochMillis / 1000,
    millisecondFraction: epochMillis % 1000,
    unit: EpochSeconds,
  }

  pure def wireToEpochMillis(wire: WireEpoch): int =
    wire.wholeSeconds * 1000 + wire.millisecondFraction

  pure def wrongUnitMutant(epochMillis: int): WireEpoch = {
    wholeSeconds: epochMillis,
    millisecondFraction: 0,
    unit: EpochMilliseconds,
  }

  pure def wellDimensioned(wire: WireEpoch): bool = and {
    wire.unit == EpochSeconds,
    wire.millisecondFraction >= 0,
    wire.millisecondFraction < 1000,
  }
}
```

```quint target/formal/quint/durableLeaseTimeTest.qnt +=
module durableLeaseTimeTest {
  import durableLeaseTime.* from "./durableLeaseTime"

  run fractionalEpochRoundTripsTest = {
    val epochMillis = 1788230400125
    val wire = epochMillisToWire(epochMillis)
    and {
      wellDimensioned(wire),
      wire.wholeSeconds == 1788230400,
      wire.millisecondFraction == 125,
      wireToEpochMillis(wire) == epochMillis,
    }
  }

  run zeroEpochRoundTripsTest = {
    val wire = epochMillisToWire(0)
    and {
      wellDimensioned(wire),
      wireToEpochMillis(wire) == 0,
    }
  }

  run wrongUnitMutantIsDetectedTest =
    not(wellDimensioned(wrongUnitMutant(1788230400125)))
}
```
