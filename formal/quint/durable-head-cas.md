# Durable head-CAS Quint model

This document is the authoritative executable, literate companion to the bounded SMT
models in `formal/durable-head-cas*.smt2`. It models two writers coordinating
through one shared, linearizable `head.json` CAS register. It uses plain Quint,
not Choreo, because the writers do not exchange protocol messages.
The `lmt` tangler extracts its Quint fences into ignored files under
`target/formal/quint/`; generated `.qnt` files are never edited directly.

## Authority and scope

The normative source is chDB commit
`db10b548a3e1e21e51c213baf863cb1050963d9c`,
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
model retains that identity in exact references and checks two explicit
refinements: erasing the attempt yields the sequenced-reference view, and
erasing generation and sequence yields the content-only view. The ITF driver
maps every finite attempt to exactly one runtime UUID-bearing reference.

The model deliberately excludes wall-clock time and heartbeat scheduling, but
includes an abstract lease revision so a same-owner renewal can occur between
an ambiguous CAS landing and its reconciliation read. It excludes retry/backoff,
generation exhaustion, full JSON/ETag encoding, WAL versus checkpoint manifest
shape, scratch recovery, close, and process lifecycle. It establishes
no liveness claim.

## Executable structure

- The generated `durableHeadCas.qnt` owns the types, pure transition functions,
  the split ambiguous-land/renew/reconcile state machine, witnesses, and safety
  invariants.
- The generated `durableHeadCasTest.qnt` owns deterministic corrected boundaries
  and all mutation witnesses.
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
reconciliation, and both per-transition refinements through six transitions.
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

The following block tangles to the generated model consumed by the check script.
Do not edit the generated file; edit this document and rerun the script.

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

  pure val WRITERS: Set[WriterId] = Set(Writer1, Writer2)
  pure val OBJECTS: Set[ObjectId] = Set(Object1, Object2)
  pure val ATTEMPTS: Set[AttemptId] =
    Set(Attempt1, Attempt2, Attempt3, Attempt4, Attempt5, Attempt6)

  var state: State

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

  pure def applyBeginAmbiguousLanded(
    s: State,
    writer: WriterId,
    objectId: ObjectId,
    attemptId: AttemptId
  ): State = {
    val evaluated = evaluateCommit(
      s, writer, objectId, attemptId, AmbiguousLanded
    )
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
      head: evaluated.after,
      pendingReconciliation: AwaitingReconciliation(pending),
      lastTransitionRefinesExactView: true,
      lastTransitionRefinesContentView: true,
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
      landed: true,
      acknowledged: accepted,
      result: result,
      desiredHead: pending.desiredHead,
      operationSpecificProofHeld: operationProof,
      wholeHeadProofHeld: wholeHeadProof,
      before: pending.casBefore,
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
    state.pendingReconciliation == NoPendingReconciliation,
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
              and data.landed
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

module durableHeadCasCorrected {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = false,
    USE_ATTEMPT_REUSE_MUTANT = false,
    USE_REFERENCE_GENERATION_MUTANT = false,
    USE_REFERENCE_SEQUENCE_MUTANT = false,
    USE_COMMIT_ATTEMPT_MUTANT = false,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false
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
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false
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
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false
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
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false
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
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false
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
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false
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
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = true
  ).*
}
```

## Executable boundary and mutation tests

These deterministic traces tangle beside the generated model so relative imports
remain stable.

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
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false
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

module durableHeadCasMutantTest {
  import durableHeadCas(
    MAX_GENERATION = 7,
    MAX_SEQUENCE = 6,
    USE_STALE_OWNERSHIP_MUTANT = true,
    USE_ATTEMPT_REUSE_MUTANT = false,
    USE_REFERENCE_GENERATION_MUTANT = false,
    USE_REFERENCE_SEQUENCE_MUTANT = false,
    USE_COMMIT_ATTEMPT_MUTANT = false,
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false
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
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false
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
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false
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
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false
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
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = false
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
    USE_WHOLE_HEAD_RECONCILIATION_MUTANT = true
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
```
