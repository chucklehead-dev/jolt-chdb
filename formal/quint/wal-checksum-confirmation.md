# WAL checksum confirmation

This is an adjacent bounded model, not a replacement for the head-CAS model.
The approved sketch is `evidence/chdb-s3-checksum-confirmation-type-sketch-20261016.md`
in the workspace (approved 2026-10-16). It adds an explicit provider trust premise:
a qualified single-PUT provider validates the supplied whole-object SHA256.
It does not prove that premise, HTTP parsing, hashing or actual S3 behavior.

Two writers coordinate through a shared linearizable object/head store. Pending
storage completions are separate from local state: creation, observing its
response, validating evidence and head CAS are not one atomic operation.
Files, hashes, owners and keys are finite symbolic identities. No timing,
retention, Byzantine-provider, native database, or performance claim follows.

Edit this Markdown; `lmt` extracts ignored generated files below `target/`.

```quint target/formal/quint/walChecksumConfirmation.qnt +=
module walChecksumProtocol {
  type Phase = Idle | Selected | Uploaded | Observed | Verified | Ready | Finished
  type Outcome = Unknown | Created | Exists | Ambiguous | Rejected
  type Checksum = Matching | Missing | Conflicting | Composite
  type Proof = NoProof | ReadbackProof | ChecksumProof
  type Reference = { store: int, key: int, size: int, digest: int }
  type Attempt = { id: int, writer: int, generation: int, reference: Reference }
  type Receipt = {
    attempt: Attempt, outcome: Outcome, qualified: bool, checksum: Checksum,
  }
  type LocalState = {
    phase: Phase, attempt: Attempt, response: Receipt, proof: Proof,
    proofBinding: Attempt, expectedRevision: int,
  }
  type ObjectState = { present: bool, reference: Reference }
  type HeadState = { owner: int, generation: int, revision: int }
  type Commit = { local: LocalState, authority: HeadState }
  const TRUST_UNQUALIFIED_MUTANT: bool
  const IGNORE_BINDING_MUTANT: bool
  const SKIP_FENCING_MUTANT: bool

  pure val WRITERS = Set(0, 1)
  pure val KEYS = Set(0, 1)
  pure def reference(key: int): Reference =
    { store: 0, key: key, size: 1, digest: 1 }
  pure def initialAttempt(writer: int): Attempt =
    { id: writer, writer: writer, generation: 1, reference: reference(writer) }
  pure def emptyReceipt(writer: int): Receipt =
    { attempt: initialAttempt(writer), outcome: Unknown,
      qualified: false, checksum: Missing }
  pure def initialLocal(writer: int): LocalState = {
    phase: Idle, attempt: initialAttempt(writer), response: emptyReceipt(writer),
    proof: NoProof, proofBinding: initialAttempt(writer), expectedRevision: 0,
  }

  var writers: int -> LocalState
  var objects: int -> ObjectState
  var pending: int -> Receipt
  var currentHead: HeadState
  var events: Set[str]
  var commits: Set[Commit]

  action init = all {
    writers' = WRITERS.mapBy(w => initialLocal(w)),
    objects' = KEYS.mapBy(k => { present: false, reference: reference(k) }),
    pending' = WRITERS.mapBy(w => emptyReceipt(w)),
    currentHead' = { owner: 0, generation: 1, revision: 0 },
    events' = Set(),
    commits' = Set(),
  }

  pure def canSelect(local: LocalState, current: HeadState, writer: int): bool =
    local.phase == Idle and current.owner == writer
  pure def selected(local: LocalState, current: HeadState, key: int): LocalState =
    { ...local, phase: Selected,
      attempt: { ...local.attempt, generation: current.generation,
                 reference: reference(key) } }
  action selectAttempt(writer: int, key: int): bool = all {
    canSelect(writers.get(writer), currentHead, writer),
    writers' = writers.set(writer, selected(writers.get(writer), currentHead, key)),
    objects' = objects, pending' = pending, currentHead' = currentHead, commits' = commits,
    events' = events.union(Set("selected")),
  }

  pure def canUpload(local: LocalState): bool = local.phase == Selected
  pure def uploadOutcome(existing: ObjectState, qualified: bool,
                         bodySize: int, digest: int, ambiguous: bool): Outcome =
    if (qualified and (bodySize != 1 or digest != 1)) Rejected
    else if (existing.present) Exists
    else if (ambiguous) Ambiguous else Created
  pure def storedObject(existing: ObjectState, ref: Reference,
                        outcome: Outcome, bodySize: int, digest: int): ObjectState =
    if (outcome == Created or outcome == Ambiguous)
      { present: true, reference: { ...ref, size: bodySize, digest: digest } }
    else existing

  action upload(writer: int, qualified: bool, checksum: Checksum,
                bodySize: int, digest: int, ambiguous: bool): bool = {
    val local = writers.get(writer)
    val ref = local.attempt.reference
    val existing = objects.get(ref.key)
    val outcome = uploadOutcome(existing, qualified, bodySize, digest, ambiguous)
    all {
      canUpload(local),
      writers' = writers.set(writer, { ...local, phase: Uploaded }),
      objects' = objects.set(ref.key, storedObject(existing, ref, outcome, bodySize, digest)),
      pending' = pending.set(writer, { attempt: local.attempt, outcome: outcome,
                                     qualified: qualified, checksum: checksum }),
      currentHead' = currentHead,
      commits' = commits,
      events' = events.union(Set("uploaded")),
    }
  }

  pure def canObserve(local: LocalState, receipt: Receipt): bool =
    local.phase == Uploaded and receipt.outcome != Unknown
  pure def observed(local: LocalState, receipt: Receipt): LocalState =
    { ...local, phase: Observed, response: receipt }
  action observeResponse(writer: int, source: int): bool = all {
    canObserve(writers.get(writer), pending.get(source)),
    writers' = writers.set(writer, observed(writers.get(writer), pending.get(source))),
    objects' = objects, pending' = pending, currentHead' = currentHead, commits' = commits,
    events' = events.union(Set("observed")),
  }

  pure def objectMatches(table: int -> ObjectState, ref: Reference): bool =
    table.get(ref.key).present and table.get(ref.key).reference == ref
  pure def boundResponse(local: LocalState): bool =
    local.response.attempt == local.attempt
  pure def checksumEligible(local: LocalState): bool = and {
    local.response.outcome == Created,
    local.response.qualified or TRUST_UNQUALIFIED_MUTANT,
    local.response.checksum == Matching,
    boundResponse(local) or IGNORE_BINDING_MUTANT,
  }
  pure def canConfirm(local: LocalState): bool =
    local.phase == Observed and checksumEligible(local)
  pure def confirmed(local: LocalState): LocalState = {
    ...local, phase: Verified, proof: ChecksumProof,
    proofBinding: local.response.attempt,
  }
  action confirmChecksum(writer: int): bool = all {
    canConfirm(writers.get(writer)),
    writers' = writers.set(writer, confirmed(writers.get(writer))),
    objects' = objects, pending' = pending, currentHead' = currentHead, commits' = commits,
    events' = events.union(Set("checksum")),
  }

  pure def needsReadback(local: LocalState): bool = and {
    local.phase == Observed, boundResponse(local),
    Set(Created, Exists, Ambiguous).contains(local.response.outcome),
    local.response.checksum != Conflicting,
    not(checksumEligible(local)),
  }
  pure def readbackVerified(local: LocalState): LocalState =
    { ...local, phase: Verified, proof: ReadbackProof, proofBinding: local.attempt }
  action verifyReadback(writer: int): bool = {
    val local = writers.get(writer)
    all {
      needsReadback(local), objectMatches(objects, local.attempt.reference),
      writers' = writers.set(writer, readbackVerified(local)),
      objects' = objects, pending' = pending, currentHead' = currentHead, commits' = commits,
      events' = events.union(Set("readback")).union(
        if (local.response.outcome == Exists) Set("existing")
        else if (local.response.outcome == Ambiguous) Set("ambiguous")
        else Set("fallback")),
    }
  }
  pure def canReject(local: LocalState, table: int -> ObjectState): bool =
    local.phase == Observed and or {
      not(boundResponse(local)) and not(IGNORE_BINDING_MUTANT),
      local.response.outcome == Rejected,
      local.response.checksum == Conflicting,
      needsReadback(local) and not(objectMatches(table, local.attempt.reference)),
    }
  action rejectEvidence(writer: int): bool = all {
    canReject(writers.get(writer), objects),
    writers' = writers.set(writer, { ...writers.get(writer), phase: Finished }),
    objects' = objects, pending' = pending, currentHead' = currentHead, commits' = commits,
    events' = events.union(Set("rejected")),
  }

  pure def owns(local: LocalState, current: HeadState): bool =
    local.attempt.writer == current.owner and
      local.attempt.generation == current.generation
  pure def canPrepare(local: LocalState, current: HeadState): bool =
    local.phase == Verified and (owns(local, current) or SKIP_FENCING_MUTANT)
  pure def prepared(local: LocalState, current: HeadState): LocalState =
    { ...local, phase: Ready, expectedRevision: current.revision }
  action prepareCommit(writer: int): bool = all {
    canPrepare(writers.get(writer), currentHead),
    writers' = writers.set(writer, prepared(writers.get(writer), currentHead)),
    objects' = objects, pending' = pending, currentHead' = currentHead, commits' = commits,
    events' = events.union(Set("prepared")),
  }
  pure def canCommit(local: LocalState, current: HeadState): bool = and {
    local.phase == Ready, local.expectedRevision == current.revision,
    owns(local, current) or SKIP_FENCING_MUTANT,
  }
  action commitReference(writer: int): bool = {
    val local = writers.get(writer)
    all {
      canCommit(local, currentHead),
      writers' = writers.set(writer, { ...local, phase: Finished }),
      objects' = objects, pending' = pending,
      currentHead' = { ...currentHead, revision: currentHead.revision + 1 },
      commits' = commits.union(Set({ local: local, authority: currentHead })),
      events' = events.union(Set("committed")),
    }
  }
  pure def canConflict(local: LocalState, current: HeadState): bool =
    local.phase == Ready and local.expectedRevision != current.revision
  action casConflict(writer: int): bool = all {
    canConflict(writers.get(writer), currentHead),
    writers' = writers.set(writer, { ...writers.get(writer), phase: Verified }),
    objects' = objects, pending' = pending, currentHead' = currentHead, commits' = commits,
    events' = events.union(Set("conflict")),
  }
  pure def canFence(local: LocalState, current: HeadState): bool =
    Set(Verified, Ready).contains(local.phase) and not(owns(local, current))
  action rejectStaleOwner(writer: int): bool = all {
    canFence(writers.get(writer), currentHead),
    writers' = writers.set(writer, { ...writers.get(writer), phase: Finished }),
    objects' = objects, pending' = pending, currentHead' = currentHead, commits' = commits,
    events' = events.union(Set("fenced")),
  }
  pure def canTakeover(current: HeadState): bool = current.generation == 1
  action takeover = all {
    canTakeover(currentHead),
    currentHead' = { owner: 1, generation: 2, revision: currentHead.revision + 1 },
    writers' = writers, objects' = objects, pending' = pending, commits' = commits,
    events' = events.union(Set("takeover")).union(
      if (writers.get(0).phase == Uploaded) Set("takeoverBeforeResponse") else Set()),
  }
  pure def canRenew(current: HeadState): bool = current.revision < 6
  action renew = all {
    canRenew(currentHead),
    currentHead' = { ...currentHead, revision: currentHead.revision + 1 },
    writers' = writers, objects' = objects, pending' = pending, commits' = commits,
    events' = events.union(Set("renewed")),
  }

  action step = {
    nondet writer = WRITERS.oneOf()
    nondet key = KEYS.oneOf()
    nondet qualified = Set(false, true).oneOf()
    nondet checksum = Set(Matching, Missing, Conflicting, Composite).oneOf()
    nondet bodySize = Set(1, 2).oneOf()
    nondet digest = Set(1, 2).oneOf()
    nondet ambiguous = Set(false, true).oneOf()
    nondet source = WRITERS.oneOf()
    any {
      selectAttempt(writer, key),
      upload(writer, qualified, checksum, bodySize, digest, ambiguous),
      observeResponse(writer, source),
      confirmChecksum(writer), verifyReadback(writer), rejectEvidence(writer),
      prepareCommit(writer), commitReference(writer), casConflict(writer),
      rejectStaleOwner(writer), takeover, renew,
    }
  }
  val selectionReached = events.contains("selected")
  val uploadReached = events.contains("uploaded")
  val responseReached = events.contains("observed")
  val checksumReached = events.contains("checksum")
  val readbackReached = events.contains("readback")
  val fallbackReached = events.contains("fallback")
  val existingReached = events.contains("existing")
  val ambiguousReached = events.contains("ambiguous")
  val rejectionReached = events.contains("rejected")
  val prepareReached = events.contains("prepared")
  val commitReached = events.contains("committed")
  val conflictReached = events.contains("conflict")
  val fencedReached = events.contains("fenced")
  val takeoverReached = events.contains("takeover")
  val takeoverBeforeResponseReached = events.contains("takeoverBeforeResponse")
  val renewalReached = events.contains("renewed")

  pure def soundEvidence(local: LocalState, table: int -> ObjectState): bool = and {
    local.proof != NoProof, local.proofBinding == local.attempt,
    objectMatches(table, local.attempt.reference),
    local.proof == ChecksumProof implies and {
      local.response.qualified, local.response.outcome == Created,
      local.response.checksum == Matching, boundResponse(local),
    },
  }
  val evidenceIsSound = WRITERS.forall(w =>
    (writers.get(w).proof != NoProof) implies soundEvidence(writers.get(w), objects))
  val commitsAreSound = commits.forall(c => and {
    soundEvidence(c.local, objects), owns(c.local, c.authority),
    c.local.expectedRevision == c.authority.revision,
  })
}

module walChecksumConfirmation {
  import walChecksumProtocol(TRUST_UNQUALIFIED_MUTANT = false,
    IGNORE_BINDING_MUTANT = false, SKIP_FENCING_MUTANT = false).*
}
```

## Deterministic controls

The bad-provider variant trusts an unqualified response, the binding variant
accepts another attempt's response, and the fencing variant allows a previous
owner to commit. Their tests must reach an actual false safety predicate—not
just call a different branch or observe an expected error string.

```quint target/formal/quint/walChecksumConfirmationTest.qnt +=
module walChecksumConfirmationTest {
  import walChecksumProtocol(TRUST_UNQUALIFIED_MUTANT = false,
    IGNORE_BINDING_MUTANT = false, SKIP_FENCING_MUTANT = false).*
    from "./walChecksumConfirmation"

  run qualifiedCommitTest = init.then(selectAttempt(0, 0))
    .then(upload(0, true, Matching, 1, 1, false))
    .then(observeResponse(0, 0)).then(confirmChecksum(0))
    .expect(evidenceIsSound and checksumReached and not(readbackReached))
    .then(prepareCommit(0)).then(commitReference(0))
    .expect(commitsAreSound and commits.size() == 1)

  run genericFallbackTest = init.then(selectAttempt(0, 0))
    .then(upload(0, false, Matching, 1, 1, false))
    .then(observeResponse(0, 0)).expect(not(canConfirm(writers.get(0))))
    .then(verifyReadback(0)).expect(fallbackReached and evidenceIsSound)

  run missingChecksumTest = init.then(selectAttempt(0, 0))
    .then(upload(0, true, Missing, 1, 1, false))
    .then(observeResponse(0, 0)).expect(not(canConfirm(writers.get(0))))
    .then(verifyReadback(0)).expect(fallbackReached and evidenceIsSound)

  run compositeChecksumTest = init.then(selectAttempt(0, 0))
    .then(upload(0, true, Composite, 1, 1, false))
    .then(observeResponse(0, 0)).expect(not(canConfirm(writers.get(0))))
    .then(verifyReadback(0)).expect(fallbackReached and evidenceIsSound)

  run conflictingChecksumTest = init.then(selectAttempt(0, 0))
    .then(upload(0, true, Conflicting, 1, 1, false))
    .then(observeResponse(0, 0)).expect(not(canConfirm(writers.get(0))))
    .expect(not(needsReadback(writers.get(0)))).then(rejectEvidence(0))
    .expect(rejectionReached and not(checksumReached) and commitsAreSound)

  run ambiguousRequiresReadbackTest = init.then(selectAttempt(0, 0))
    .then(upload(0, true, Matching, 1, 1, true))
    .then(observeResponse(0, 0)).expect(not(canConfirm(writers.get(0))))
    .then(verifyReadback(0)).expect(ambiguousReached and evidenceIsSound)

  run existingRequiresReadbackTest = init.then(selectAttempt(0, 0))
    .then(upload(0, true, Matching, 1, 1, false)).then(takeover)
    .then(selectAttempt(1, 0)).then(upload(1, true, Matching, 1, 1, false))
    .then(observeResponse(1, 1)).expect(not(canConfirm(writers.get(1))))
    .then(verifyReadback(1)).expect(existingReached and evidenceIsSound)

  run changedBodyRejectedTest = init.then(selectAttempt(0, 0))
    .then(upload(0, true, Matching, 1, 2, false))
    .then(observeResponse(0, 0)).expect(not(canConfirm(writers.get(0))))
    .then(rejectEvidence(0)).expect(not(objects.get(0).present) and rejectionReached)

  run wrongSizeRejectedTest = init.then(selectAttempt(0, 0))
    .then(upload(0, true, Matching, 2, 1, false))
    .then(observeResponse(0, 0)).expect(not(canConfirm(writers.get(0))))
    .then(rejectEvidence(0)).expect(not(objects.get(0).present) and rejectionReached)

  run genericCorruptionRejectedTest = init.then(selectAttempt(0, 0))
    .then(upload(0, false, Matching, 1, 2, false))
    .then(observeResponse(0, 0)).expect(not(canConfirm(writers.get(0))))
    .expect(not(objectMatches(objects, writers.get(0).attempt.reference)))
    .then(rejectEvidence(0))
    .expect(rejectionReached and commits.size() == 0)

  run lateResponseFencedTest = init.then(selectAttempt(0, 0))
    .then(upload(0, true, Matching, 1, 1, false)).then(takeover)
    .then(observeResponse(0, 0)).then(confirmChecksum(0))
    .expect(not(canPrepare(writers.get(0), currentHead))).then(rejectStaleOwner(0))
    .expect(takeoverBeforeResponseReached and fencedReached and commitsAreSound)

  run takeoverAfterPrepareTest = init.then(selectAttempt(0, 0))
    .then(upload(0, true, Matching, 1, 1, false))
    .then(observeResponse(0, 0)).then(confirmChecksum(0)).then(prepareCommit(0))
    .then(takeover).expect(not(canCommit(writers.get(0), currentHead)))
    .then(rejectStaleOwner(0))
    .expect(fencedReached and commits.size() == 0)

  run renewalConflictRetryTest = init.then(selectAttempt(0, 0))
    .then(upload(0, true, Matching, 1, 1, false))
    .then(observeResponse(0, 0)).then(confirmChecksum(0)).then(prepareCommit(0))
    .then(renew).expect(not(canCommit(writers.get(0), currentHead))).then(casConflict(0))
    .then(prepareCommit(0)).then(commitReference(0))
    .expect(conflictReached and commitsAreSound and commits.size() == 1)

  run misassociatedResponseRejectedTest = init.then(selectAttempt(0, 0))
    .then(upload(0, true, Matching, 1, 1, false)).then(takeover)
    .then(selectAttempt(1, 1)).then(upload(1, true, Matching, 1, 1, false))
    .then(observeResponse(1, 0)).expect(not(canConfirm(writers.get(1))))
    .expect(not(needsReadback(writers.get(1)))).then(rejectEvidence(1))
    .expect(rejectionReached and evidenceIsSound)

  run exactBindingTest = {
    val a = initialAttempt(0)
    val r = a.reference
    val good = { ...initialLocal(0), phase: Observed,
      response: { attempt: a, outcome: Created, qualified: true, checksum: Matching } }
    all {
      canConfirm(good),
      not(canConfirm({ ...good, attempt: { ...a, id: 99 } })),
      not(canConfirm({ ...good, attempt: { ...a, writer: 1 } })),
      not(canConfirm({ ...good, attempt: { ...a, generation: 2 } })),
      not(canConfirm({ ...good, attempt: { ...a, reference: { ...r, store: 1 } } })),
      not(canConfirm({ ...good, attempt: { ...a, reference: { ...r, key: 1 } } })),
      not(canConfirm({ ...good, attempt: { ...a, reference: { ...r, size: 2 } } })),
      not(canConfirm({ ...good, attempt: { ...a, reference: { ...r, digest: 2 } } })),
    }
  }
}

module walChecksumUnqualifiedMutantTest {
  import walChecksumProtocol(TRUST_UNQUALIFIED_MUTANT = true,
    IGNORE_BINDING_MUTANT = false, SKIP_FENCING_MUTANT = false).*
    from "./walChecksumConfirmation"
  run unqualifiedReceiptBreaksSafetyTest = init.then(selectAttempt(0, 0))
    .then(upload(0, false, Matching, 1, 2, false))
    .then(observeResponse(0, 0)).then(confirmChecksum(0))
    .expect(not(evidenceIsSound))
    .then(prepareCommit(0)).then(commitReference(0)).expect(not(commitsAreSound))
}

module walChecksumBindingMutantTest {
  import walChecksumProtocol(TRUST_UNQUALIFIED_MUTANT = false,
    IGNORE_BINDING_MUTANT = true, SKIP_FENCING_MUTANT = false).*
    from "./walChecksumConfirmation"
  run reusedReceiptBreaksSafetyTest = init.then(selectAttempt(0, 0))
    .then(upload(0, true, Matching, 1, 1, false)).then(takeover)
    .then(selectAttempt(1, 1)).then(upload(1, true, Matching, 1, 1, false))
    .then(observeResponse(1, 0)).then(confirmChecksum(1))
    .expect(not(evidenceIsSound))
    .then(prepareCommit(1)).then(commitReference(1)).expect(not(commitsAreSound))
}

module walChecksumFencingMutantTest {
  import walChecksumProtocol(TRUST_UNQUALIFIED_MUTANT = false,
    IGNORE_BINDING_MUTANT = false, SKIP_FENCING_MUTANT = true).*
    from "./walChecksumConfirmation"
  run staleOwnerBreaksSafetyTest = init.then(selectAttempt(0, 0))
    .then(upload(0, true, Matching, 1, 1, false)).then(takeover)
    .then(observeResponse(0, 0)).then(confirmChecksum(0))
    .then(prepareCommit(0)).then(commitReference(0)).expect(not(commitsAreSound))
}
```
