# Buffered Durable publication boundary

This is a bounded, executable model of the approved admission-only contract in
[`buffered-publication-type-sketch.md`](buffered-publication-type-sketch.md).
It does not change the existing confirmed `execute-and-flush!` contract.

The two callers share one live writer and a capacity-two FIFO queue. Request
IDs `1..3` denote *successful* admission-order slots, not globally durable
identities; `callerOf` records which caller owns each slot. A full-queue
rejection increments a bounded rejection count without allocating an ID. A
later admission may therefore receive the next ordinal; this is not reuse of
a rejected request identity. There is one worker, one lease
owner, one abstract immutable store/head, and one recovered reader. Time,
native bytes, retries/backoff, S3 transport, and throughput are outside this
model. A sampled `quint run` is evidence about this finite abstraction, not an
exhaustive proof or a runtime qualification.

Generated `.qnt` files are extracted by pinned `lmt` into `target/formal/quint`;
edit this Markdown, never the generated file.

```quint target/formal/quint/bufferedPublication.qnt +=
module bufferedPublication {
  type WorkPhase = Idle | Preparing | Applied
  type PubPhase = NoAttempt | Selected | Immutable | Verified | Ambiguous | Confirmed | PubFailed

  const EARLY_SETTLE_MUTANT: bool
  const STALE_CAS_MUTANT: bool
  const EARLY_READER_MUTANT: bool
  const DROP_PENDING_MUTANT: bool
  const BROAD_CHECKPOINT_MUTANT: bool

  type State = {
    open: bool,
    closeRequested: bool,
    flushRequested: bool,
    closed: bool,
    queue: List[int],
    order: List[int],
    callerOf: int -> int,
    active: int,
    workPhase: WorkPhase,
    admitted: Set[int],
    fullQueueRejections: int,
    admissionReturns: Set[int],
    prepared: Set[int],
    applied: Set[int],
    staged: Set[int],
    checkpointNeeded: Set[int],
    checkpointPrepared: Set[int],
    obligation: Set[int],
    selected: Set[int],
    attempt: int,
    attemptSeq: int,
    attemptGen: int,
    pubPhase: PubPhase,
    immutable: Set[int],
    headRef: int,
    headSeq: int,
    headGen: int,
    headCovered: Set[int],
    settled: Set[int],
    reader: Set[int],
    cleaned: Set[int],
    cleanupDenied: Set[int],
    leaseGen: int,
    owned: bool,
    commitWasOwned: bool,
    reconciled: bool,
    renewals: int,
    heartbeatJoined: bool,
    leaseReleased: bool,
  }

  var s: State

  action init: bool =
    s' = {
      open: true, closeRequested: false, flushRequested: false, closed: false,
      queue: List(), order: List(), callerOf: Map(1 -> 0, 2 -> 0, 3 -> 0),
      active: 0, workPhase: Idle,
      admitted: Set(), fullQueueRejections: 0, admissionReturns: Set(),
      prepared: Set(), applied: Set(), staged: Set(),
      checkpointNeeded: Set(), checkpointPrepared: Set(),
      obligation: Set(), selected: Set(),
      attempt: 0, attemptSeq: 0, attemptGen: 0, pubPhase: NoAttempt,
      immutable: Set(), headRef: 0, headSeq: 0, headGen: 1,
      headCovered: Set(), settled: Set(), reader: Set(), cleaned: Set(),
      cleanupDenied: Set(),
      leaseGen: 1, owned: true, commitWasOwned: true, reconciled: false,
      renewals: 0,
      heartbeatJoined: false, leaseReleased: false,
    }

  pure def canAdmit(st: State, caller: int): bool =
    st.open and caller >= 1 and caller <= 2 and
    st.order.length() < 3 and st.queue.length() < 2

  pure def applyAdmit(st: State, caller: int): State = {
    val id = st.order.length() + 1
    { ...st,
      queue: st.queue.append(id), order: st.order.append(id),
      callerOf: st.callerOf.put(id, caller),
      admitted: st.admitted.union(Set(id)),
      admissionReturns: st.admissionReturns.union(Set(id)),
    }
  }

  action admit(caller: int): bool = all {
    canAdmit(s, caller),
    s' = applyAdmit(s, caller),
  }

  action rejectFull: bool = all {
    s.open,
    s.order.length() < 3,
    s.queue.length() == 2,
    s.fullQueueRejections < 2,
    s' = { ...s, fullQueueRejections: s.fullQueueRejections + 1 },
  }

  action dequeue: bool = all {
    s.active == 0,
    s.queue.length() > 0,
    s' = { ...s,
      active: s.queue.head(), queue: s.queue.tail(), workPhase: Preparing,
    },
  }

  action prepareReplay: bool = all {
    s.active != 0,
    s.workPhase == Preparing,
    not(s.prepared.contains(s.active)),
    s' = { ...s, prepared: s.prepared.union(Set(s.active)) },
  }

  action nativeApply: bool = all {
    s.active != 0,
    s.workPhase == Preparing,
    s.prepared.contains(s.active),
    s' = { ...s,
      applied: s.applied.union(Set(s.active)),
      obligation: s.obligation.union(Set(s.active)),
      workPhase: Applied,
    },
  }

  action stageReplay: bool = all {
    s.active != 0,
    s.workPhase == Applied,
    s' = { ...s,
      staged: s.staged.union(Set(s.active)),
      active: 0, workPhase: Idle,
    },
  }

  action stageFailureRequiresCheckpoint: bool = all {
    s.active != 0,
    s.workPhase == Applied,
    s' = { ...s,
      checkpointNeeded: s.checkpointNeeded.union(Set(s.active)),
      active: 0, workPhase: Idle,
    },
  }

  action prepareCheckpoint: bool = all {
    s.checkpointNeeded.size() > 0,
    s.active == 0 and s.queue.length() == 0,
    s.pubPhase == NoAttempt or s.pubPhase == PubFailed,
    not(s.applied.subseteq(s.checkpointPrepared)),
    s' = { ...s, checkpointPrepared: s.applied },
  }

  action selectPublishPrefix: bool = all {
    s.owned,
    s.flushRequested or s.closeRequested,
    s.active == 0,
    s.queue.length() == 0,
    s.applied.size() > s.settled.size(),
    s.pubPhase == NoAttempt or s.pubPhase == PubFailed,
    s.applied.subseteq(s.staged.union(s.checkpointPrepared)) or
      (BROAD_CHECKPOINT_MUTANT and s.checkpointNeeded.size() > 0 and
       s.applied.exclude(s.staged).size() >= 2),
    s' = { ...s,
      selected: s.applied,
      attempt: s.attempt + 1,
      attemptSeq: s.headSeq + 1,
      attemptGen: s.leaseGen,
      pubPhase: Selected,
      flushRequested: false,
    },
  }

  action requestFlush: bool = all {
    s.open,
    not(s.flushRequested),
    s.pubPhase == NoAttempt or s.pubPhase == PubFailed,
    s.applied.size() > s.settled.size(),
    s' = { ...s, flushRequested: true },
  }

  action closeAdmission: bool = all {
    s.open,
    s' = { ...s, open: false, closeRequested: true },
  }

  action publishImmutable: bool = all {
    s.pubPhase == Selected,
    s' = { ...s,
      immutable: s.immutable.union(Set(s.attempt)),
      pubPhase: Immutable,
    },
  }

  action verifyImmutable: bool = all {
    s.pubPhase == Immutable,
    s.immutable.contains(s.attempt),
    s' = { ...s, pubPhase: Verified },
  }

  action failPublication: bool = all {
    Set(Selected, Immutable, Verified).contains(s.pubPhase),
    s' = { ...s,
      pubPhase: PubFailed,
      obligation: if (DROP_PENDING_MUTANT) Set() else s.obligation,
    },
  }

  action attemptHeadCasConfirmed: bool = all {
    s.pubPhase == Verified,
    s.immutable.contains(s.attempt),
    (s.owned and s.attemptGen == s.leaseGen) or STALE_CAS_MUTANT,
    s' = { ...s,
      headRef: s.attempt, headSeq: s.attemptSeq,
      headGen: s.attemptGen, headCovered: s.selected,
      commitWasOwned: s.owned and s.attemptGen == s.leaseGen,
      pubPhase: Confirmed,
    },
  }

  action attemptHeadCasAmbiguousLand: bool = all {
    s.pubPhase == Verified,
    s.immutable.contains(s.attempt),
    s.owned and s.attemptGen == s.leaseGen,
    s' = { ...s,
      headRef: s.attempt, headSeq: s.attemptSeq,
      headGen: s.attemptGen, headCovered: s.selected,
      commitWasOwned: true,
      pubPhase: Ambiguous,
    },
  }

  action attemptHeadCasAmbiguousMiss: bool = all {
    s.pubPhase == Verified,
    s.immutable.contains(s.attempt),
    s.owned and s.attemptGen == s.leaseGen,
    s' = { ...s, pubPhase: Ambiguous },
  }

  action reconcileAmbiguousCas: bool = all {
    s.pubPhase == Ambiguous,
    s.owned and s.attemptGen == s.leaseGen,
    s.headRef == s.attempt,
    s.headSeq == s.attemptSeq,
    s.headGen == s.attemptGen,
    s.immutable.contains(s.headRef),
    s' = { ...s, pubPhase: Confirmed, reconciled: true },
  }

  action takeover: bool = all {
    s.owned,
    not(s.leaseReleased),
    Set(Selected, Immutable, Verified, Ambiguous).contains(s.pubPhase),
    s' = { ...s, owned: false, leaseGen: s.leaseGen + 1 },
  }

  action renewHeartbeat: bool = all {
    s.owned,
    not(s.heartbeatJoined),
    s.renewals < 1,
    Set(Selected, Immutable, Verified, Ambiguous).contains(s.pubPhase),
    s' = { ...s, renewals: s.renewals + 1 },
  }

  action reconcileUnresolved: bool = all {
    s.pubPhase == Ambiguous,
    not(s.owned and s.attemptGen == s.leaseGen and
        s.headRef == s.attempt and s.headSeq == s.attemptSeq and
        s.headGen == s.attemptGen),
    s' = { ...s, pubPhase: PubFailed },
  }

  pure def canSettle(st: State): bool =
    st.selected.size() > 0 and
    ((st.pubPhase == Confirmed and st.headRef == st.attempt and
      st.headSeq == st.attemptSeq and st.headGen == st.attemptGen and
      st.immutable.contains(st.headRef)) or
     (EARLY_SETTLE_MUTANT and st.pubPhase == Verified))

  action settleCoveredRequests: bool = all {
    canSettle(s),
    s' = { ...s,
      settled: s.settled.union(s.selected),
      obligation: s.obligation.exclude(s.selected),
      checkpointNeeded: s.checkpointNeeded.exclude(s.selected),
      checkpointPrepared: s.checkpointPrepared.exclude(s.selected),
      selected: Set(), pubPhase: NoAttempt,
    },
  }

  action observeReader: bool = all {
    (s.headRef != 0 and s.immutable.contains(s.headRef)) or
      (EARLY_READER_MUTANT and s.immutable.contains(s.attempt)),
    s.reader != s.headCovered or
      (EARLY_READER_MUTANT and s.headRef != s.attempt and
       s.reader != s.selected),
    s' = { ...s,
      reader: if (EARLY_READER_MUTANT and s.headRef != s.attempt)
                s.selected
              else s.headCovered,
    },
  }

  action joinHeartbeat: bool = all {
    s.closeRequested,
    s.owned,
    s.queue.length() == 0 and s.active == 0,
    s.admitted.subseteq(s.settled),
    s.obligation.size() == 0,
    s.pubPhase == NoAttempt,
    not(s.heartbeatJoined),
    s' = { ...s, heartbeatJoined: true },
  }

  action releaseLease: bool = all {
    s.closeRequested and s.heartbeatJoined,
    s.owned,
    not(s.leaseReleased),
    s' = { ...s, leaseReleased: true, owned: false },
  }

  action finishClose: bool = all {
    s.closeRequested and s.leaseReleased,
    not(s.closed),
    s.queue.length() == 0 and s.active == 0,
    s.admitted.subseteq(s.settled),
    s' = { ...s, closed: true },
  }

  action consumerCleanup(id: int): bool = all {
    s.settled.contains(id),
    not(s.cleaned.contains(id)),
    s' = { ...s, cleaned: s.cleaned.union(Set(id)) },
  }

  action rejectPrematureCleanup(id: int): bool = all {
    s.admissionReturns.contains(id),
    not(s.settled.contains(id)),
    not(s.cleanupDenied.contains(id)),
    s' = { ...s, cleanupDenied: s.cleanupDenied.union(Set(id)) },
  }

  action step: bool = any {
    admit(1), admit(2), rejectFull, dequeue, prepareReplay, nativeApply,
    stageReplay, stageFailureRequiresCheckpoint, prepareCheckpoint,
    selectPublishPrefix,
    requestFlush, closeAdmission, publishImmutable, verifyImmutable,
    failPublication,
    attemptHeadCasConfirmed, attemptHeadCasAmbiguousLand,
    attemptHeadCasAmbiguousMiss, reconcileAmbiguousCas, takeover,
    renewHeartbeat,
    reconcileUnresolved, settleCoveredRequests, observeReader,
    joinHeartbeat, releaseLease, finishClose,
    consumerCleanup(1), consumerCleanup(2), consumerCleanup(3),
    rejectPrematureCleanup(1), rejectPrematureCleanup(2),
    rejectPrematureCleanup(3),
  }

  val admissionIsNotSettlement: bool = s.settled.subseteq(s.admitted)
  val nativeAppliedPrepared: bool = s.applied.subseteq(s.prepared)
  val appliedHasRecoveryObligation: bool =
    s.applied.exclude(s.staged).forall(id =>
      s.headCovered.contains(id) or s.checkpointNeeded.contains(id) or
      (id == s.active and s.workPhase == Applied))
  val selectedHasExactReplay: bool =
    s.selected.subseteq(s.staged.union(s.checkpointPrepared))
  val pendingRetained: bool = s.applied.exclude(s.settled).subseteq(s.obligation)
  val settlementHeadGated: bool = s.settled.subseteq(s.headCovered)
  val publicationIsAdmissionPrefix: bool =
    s.headCovered.forall(id => id == 1 or s.headCovered.contains(id - 1))
  val settlementIsAdmissionPrefix: bool =
    s.settled.forall(id => id == 1 or s.settled.contains(id - 1))
  val recoveredReaderHeadGated: bool = s.reader.subseteq(s.headCovered)
  val cleanupRequiresSettlement: bool = s.cleaned.subseteq(s.settled)
  val admissionReached: bool = s.admissionReturns.size() > 0
  val admissionOnlyPending: bool =
    s.admissionReturns.size() > 0 and s.settled.size() == 0
  val fullQueueRejected: bool = s.fullQueueRejections > 0
  val workerDequeued: bool = s.active != 0
  val exactReplayPrepared: bool = s.prepared.size() > 0
  val nativeMutationReached: bool = s.applied.size() > 0
  val stagedReplayReached: bool = s.staged.size() > 0
  val checkpointFallbackReached: bool = s.checkpointNeeded.size() > 0
  val checkpointPreparedReached: bool = s.checkpointPrepared.size() > 0
  val unrelatedFailureMisused: bool =
    s.selected.exclude(s.staged.union(s.checkpointPrepared)).size() >= 2
  val prefixSelected: bool = s.pubPhase == Selected
  val closeAdmissionReached: bool = s.closeRequested
  val closeWithUnpublished: bool =
    s.closeRequested and s.obligation.size() > 0 and s.settled.size() == 0
  val explicitFlushRequested: bool = s.flushRequested
  val immutableUncommitted: bool =
    s.pubPhase == Immutable and s.headRef != s.attempt
  val immutableVerified: bool = s.pubPhase == Verified
  val failedPublicationRetained: bool =
    s.pubPhase == PubFailed and s.obligation.size() > 0
  val headCommitReached: bool = s.headRef != 0
  val onlyCurrentOwnerCommits: bool = s.commitWasOwned
  val ambiguousLandingReached: bool =
    s.pubPhase == Ambiguous and s.headRef == s.attempt
  val ambiguousMissReached: bool =
    s.pubPhase == Ambiguous and s.headRef != s.attempt
  val ambiguousReconciled: bool =
    s.reconciled and s.pubPhase == Confirmed
  val ambiguousTakeover: bool =
    s.pubPhase == Ambiguous and not(s.owned)
  val heartbeatDuringPublication: bool = s.renewals > 0
  val unresolvedRetained: bool =
    s.pubPhase == PubFailed and s.obligation.size() > 0
  val settledTicketReached: bool = s.settled.size() > 0
  val multiPrefixSettled: bool = s.settled.size() >= 2
  val recoveredReadReached: bool = s.reader.size() > 0
  val heartbeatJoinedAfterDrain: bool = s.heartbeatJoined
  val leaseReleasedAfterJoin: bool = s.leaseReleased implies s.heartbeatJoined
  val returnedCloseWasSettled: bool =
    not(s.closed) or (s.admitted.subseteq(s.settled) and
                      s.heartbeatJoined and s.leaseReleased)
  val releaseReached: bool = s.leaseReleased
  val closeWithWorkReached: bool = s.closed and s.settled.size() > 0
  val cleanupReached: bool = s.cleaned.size() > 0
  val prematureCleanupDenied: bool = s.cleanupDenied.size() > 0
}

module bufferedPublicationCorrected {
  import bufferedPublication(
    EARLY_SETTLE_MUTANT = false,
    STALE_CAS_MUTANT = false,
    EARLY_READER_MUTANT = false,
    DROP_PENDING_MUTANT = false,
    BROAD_CHECKPOINT_MUTANT = false,
  ).*
}

module bufferedPublicationEarlySettleMutant {
  import bufferedPublication(
    EARLY_SETTLE_MUTANT = true,
    STALE_CAS_MUTANT = false,
    EARLY_READER_MUTANT = false,
    DROP_PENDING_MUTANT = false,
    BROAD_CHECKPOINT_MUTANT = false,
  ).*
}

module bufferedPublicationStaleCasMutant {
  import bufferedPublication(
    EARLY_SETTLE_MUTANT = false,
    STALE_CAS_MUTANT = true,
    EARLY_READER_MUTANT = false,
    DROP_PENDING_MUTANT = false,
    BROAD_CHECKPOINT_MUTANT = false,
  ).*
}

module bufferedPublicationEarlyReaderMutant {
  import bufferedPublication(
    EARLY_SETTLE_MUTANT = false,
    STALE_CAS_MUTANT = false,
    EARLY_READER_MUTANT = true,
    DROP_PENDING_MUTANT = false,
    BROAD_CHECKPOINT_MUTANT = false,
  ).*
}

module bufferedPublicationDropPendingMutant {
  import bufferedPublication(
    EARLY_SETTLE_MUTANT = false,
    STALE_CAS_MUTANT = false,
    EARLY_READER_MUTANT = false,
    DROP_PENDING_MUTANT = true,
    BROAD_CHECKPOINT_MUTANT = false,
  ).*
}

module bufferedPublicationBroadCheckpointMutant {
  import bufferedPublication(
    EARLY_SETTLE_MUTANT = false,
    STALE_CAS_MUTANT = false,
    EARLY_READER_MUTANT = false,
    DROP_PENDING_MUTANT = false,
    BROAD_CHECKPOINT_MUTANT = true,
  ).*
}
```

## What the finite model checks

| Approved boundary | Model transition / property |
| --- | --- |
| Admission is local and queue-full rejects | `admit`, `rejectFull`; `admissionOnlyPending`, `fullQueueRejected` |
| Prepare before native mutation; staging failure requires exact checkpoint coverage | `prepareReplay`, `nativeApply`, `stageReplay`, `stageFailureRequiresCheckpoint`, `prepareCheckpoint`; `nativeAppliedPrepared`, `appliedHasRecoveryObligation`, `selectedHasExactReplay` |
| A later ticket/flush confirms an ordered prefix | `requestFlush`, `selectPublishPrefix`, `settleCoveredRequests`; `settlementHeadGated`, `publicationIsAdmissionPrefix`, `settlementIsAdmissionPrefix` |
| Immutable upload alone is invisible to recovery | `publishImmutable`, `verifyImmutable`, `observeReader`; `recoveredReaderHeadGated` |
| Exact head CAS and current lease ownership gate settlement | `attemptHeadCasConfirmed`, ambiguous-land/miss, `reconcileAmbiguousCas`, `takeover`; `onlyCurrentOwnerCommits` |
| Failed publication retains work; cleanup waits for settlement | `failPublication`, `reconcileUnresolved`, `rejectPrematureCleanup`, `consumerCleanup`; `pendingRetained`, `cleanupRequiresSettlement`, `prematureCleanupDenied` |
| Successful normal close drains and confirms before release | `closeAdmission`, worker drain, `joinHeartbeat`, `releaseLease`, `finishClose`; `returnedCloseWasSettled`, `leaseReleasedAfterJoin` |

The five deliberately faulty instances make one violation each: early settlement
without a committed head, stale-owner CAS, reader visibility from an uncommitted
immutable object, and deletion of a pending recovery obligation. They are red
controls, not alternative designs. The fifth mutant treats the existence of
failed-stage work as if it covered two unstaged requests, without preparing an
exact checkpoint snapshot. Sampled `quint run` must report a violation
for each corresponding invariant; a parser/runtime error is **not** a valid red
result.

To reproduce the bounded checks from the repository root:

```sh
lmt formal/quint/buffered-publication.md
quint typecheck target/formal/quint/bufferedPublication.qnt
quint run target/formal/quint/bufferedPublication.qnt \
  --main bufferedPublicationCorrected --max-steps 35 --max-samples 20000 \
  --invariants nativeAppliedPrepared appliedHasRecoveryObligation \
  selectedHasExactReplay pendingRetained \
  settlementHeadGated publicationIsAdmissionPrefix settlementIsAdmissionPrefix \
  recoveredReaderHeadGated cleanupRequiresSettlement onlyCurrentOwnerCommits \
  leaseReleasedAfterJoin returnedCloseWasSettled \
  --witnesses fullQueueRejected checkpointFallbackReached \
  checkpointPreparedReached immutableUncommitted \
  failedPublicationRetained ambiguousReconciled ambiguousTakeover \
  multiPrefixSettled closeWithWorkReached prematureCleanupDenied cleanupReached
```

Run each mutant with `quint run --main <module> --invariant <property>
--max-steps 22 --max-samples 10000`; the pairs are
`bufferedPublicationEarlySettleMutant` / `settlementHeadGated`,
`bufferedPublicationStaleCasMutant` / `onlyCurrentOwnerCommits`,
`bufferedPublicationEarlyReaderMutant` / `recoveredReaderHeadGated`, and
`bufferedPublicationDropPendingMutant` / `pendingRetained`. The additional
`bufferedPublicationBroadCheckpointMutant` / `selectedHasExactReplay` run uses
`--max-steps 26 --max-samples 10000` and witnesses `unrelatedFailureMisused`.
For one reproducible red trace, use `--seed 0xa901a8edd96e8276`
instead of `--max-samples 10000`; that single sampled trace reaches the
two-request uncovered selection and violates `selectedHasExactReplay`.

On 2026-09-22 with Quint 0.32.0 and the pinned `lmt`, the corrected
20,000-sample, 35-step run found no listed invariant violation and reached
every listed witness (the rarest, ambiguous reconciliation, in 261 traces).
All five mutant runs reported the intended invariant violation; the new broad
checkpoint red control also witnessed two selected requests without exact
staged or checkpoint replay coverage. These are sampled observations, not
exhaustive model-checking results.

The checkpoint action is an abstract trusted claim that it prepared exact
replay content for `checkpointPrepared`, the applied set at that instant. The
claim assumes a full native archive taken on the serialized writer handle
includes every successful local mutation, including one whose WAL append
failed. The current writer routes that failure to full backup, but the exact
injected append-failure → checkpoint → fresh-process reopen path has not yet
been qualified end-to-end. The
model tests that selection cannot use a failed-stage flag as proof of content
for another request; it does not prove that a runtime checkpoint actually
contains those bytes. Likewise, the corrected reader action is guarded by a
committed head reference and assigns `headCovered`; the
`recoveredReaderHeadGated` invariant follows from that model guard/update, with
the early-reader mutant showing the omitted-head-check failure. It is not an
independent validation of runtime object-store reads or head decoding.

This model does **not** establish runtime correctness, fairness, delivery
latency, crash survival of admitted work, or the 25k/20k throughput goal. It
abstracts away exact WAL bytes, head JSON/ETags, SHA verification, S3 outcomes,
checkpoint file handling, native resources, and first-error identity on a
failed close. A successful close is modelled; error-returning close, timed
flushes, ticket API shape, local-writer visibility, and cancellation are not.
The existing head-CAS, spool, and close models remain authoritative for their
respective implementation boundaries. Update this model when the buffered
contract changes, before using it to justify runtime work.
