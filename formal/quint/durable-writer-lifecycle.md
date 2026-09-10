# Durable writer close lifecycle

This executable companion models the part of `jdbc.chdb.durable.writer` that
the head-CAS model intentionally omits: an admitted close waiting behind an
in-flight operation while the lease heartbeat remains responsible for keeping
the eventual flush writable.

The implementation correspondence is deliberately small:

| Model action | Runtime boundary |
| --- | --- |
| `admitClose` | `close!` changes `:open` to `:closing` under `admission-lock` and enqueues behind earlier work |
| `elapseWhileHeartbeatResponsible` | an owned heartbeat OS thread renews independently while the worker may be blocked in native code, immutable publication, verification, or a head CAS/reconciliation call |
| `finishPriorOperation` | the serialized worker reaches the close request after FIFO drain |
| `finishPublication` | immutable WAL/checkpoint publication returns |
| `finishVerification` | the immutable size/digest verification returns |
| `flush` | the manifest CAS/reconciliation returns and `do-flush!` clears the committed obligation |
| `stopAndJoinHeartbeat` | the stop promise is delivered and the completion handshake succeeds |
| `release` | lease release happens only after that positive handshake |
| `postReleaseHeartbeatTick` | mutant-only scheduler opportunity when release skips the handshake |
| `cleanup` | native close and scratch cleanup finish the terminal transition |

`STOP_HEARTBEAT_AT_ADMISSION_MUTANT` encodes the previous defect. It stops the
heartbeat as soon as close changes the lifecycle to `Closing`, allowing time to
expire the lease before or during the queued flush.
`RELEASE_BEFORE_HEARTBEAT_JOIN_MUTANT` skips the positive completion handshake;
the still-running heartbeat can then take a post-release tick. The corrected
model retains the heartbeat through `Flushed`, then joins it before release.
`LOCK_DURING_BLOCKING_IO_MUTANT` represents the implementation defect in which
the worker held a writer-local head lock across publication, verification, and
the remote head CAS/reconciliation. A blocked storage call then excludes the
heartbeat even though its owned OS thread is scheduled. The corrected runtime
has no such shared lock: manifest and renewal transitions coordinate through
backend CAS and semantic reread reconciliation, without assuming any storage
call returns before the lease TTL.
This is a safety model: OS-thread scheduling is represented only by independent
renewal/expiry opportunities while heartbeat is responsible. It makes no
fairness or upper-latency claim; instead it makes the absence of local
exclusion during every blocking storage phase an explicit invariant. The
existing head-CAS state and ITF projection are unchanged.

Generated `.qnt` files live under `target/formal/quint/`; edit this Markdown,
not those files.

## State machine

```quint target/formal/quint/durableWriterLifecycle.qnt +=
module durableWriterLifecycle {
  type ClosePhase =
    | Open
    | Draining
    | Publishing
    | Verifying
    | Committing
    | Flushed
    | HeartbeatJoined
    | Released
    | Closed

  type LifecycleState = {
    phase: ClosePhase,
    heartbeatRunning: bool,
    heartbeatJoined: bool,
    headLockHeld: bool,
    leaseLive: bool,
    pending: bool,
    flushed: bool,
    released: bool,
    renewedAfterRelease: bool,
    renewedDuringDrain: bool,
    renewedDuringPublication: bool,
    renewedDuringVerification: bool,
    renewedDuringCommit: bool,
    renewals: int,
  }

  const STOP_HEARTBEAT_AT_ADMISSION_MUTANT: bool
  const RELEASE_BEFORE_HEARTBEAT_JOIN_MUTANT: bool
  const LOCK_DURING_BLOCKING_IO_MUTANT: bool
  var lifecycle: LifecycleState

  action init: bool =
    lifecycle' = {
      phase: Open,
      heartbeatRunning: true,
      heartbeatJoined: false,
      headLockHeld: false,
      leaseLive: true,
      pending: true,
      flushed: false,
      released: false,
      renewedAfterRelease: false,
      renewedDuringDrain: false,
      renewedDuringPublication: false,
      renewedDuringVerification: false,
      renewedDuringCommit: false,
      renewals: 0,
    }

  action admitClose: bool = all {
    lifecycle.phase == Open,
    lifecycle' = {
      ...lifecycle,
      phase: Draining,
      heartbeatRunning: not(STOP_HEARTBEAT_AT_ADMISSION_MUTANT),
    },
  }

  action elapseWhileHeartbeatResponsible: bool = all {
    Set(Draining, Publishing, Verifying, Committing, Flushed)
      .contains(lifecycle.phase),
    lifecycle' = {
      ...lifecycle,
      leaseLive: lifecycle.heartbeatRunning and not(lifecycle.headLockHeld),
      renewals:
        if (lifecycle.heartbeatRunning and not(lifecycle.headLockHeld))
          lifecycle.renewals + 1
        else lifecycle.renewals,
      renewedAfterRelease:
        lifecycle.renewedAfterRelease
          or (lifecycle.released and lifecycle.heartbeatRunning),
      renewedDuringDrain:
        lifecycle.renewedDuringDrain
          or (lifecycle.phase == Draining and lifecycle.heartbeatRunning
              and not(lifecycle.headLockHeld)),
      renewedDuringPublication:
        lifecycle.renewedDuringPublication
          or (lifecycle.phase == Publishing and lifecycle.heartbeatRunning
              and not(lifecycle.headLockHeld)),
      renewedDuringVerification:
        lifecycle.renewedDuringVerification
          or (lifecycle.phase == Verifying and lifecycle.heartbeatRunning
              and not(lifecycle.headLockHeld)),
      renewedDuringCommit:
        lifecycle.renewedDuringCommit
          or (lifecycle.phase == Committing and lifecycle.heartbeatRunning
              and not(lifecycle.headLockHeld)),
    },
  }

  action finishPriorOperation: bool = all {
    lifecycle.phase == Draining,
    lifecycle' = {
      ...lifecycle,
      phase: Publishing,
      headLockHeld: LOCK_DURING_BLOCKING_IO_MUTANT,
    },
  }

  action finishPublication: bool = all {
    lifecycle.phase == Publishing,
    lifecycle' = { ...lifecycle, phase: Verifying },
  }

  action finishVerification: bool = all {
    lifecycle.phase == Verifying,
    lifecycle' = { ...lifecycle, phase: Committing },
  }

  action flush: bool = all {
    lifecycle.phase == Committing,
    lifecycle.leaseLive,
    lifecycle' = {
      ...lifecycle,
      phase: Flushed,
      pending: false,
      flushed: true,
      headLockHeld: false,
    },
  }

  action stopAndJoinHeartbeat: bool = all {
    lifecycle.phase == Flushed,
    lifecycle' = {
      ...lifecycle,
      phase: HeartbeatJoined,
      heartbeatRunning: false,
      heartbeatJoined: true,
    },
  }

  action release: bool = all {
    if (RELEASE_BEFORE_HEARTBEAT_JOIN_MUTANT)
      lifecycle.phase == Flushed
    else
      lifecycle.phase == HeartbeatJoined,
    if (RELEASE_BEFORE_HEARTBEAT_JOIN_MUTANT)
      lifecycle.heartbeatRunning
    else
      not(lifecycle.heartbeatRunning),
    lifecycle' = { ...lifecycle, phase: Released, released: true },
  }

  action postReleaseHeartbeatTick: bool = all {
    lifecycle.phase == Released,
    lifecycle.heartbeatRunning,
    lifecycle' = {
      ...lifecycle,
      leaseLive: true,
      renewedAfterRelease: true,
      renewals: lifecycle.renewals + 1,
    },
  }

  action cleanup: bool = all {
    lifecycle.phase == Released,
    lifecycle' = { ...lifecycle, phase: Closed },
  }

  action step: bool = any {
    admitClose,
    elapseWhileHeartbeatResponsible,
    finishPriorOperation,
    finishPublication,
    finishVerification,
    flush,
    stopAndJoinHeartbeat,
    release,
    postReleaseHeartbeatTick,
    cleanup,
  }

  val heartbeatCoversCloseWork: bool =
    if (Set(Draining, Publishing, Verifying, Committing, Flushed)
        .contains(lifecycle.phase))
      lifecycle.heartbeatRunning
    else true

  val leaseCoversCloseFlush: bool =
    if (Set(Publishing, Verifying, Committing).contains(lifecycle.phase))
      lifecycle.leaseLive
    else true

  val blockingIOLeavesHeartbeatIndependent: bool =
    if (Set(Publishing, Verifying, Committing).contains(lifecycle.phase))
      not(lifecycle.headLockHeld)
    else true

  val releaseFollowsHeartbeatJoin: bool =
    if (lifecycle.released)
      and {
        lifecycle.flushed,
        lifecycle.heartbeatJoined,
        not(lifecycle.heartbeatRunning),
      }
    else true

  val noRenewAfterRelease: bool = not(lifecycle.renewedAfterRelease)
  val closeReached: bool = lifecycle.phase == Closed
  val drainRenewalReached: bool = lifecycle.renewedDuringDrain
  val publicationRenewalReached: bool = lifecycle.renewedDuringPublication
  val verificationRenewalReached: bool = lifecycle.renewedDuringVerification
  val commitRenewalReached: bool = lifecycle.renewedDuringCommit
}

module durableWriterLifecycleCorrected {
  import durableWriterLifecycle(
    STOP_HEARTBEAT_AT_ADMISSION_MUTANT = false,
    RELEASE_BEFORE_HEARTBEAT_JOIN_MUTANT = false,
    LOCK_DURING_BLOCKING_IO_MUTANT = false
  ).* from "./durableWriterLifecycle"
}

module durableWriterLifecycleMutant {
  import durableWriterLifecycle(
    STOP_HEARTBEAT_AT_ADMISSION_MUTANT = true,
    RELEASE_BEFORE_HEARTBEAT_JOIN_MUTANT = false,
    LOCK_DURING_BLOCKING_IO_MUTANT = false
  ).* from "./durableWriterLifecycle"
}

module durableWriterLifecycleReleaseBeforeJoinMutant {
  import durableWriterLifecycle(
    STOP_HEARTBEAT_AT_ADMISSION_MUTANT = false,
    RELEASE_BEFORE_HEARTBEAT_JOIN_MUTANT = true,
    LOCK_DURING_BLOCKING_IO_MUTANT = false
  ).* from "./durableWriterLifecycle"
}

module durableWriterLifecycleBlockingIOMutant {
  import durableWriterLifecycle(
    STOP_HEARTBEAT_AT_ADMISSION_MUTANT = false,
    RELEASE_BEFORE_HEARTBEAT_JOIN_MUTANT = false,
    LOCK_DURING_BLOCKING_IO_MUTANT = true
  ).* from "./durableWriterLifecycle"
}
```

## Executable examples

The corrected trace is the runtime tests' abstract projection: close admission,
renewal during drain, then independent renewal while publication, verification,
and the manifest CAS/reconciliation are each blocked, followed by flush,
heartbeat stop/join, release, and cleanup. The admission mutant violates
heartbeat coverage; the blocking-I/O mutant makes renewal unavailable and loses
the lease while storage is blocked.

```quint target/formal/quint/durableWriterLifecycleTest.qnt +=
module durableWriterLifecycleCorrectedTest {
  import durableWriterLifecycle(
    STOP_HEARTBEAT_AT_ADMISSION_MUTANT = false,
    RELEASE_BEFORE_HEARTBEAT_JOIN_MUTANT = false,
    LOCK_DURING_BLOCKING_IO_MUTANT = false
  ).* from "./durableWriterLifecycle"

  run closeDrainFlushReleaseTest =
    init
      .then(admitClose)
      .then(elapseWhileHeartbeatResponsible)
      .then(finishPriorOperation)
      .then(elapseWhileHeartbeatResponsible)
      .then(finishPublication)
      .then(elapseWhileHeartbeatResponsible)
      .then(finishVerification)
      .then(elapseWhileHeartbeatResponsible)
      .then(flush)
      .then(stopAndJoinHeartbeat)
      .then(release)
      .then(cleanup)
      .expect(and {
        closeReached,
        drainRenewalReached,
        publicationRenewalReached,
        verificationRenewalReached,
        commitRenewalReached,
        blockingIOLeavesHeartbeatIndependent,
        releaseFollowsHeartbeatJoin,
        noRenewAfterRelease,
      })
}

module durableWriterLifecycleMutantTest {
  import durableWriterLifecycle(
    STOP_HEARTBEAT_AT_ADMISSION_MUTANT = true,
    RELEASE_BEFORE_HEARTBEAT_JOIN_MUTANT = false,
    LOCK_DURING_BLOCKING_IO_MUTANT = false
  ).* from "./durableWriterLifecycle"

  run stopAtAdmissionWitnessTest =
    init
      .then(admitClose)
      .expect(not(heartbeatCoversCloseWork))

  run expiryBeforeFlushWitnessTest =
    init
      .then(admitClose)
      .then(finishPriorOperation)
      .then(elapseWhileHeartbeatResponsible)
      .expect(not(leaseCoversCloseFlush))
}

module durableWriterLifecycleReleaseBeforeJoinMutantTest {
  import durableWriterLifecycle(
    STOP_HEARTBEAT_AT_ADMISSION_MUTANT = false,
    RELEASE_BEFORE_HEARTBEAT_JOIN_MUTANT = true,
    LOCK_DURING_BLOCKING_IO_MUTANT = false
  ).* from "./durableWriterLifecycle"

  run lateRenewalAfterReleaseWitnessTest =
    init
      .then(admitClose)
      .then(finishPriorOperation)
      .then(finishPublication)
      .then(finishVerification)
      .then(flush)
      .then(release)
      .then(postReleaseHeartbeatTick)
      .expect(and {
        not(noRenewAfterRelease),
        not(releaseFollowsHeartbeatJoin),
      })
}

module durableWriterLifecycleBlockingIOMutantTest {
  import durableWriterLifecycle(
    STOP_HEARTBEAT_AT_ADMISSION_MUTANT = false,
    RELEASE_BEFORE_HEARTBEAT_JOIN_MUTANT = false,
    LOCK_DURING_BLOCKING_IO_MUTANT = true
  ).* from "./durableWriterLifecycle"

  run blockingPublicationExcludesHeartbeatWitnessTest =
    init
      .then(admitClose)
      .then(finishPriorOperation)
      .expect(not(blockingIOLeavesHeartbeatIndependent))

  run blockedPublicationLosesLeaseWitnessTest =
    init
      .then(admitClose)
      .then(finishPriorOperation)
      .then(elapseWhileHeartbeatResponsible)
      .expect(not(leaseCoversCloseFlush))
}
```

## Renewal loss and public operation fencing

The close model above deliberately assumes that heartbeat renewal remains
available. The public writer has a separate boundary when replacement keeps
failing: one failure before the current expiry is tolerated, but failure
through expiry self-fences mutation and persistence while queries against the
already-restored local database remain available. This small model does not
duplicate head CAS or close sequencing. It records only the causal renewal-loss
trace and the four public outcomes required by the pinned upstream case.

`IGNORE_EXPIRY_FENCE_MUTANT` represents a heartbeat that observes expiry after
renewal loss but leaves the writer writable. `ALLOW_FENCED_EFFECTS_MUTANT`
allows execute, flush, and checkpoint effects after fencing.
`DROP_FENCED_READ_MUTANT` incorrectly disables the established local read.

```quint target/formal/quint/durableWriterLifecycle.qnt +=
module durableWriterRenewalLoss {
  type RenewalLossPhase =
    | RenewalLive
    | RenewalExtended
    | RenewalFailed
    | RenewalExpired
    | RenewalOutcomesObserved

  type RenewalLossState = {
    phase: RenewalLossPhase,
    leaseLive: bool,
    fenced: bool,
    renewalAttempts: int,
    expiryExtensions: int,
    executeEffects: int,
    flushEffects: int,
    checkpointEffects: int,
    readAvailable: bool,
    readResult: int,
  }

  const IGNORE_EXPIRY_FENCE_MUTANT: bool
  const ALLOW_FENCED_EFFECTS_MUTANT: bool
  const DROP_FENCED_READ_MUTANT: bool
  var renewalLoss: RenewalLossState

  action init: bool =
    renewalLoss' = {
      phase: RenewalLive,
      leaseLive: true,
      fenced: false,
      renewalAttempts: 0,
      expiryExtensions: 0,
      executeEffects: 0,
      flushEffects: 0,
      checkpointEffects: 0,
      readAvailable: true,
      readResult: 4,
    }

  action extendLease: bool = all {
    renewalLoss.phase == RenewalLive,
    renewalLoss' = {
      ...renewalLoss,
      phase: RenewalExtended,
      renewalAttempts: renewalLoss.renewalAttempts + 1,
      expiryExtensions: renewalLoss.expiryExtensions + 1,
    },
  }

  action failRenewalBeforeExpiry: bool = all {
    renewalLoss.phase == RenewalExtended,
    renewalLoss.leaseLive,
    renewalLoss' = {
      ...renewalLoss,
      phase: RenewalFailed,
      renewalAttempts: renewalLoss.renewalAttempts + 1,
    },
  }

  action elapseThroughExpiry: bool = all {
    renewalLoss.phase == RenewalFailed,
    renewalLoss' = {
      ...renewalLoss,
      phase: RenewalExpired,
      leaseLive: false,
      fenced: not(IGNORE_EXPIRY_FENCE_MUTANT),
    },
  }

  action observePublicOutcomes: bool = all {
    renewalLoss.phase == RenewalExpired,
    renewalLoss' = {
      ...renewalLoss,
      phase: RenewalOutcomesObserved,
      executeEffects:
        if (renewalLoss.fenced and not(ALLOW_FENCED_EFFECTS_MUTANT)) 0 else 1,
      flushEffects:
        if (renewalLoss.fenced and not(ALLOW_FENCED_EFFECTS_MUTANT)) 0 else 1,
      checkpointEffects:
        if (renewalLoss.fenced and not(ALLOW_FENCED_EFFECTS_MUTANT)) 0 else 1,
      readAvailable: not(DROP_FENCED_READ_MUTANT),
      readResult: if (DROP_FENCED_READ_MUTANT) -1 else 4,
    },
  }

  action step: bool = any {
    extendLease,
    failRenewalBeforeExpiry,
    elapseThroughExpiry,
    observePublicOutcomes,
  }

  val failedBeforeExpiryStaysWritable: bool =
    if (renewalLoss.phase == RenewalFailed)
      and { renewalLoss.leaseLive, not(renewalLoss.fenced) }
    else true

  val failedRenewalThroughExpiryFences: bool =
    if (Set(RenewalExpired, RenewalOutcomesObserved)
        .contains(renewalLoss.phase))
      renewalLoss.fenced
    else true

  val fencedWriteEffectsAreZero: bool =
    if (renewalLoss.phase == RenewalOutcomesObserved)
      and {
        renewalLoss.executeEffects == 0,
        renewalLoss.flushEffects == 0,
        renewalLoss.checkpointEffects == 0,
      }
    else true

  val fencedReadSurvives: bool =
    if (renewalLoss.phase == RenewalOutcomesObserved)
      and { renewalLoss.readAvailable, renewalLoss.readResult == 4 }
    else true

  val successfulRenewalReached: bool =
    renewalLoss.expiryExtensions == 1
  val failedRenewalReached: bool =
    renewalLoss.renewalAttempts == 2
  val renewalExpiryReached: bool =
    Set(RenewalExpired, RenewalOutcomesObserved).contains(renewalLoss.phase)
  val publicOutcomesReached: bool =
    renewalLoss.phase == RenewalOutcomesObserved
}

module durableWriterRenewalLossCorrected {
  import durableWriterRenewalLoss(
    IGNORE_EXPIRY_FENCE_MUTANT = false,
    ALLOW_FENCED_EFFECTS_MUTANT = false,
    DROP_FENCED_READ_MUTANT = false
  ).* from "./durableWriterLifecycle"
}

module durableWriterRenewalLossIgnoreExpiryMutant {
  import durableWriterRenewalLoss(
    IGNORE_EXPIRY_FENCE_MUTANT = true,
    ALLOW_FENCED_EFFECTS_MUTANT = false,
    DROP_FENCED_READ_MUTANT = false
  ).* from "./durableWriterLifecycle"
}

module durableWriterRenewalLossAllowEffectsMutant {
  import durableWriterRenewalLoss(
    IGNORE_EXPIRY_FENCE_MUTANT = false,
    ALLOW_FENCED_EFFECTS_MUTANT = true,
    DROP_FENCED_READ_MUTANT = false
  ).* from "./durableWriterLifecycle"
}

module durableWriterRenewalLossDropReadMutant {
  import durableWriterRenewalLoss(
    IGNORE_EXPIRY_FENCE_MUTANT = false,
    ALLOW_FENCED_EFFECTS_MUTANT = false,
    DROP_FENCED_READ_MUTANT = true
  ).* from "./durableWriterLifecycle"
}
```

The examples share one exact four-step trace. The corrected module requires all
four invariants. Each mutant must expose its own rejected boundary through the
same sequence rather than through an unrelated fixture.

```quint target/formal/quint/durableWriterLifecycleTest.qnt +=
module durableWriterRenewalLossCorrectedTest {
  import durableWriterRenewalLoss(
    IGNORE_EXPIRY_FENCE_MUTANT = false,
    ALLOW_FENCED_EFFECTS_MUTANT = false,
    DROP_FENCED_READ_MUTANT = false
  ).* from "./durableWriterLifecycle"

  run renewalLossPublicOutcomesTest =
    init
      .then(extendLease)
      .then(failRenewalBeforeExpiry)
      .then(elapseThroughExpiry)
      .then(observePublicOutcomes)
      .expect(and {
        successfulRenewalReached,
        failedRenewalReached,
        renewalExpiryReached,
        publicOutcomesReached,
        failedBeforeExpiryStaysWritable,
        failedRenewalThroughExpiryFences,
        fencedWriteEffectsAreZero,
        fencedReadSurvives,
      })
}

module durableWriterRenewalLossIgnoreExpiryMutantTest {
  import durableWriterRenewalLoss(
    IGNORE_EXPIRY_FENCE_MUTANT = true,
    ALLOW_FENCED_EFFECTS_MUTANT = false,
    DROP_FENCED_READ_MUTANT = false
  ).* from "./durableWriterLifecycle"

  run ignoredExpiryWitnessTest =
    init
      .then(extendLease)
      .then(failRenewalBeforeExpiry)
      .then(elapseThroughExpiry)
      .expect(not(failedRenewalThroughExpiryFences))
}

module durableWriterRenewalLossAllowEffectsMutantTest {
  import durableWriterRenewalLoss(
    IGNORE_EXPIRY_FENCE_MUTANT = false,
    ALLOW_FENCED_EFFECTS_MUTANT = true,
    DROP_FENCED_READ_MUTANT = false
  ).* from "./durableWriterLifecycle"

  run allowedEffectsWitnessTest =
    init
      .then(extendLease)
      .then(failRenewalBeforeExpiry)
      .then(elapseThroughExpiry)
      .then(observePublicOutcomes)
      .expect(not(fencedWriteEffectsAreZero))
}

module durableWriterRenewalLossDropReadMutantTest {
  import durableWriterRenewalLoss(
    IGNORE_EXPIRY_FENCE_MUTANT = false,
    ALLOW_FENCED_EFFECTS_MUTANT = false,
    DROP_FENCED_READ_MUTANT = true
  ).* from "./durableWriterLifecycle"

  run droppedReadWitnessTest =
    init
      .then(extendLease)
      .then(failRenewalBeforeExpiry)
      .then(elapseThroughExpiry)
      .then(observePublicOutcomes)
      .expect(not(fencedReadSurvives))
}
```

## Commands and evidence

Run the same pinned literate-model gate used by the head-CAS model:

```sh
scripts/check-durable-head-quint.sh
scripts/check-durable-head-quint.sh --verify
scripts/check-durable-renewal-loss-quint.sh
```

The first command tangles, typechecks, tests, and samples all lifecycle
variants. `--verify` additionally checks the corrected lifecycle invariants and
requires the stopped-at-admission, blocking-I/O, and release-before-join mutants
to produce their bounded counterexamples. The renewal-loss command is a smaller
TypeScript-only gate: it tangles and typechecks the lifecycle files, runs the
corrected trace and all three causal mutants, and compares the deterministic
ITF projection. It does not invoke Apalache or the broader model suite.
