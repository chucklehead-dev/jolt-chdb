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

## Commands and evidence

Run the same pinned literate-model gate used by the head-CAS model:

```sh
scripts/check-durable-head-quint.sh
scripts/check-durable-head-quint.sh --verify
```

The first command tangles, typechecks, tests, and samples all lifecycle
variants. `--verify` additionally checks the corrected lifecycle invariants and
requires the stopped-at-admission, blocking-I/O, and release-before-join mutants
to produce their bounded counterexamples.
