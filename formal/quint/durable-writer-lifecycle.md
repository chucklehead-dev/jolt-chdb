# Durable writer close lifecycle

This executable companion models the part of `jdbc.chdb.durable.writer` that
the head-CAS model intentionally omits: an admitted close waiting behind an
in-flight operation while the lease heartbeat remains responsible for keeping
the eventual flush writable.

The implementation correspondence is deliberately small:

| Model action | Runtime boundary |
| --- | --- |
| `admitClose` | `close!` changes `:open` to `:closing` under `admission-lock` and enqueues behind earlier work |
| `elapseDuringDrain` | an owned heartbeat OS thread renews while the worker may be blocked in native code |
| `finishPriorOperation` | the serialized worker reaches the close request after FIFO drain |
| `flush` | `do-close!` calls `do-flush!` before stopping heartbeat |
| `stopAndJoinHeartbeat` | the stop promise is delivered and the completion handshake succeeds |
| `release` | lease release happens only after that positive handshake |
| `cleanup` | native close and scratch cleanup finish the terminal transition |

`STOP_HEARTBEAT_AT_ADMISSION_MUTANT` encodes the previous defect. It stops the
heartbeat as soon as close changes the lifecycle to `Closing`, allowing time to
expire the lease before the queued flush. The corrected model retains the
heartbeat through `Flushed`, then joins it before release. This is a safety
model: OS-thread scheduling is represented only by the independent
`elapseDuringDrain` renewal opportunity, and it makes no fairness or latency
claim. The existing head-CAS state and ITF projection are unchanged.

Generated `.qnt` files live under `target/formal/quint/`; edit this Markdown,
not those files.

## State machine

```quint target/formal/quint/durableWriterLifecycle.qnt +=
module durableWriterLifecycle {
  type ClosePhase =
    | Open
    | Draining
    | Flushing
    | Flushed
    | HeartbeatJoined
    | Released
    | Closed

  type LifecycleState = {
    phase: ClosePhase,
    heartbeatRunning: bool,
    leaseLive: bool,
    pending: bool,
    flushed: bool,
    released: bool,
    renewedAfterRelease: bool,
    renewals: int,
  }

  const STOP_HEARTBEAT_AT_ADMISSION_MUTANT: bool
  var lifecycle: LifecycleState

  action init: bool =
    lifecycle' = {
      phase: Open,
      heartbeatRunning: true,
      leaseLive: true,
      pending: true,
      flushed: false,
      released: false,
      renewedAfterRelease: false,
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

  action elapseDuringDrain: bool = all {
    lifecycle.phase == Draining,
    lifecycle.pending,
    lifecycle' = {
      ...lifecycle,
      leaseLive: lifecycle.heartbeatRunning,
      renewals:
        if (lifecycle.heartbeatRunning) lifecycle.renewals + 1
        else lifecycle.renewals,
      renewedAfterRelease:
        lifecycle.renewedAfterRelease
          or (lifecycle.released and lifecycle.heartbeatRunning),
    },
  }

  action finishPriorOperation: bool = all {
    lifecycle.phase == Draining,
    lifecycle' = { ...lifecycle, phase: Flushing },
  }

  action flush: bool = all {
    lifecycle.phase == Flushing,
    lifecycle.leaseLive,
    lifecycle' = {
      ...lifecycle,
      phase: Flushed,
      pending: false,
      flushed: true,
    },
  }

  action stopAndJoinHeartbeat: bool = all {
    lifecycle.phase == Flushed,
    lifecycle' = {
      ...lifecycle,
      phase: HeartbeatJoined,
      heartbeatRunning: false,
    },
  }

  action release: bool = all {
    lifecycle.phase == HeartbeatJoined,
    not(lifecycle.heartbeatRunning),
    lifecycle' = { ...lifecycle, phase: Released, released: true },
  }

  action cleanup: bool = all {
    lifecycle.phase == Released,
    lifecycle' = { ...lifecycle, phase: Closed },
  }

  action step: bool = any {
    admitClose,
    elapseDuringDrain,
    finishPriorOperation,
    flush,
    stopAndJoinHeartbeat,
    release,
    cleanup,
  }

  val heartbeatCoversCloseWork: bool =
    if (Set(Draining, Flushing, Flushed).contains(lifecycle.phase))
      lifecycle.heartbeatRunning
    else true

  val leaseCoversCloseFlush: bool =
    if (lifecycle.phase == Flushing) lifecycle.leaseLive else true

  val releaseFollowsHeartbeatJoin: bool =
    if (lifecycle.released)
      lifecycle.flushed and not(lifecycle.heartbeatRunning)
    else true

  val noRenewAfterRelease: bool = not(lifecycle.renewedAfterRelease)
  val closeReached: bool = lifecycle.phase == Closed
  val drainRenewalReached: bool = lifecycle.renewals > 0
}

module durableWriterLifecycleCorrected {
  import durableWriterLifecycle(
    STOP_HEARTBEAT_AT_ADMISSION_MUTANT = false
  ).* from "./durableWriterLifecycle"
}

module durableWriterLifecycleMutant {
  import durableWriterLifecycle(
    STOP_HEARTBEAT_AT_ADMISSION_MUTANT = true
  ).* from "./durableWriterLifecycle"
}
```

## Executable examples

The corrected trace is the runtime test's abstract projection: close admission,
renewal during drain, operation completion, flush, heartbeat stop/join, release,
and cleanup. The mutant is expected to violate heartbeat coverage immediately
after close admission and to lose the lease if time elapses during drain.

```quint target/formal/quint/durableWriterLifecycleTest.qnt +=
module durableWriterLifecycleCorrectedTest {
  import durableWriterLifecycle(
    STOP_HEARTBEAT_AT_ADMISSION_MUTANT = false
  ).* from "./durableWriterLifecycle"

  run closeDrainFlushReleaseTest =
    init
      .then(admitClose)
      .then(elapseDuringDrain)
      .then(finishPriorOperation)
      .then(flush)
      .then(stopAndJoinHeartbeat)
      .then(release)
      .then(cleanup)
      .expect(and {
        closeReached,
        drainRenewalReached,
        releaseFollowsHeartbeatJoin,
        noRenewAfterRelease,
      })
}

module durableWriterLifecycleMutantTest {
  import durableWriterLifecycle(
    STOP_HEARTBEAT_AT_ADMISSION_MUTANT = true
  ).* from "./durableWriterLifecycle"

  run stopAtAdmissionWitnessTest =
    init
      .then(admitClose)
      .expect(not(heartbeatCoversCloseWork))

  run expiryBeforeFlushWitnessTest =
    init
      .then(admitClose)
      .then(elapseDuringDrain)
      .then(finishPriorOperation)
      .expect(not(leaseCoversCloseFlush))
}
```

## Commands and evidence

Run the same pinned literate-model gate used by the head-CAS model:

```sh
scripts/check-durable-head-quint.sh
scripts/check-durable-head-quint.sh --verify
```

The first command tangles, typechecks, tests, and samples both lifecycle
variants. `--verify` additionally checks the corrected lifecycle invariants and
requires the stopped-at-admission mutant to produce a bounded counterexample.
