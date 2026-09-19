# Durable persistence observation lifecycle

`jdbc.chdb.durable/persistence-observation` is deliberately a local, closed
projection.  It is not a second Durable control plane: it neither rereads a
HEAD nor turns a successful native operation into a persistence fact.  This
small executable model records the stricter boundary used by the public API
and the internal reader/writer handles.

The model has one recovered reader or writer.  `confirmed` is private model
state representing the last validated recovery/control witness; `reported` is
the scalar/enum witness exposed by the projection.  A writer's native mutation
can become pending, fail definitely, or have an ambiguous control outcome.
Only a canonical WAL or checkpoint can advance a witness.  An ambiguous WAL is
not erased by a later WAL; a canonical checkpoint can clear the ambiguity
because it covers the complete native state.  A reader only has a recovered
snapshot and never claims an object-store-current view.

`beginPublicClose` models a caller-owned close. `beginForcedTeardown` models a
worker-owned terminal cleanup while the JDBC wrapper may still be structurally
open.  Both make the internal projection unavailable *before* the later native
teardown action.  Public JDBC lookup itself is intentionally outside this
model: the real extension boundary rejects a closed wrapper.  That distinction
is why the model represents `publicLive` separately from `nativeLive`.

This is a projection/refinement model only. It does not model a native engine,
payload bytes, delivery, timing, retry fairness, object-store freshness,
external readers, or proof that a backend operation persisted data. Its
`canonical` actions stand only for the existing validated `:committed` or
`:reconciled` control results consumed by `observation/confirmed!`.

Generated `.qnt` files live under `target/formal/quint/`; edit this Markdown,
not those files.

## State machine

```quint target/formal/quint/durablePersistenceObservation.qnt +=
module durablePersistenceObservation {
  type Role = Writer | Reader
  type Lifecycle = Open | Closing | NativeClosed
  type Availability = Available | Unavailable
  type ObservationState = Recovered | Snapshot | Pending | Unconfirmed | Confirmed
  type Boundary = RecoveryBoundary | WalBoundary | CheckpointBoundary
  type Persistence = NoPersistence | WalPersistence | CheckpointPersistence

  type ObservationStateRecord = {
    role: Role,
    lifecycle: Lifecycle,
    publicLive: bool,
    nativeLive: bool,
    availability: Availability,
    state: ObservationState,
    viewCurrent: bool,
    confirmedBoundary: Boundary,
    confirmedSequence: int,
    lastSuccessfulPersistence: Persistence,
    reportedBoundary: Boundary,
    reportedSequence: int,
    reportedLastSuccessfulPersistence: Persistence,
    events: int,
  }

  const WRITER_ROLE: bool
  var observation: ObservationStateRecord

  pure def initialState(role: Role): ObservationState =
    if (role == Writer) Recovered else Snapshot

  pure def initialViewCurrent(role: Role): bool = role == Writer

  action init: bool =
    observation' = {
      role: if (WRITER_ROLE) Writer else Reader,
      lifecycle: Open,
      publicLive: true,
      nativeLive: true,
      availability: Available,
      state: initialState(if (WRITER_ROLE) Writer else Reader),
      viewCurrent: initialViewCurrent(if (WRITER_ROLE) Writer else Reader),
      confirmedBoundary: RecoveryBoundary,
      confirmedSequence: 0,
      lastSuccessfulPersistence: NoPersistence,
      reportedBoundary: RecoveryBoundary,
      reportedSequence: 0,
      reportedLastSuccessfulPersistence: NoPersistence,
      events: 0,
    }

  action admitNativeMutation: bool = all {
    observation.role == Writer,
    observation.lifecycle == Open,
    observation.availability == Available,
    observation' = {
      ...observation,
      // A newly admitted mutation does not make an earlier ambiguous WAL
      // provable; preserve that status until a checkpoint covers it.
      state: if (observation.state == Unconfirmed) Unconfirmed else Pending,
      viewCurrent: false,
      events: observation.events + 1,
    },
  }

  action definitePersistenceFailure: bool = all {
    observation.role == Writer,
    observation.lifecycle == Open,
    observation.state == Pending,
    observation' = { ...observation, events: observation.events + 1 },
  }

  action ambiguousControlOutcome: bool = all {
    observation.role == Writer,
    observation.lifecycle == Open,
    observation.state == Pending,
    observation' = {
      ...observation,
      state: Unconfirmed,
      viewCurrent: false,
      events: observation.events + 1,
    },
  }

  action canonicalWal: bool = all {
    observation.role == Writer,
    observation.lifecycle == Open,
    observation.availability == Available,
    observation.state != Unconfirmed,
    observation' = {
      ...observation,
      state: Confirmed,
      viewCurrent: true,
      confirmedBoundary: WalBoundary,
      confirmedSequence: observation.confirmedSequence + 1,
      lastSuccessfulPersistence: WalPersistence,
      reportedBoundary: WalBoundary,
      reportedSequence: observation.confirmedSequence + 1,
      reportedLastSuccessfulPersistence: WalPersistence,
      events: observation.events + 1,
    },
  }

  // The real WAL commit can be canonical, but must not erase an earlier
  // ambiguous WAL: it is an event with no stronger projection.
  action canonicalWalAfterAmbiguity: bool = all {
    observation.role == Writer,
    observation.lifecycle == Open,
    observation.state == Unconfirmed,
    observation' = { ...observation, events: observation.events + 1 },
  }

  action canonicalCheckpoint: bool = all {
    observation.role == Writer,
    observation.lifecycle == Open,
    observation.availability == Available,
    observation' = {
      ...observation,
      state: Confirmed,
      viewCurrent: true,
      confirmedBoundary: CheckpointBoundary,
      confirmedSequence: observation.confirmedSequence + 1,
      lastSuccessfulPersistence: CheckpointPersistence,
      reportedBoundary: CheckpointBoundary,
      reportedSequence: observation.confirmedSequence + 1,
      reportedLastSuccessfulPersistence: CheckpointPersistence,
      events: observation.events + 1,
    },
  }

  action beginPublicClose: bool = all {
    observation.lifecycle == Open,
    observation' = {
      ...observation,
      lifecycle: Closing,
      publicLive: false,
      availability: Unavailable,
      viewCurrent: false,
      events: observation.events + 1,
    },
  }

  action beginForcedTeardown: bool = all {
    observation.lifecycle == Open,
    observation' = {
      ...observation,
      lifecycle: Closing,
      availability: Unavailable,
      viewCurrent: false,
      events: observation.events + 1,
    },
  }

  action finishNativeTeardown: bool = all {
    observation.lifecycle == Closing,
    observation.nativeLive,
    observation' = {
      ...observation,
      lifecycle: NativeClosed,
      nativeLive: false,
      events: observation.events + 1,
    },
  }

  action step: bool = any {
    admitNativeMutation,
    definitePersistenceFailure,
    ambiguousControlOutcome,
    canonicalWal,
    canonicalWalAfterAmbiguity,
    canonicalCheckpoint,
    beginPublicClose,
    beginForcedTeardown,
    finishNativeTeardown,
  }

  // Available projections retain exactly the private witness. This is stronger
  // than merely checking the sequence order, and excludes a fabricated kind,
  // sequence, or last-success value.
  val availableProjectionIsConfirmedWitness: bool =
    if (observation.availability == Available)
      and {
        observation.reportedBoundary == observation.confirmedBoundary,
        observation.reportedSequence == observation.confirmedSequence,
        observation.reportedLastSuccessfulPersistence
          == observation.lastSuccessfulPersistence,
      }
    else true

  val currentViewHasOwnedConfirmedWitness: bool =
    if (observation.viewCurrent)
      and {
        observation.role == Writer,
        observation.lifecycle == Open,
        observation.availability == Available,
        Set(Recovered, Confirmed).contains(observation.state),
      }
    else true

  val readerNeverClaimsCurrentHead: bool =
    if (observation.role == Reader) not(observation.viewCurrent) else true

  val ambiguityNeverClaimsCurrent: bool =
    if (observation.state == Unconfirmed) not(observation.viewCurrent) else true

  // This includes forced cleanup, where the public wrapper can remain live
  // while the worker owns potentially blocking native teardown.
  val teardownHidesObservationBeforeNativeClose: bool =
    if (Set(Closing, NativeClosed).contains(observation.lifecycle))
      observation.availability == Unavailable
    else true

  val publicCloseIsNotRequiredForForcedTeardown: bool =
    if (observation.lifecycle == Closing and observation.publicLive)
      observation.availability == Unavailable
    else true

  val pendingAndDefiniteFailureDoNotAdvanceWitness: bool =
    if (Set(Pending, Unconfirmed).contains(observation.state))
      and {
        observation.reportedBoundary == observation.confirmedBoundary,
        observation.reportedSequence == observation.confirmedSequence,
      }
    else true

  val noConfirmationReached: bool =
    observation.state == Pending and observation.events >= 2
  val walConfirmationReached: bool =
    observation.confirmedBoundary == WalBoundary
  val ambiguityReached: bool = observation.state == Unconfirmed
  val checkpointClearsAmbiguityReached: bool =
    observation.confirmedBoundary == CheckpointBoundary
      and observation.state == Confirmed
  val publicCloseBeforeNativeCloseReached: bool =
    observation.lifecycle == Closing
      and not(observation.publicLive)
      and observation.nativeLive
  val forcedTeardownBeforeNativeCloseReached: bool =
    observation.lifecycle == Closing
      and observation.publicLive
      and observation.nativeLive
}

```

## Executable boundary traces

The writer traces cover no confirmation, a canonical confirmation, ambiguity
that a later WAL cannot erase, then the checkpoint that can clear it. The
reader and close traces make the ownership distinction observable without
asserting native persistence or JDBC wrapper behavior that the projection does
not own.

```quint target/formal/quint/durablePersistenceObservationTest.qnt +=
module durablePersistenceObservationWriterTest {
  import durablePersistenceObservation(WRITER_ROLE = true).*
    from "./durablePersistenceObservation"

  run noConfirmationKeepsRecoveredWitnessTest =
    init
      .then(admitNativeMutation)
      .then(definitePersistenceFailure)
      .expect(and {
        noConfirmationReached,
        availableProjectionIsConfirmedWitness,
        pendingAndDefiniteFailureDoNotAdvanceWitness,
      })

  run canonicalWalEstablishesWitnessTest =
    init
      .then(admitNativeMutation)
      .then(canonicalWal)
      .expect(and {
        walConfirmationReached,
        currentViewHasOwnedConfirmedWitness,
        availableProjectionIsConfirmedWitness,
      })

  run ambiguityNeedsCheckpointTest =
    init
      .then(admitNativeMutation)
      .then(ambiguousControlOutcome)
      .then(admitNativeMutation)
      .then(canonicalWalAfterAmbiguity)
      .expect(and {
        ambiguityReached,
        ambiguityNeverClaimsCurrent,
        pendingAndDefiniteFailureDoNotAdvanceWitness,
      })

  run checkpointClearsAmbiguityTest =
    init
      .then(admitNativeMutation)
      .then(ambiguousControlOutcome)
      .then(canonicalCheckpoint)
      .expect(and {
        checkpointClearsAmbiguityReached,
        currentViewHasOwnedConfirmedWitness,
        availableProjectionIsConfirmedWitness,
      })

  run publicCloseHidesBeforeNativeTeardownTest =
    init
      .then(beginPublicClose)
      .expect(and {
        publicCloseBeforeNativeCloseReached,
        teardownHidesObservationBeforeNativeClose,
      })

  run forcedTeardownHidesBeforeNativeTeardownTest =
    init
      .then(beginForcedTeardown)
      .expect(and {
        forcedTeardownBeforeNativeCloseReached,
        teardownHidesObservationBeforeNativeClose,
        publicCloseIsNotRequiredForForcedTeardown,
      })
}

module durablePersistenceObservationReaderTest {
  import durablePersistenceObservation(WRITER_ROLE = false).*
    from "./durablePersistenceObservation"

  run snapshotNeverClaimsCurrentHeadTest =
    init.expect(and {
      readerNeverClaimsCurrentHead,
      availableProjectionIsConfirmedWitness,
    })

  run forcedReaderTeardownHidesBeforeNativeCloseTest =
    init
      .then(beginForcedTeardown)
      .expect(and {
        forcedTeardownBeforeNativeCloseReached,
        readerNeverClaimsCurrentHead,
        teardownHidesObservationBeforeNativeClose,
      })
}
```
