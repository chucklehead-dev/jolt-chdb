# Durable file-WAL spool lifecycle

This bounded, literate Quint model covers the Phase 2 transition from a
writer-held vector of WAL line byte arrays to one buffered file below the
writer's existing private scratch directory. It models the safety boundary,
not file-system crash durability, fsync behavior, object-store latency, or
native database contents.

The state is intentionally scalar. `pending` is the number of successfully
appended records, `spool` says whether their file is open or sealed, and
`checkpointRequired` represents the conservative recovery fallback after a
native mutation cannot be proven represented by a publishable WAL file.

| Model action | Runtime correspondence |
| --- | --- |
| `nativeSuccess` | native mutation returns before `append-wal!` |
| `appendSuccess` / `appendFailure` | buffered spool write succeeds / throws |
| `sealSuccess` / `sealFailure` | flush plus close makes the file eligible / fails after native work |
| `publishFailure` | immutable put, verify, or head CAS is not confirmed; sealed file and counters remain |
| `commit` | confirmed or reconciled head commit clears scalar state before local deletion |
| `deleteAfterClear` | best-effort staged-file deletion after the state transition |
| `closeAfterPublicationFailure` | close retains its first publication failure while continuing cleanup |

The four retained mutants are non-vacuity controls:

- append failure fails to require a checkpoint;
- sealed WAL publication is allowed despite checkpoint-required;
- an unconfirmed publication clears pending state;
- local deletion happens before the committed state is cleared.

The model makes no delivery, timing, S3, fsync, native-persistence, or host-RSS
claim. Runtime tests cover byte-exact JSONL, real memory-backend immutable
publication and CAS/reconciliation, and injected append/seal/cleanup failures.

Generated files are under `target/formal/quint/`; edit this Markdown rather
than the tangled files.

```quint target/formal/quint/durableFileWalSpool.qnt +=
module durableFileWalSpool {
  type Spool = NoSpool | OpenSpool | SealedSpool
  type State = {
    spool: Spool,
    nativeApplied: bool,
    pending: int,
    checkpointRequired: bool,
    walPublishAllowed: bool,
    walPublicationAttempted: bool,
    committed: bool,
    deleted: bool,
    firstError: int,
  }

  const APPEND_FAILURE_OMITS_CHECKPOINT_MUTANT: bool
  const ALLOW_WAL_WHEN_CHECKPOINT_REQUIRED_MUTANT: bool
  const CLEAR_ON_PUBLICATION_FAILURE_MUTANT: bool
  const DELETE_BEFORE_CLEAR_MUTANT: bool
  var state: State

  action init: bool =
    state' = {
      spool: NoSpool,
      nativeApplied: false,
      pending: 0,
      checkpointRequired: false,
      walPublishAllowed: true,
      walPublicationAttempted: false,
      committed: false,
      deleted: false,
      firstError: 0,
    }

  action nativeSuccess: bool = all {
    // This bounded model follows one spool generation.  A confirmed commit
    // clears that generation; the runtime may later allocate a distinct
    // spool, which is outside this scalar instance.
    not(state.committed),
    state.spool == NoSpool,
    state' = { ...state, spool: OpenSpool, nativeApplied: true },
  }

  action appendSuccess: bool = all {
    state.nativeApplied,
    state.spool == OpenSpool,
    state' = { ...state, pending: state.pending + 1 },
  }

  action appendFailure: bool = all {
    state.nativeApplied,
    state.spool == OpenSpool,
    state' = {
      ...state,
      checkpointRequired: not(APPEND_FAILURE_OMITS_CHECKPOINT_MUTANT),
      walPublishAllowed: APPEND_FAILURE_OMITS_CHECKPOINT_MUTANT,
      firstError: 1,
    },
  }

  action sealSuccess: bool = all {
    state.pending > 0,
    state.spool == OpenSpool,
    state' = { ...state, spool: SealedSpool },
  }

  action sealFailure: bool = all {
    state.pending > 0,
    state.spool == OpenSpool,
    state' = {
      ...state,
      spool: SealedSpool,
      checkpointRequired: true,
      walPublishAllowed: false,
      firstError: 1,
    },
  }

  action publishFailure: bool = all {
    state.spool == SealedSpool,
    state.walPublishAllowed
      or ALLOW_WAL_WHEN_CHECKPOINT_REQUIRED_MUTANT,
    state' = if (CLEAR_ON_PUBLICATION_FAILURE_MUTANT)
      { ...state, spool: NoSpool, pending: 0, firstError: 2,
        walPublicationAttempted: true }
    else
      { ...state, firstError: 2, walPublicationAttempted: true },
  }

  action commit: bool = all {
    state.spool == SealedSpool,
    state.walPublishAllowed
      or ALLOW_WAL_WHEN_CHECKPOINT_REQUIRED_MUTANT,
    state.pending > 0,
    state' = if (DELETE_BEFORE_CLEAR_MUTANT)
      { ...state, committed: true, deleted: true }
    else
      { ...state, spool: NoSpool, pending: 0, committed: true },
  }

  action deleteAfterClear: bool = all {
    state.committed,
    state.spool == NoSpool,
    state.pending == 0,
    not(state.deleted),
    state' = { ...state, deleted: true },
  }

  action closeAfterPublicationFailure: bool = all {
    state.firstError == 2,
    state.spool == SealedSpool,
    state' = { ...state, deleted: true },
  }

  action step: bool = any {
    nativeSuccess,
    appendSuccess,
    appendFailure,
    sealSuccess,
    sealFailure,
    publishFailure,
    commit,
    deleteAfterClear,
    closeAfterPublicationFailure,
  }

  val appendFailureRequiresCheckpoint: bool =
    // An append can fail after earlier records are already staged.  The
    // checkpoint obligation must therefore not depend on pending == 0.
    if (state.nativeApplied and state.firstError == 1)
      state.checkpointRequired
    else true

  val checkpointRequirementForbidsWal: bool =
    if (state.checkpointRequired) not(state.walPublicationAttempted) else true

  val unconfirmedPublicationRetainsSealedWork: bool =
    if (state.firstError == 2 and not(state.committed))
      state.spool == SealedSpool and state.pending > 0
    else true

  val deletionOnlyAfterClear: bool =
    if (state.deleted and state.committed)
      state.spool == NoSpool and state.pending == 0
    else true

  val closeRetainsFirstPublicationError: bool =
    if (state.deleted and state.firstError == 2 and not(state.committed))
      state.firstError == 2
    else true

  val committedDeletionReached: bool = state.committed and state.deleted
  val publicationFailureRetained: bool =
    state.firstError == 2 and state.spool == SealedSpool and state.pending > 0
}

module durableFileWalSpoolCorrected {
  import durableFileWalSpool(
    APPEND_FAILURE_OMITS_CHECKPOINT_MUTANT = false,
    ALLOW_WAL_WHEN_CHECKPOINT_REQUIRED_MUTANT = false,
    CLEAR_ON_PUBLICATION_FAILURE_MUTANT = false,
    DELETE_BEFORE_CLEAR_MUTANT = false
  ).* from "./durableFileWalSpool"
}

module durableFileWalSpoolAppendMutant {
  import durableFileWalSpool(
    APPEND_FAILURE_OMITS_CHECKPOINT_MUTANT = true,
    ALLOW_WAL_WHEN_CHECKPOINT_REQUIRED_MUTANT = false,
    CLEAR_ON_PUBLICATION_FAILURE_MUTANT = false,
    DELETE_BEFORE_CLEAR_MUTANT = false
  ).* from "./durableFileWalSpool"
}

module durableFileWalSpoolPublicationMutant {
  import durableFileWalSpool(
    APPEND_FAILURE_OMITS_CHECKPOINT_MUTANT = false,
    ALLOW_WAL_WHEN_CHECKPOINT_REQUIRED_MUTANT = true,
    CLEAR_ON_PUBLICATION_FAILURE_MUTANT = false,
    DELETE_BEFORE_CLEAR_MUTANT = false
  ).* from "./durableFileWalSpool"
}

module durableFileWalSpoolClearMutant {
  import durableFileWalSpool(
    APPEND_FAILURE_OMITS_CHECKPOINT_MUTANT = false,
    ALLOW_WAL_WHEN_CHECKPOINT_REQUIRED_MUTANT = false,
    CLEAR_ON_PUBLICATION_FAILURE_MUTANT = true,
    DELETE_BEFORE_CLEAR_MUTANT = false
  ).* from "./durableFileWalSpool"
}

module durableFileWalSpoolDeleteMutant {
  import durableFileWalSpool(
    APPEND_FAILURE_OMITS_CHECKPOINT_MUTANT = false,
    ALLOW_WAL_WHEN_CHECKPOINT_REQUIRED_MUTANT = false,
    CLEAR_ON_PUBLICATION_FAILURE_MUTANT = false,
    DELETE_BEFORE_CLEAR_MUTANT = true
  ).* from "./durableFileWalSpool"
}
```

## Executable traces and mutants

```quint target/formal/quint/durableFileWalSpoolTest.qnt +=
module durableFileWalSpoolCorrectedTest {
  import durableFileWalSpool(
    APPEND_FAILURE_OMITS_CHECKPOINT_MUTANT = false,
    ALLOW_WAL_WHEN_CHECKPOINT_REQUIRED_MUTANT = false,
    CLEAR_ON_PUBLICATION_FAILURE_MUTANT = false,
    DELETE_BEFORE_CLEAR_MUTANT = false
  ).* from "./durableFileWalSpool"

  run committedFileSpoolClearsBeforeDeletionTest =
    init.then(nativeSuccess).then(appendSuccess).then(sealSuccess)
      .then(commit).then(deleteAfterClear)
      .expect(and {
        committedDeletionReached,
        appendFailureRequiresCheckpoint,
        checkpointRequirementForbidsWal,
        unconfirmedPublicationRetainsSealedWork,
        deletionOnlyAfterClear,
      })

  run failedPublicationRetainsSealedSpoolTest =
    init.then(nativeSuccess).then(appendSuccess).then(sealSuccess)
      .then(publishFailure).then(closeAfterPublicationFailure)
      .expect(and {
        publicationFailureRetained,
        unconfirmedPublicationRetainsSealedWork,
        closeRetainsFirstPublicationError,
      })
}

module durableFileWalSpoolAppendMutantTest {
  import durableFileWalSpool(
    APPEND_FAILURE_OMITS_CHECKPOINT_MUTANT = true,
    ALLOW_WAL_WHEN_CHECKPOINT_REQUIRED_MUTANT = false,
    CLEAR_ON_PUBLICATION_FAILURE_MUTANT = false,
    DELETE_BEFORE_CLEAR_MUTANT = false
  ).* from "./durableFileWalSpool"

  run appendFailureWitnessTest =
    // This is intentionally after one earlier append.  The old predicate
    // mentioned pending == 0 and therefore made this mutant vacuous on the
    // state that actually matters for a buffered file spool.
    init.then(nativeSuccess).then(appendSuccess).then(appendFailure)
      .expect(not(appendFailureRequiresCheckpoint))
}

module durableFileWalSpoolPublicationMutantTest {
  import durableFileWalSpool(
    APPEND_FAILURE_OMITS_CHECKPOINT_MUTANT = false,
    ALLOW_WAL_WHEN_CHECKPOINT_REQUIRED_MUTANT = true,
    CLEAR_ON_PUBLICATION_FAILURE_MUTANT = false,
    DELETE_BEFORE_CLEAR_MUTANT = false
  ).* from "./durableFileWalSpool"

  run checkpointRequiredPublicationWitnessTest =
    init.then(nativeSuccess).then(appendSuccess).then(sealFailure)
      .then(publishFailure)
      .expect(not(checkpointRequirementForbidsWal))
}

module durableFileWalSpoolClearMutantTest {
  import durableFileWalSpool(
    APPEND_FAILURE_OMITS_CHECKPOINT_MUTANT = false,
    ALLOW_WAL_WHEN_CHECKPOINT_REQUIRED_MUTANT = false,
    CLEAR_ON_PUBLICATION_FAILURE_MUTANT = true,
    DELETE_BEFORE_CLEAR_MUTANT = false
  ).* from "./durableFileWalSpool"

  run publicationFailureClearsWitnessTest =
    init.then(nativeSuccess).then(appendSuccess).then(sealSuccess)
      .then(publishFailure)
      .expect(not(unconfirmedPublicationRetainsSealedWork))
}

module durableFileWalSpoolDeleteMutantTest {
  import durableFileWalSpool(
    APPEND_FAILURE_OMITS_CHECKPOINT_MUTANT = false,
    ALLOW_WAL_WHEN_CHECKPOINT_REQUIRED_MUTANT = false,
    CLEAR_ON_PUBLICATION_FAILURE_MUTANT = false,
    DELETE_BEFORE_CLEAR_MUTANT = true
  ).* from "./durableFileWalSpool"

  run deletionBeforeClearWitnessTest =
    init.then(nativeSuccess).then(appendSuccess).then(sealSuccess)
      .then(commit)
      .expect(not(deletionOnlyAfterClear))
}
```
