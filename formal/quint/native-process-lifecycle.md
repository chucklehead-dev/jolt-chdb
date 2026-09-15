# Native process lifecycle

chDB is an embedded process-global engine. Its C API permits another storage
path after every connection has closed, but the pinned upstream header warns
that repeatedly crossing that last-close/reinitialize boundary can corrupt the
allocator on macOS. `jolt-chdb` therefore adopts a stricter, driver-owned
policy: the first successful open creates a hidden connection that remains
owned until process exit. Public close releases only the public connection.

This is a conservative host-lifecycle policy, not a Durable V1 protocol rule.
It has three observable consequences:

- logical last close leaves the native engine and its original path anchored;
- reopening the same path does not initialize another engine; and
- a different path is rejected before a native connect call.

Independent physical paths, including independent Durable recovery scratch
directories, belong in separate processes. `:memory:` remains reusable within
one process because every public handle shares the one anchored in-memory
engine; a fresh process supplies a fresh in-memory engine.

The model projects bootstrap configuration into the claimed path. Runtime
tests separately require the first `:backups-allowed-path` to remain immutable;
the model does not claim to enumerate native command-line options.

The two constants below are executable negative controls. Dropping the anchor
at logical last close recreates the unsafe reinitialization boundary. Accepting
a different path makes the process path claim mutable.

Generated `.qnt` files live under `target/formal/quint/`; edit this Markdown,
not those files.

## State machine

```quint target/formal/quint/nativeProcessLifecycle.qnt +=
module nativeProcessLifecycle {
  type StoragePath = Memory | Disk
  type NativeAction = Initialized | Opened | Closed | RejectedDifferent

  type Lifecycle = {
    anchored: bool,
    path: StoragePath,
    references: int,
    boots: int,
    differentPathAccepted: bool,
    lastAction: NativeAction,
  }

  const DROP_ANCHOR_AT_LAST_CLOSE_MUTANT: bool
  const ACCEPT_DIFFERENT_PATH_MUTANT: bool
  var lifecycle: Lifecycle

  action init: bool =
    lifecycle' = {
      anchored: false,
      path: Memory,
      references: 0,
      boots: 0,
      differentPathAccepted: false,
      lastAction: Initialized,
    }

  action openMemory: bool = all {
    lifecycle.references < 2,
    not(lifecycle.anchored) or lifecycle.path == Memory,
    lifecycle' = {
      ...lifecycle,
      anchored: true,
      path: Memory,
      references: lifecycle.references + 1,
      boots: if (lifecycle.anchored) lifecycle.boots else lifecycle.boots + 1,
      lastAction: Opened,
    },
  }

  action closePublic: bool = all {
    lifecycle.references > 0,
    lifecycle' = {
      ...lifecycle,
      anchored:
        if (DROP_ANCHOR_AT_LAST_CLOSE_MUTANT and lifecycle.references == 1)
          false
        else lifecycle.anchored,
      references: lifecycle.references - 1,
      lastAction: Closed,
    },
  }

  action openDifferent: bool = all {
    lifecycle.boots > 0,
    lifecycle.references == 0,
    if (ACCEPT_DIFFERENT_PATH_MUTANT)
      lifecycle' = {
        ...lifecycle,
        anchored: true,
        path: Disk,
        references: 1,
        boots: lifecycle.boots + 1,
        differentPathAccepted: true,
        lastAction: Opened,
      }
    else
      lifecycle' = { ...lifecycle, lastAction: RejectedDifferent },
  }

  action step: bool = any { openMemory, closePublic, openDifferent }

  val anchorSurvivesLogicalLastClose: bool =
    if (lifecycle.boots > 0 and lifecycle.references == 0)
      lifecycle.anchored
    else true

  val engineInitializesAtMostOnce: bool = lifecycle.boots <= 1
  val processPathIsImmutable: bool = not(lifecycle.differentPathAccepted)
  val logicalLastCloseReached: bool =
    lifecycle.lastAction == Closed and lifecycle.references == 0
  val samePathReopenReached: bool =
    lifecycle.lastAction == Opened
      and lifecycle.path == Memory
      and lifecycle.boots == 1
      and lifecycle.references == 1
  val differentPathRejectionReached: bool =
    lifecycle.lastAction == RejectedDifferent
}

module nativeProcessLifecycleCorrected {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = false,
    ACCEPT_DIFFERENT_PATH_MUTANT = false
  ).* from "./nativeProcessLifecycle"
}

module nativeProcessLifecycleDropAnchorMutant {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = true,
    ACCEPT_DIFFERENT_PATH_MUTANT = false
  ).* from "./nativeProcessLifecycle"
}

module nativeProcessLifecycleDifferentPathMutant {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = false,
    ACCEPT_DIFFERENT_PATH_MUTANT = true
  ).* from "./nativeProcessLifecycle"
}
```

## Executable traces

The corrected trace opens, reaches logical last close without losing the
anchor, reopens the same path without another boot, closes again, and rejects a
different path. Each mutant has a direct causal witness rather than relying on
an unreachable invariant.

```quint target/formal/quint/nativeProcessLifecycleTest.qnt +=
module nativeProcessLifecycleCorrectedTest {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = false,
    ACCEPT_DIFFERENT_PATH_MUTANT = false
  ).* from "./nativeProcessLifecycle"

  run anchoredReopenAndRejectDifferentTest =
    init
      .then(openMemory)
      .then(closePublic)
      .then(openMemory)
      .then(closePublic)
      .then(openDifferent)
      .expect(and {
        anchorSurvivesLogicalLastClose,
        engineInitializesAtMostOnce,
        processPathIsImmutable,
        differentPathRejectionReached,
      })
}

module nativeProcessLifecycleDropAnchorMutantTest {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = true,
    ACCEPT_DIFFERENT_PATH_MUTANT = false
  ).* from "./nativeProcessLifecycle"

  run lastCloseDropsAnchorWitnessTest =
    init
      .then(openMemory)
      .then(closePublic)
      .expect(not(anchorSurvivesLogicalLastClose))
}

module nativeProcessLifecycleDifferentPathMutantTest {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = false,
    ACCEPT_DIFFERENT_PATH_MUTANT = true
  ).* from "./nativeProcessLifecycle"

  run differentPathReinitializesWitnessTest =
    init
      .then(openMemory)
      .then(closePublic)
      .then(openDifferent)
      .expect(and {
        not(engineInitializesAtMostOnce),
        not(processPathIsImmutable),
      })
}
```
