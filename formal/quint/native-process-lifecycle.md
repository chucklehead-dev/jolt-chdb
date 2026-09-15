# Native process lifecycle

chDB is an embedded process-global engine. Its C API permits another storage
path after every connection has closed, but the pinned upstream header warns
that repeatedly crossing that last-close/reinitialize boundary can corrupt the
allocator on macOS. `jolt-chdb` therefore adopts a stricter, driver-owned
policy: the first successful open creates a hidden connection that remains
owned across logical last close. A process shutdown hook explicitly closes the
hidden owner before host runtime teardown. Public close releases only the
public connection.

This is a conservative host-lifecycle policy, not a Durable V1 protocol rule.
It has three observable consequences:

- logical last close leaves the native engine and its original path anchored;
- reopening the same path does not initialize another engine; and
- a different path is rejected before a native connect call.

Independent physical paths, including independent Durable recovery scratch
directories, belong in separate processes. `:memory:` remains reusable within
one process because every public handle shares the one anchored in-memory
engine; a fresh process supplies a fresh in-memory engine.

The model keeps path identity and bootstrap-option identity separate. It does
not enumerate native command-line serialization; `BackupsA` and `BackupsB`
are abstract identities sufficient to check that a successful bootstrap fixes
the options for every later public connection.

A failure before entering `chdb_connect`, or its documented null-owner return,
is safe to retry because it owns no native engine. Any other exception after
entering the native call has uncertain engine ownership and makes the process
lifecycle terminal. A terminal lifecycle rejects every later open.

The host-exit transition is intentionally narrower than the Durable protocol:
it represents an orderly process exit after every public owner has closed. It
requires the retained anchor to be claimed and closed exactly once before the
host tears down native libraries. Leaked public connections and OS-forced
termination remain host concerns outside this state machine.

The five constants below are executable negative controls. They cover dropping
the anchor, accepting a different path, retrying after uncertain bootstrap
failure, replacing the successful bootstrap's option identity, and skipping
the anchor close at orderly process exit.

Generated `.qnt` files live under `target/formal/quint/`; edit this Markdown,
not those files.

## State machine

```quint target/formal/quint/nativeProcessLifecycle.qnt +=
module nativeProcessLifecycle {
  type StoragePath = Memory | Disk
  type BootstrapOptions = NoBackups | BackupsA | BackupsB
  type NativeAction =
    Initialized | Opened | Closed | RejectedDifferent | RejectedOptions
      | BootstrapFailedSafe | BootstrapFailedTerminal | RejectedTerminal
      | ProcessExited

  type Lifecycle = {
    anchored: bool,
    terminal: bool,
    path: StoragePath,
    bootstrapOptions: BootstrapOptions,
    references: int,
    boots: int,
    differentPathAccepted: bool,
    optionChangeAccepted: bool,
    terminalRetryAccepted: bool,
    uncertainBootstrapFailureOccurred: bool,
    samePathReopened: bool,
    hostExitStarted: bool,
    anchorCloseCount: int,
    lastAction: NativeAction,
  }

  const DROP_ANCHOR_AT_LAST_CLOSE_MUTANT: bool
  const ACCEPT_DIFFERENT_PATH_MUTANT: bool
  const RETRY_UNCERTAIN_BOOTSTRAP_MUTANT: bool
  const ACCEPT_DIFFERENT_OPTIONS_MUTANT: bool
  const SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT: bool
  var lifecycle: Lifecycle

  action init: bool =
    lifecycle' = {
      anchored: false,
      terminal: false,
      path: Memory,
      bootstrapOptions: NoBackups,
      references: 0,
      boots: 0,
      differentPathAccepted: false,
      optionChangeAccepted: false,
      terminalRetryAccepted: false,
      uncertainBootstrapFailureOccurred: false,
      samePathReopened: false,
      hostExitStarted: false,
      anchorCloseCount: 0,
      lastAction: Initialized,
    }

  action openMemory: bool = all {
    not(lifecycle.hostExitStarted),
    lifecycle.references < 2,
    not(lifecycle.terminal),
    not(lifecycle.anchored) or lifecycle.path == Memory,
    lifecycle' = {
      ...lifecycle,
      anchored: true,
      path: Memory,
      bootstrapOptions: NoBackups,
      references: lifecycle.references + 1,
      boots: if (lifecycle.anchored) lifecycle.boots else lifecycle.boots + 1,
      samePathReopened: lifecycle.anchored,
      lastAction: Opened,
    },
  }

  action closePublic: bool = all {
    not(lifecycle.hostExitStarted),
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
    not(lifecycle.hostExitStarted),
    lifecycle.boots > 0,
    lifecycle.references == 0,
    not(lifecycle.terminal),
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

  action openDiskWithBackups: bool = all {
    not(lifecycle.hostExitStarted),
    not(lifecycle.anchored),
    not(lifecycle.terminal),
    lifecycle.boots == 0,
    lifecycle' = {
      ...lifecycle,
      anchored: true,
      path: Disk,
      bootstrapOptions: BackupsA,
      references: 1,
      boots: 1,
      lastAction: Opened,
    },
  }

  action openDifferentOptions: bool = all {
    not(lifecycle.hostExitStarted),
    lifecycle.anchored,
    not(lifecycle.terminal),
    lifecycle.path == Disk,
    lifecycle.references == 0,
    if (ACCEPT_DIFFERENT_OPTIONS_MUTANT)
      lifecycle' = {
        ...lifecycle,
        bootstrapOptions: BackupsB,
        references: 1,
        optionChangeAccepted: true,
        lastAction: Opened,
      }
    else
      lifecycle' = { ...lifecycle, lastAction: RejectedOptions },
  }

  action safeBootstrapFailure: bool = all {
    not(lifecycle.hostExitStarted),
    not(lifecycle.anchored),
    not(lifecycle.terminal),
    lifecycle.boots == 0,
    lifecycle' = { ...lifecycle, lastAction: BootstrapFailedSafe },
  }

  action uncertainBootstrapFailure: bool = all {
    not(lifecycle.hostExitStarted),
    not(lifecycle.anchored),
    not(lifecycle.terminal),
    lifecycle.boots == 0,
    lifecycle' = {
      ...lifecycle,
      terminal: not(RETRY_UNCERTAIN_BOOTSTRAP_MUTANT),
      uncertainBootstrapFailureOccurred: true,
      lastAction: BootstrapFailedTerminal,
    },
  }

  action retryAfterTerminal: bool = all {
    not(lifecycle.hostExitStarted),
    lifecycle.uncertainBootstrapFailureOccurred,
    lifecycle.lastAction == BootstrapFailedTerminal,
    if (RETRY_UNCERTAIN_BOOTSTRAP_MUTANT)
      lifecycle' = {
        ...lifecycle,
        anchored: true,
        terminal: false,
        references: 1,
        boots: 1,
        terminalRetryAccepted: true,
        lastAction: Opened,
      }
    else
      lifecycle' = { ...lifecycle, lastAction: RejectedTerminal },
  }

  action processExit: bool = all {
    // This host-lifecycle transition models only an orderly exit after logical
    // last close. refs > 0 means a leaked public owner; forced teardown and its
    // cleanup ordering are deliberately outside the Durable protocol model.
    not(lifecycle.hostExitStarted),
    lifecycle.anchored,
    lifecycle.references == 0,
    not(lifecycle.terminal),
    lifecycle' = {
      ...lifecycle,
      anchored: SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT,
      hostExitStarted: true,
      anchorCloseCount:
        if (SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT)
          lifecycle.anchorCloseCount
        else lifecycle.anchorCloseCount + 1,
      lastAction: ProcessExited,
    },
  }

  action step: bool = any {
    openMemory,
    closePublic,
    openDifferent,
    openDiskWithBackups,
    openDifferentOptions,
    safeBootstrapFailure,
    uncertainBootstrapFailure,
    retryAfterTerminal,
    processExit,
  }

  val anchorSurvivesLogicalLastClose: bool =
    if (lifecycle.boots > 0 and lifecycle.references == 0
        and not(lifecycle.hostExitStarted))
      lifecycle.anchored
    else true

  val engineInitializesAtMostOnce: bool = lifecycle.boots <= 1
  val processPathIsImmutable: bool = not(lifecycle.differentPathAccepted)
  val bootstrapOptionsAreImmutable: bool = not(lifecycle.optionChangeAccepted)
  val uncertainBootstrapFailureIsTerminal: bool =
    if (lifecycle.uncertainBootstrapFailureOccurred)
      lifecycle.terminal
    else true
  val terminalLifecycleIsClosed: bool =
    if (lifecycle.terminal)
      not(lifecycle.anchored) and lifecycle.references == 0
    else true
  val terminalRetryIsRejected: bool = not(lifecycle.terminalRetryAccepted)
  val processExitReleasesAnchorExactlyOnce: bool =
    if (lifecycle.hostExitStarted)
      not(lifecycle.anchored) and lifecycle.anchorCloseCount == 1
    else lifecycle.anchorCloseCount == 0
  val logicalLastCloseReached: bool =
    lifecycle.lastAction == Closed and lifecycle.references == 0
  val samePathReopenReached: bool =
    lifecycle.lastAction == Opened
      and lifecycle.path == Memory
      and lifecycle.boots == 1
      and lifecycle.references == 1
  val differentPathRejectionReached: bool =
    lifecycle.lastAction == RejectedDifferent
  val differentOptionsRejectionReached: bool =
    lifecycle.lastAction == RejectedOptions
  val terminalRejectionReached: bool =
    lifecycle.lastAction == RejectedTerminal
  val safeBootstrapFailureRemainsCold: bool =
    if (lifecycle.lastAction == BootstrapFailedSafe)
      not(lifecycle.anchored) and not(lifecycle.terminal) and lifecycle.boots == 0
    else true
  val orderlyProcessExitReached: bool =
    lifecycle.lastAction == ProcessExited
      and lifecycle.hostExitStarted
      and lifecycle.references == 0
}

module nativeProcessLifecycleCorrected {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = false,
    ACCEPT_DIFFERENT_PATH_MUTANT = false,
    RETRY_UNCERTAIN_BOOTSTRAP_MUTANT = false,
    ACCEPT_DIFFERENT_OPTIONS_MUTANT = false,
    SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT = false
  ).* from "./nativeProcessLifecycle"
}

module nativeProcessLifecycleDropAnchorMutant {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = true,
    ACCEPT_DIFFERENT_PATH_MUTANT = false,
    RETRY_UNCERTAIN_BOOTSTRAP_MUTANT = false,
    ACCEPT_DIFFERENT_OPTIONS_MUTANT = false,
    SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT = false
  ).* from "./nativeProcessLifecycle"
}

module nativeProcessLifecycleDifferentPathMutant {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = false,
    ACCEPT_DIFFERENT_PATH_MUTANT = true,
    RETRY_UNCERTAIN_BOOTSTRAP_MUTANT = false,
    ACCEPT_DIFFERENT_OPTIONS_MUTANT = false,
    SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT = false
  ).* from "./nativeProcessLifecycle"
}

module nativeProcessLifecycleTerminalRetryMutant {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = false,
    ACCEPT_DIFFERENT_PATH_MUTANT = false,
    RETRY_UNCERTAIN_BOOTSTRAP_MUTANT = true,
    ACCEPT_DIFFERENT_OPTIONS_MUTANT = false,
    SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT = false
  ).* from "./nativeProcessLifecycle"
}

module nativeProcessLifecycleDifferentOptionsMutant {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = false,
    ACCEPT_DIFFERENT_PATH_MUTANT = false,
    RETRY_UNCERTAIN_BOOTSTRAP_MUTANT = false,
    ACCEPT_DIFFERENT_OPTIONS_MUTANT = true,
    SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT = false
  ).* from "./nativeProcessLifecycle"
}

module nativeProcessLifecycleSkipAnchorExitCloseMutant {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = false,
    ACCEPT_DIFFERENT_PATH_MUTANT = false,
    RETRY_UNCERTAIN_BOOTSTRAP_MUTANT = false,
    ACCEPT_DIFFERENT_OPTIONS_MUTANT = false,
    SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT = true
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
    ACCEPT_DIFFERENT_PATH_MUTANT = false,
    RETRY_UNCERTAIN_BOOTSTRAP_MUTANT = false,
    ACCEPT_DIFFERENT_OPTIONS_MUTANT = false,
    SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT = false
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
        bootstrapOptionsAreImmutable,
        differentPathRejectionReached,
      })

  run terminalBootstrapFailureTest =
    init
      .then(uncertainBootstrapFailure)
      .then(retryAfterTerminal)
      .expect(and {
        uncertainBootstrapFailureIsTerminal,
        terminalLifecycleIsClosed,
        terminalRetryIsRejected,
        terminalRejectionReached,
      })

  run bootstrapOptionsImmutableTest =
    init
      .then(openDiskWithBackups)
      .then(closePublic)
      .then(openDifferentOptions)
      .expect(and {
        bootstrapOptionsAreImmutable,
        differentOptionsRejectionReached,
      })

  run safeBootstrapFailureTest =
    init.then(safeBootstrapFailure).expect(safeBootstrapFailureRemainsCold)

  run orderlyProcessExitClosesAnchorTest =
    init
      .then(openMemory)
      .then(closePublic)
      .then(processExit)
      .expect(and {
        processExitReleasesAnchorExactlyOnce,
        orderlyProcessExitReached,
      })
}

module nativeProcessLifecycleDropAnchorMutantTest {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = true,
    ACCEPT_DIFFERENT_PATH_MUTANT = false,
    RETRY_UNCERTAIN_BOOTSTRAP_MUTANT = false,
    ACCEPT_DIFFERENT_OPTIONS_MUTANT = false,
    SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT = false
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
    ACCEPT_DIFFERENT_PATH_MUTANT = true,
    RETRY_UNCERTAIN_BOOTSTRAP_MUTANT = false,
    ACCEPT_DIFFERENT_OPTIONS_MUTANT = false,
    SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT = false
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

module nativeProcessLifecycleTerminalRetryMutantTest {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = false,
    ACCEPT_DIFFERENT_PATH_MUTANT = false,
    RETRY_UNCERTAIN_BOOTSTRAP_MUTANT = true,
    ACCEPT_DIFFERENT_OPTIONS_MUTANT = false,
    SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT = false
  ).* from "./nativeProcessLifecycle"

  run uncertainFailureRetryWitnessTest =
    init
      .then(uncertainBootstrapFailure)
      .then(retryAfterTerminal)
      .expect(and {
        not(uncertainBootstrapFailureIsTerminal),
        not(terminalRetryIsRejected),
      })
}

module nativeProcessLifecycleDifferentOptionsMutantTest {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = false,
    ACCEPT_DIFFERENT_PATH_MUTANT = false,
    RETRY_UNCERTAIN_BOOTSTRAP_MUTANT = false,
    ACCEPT_DIFFERENT_OPTIONS_MUTANT = true,
    SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT = false
  ).* from "./nativeProcessLifecycle"

  run differentOptionsAcceptedWitnessTest =
    init
      .then(openDiskWithBackups)
      .then(closePublic)
      .then(openDifferentOptions)
      .expect(not(bootstrapOptionsAreImmutable))
}

module nativeProcessLifecycleSkipAnchorExitCloseMutantTest {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = false,
    ACCEPT_DIFFERENT_PATH_MUTANT = false,
    RETRY_UNCERTAIN_BOOTSTRAP_MUTANT = false,
    ACCEPT_DIFFERENT_OPTIONS_MUTANT = false,
    SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT = true
  ).* from "./nativeProcessLifecycle"

  run skippedAnchorExitCloseWitnessTest =
    init
      .then(openMemory)
      .then(closePublic)
      .then(processExit)
      .expect(not(processExitReleasesAnchorExactlyOnce))
}

module nativeProcessLifecycleAnchorTrace {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = false,
    ACCEPT_DIFFERENT_PATH_MUTANT = false,
    RETRY_UNCERTAIN_BOOTSTRAP_MUTANT = false,
    ACCEPT_DIFFERENT_OPTIONS_MUTANT = false,
    SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT = false
  ).* from "./nativeProcessLifecycle"

  action traceStep: bool =
    if (lifecycle.lastAction == Initialized) openMemory
    else if (lifecycle.lastAction == Opened) closePublic
    else if (not(lifecycle.samePathReopened)) openMemory
    else openDifferent
}

module nativeProcessLifecycleTerminalTrace {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = false,
    ACCEPT_DIFFERENT_PATH_MUTANT = false,
    RETRY_UNCERTAIN_BOOTSTRAP_MUTANT = false,
    ACCEPT_DIFFERENT_OPTIONS_MUTANT = false,
    SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT = false
  ).* from "./nativeProcessLifecycle"

  action traceStep: bool =
    if (lifecycle.lastAction == Initialized) uncertainBootstrapFailure
    else retryAfterTerminal
}

module nativeProcessLifecycleOptionsTrace {
  import nativeProcessLifecycle(
    DROP_ANCHOR_AT_LAST_CLOSE_MUTANT = false,
    ACCEPT_DIFFERENT_PATH_MUTANT = false,
    RETRY_UNCERTAIN_BOOTSTRAP_MUTANT = false,
    ACCEPT_DIFFERENT_OPTIONS_MUTANT = false,
    SKIP_ANCHOR_CLOSE_AT_PROCESS_EXIT_MUTANT = false
  ).* from "./nativeProcessLifecycle"

  action traceStep: bool =
    if (lifecycle.lastAction == Initialized) openDiskWithBackups
    else if (lifecycle.lastAction == Opened) closePublic
    else openDifferentOptions
}
```
