# Durable Quint, Hegel, and aspect trace validation

The Durable head-CAS correctness loop uses one command vocabulary across the
formal model, implementation replay, and compiler aspects:

| Command | Current implementation seam |
| --- | --- |
| `acquire` | `jdbc.chdb.durable.control/acquire!` |
| `publish` | `jdbc.chdb.durable.control/publish-wal-bytes!` |
| `commit-attempt` | `jdbc.chdb.durable.control/commit-reference!` |
| `renew-attempt` | `jdbc.chdb.durable.control/renew!` |
| `checkpoint-publish` | `jdbc.chdb.durable.control/publish-checkpoint-file!` |
| `release-attempt` | `jdbc.chdb.durable.control/release!` |

The literate Quint specification produces ADR-015 ITF traces with
`mbt::actionTaken` and `mbt::nondetPicks`. The checked fixture under
`formal/quint/traces/` is replayed by
`test/jdbc/chdb_durable_itf_test.clj`. After every command, the driver compares
the implementation outcome and abstract head with the corresponding complete
ITF state. It also records a bounded pair of lifecycle events:

```clojure
{:seq 1
 :operation-id 1
 :phase :invoke
 :operation :durable/acquire
 :input {:op :acquire :writer :writer-2}}

{:seq 2
 :operation-id 1
 :phase :return
 :value {:outcome :acquired
         :head {:generation 2 :owner :writer-2
                :sequence 0 :reference nil}}}
```

Hegel first checks the explicit `hegel.operation-events` revision 1 envelope,
including contiguous sequence, complete invoke/terminal lifecycles, parentage,
causal links, and context. Its Durable model then checks known outcomes and
monotonic generation and manifest sequence. Those checks run after the
operation completes. They must not run inside aspect advice:
Jolt advice is fail-open, so an assertion thrown by advice is not a reliable
test verdict.

## Aspect integration boundary

An observation-only aspect pack can record `acquire!`, `publish-wal-bytes!`,
`publish-checkpoint-file!`, `commit-reference!`, `renew!`, and `release!`
without changing application behavior.
Advice should emit bounded, privacy-shaped inputs and completed outcomes into
a journal, then an ordinary test should snapshot and validate that journal.
It must retain a plain, non-woven test lane.

Do not instrument `ObjectBackend/put-bytes-if-absent!` and guess that every
call is a semantic Durable publication. The backend operation lacks the
writer/token context required by the exact reference model and also serves
head creation and unrelated immutable writes. `publish-wal-bytes!` and
`publish-checkpoint-file!` are the sound immutable-publication join points. At
the higher layer, writer `execute!`, `flush!`, `checkpoint!`, and `close!` are
the semantic queue boundaries. Advice should choose one layer per journal
rather than double-counting both.

The actual observation provider now lives in the sibling `jolt-aspect-packs`
repository. Its woven scenario executes this control implementation against the
in-memory backend, emits privacy-shaped commands, and checks the completed
journal offline. The ITF replay remains the model-generated direction of the
bridge; neither artifact replaces the other.

## What is and is not model checking

Model checking explores every transition permitted by the bounded Quint
model. Replaying an ITF trace or an aspect journal validates one execution; it
does not turn that execution into exhaustive model checking.

There are two sound directions:

1. Quint generates commands and expected states; the implementation executes
   them and aspects or an explicit journal record the concrete behavior.
2. A real woven execution records the same commands, outcomes, and head
   observations; an offline validator checks that sequence against the Quint
   transition relation and retains it as a deterministic regression trace.

Observed values can also inform model bounds and witness families. They must
not silently restrict the model to only previously observed behavior.

For a real aspect journal, validate in this order:

1. Compiler report and provider provenance.
2. Journal envelope and contiguous, closed lifecycle structure.
3. Durable command and privacy shape.
4. Outcome/head agreement with the Quint transition relation.
5. Mutation controls and a valid acknowledged-commit boundary.

Keep the original ITF or aspect journal as the lossless evidence artifact.
Derived command JSON is a driver convenience, not the state oracle.

## Repeatable development and CI loop

Use one transition vocabulary but keep four different claims explicit:

1. The literate Markdown is tangled, typechecked, deterministically tested,
   and mutation-tested. This catches specification and extraction drift.
2. Apalache explores the bounded model and proves or finds counterexamples for
   the selected invariants. It says nothing directly about Clojure execution.
3. Quint ITF traces drive the real control implementation and compare every
   abstract state. Hegel validates both generated state-machine traces and the
   versioned operation-event envelope.
4. Instrumented app tests record the same privacy-shaped commands and outcomes;
   the offline validator accepts them only when the Quint transition relation
   can replay the whole trace.

The intended CI split is:

- every change: tangle, typecheck, deterministic Quint tests, sampling,
  focused Jolt suites, checked ITF replay, and Hegel properties;
- formal/model changes: the bounded Apalache corrected invariants plus required
  mutant counterexamples;
- integration/release: the oscope local-root Durable lane against native chDB,
  followed by the same offline journal validator once the aspect provider is
  wired.

An aspect journal is therefore a trace-validation input, never the model
checker itself. The same normalized commands can be fed to Quint as a fixed
execution constraint for replay, while unconstrained Quint verification
continues to explore behaviors the application test did not happen to observe.
