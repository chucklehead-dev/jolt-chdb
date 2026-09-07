# Proposed publication-attempt refinement

Status: **approved and applied to the authoritative executable model on
2026-09-07**.

This change was approved before editing `formal/quint/durable-head-cas.md`. It
follows from comparing the prior
content-identity abstraction with the pinned Durable V1 rule that every WAL or
checkpoint publication attempt uses a fresh UUIDv4-derived key.

## Current abstraction

The current reference is:

```quint
type Reference = {
  objectId: ObjectId,
  generation: int,
  sequence: int,
}
```

`ObjectId` identifies immutable content. Concrete UUID attempt tokens are
erased, so more than one concrete publication can map to the same abstract
reference. The Jolt ITF driver records that many-to-one map and has replayed a
64-trace deterministic corpus, but the erasure itself is not represented as a
separate checked Quint refinement.

## Proposed exact state

Keep two bounded content identities and add six attempt identities, enough for
the complete six-transition horizon:

```quint
type AttemptId =
  | Attempt1
  | Attempt2
  | Attempt3
  | Attempt4
  | Attempt5
  | Attempt6

type Reference = {
  objectId: ObjectId,
  attemptId: AttemptId,
  generation: int,
  sequence: int,
}
```

Add `usedAttempts: Set[AttemptId]` to state. `published` remains a set of exact
references. `PublishEventData` and `CommitEventData` carry `attemptId`.

## Proposed transitions

`publish(writer, objectId, attemptId)` is enabled only when:

- the writer has a remembered generation;
- `attemptId` is absent from `usedAttempts`; and
- the exact reference is not already in `published`.

It derives generation from the writer token and sequence from
`head.sequence + 1`, then adds both the exact reference and the attempt ID.
There is no transition that reuses an attempt ID. Backend retry/reconciliation
within one API call is one atomic publication action, not another protocol
publication.

`commit(writer, objectId, attemptId, mode)` reconstructs the exact candidate
from the writer generation, current next sequence, and chosen attempt. It
commits only if that exact candidate is already published. A different or
never-published attempt produces `ObjectUnverified`; stale ownership is still
checked first.

Acquisition and release are unchanged. In particular, acquisition may leave a
previously committed older-generation reference in the head, so global head
canonicality continues to require `reference.generation <= head.generation`.
The acknowledged-commit rule continues to require equality with the committing
token for the newly installed reference.

## Refinement maps

Define two explicit projections:

1. `eraseAttemptReference` removes only `attemptId`, producing the current
   `{objectId, generation, sequence}` reference.
2. `projectReference` additionally removes generation and sequence, producing
   the existing content-only view.

Maintain an inductive per-transition monitor for each projection:

- `attemptTransitionsRefineExactView`
- `exactTransitionsRefineContentView`

The first establishes that fresh physical-attempt behavior refines the current
sequenced reference model. The second retains the already-checked relationship
to the content-only abstraction. Checking the two smaller simulations avoids a
single solver-expensive whole-history quantifier.

## Required invariants and controls

Corrected invariants:

- `publicationAttemptIsFresh`: a `Published` event's attempt is absent before
  and present after the transition.
- `attemptIdentityIsUnique`: no two distinct published references share an
  attempt ID.
- `headReferenceWasPublished`: the full attempt-bearing head reference is in
  `published`.
- `headReferenceIsCanonical`: sequence equals the head sequence and generation
  does not exceed the current lease generation.
- `acknowledgedCommitIsExact`: a successful commit uses the token generation,
  next sequence, selected attempt, and a previously published exact reference.
- both inductive refinement monitors remain true.

Mutation controls:

- reuse an already-used attempt ID;
- write the wrong generation;
- write the wrong sequence; and
- commit a different attempt ID than the published one.

Each mutant must have one deterministic witness and a bounded Apalache
counterexample while the corrected model passes the same property.

## ITF and implementation migration

- Add `attemptId` to `choosePublish` and `chooseCommit` nondeterministic picks.
- Add the attempt to `scripts/durable-head-itf-commands.jq` output.
- Map each model `AttemptId` to exactly one runtime UUID-bearing physical
  reference during replay.
- Reject a second publication using the same model attempt.
- Require distinct model attempts to produce distinct physical keys.
- Keep the full ADR-015 state as oracle; do not trust only command projection.
- Regenerate the checked fixture and replay at least 64 deterministic traces.

The production `publish-wal-bytes!` API remains unchanged: it creates a fresh
UUIDv4 token internally for each call. Attempt selection exists only in the
finite formal model and test adapter; callers cannot inject production object
keys.

## Verification gates

After approval:

1. Tangle and typecheck both generated Quint modules.
2. Run all corrected deterministic tests and every mutation witness.
3. Sample 10,000 six-step traces with nonzero acquisition, publication,
   confirmed/reconciled/ambiguous commit, stale rejection, and release
   witnesses.
4. Replay the deterministic 64-trace ITF corpus against Jolt.
5. Model-check freshness, exact-reference canonicality, stale-writer safety,
   and both refinement monitors separately through six transitions.
6. Demand bounded counterexamples from all four mutants.
7. Run the focused Durable control/ITF tests and the aggregate Jolt suite.

Rollback criterion: if adding attempt identity makes a current valid boundary
unreachable or either refinement fails, stop and inspect the counterexample;
do not weaken the projection or invariant.
