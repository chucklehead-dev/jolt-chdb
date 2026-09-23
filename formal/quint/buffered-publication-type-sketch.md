# Buffered publication: proposed type sketch for #190

Status: caller contract approved for a bounded executable model. The companion
[`buffered-publication.md`](buffered-publication.md) models it. Neither document
approves a buffered runtime mode or changes the default confirmed contract.

## Approved bounded caller contract

`:admitted` means only that this live writer accepted the request into its
bounded queue. It is not a persistence receipt and does not authorize consumer
cleanup. A separate ticket/flush result confirms publication. A full queue
rejects before admission. Normal successful close drains admitted work,
confirms its publication, joins the heartbeat, and releases the lease. Fresh
readers see only work referenced by the committed head; no local-writer
read-after-write promise is made by admission. The model leaves exact API
names, timeout/error payloads, and runtime implementation to a later slice.

## Why model this boundary

The exact-SQL control in [#187](https://github.com/chucklehead-dev/jolt-chdb/issues/187#issuecomment-5787579844)
preconstructed the exporter ABBA SQL (SHA-256
`94796b1de490cd43c28bcf5b25cef0828994e3d1f1ea3c3652544827d2910b32`),
then called `execute-and-flush!` once per 512-row export. The 20 individually
confirmed calls measured 59.459 ms p50 and 93.648 ms p99, above the 20.48 and
25.6 ms target ceilings. All calls returned `:committed`; a fresh reader
recovered all 11,776 rows with matching full-row digest. This is a local,
preconstructed-SQL control on private Jolt compiler 953 and chDB 26.7.3. It
isolates the confirmed publication boundary for that SQL; it does not qualify
an exporter, S3, or a buffered design. The earlier 53.165/92.960 ms control
used different SQL and is not the exact-SQL comparison.

The current default `execute-and-flush!` remains one FIFO worker request whose
return is a confirmed or reconciled publication receipt covering the caller's
mutation. `execute!` alone changes local engine state and stages recovery work,
without acknowledging persistence. The proposed mode needs a separate name and
an explicit caller contract. See [Durable writer](../../docs/durable-writer.md)
and [Durable storage](../../docs/durable.md).

## Proposed finite world

Model two callers, one writer worker, one independently scheduled heartbeat,
one abstract immutable-object store and head-CAS authority, and one recovered
reader. The callers and worker share an ordered, bounded request queue; FIFO
order matters because a flush barrier must cover every earlier admitted
mutation. The store's immutable publication and head CAS are separate atomic
events. An adversary may delay, fail, or make the CAS response ambiguous, and
may advance the lease generation through takeover. Use plain Quint with shared
state and an ordered queue, not a message-passing choreography. This structure
is proposed for type-sketch approval, not yet selected as executable logic.

Suggested small domains: `CallerId = {Caller1, Caller2}`;
`RequestId = {Req1, Req2, Req3}`; `ObjectId = {Obj1, Obj2}`;
`AttemptId = {Attempt1, Attempt2, Attempt3}`; queue capacity two; sequence and
generation bounded only for exploration. IDs are opaque. No SQL bytes, clocks,
backoff, native pointer ownership, or throughput timing enter this first model.

| Cohesive state | Proposed fields and meaning |
| --- | --- |
| `requests: RequestId -> RequestState` | caller, admission order, phase (`New`, `Queued`, `Prepared`, `NativeApplied`, `ReplayStaged`, `Publishing`, `Confirmed`, `Failed`), exact replay preparation, completion observation, and covered publication sequence if any |
| `writer: WriterState` | `Open/Closing/Closed`, ordered queue, active request, local applied set, exact staged replay set, checkpoint-required marker, first terminal error, and heartbeat/lease token state |
| `publication: PublicationState` | selected staged prefix, fresh attempt, immutable reference state, head sequence/reference, CAS outcome (`None`, `Confirmed`, `Ambiguous`, `Failed`), and reconciliation result |
| `reader: ReaderState` | last recovered head sequence and visible request set, derived only from committed head references |

Keep the object store's published exact references and the current head as
shared state outside the writer record. A request's observed return is separate
from its publication state: otherwise a successful admission can accidentally
be treated as a confirmed commit. A compact per-request phase may need two
axes (`work phase` and `caller result`) once the choices below are resolved.

## Proposed action grain

The model should split `admit`, `prepare`, `nativeApply`, `stageReplay`,
`selectPublishPrefix`, `publishImmutable`, `verifyImmutable`, `attemptHeadCas`,
`reconcileAmbiguousCas`, `settleCoveredRequests`, `observeReader`, and
`consumerCleanup`. These cuts expose native success followed by staging
failure, an immutable object without a committed head, and a landed CAS whose
response was lost. `closeAdmission`, `drain`, `flushOnClose`, `joinHeartbeat`,
`releaseLease`, and `finishClose` should remain separate where their ordering
matters. `renew` and `takeover` can interleave with blocked publication; the
worker may not hold a writer-local lock across storage calls.

Admission to the ordered queue and removal by the worker are distinct actions.
Preparation of exact replay content and size/policy validation precede native
mutation. A failed native mutation creates no staged replay record. A staging
failure after native success sets checkpoint-required before any result that
promises executed and replay-staged work can be reported.
Settlement requires a confirmed or exactly reconciled head reference; a failed
or unresolved publication retains the covered recovery obligation. Existing
head-CAS and close models supply the ownership and ordering rules rather than
being silently replaced: [head CAS](durable-head-cas.md),
[writer lifecycle](durable-writer-lifecycle.md), and
[file-WAL spool](durable-file-wal-spool.md).

## Properties and witnesses to check after approval

- No successful confirmed receipt precedes an exact committed head reference
  covering that request. Existing default calls retain this rule unchanged.
- Every native-applied mutation had exact replay content prepared first, and
  then has staged replay state or a required full checkpoint. A result that
  promises executed, replay-staged work cannot precede that staging. A mere
  queue-admission result must be labeled only as admission.
- Only a current lease owner can advance the head; an immutable object alone
  never makes a request visible to a recovered reader. Ambiguous CAS settlement
  requires the exact reference and sequence plus current ownership.
- Publication covers an ordered prefix of staged work. A later request cannot
  settle before an earlier admitted request that its flush barrier must cover.
- Failed or unresolved publication keeps the pending obligation; cleanup cannot
  erase it before confirmed settlement. A returned close has joined its worker
  and heartbeat and reports its first relevant failure.
- A recovered reader sees only head-referenced work. If local writer queries
  are in scope, their earlier visibility is modeled separately from recovered
  visibility.

Reachability witnesses should include queue-full admission, native success
followed by staging failure, admitted-but-unpublished work at close, failed
immutable publication, ambiguous CAS that lands and reconciles, ambiguous CAS
with takeover, confirmed settlement of a multi-request prefix, and consumer
cleanup after each public result class. Mutation controls should falsely settle
before head commit, drop retained work on failed publication, accept a stale
owner, and expose an unpublished object to the reader. Each should fail its
corresponding property while the corrected bounded model reaches the valid
boundary.

## Original decision matrix and remaining runtime choices

The approved bounded caller contract above selects admission-only success,
full-queue rejection, successful close drain/confirmation, and head-gated fresh
reader visibility. The alternatives below remain useful as a record of the
design space; unselected runtime details are not silently specified by the
first model.

| Boundary | Explicit options to decide |
| --- | --- |
| Buffered caller success | Return an admission-only result after enqueue; return a prepared-work result before native execution; return an executed-and-staged result after native execution and WAL staging; or return only a pending ticket and reserve `success` for later confirmed settlement. In-process staging is crash-lossy, so each earlier result needs a name that does not imply persistence. Decide whether callers receive a later settlement handle/event and what failure it reports. |
| Queue pressure and cancellation | On a full independent bounded queue, block with a deadline or reject before admission. Decide whether cancellation can withdraw only queued work, or whether admitted work always runs to a terminal result. Preserve FIFO publication barriers. |
| Explicit flush and shutdown | Decide whether flush returns one receipt for the admitted prefix or per-request settlement, and whether close drains/flushes all admitted work or can fail with unresolved tickets. Define the result when publication is committed but cleanup throws, and the identity of the first failure shared by concurrent close callers. |
| Visibility and recovery | Decide whether buffered success promises local-writer read-after-write, only eventual recovered-reader visibility after a later receipt, or no read guarantee before settlement. Define crash loss for admitted but unpublished work and whether consumers may clear source data before confirmation. Recovered readers remain head-gated in every option. |

The approved contract supports the bounded executable companion. Runtime API
shape, deadline/cancellation policy, exact failure payloads, and local-writer
visibility still require separate decisions before implementation. Local
qualification should precede S3 qualification, with receipts labeled by mode;
confirmed-mode results cannot be transferred to buffered mode.
