# Changelog

## Unreleased

- Pin all hosted Jolt lanes and performance provenance fixtures to the merged
  `casselc/jolt` `OutputStreamWriter` fast path. Each consuming workflow now
  checks the shared install action's exact source revision and version output,
  as well as the selected binary banner, before running its gate.

- Add a manual cross-binding Durable recovery oracle that makes Jolt and a
  pinned `chdb-rust` revision recover one byte-identical immutable local
  fixture with the same native library, aggregate reconciliation, cache
  condition, per-trial provenance, peak RSS, and explicit 80% throughput / 1.25x
  elapsed acceptance ratios. Jolt trials use project-only `-Srepro` resolution
  and run-scoped caches, record the caller-asserted full source SHA separately
  from the banner-confirmed abbreviated revision, executable digest, and
  captured `-Sdescribe`, and fail if repository state changes between any prime
  or measured process.

- Use the supported primitive byte-array equality operation for canonical
  Durable WAL UTF-8 validation, avoiding byte-at-a-time Clojure traversal while
  preserving complete validation before recovery effects and failure ordering.

- Require performance qualification to record the exact Jolt source revision
  and executable digest, and let the Durable native qualification script select
  an explicit `JOLT_BIN` instead of silently depending on ambient `PATH`.

- Add fresh-process Durable scale selectors for 512, 1,000, 5,000, and 10,000
  rows per batch plus staged 512-row recovery diagnostics at 10, 25, and 50 WAL
  records. Reports now retain exact recovery endpoint allocator observations,
  distinguish cumulative WAL growth from maximum input-batch size, and require
  external GNU `time -v` maximum RSS rather than claiming an unsupported
  two-sample plateau oracle. S3 remains a separate qualification slice.

- Validate and replay Durable WAL segments with two bounded streaming passes
  over the private, already size-and-digest-verified scratch file. Recovery now
  retains at most one JSONL record instead of materializing the complete WAL as
  bytes, text, persistent byte vectors, split lines, and decoded SQL, while
  preserving complete-segment validation before the first engine effect.

- Bound Durable scale and qualification harness retention to one row batch,
  including pre-encoded modes, and fold fresh-reader recovery aggregates
  incrementally instead of retaining the complete approximately 50,000-row
  trial workload. Per-batch admission timing boundaries, deterministic indices,
  warmup, pending-WAL checks, one-flush cadence, and recovery reconciliation are
  preserved.

- Make Durable throughput evidence reproducible across the requested 512,
  1,000, 5,000, and 10,000-row batch sweep. The manual harness now has explicit
  `scale` and `qualification` profiles, rejects unknown profile names, requires
  exact clean-worktree provenance for evidence-producing runs, and labels p99
  summaries non-qualifying when fewer than 100 batch observations exist.

- Stream Durable V1 WAL JSON through the generic `data.json` writer directly
  into UTF-8 bytes, avoiding an intermediate complete JSON string while keeping
  statement and segment validation before native mutation and WAL append after
  native success. This path requires the Jolt
  `OutputStreamWriter.append(CharSequence, start, end)` range fix tracked by
  `casselc/jolt#73`; writer construction now rejects affected runtimes before
  acquisition or native work instead of risking malformed replay bytes.
- Raise the supported Jolt floor to 0.8.6 and make every hosted workflow build
  and verify the same immutable revision from the canonical append-only
  `integration/aspects` line. Multi-runtime ABI qualification accepts the exact
  release or a git-described compiler derived from it, while the install action
  separately asserts the selected compiler commit and complete version banner;
  causal cross-file mutants prevent that hosted pin from drifting away from the
  checked compatibility manifest.
  The main CI gate also makes an unsupported WAL byte-writer capability a
  failure, so its functional and Hegel coverage cannot be reported green
  through a skip.

- Preserve the original SQL object when an empty-parameter request contains no
  question mark, avoiding placeholder output construction while retaining the
  lexical scanner whenever a question mark is present.

- Reuse one request-local prepared query for Durable mutation classification
  and native execution, eliminating a second placeholder rewrite without a
  global cache or any change to WAL-before-mutation ordering. Prepared values
  are explicitly ephemeral because typed bindings can contain secrets.

- Add a reproducible production-path Durable JSONEachRow throughput harness
  with explicit 512-row targets of at least 25,000 rows/s at p50 and 20,000
  rows/s at p99, causal instrumentation and exact recovery reconciliation.
  Current measurements are diagnostic baselines, not qualification evidence.

- Update the pinned `data.json` writer to bulk-append unescaped string runs.
  A bounded paired probe cuts the isolated outer-WAL JSON stage by about half
  and improves realistic Durable admission, while remaining diagnostic rather
  than evidence that the throughput targets are met.

- Cover the pinned public writer renewal-loss sequence end to end: tolerate one
  failed heartbeat before expiry, self-fence after the last proved lease
  expires, refuse execute/flush/checkpoint before effects, and preserve a public
  queued read on the opened local handle. A separate literate Quint module,
  deterministic ITF projection, and four causal mutants cover the same
  boundary. The conformance ledger is now 45 mapped, 2 blocked, and 2
  binding-level not applicable.

- Close four pinned Durable public-open conformance gaps with executable
  sorted-key/indented head recovery and a reader/writer matrix for missing,
  wrong-size, and wrong-digest checkpoint and WAL references. Valid controls
  prove restore/replay reachability; faulted writers release their lease and
  cleanup while retaining `corrupt` over injected secondary cleanup errors.
  Together with the merged live-force warning coverage, the 49-case ledger is
  now 44 mapped, 3 blocked, and 2 binding-level not applicable.

- Check public Durable acquisition and recovery-renewal `head.json` bytes
  against an offline Python `Decimal` epoch-seconds fixture, with a CI drift
  guard bound to the canonical protocol pin and digest, plus a precise
  wrong-unit identity-conversion mutant witness. This is a narrow adapter unit
  oracle, not a full upstream Python-writer interoperability claim.

- Bound Durable public TTL, heartbeat, skew, observed clocks, and derived lease
  comparisons to integral cross-runtime-safe milliseconds, while preserving
  bounded fractional epoch seconds on the wire. Direct open and raw head decode
  now reject truncating or extreme-time mutants. Configuration and the initial
  clock sample fail before acquisition; an invalid recovery sample cleans up
  the acquired attempt, while invalid live samples self-fence before subsequent
  data or renewal effects and defer release/cleanup to close. These checks are
  documented as local adapter policy; the independent cross-binding unit oracle
  is recorded separately above.

- Reject malformed Durable producer and minimum-reader releases at fresh
  acquisition, takeover, and checkpoint boundaries using the canonical pinned
  compatibility parser, before conditional publication, verification, or head
  replacement. Diagnostics retain neither the rejected release nor object
  identity.

- Bound Durable head JSON object/array nesting to 64 containers during both
  raw decoding and programmatic encoding, with redacted boundary diagnostics.

- Return one structured, redacted warning after a successful forced takeover of
  a live Durable lease, including reconciled ambiguous success, while keeping
  fresh and expired acquisitions silent and preserving CAS outcomes.

- Match the SHA-pinned upstream Durable V1 engine-version comparator exactly,
  including variable numeric tuples, numeric prerelease ranks, stable fallback
  suffixes, malformed fail-closed inputs, and non-lowering minimum-reader floors.
  A differential Python oracle, causal mutants, and bounded Hegel properties
  guard the compatibility boundary independently of the Quint rank projection.

- Accept RFC JSON whitespace around exactly one Durable V1 head value while
  retaining full-byte size limits and fail-closed duplicate, malformed,
  non-UTF-8, BOM, and multiple-value handling.

- Add an offline, SHA-pinned inventory of all 49 upstream Durable V1 cases, an
  obligation-by-obligation local mapping/blocker record, and a focused drift
  gate. This is Phase 1 qualification evidence, not a full V1 conformance claim.

- Record the running producer version when acquiring an existing Durable head,
  and atomically record that producer plus non-lowering backup-format and
  minimum-reader requirements with each full checkpoint.
- Keep a Durable V1 lease held through equality at
  `expires_at + clock_skew`, allowing normal takeover only after that boundary
  while preserving the current writer's conservative self-fence at expiry.
- Write and compare Durable V1 `lease.expires_at` as epoch seconds, including
  fractional seconds, while keeping the public `*-ms` configuration and
  monotonic retry budgets in milliseconds. Python-shaped fixtures cover both
  interoperability directions and the explicit force-takeover migration from
  legacy Jolt millisecond heads.
- Split Durable formal CI into an always-run fast model-linked tier and a
  diff-gated exhaustive Apalache tier. Preserve the historical
  `literate-model` check as an explicit successful no-op for byte-identical
  model inputs, with conservative base/head classification and tested path
  controls.
- Characterize the canonical libchdb driver ABI on pinned Babashka and JVM
  `babashka.ffi` compatibility units with an owned-thread parameterized-query
  lifecycle smoke, one Jolt-resolved native-library selection, typed exact-copy
  and selected-library negative controls, a checksum-pinned exact hosted
  Corretto compatibility unit, a checksum-pinned dynamic Babashka artifact with
  bounded loader diagnostics, and an explicit Phase 1 retirement boundary for
  the test-only adapter.
- Positively join each Durable reader and writer operation OS thread before a
  public close returns or rethrows. Concurrent and repeated closes retain the
  exact original failure while cleanup and worker exit remain exactly once;
  same-thread joins fail fast instead of deadlocking, and close still waits for
  actual heartbeat-thread exit before release.
- Add an opt-in JVM-only Typed Clojure development check for the runtime-neutral
  Durable ABI and capability contracts, including a real positive consumer and
  two mutation-specific negative controls. Normal Jolt consumers do not resolve
  the checker or include the external annotation sources.
- Propagate each live writer's self-fencing predicate into synchronous nested
  object-backend work without changing the Durable backend ABI. S3 stops before
  a first request and before/after transport backoff, maps writer-originated
  stopping to `lease-fenced`, and still never reissues an uncertain write.
- Advance the target-owned Durable aspect epoch to the retry-aware control
  surface and select its option-bearing terminal arities exactly once.
- Require the generated Durable Quint ITF corpus to cover every legacy action
  and semantic outcome before replay, and record the aggregate counts in a
  machine-readable coverage manifest.
- Add monotonic retry deadlines and capped exponential backoff to Durable
  control and S3 operations. Writers stop retrying when their locally proved
  lease expires; ambiguous writes are never reissued, while delayed
  reconciliation reads remain bounded and preserve `commit-ambiguous`.
- Check ambiguous head-CAS landing as its own refinement transition, model a
  rival lease takeover before reconciliation, and require the stale writer to
  receive `LeaseFenced` without losing the landed recovery reference. Exercise
  the same interleaving through a deterministic runtime barrier test.
- Add validated `writer-dbspec` and `snapshot-dbspec` constructors. Durable
  JDBC maps now reject unknown options, incomplete storage identity, invalid
  timing, and writer-only fields on snapshot readers before opening storage or
  native resources; writer instances default to UUIDv4 while lease generation
  remains the fencing authority.
- Run Durable reader and writer operation loops and writer lease heartbeat on
  owned OS threads, so blocking native or storage calls cannot starve other
  work on Jolt's shared fiber carriers.
- Keep writer heartbeat active through close queue drain and final flush, then
  require its successful termination before lease release and native cleanup.
