# Changelog

## Unreleased

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
  Corretto compatibility unit, and an explicit Phase 1 retirement boundary for
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
