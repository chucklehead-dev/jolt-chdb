# Changelog

## Unreleased

- Propagate each live writer's self-fencing predicate into synchronous nested
  object-backend work without changing the Durable backend ABI. S3 stops before
  a first request and before/after transport backoff, maps writer-originated
  stopping to `lease-fenced`, and still never reissues an uncertain write.
- Advance the target-owned Durable aspect epoch to the retry-aware control
  surface and select its option-bearing terminal arities exactly once.
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
