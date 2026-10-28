# Avoid redundant initialization of private native query buffers

`with-query-buffer` copies a sealed private ASCII SQL snapshot into a separate
native allocation, calls its trusted read-only consumer, then frees it. Every
logical byte is copied before the consumer runs. Using zeroed `ffi/with-alloc`
first also allocated a managed zero buffer as large as the SQL, only to overwrite
those native zeroes immediately.

The candidate uses Jolt's reserved raw `ffi/__alloc` primitive with the same
`ffi/free` allocator pair. It keeps the final private native copy, preserves the
empty zero sentinel, checks the reported complete copy length and frees on copy
or consumer failure. No managed address loan, shared buffer, publication change,
WAL encoding change or weaker ownership promise is introduced.

The zero-initialization guarantee of public `ffi/alloc` is untouched. This is
only for storage whose complete logical range this library initializes before
observation. The driver already trusts its range-checked byte-copy helper; it
does not require that copy to be atomic. A partial write followed by an exception
never invokes the consumer and still frees the allocation.

## Red / green and review

- Exact baseline driver: `312812f0dfeff5498a6d1aa29b0476a97d8b65f3`.
- Runtime change: `7cbf43395a7e3406007ab1d47c811cb1d3d0cd5a`.
- Additional partial-copy failure test: `7056d42` (no linked source change).
- Same final focused tests: baseline 33 passed / 10 expected failures / 0 errors;
  candidate 43 passed / 0 failures / 0 errors. Baseline classpath receipt confirms
  the actual pinned Git dependency, not candidate source.
- Complete owned-statement gate: 17 tests / 15,606 assertions, no failures or
  errors, plus real chDB classification/insert/readback with text fallbacks trapped.
- Self-contained candidate smoke: 24 rows independently recovered with exact
  all-field/type/timestamp/multiplicity comparison against older ordinary JSON.
- Claude read-only review of runtime commit 7cbf433 reported no blockers.
  The added partial-write test strengthens its noted error-path testing gap;
  review follow-up for that test/documentation is pending at this checkpoint.

The reserved primitive is present in immutable upstream 0.8.6 source as well
as inspected current main. That source check is not an old-runtime execution
claim. Executable qualification here uses official 0.8.23/Chez10.4.1/chDB26.9.0;
the normal CI gates remain required. The public/legacy BB/JVM paths are unchanged.

## Component allocation evidence

An independent Chez10.4.1 control checks cumulative allocation against live heap
plus reclaimed bytes across a forced collection with 16MiB of retained buffers.
The two readings agree. Both runtime component probes also compare encoded bytes
and decode prepared WAL back to exact SQL outside the measured window.

For a 1,713,940-byte SQL payload representing 1,000 synthetic rows, warmed
cumulative allocation in the native-copy stage is:

| Path | Allocated bytes/row |
| --- | --- |
| Baseline private query buffer | 1,714.928 |
| Candidate private query buffer | 0.992 |
| Public byte-array `with-c-string` alternative | 1,715.104 |

The public alternative was investigated but rejected as an allocation fix: it
replaces the zero template with a private input copy, rather than eliminating
the managed temporary. Cold initialization costs are excluded from this table;
the raw samples retain them. This is not a whole-pipeline throughput claim.

Source-only normalization, untyped row/JSON encoding, typed field projection,
snapshot, WAL and native-copy probes did not reproduce main8ba11735's larger
release-pipeline allocation. Main's source encoding components generally
allocate less, not more. Its release/worker-context difference remains separate
and unresolved; do not attribute that unexplained increase solely to JSON.

## Four-arm real SDK / Durable comparison

Same direct entrypoint, official Jolt0.8.23, SDK02a642a, codecf9ac949,
exportere5190e product source, native26.9.0. Control is driver312812; candidate
changes only the owned query-buffer implementation. Rich typed fixture,
5,000 rows/batch, 32 measured batches, 3 warmups, 16MiB insert bound. Each arm
completed independent old-reader/ordinary-JSON recovery and retired its stores
before the next timed writer. No build/test/review/model/archive task overlapped
timed windows; immutable binaries/runners and host snapshots are retained.

| Arm | Aggregate rows/s | Cadence p50/p90/p95/p99, ms | Estimated bytes/row |
| --- | --- | --- | --- |
| A1 control | 21,784 | 220.408 / 279.146 / 288.850 / 335.502 | 14,442.86 |
| B1 candidate | 22,257 | 226.535 / 250.442 / 267.675 / 283.056 | 12,715.21 |
| B2 candidate | 22,238 | 226.225 / 258.773 / 262.836 / 276.611 | 12,714.11 |
| A2 control | 18,530 | 265.441 / 373.845 / 402.509 / 404.206 | 14,443.74 |

Allocation falls consistently by about 12%. Pooled control/candidate aggregate
throughput is 20,026/22,248 rows/s, but the last control slowed materially: do
not promote the pooled 11% difference into a settled causal throughput gain.
Median cadence does not improve (pooled inverse-cadence p50 22,252/22,072 rows/s).
Candidate sampled inverse-cadence p90/p95/p99 is 19,322/19,023/17,664 rows/s,
against control 13,969/13,375/12,370. These are 64 samples per arm and p99 is the
sample maximum, not a population-p99 or per-request latency guarantee. Both
candidate tail screens improve over the controls, but 25k p50/20k p99 is unmet.

All **700,000** stored rows independently recovered correctly, with zero SDK
failures/drops. Each arm compares all physical fields/types/exact nanosecond
timestamps and per-row multiplicity to its separate ordinary-JSON oracle.

Control binary SHA256:
`0120c2c29820de90f97dc38d3dd96d5260a20da8e384e18fcd3966aa3293f760`.
Candidate binary SHA256:
`9a3dad9c86c13793cf1586b7b81cca684779786e9fb0cb848f936bc2ec8bc719`.

Local evidence: `evidence/allocation-localization-20261027.tiGN08c0`; comparison
subdirectory `comparison.lJmDgsFT`. Keep small receipts, source/version/checksum
manifests, commands, summaries, host observations and one candidate binary.
Successful stores are already retired by the guarded runner; build scratch can
be pruned now that all functional/recovery gates are terminal and successful.
No unchanged exhaustive model run was needed: publication/ownership state is
unchanged. Native initialization/error ordering is covered by executable tests,
not claimed as a theorem of the Durable protocol model.
