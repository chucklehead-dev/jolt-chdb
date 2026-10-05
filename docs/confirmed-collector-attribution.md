# Confirmed telemetry collector: where the time goes

This local screen measures the actual exporter and default Durable writer,
not just an encoder or a native insert. It does not change acknowledgement,
WAL contents, publication, replay, or ownership rules.

## Reproduction and custody

Sources: exporter `1af91f3`, chDB `7dcaec0`, data.json `993b906`;
compiler source `2223c24a`, Chez 10.4.1, libchdb 26.7.3. The selected compiler
SHA-256 is `5b8167ae087ab1ac59585e6bbaece5a32ed4502aec8d13912d2ad1cf883d7889`;
libchdb SHA-256 is
`36ad4e999882821ef13cf2d0f52c93f48e6ee1b4498b35a201c23ce4b8d15bf5`.

Workspace evidence, under `/home/chuck/ai-src/evidence/`:

- Driver: `exporter-durable-internal-phases-20261005.clj`.
- Writer: `exporter-durable-internal-20261005.edn`.
- Phases: `exporter-durable-internal-20261005.edn.phases.edn`.
- Independent reader: `exporter-durable-internal-20261005.edn.recovery.edn`.

The driver delegates fixture generation and ingestion to the existing
`otel.exporter.chdb-benchmark/run!`: ten measured batches of 5,000 items,
five physical tables, 250,000 physical rows. Timers exclude warmup, schema
setup and readback. They include measured synthetic input generation.
Both writer and fresh reader terminated successfully. The reader confirmed
50,000 rows in each table; this is count readback, not a complete value digest.

Only `:writer-phase!` is configured as a Durable operation. The default paired
SQL buffer/classification/execution path remains enabled. Coarse wrappers call
the original classifier, executor, query-buffer owner, publication and commit
functions exactly once. Every expected phase has 50 calls and zero failures.
No row content, credentials or storage payloads are included in phase reports.

## Results

Whole ingestion: **14.360 seconds**, **17,409 physical rows/s**;
approximately **9.367 GB allocated** by the coarse runtime accounting.

| Phase | Seconds | Relationship |
| --- | ---: | --- |
| Confirmed execution | 5.524 | Includes the phases below |
| WAL preparation | 0.436 | Before engine execution |
| Owned query buffer | 3.177 | Includes classification, engine and WAL append |
| SQL classification | 0.051 | Inside query-buffer scope |
| Native engine execution | 2.458 | Inside query-buffer scope |
| WAL append | 0.139 | Inside query-buffer scope |
| WAL join | 0.037 | Before publication |
| Immutable WAL publication | 1.257 | Includes put and verification |
| Immutable put | 0.711 | Inside publication scope |
| Immutable verification | 0.224 | Inside publication scope |
| Head commit | 0.531 | Includes conditional head update |
| Head CAS | 0.293 | Inside commit scope |

These are nested, inclusive timers: do not add every row. Allocation deltas
across caller and worker scopes are not additive either.

The separate collector attribution screen measured roughly 4.83 seconds of
generic JSON encoding, 1.05 seconds of metric row construction and 0.84 seconds
of fixture construction. Those measurements are from another run, not exact
partitions of the 14.360-second run above.

## Decisions supported by this evidence

- Classification is about 0.36% of whole ingestion time. It is not the current
  throughput bottleneck; keep classify-before-execute protection intact.
- The measured native phase corresponds to about 102,000 physical rows/s
  in isolation. This is not a system ceiling or a Rust comparison, but it does
  not explain an architectural limit below the 25k overall target.
- Approximately 213 MB of WAL data was appended/published. Publication and
  buffer handling are meaningful, but the unlabelled publication remainder
  must not be attributed entirely to hashing without further measurement.
- Prioritize generic encoding and row/string construction. Do not promote
  custom escaped-string loops merely because they allocate less: the previous
  actual-collector experiment slowed ingestion.

These are local diagnostic screens, not repeated p99 qualification, S3 results,
or matched Rust/JVM/Babashka comparisons. No default GC policy changes or new
model guarantees follow from them.

## Follow-up: row materialization and dispatch

`json-row-materialization-screen-20261005.clj` and its `.edn` receipt use
prebuilt physical metric rows through the same bounded public encoder: 16
payloads, 12,288 rows. Stock `sb-str` is wrapped without changing its result or
mutation, and stock UTF-8 sizing is timed separately. Exact payload parity
passed before/after. The unobserved run took 267.5 ms and allocated 202.6 MB;
the instrumented run took 291.5 ms and allocated 216.8 MB.

- Writer flattening: 21.8 ms, 12,800 calls, 8,893,840 characters. This includes
  small key-cache writers as well as row writers.
- Final batch-builder flattening: 19.8 ms, 16 calls, 8,886,320 characters.
- UTF-8 sizing: 24.5 ms, 12,288 calls.

Per-row clocks and counters add overhead. These figures establish that each
copy/scan is measurable, not a causal 25% whole-collector speedup prediction.
Skipping them all would still not explain the full throughput gap.

`json-key-dispatch-cost-screen-20261005.clj` is an explicitly **unsafe,
fixture-only** lower-bound experiment; it restores all runtime roots in
`finally`. On the same immutable fixture, direct default keyword names were
essentially flat (261.7/255.6 ms versus live 257.9/257.6 ms). Stock protocol
selection took 223.4/213.1 ms, roughly 15% below live resolution. Combining the
two bypasses did not consistently improve further (210.3/234.9 ms).

These bypasses ignore live extension/rebinding semantics and must not be used
as a backend or published as a production optimization. The useful conclusion
is narrower: do not spend the next slice optimizing default key normalization;
any protocol-site optimization must preserve live dispatch and will not, alone,
close the collector gap.

The second key/dispatch receipt (`json-key-dispatch-cost-screen-20261005-v2.edn`)
also counts selected methods on one untimed payload: 3,072 map, 4,352 array,
8,448 string, 4,864 plain-number/Boolean and 1,792 double selections; no unknown
methods. There is no hidden unknown-writer fallback in these metric rows.

Finally, `json-scratch-port-cost-screen-20261005.clj` compares stock row-local
scratch ports against an unsafe fixture-only shared port. Stock took
283.6/265.8 ms and 202.12 MB; shared took 263.8/259.0 ms and 200.35 MB, with
exact fixture parity. This is only about 0.9% less allocation and a small noisy
time difference. It does not justify adding scratch-pool ownership complexity
to the public writer. Nested/custom writers would require independent leases
and bounded retained capacity; none of those guarantees is claimed by this
diagnostic.
