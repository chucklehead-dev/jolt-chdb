# Matched confirmed-pipeline diagnostic

This is a **wide16 synthetic-fixture-specific Rust projection**, not a general
OTel SDK/exporter or a replacement for the maintained recovery oracle. The
fixture comes from the current exporter's installed typed plan after its exact
SQL witness; the Rust program rejects unsupported input shapes.

Each timed operation freshly projects the source spans, serializes 28 physical
columns with serde_json, executes, and confirms that batch's ticket through the
Rust Durable API. Input spans are reused, encoded SQL is not. Report encoding,
execute and confirmation phases separately. Three warmups precede measurement.
Do not compare a flush-once trial or buffered admission to these operations.

Build offline with one Cargo job and the **existing** native package 26.9.0
library and header (whose SQL engine reports **26.9.2.1**). Package and engine
versions differ intentionally. Set CHDB_LIB_DIR and CHDB_INCLUDE_DIR explicitly. Use a bounded,
external CARGO_TARGET_DIR; no C++ engine build or download is needed. Record the
executable, native library and header checksums. The executable checks engine
26.9.2.1 at startup. Linking another native version invalidates the comparison.

Use a fresh empty scratch root. Never automatically delete it on failure;
diagnose first and retain a bounded reproduction/receipt before explicit cleanup.
A successful writer
count/close is not sufficient: recover with the stock-Jolt full-physical-row and
typed-value reader against the same fixture. Preserve bounded reports, then
remove the synthetic stores and generated input fixture. The runner must check
both independent recovery and the writer's report before cleanup.

Rust's local backend publishes its CAS head as a symlink into `.head-versions`;
Jolt's production POSIX backend intentionally rejects that representation.
Use the existing **read-only cross-binding fixture backend** for the stock-Jolt
reader, without changing production path protections, copying a different head,
or rewriting payloads. This checks the Durable wire format and physical values,
not interchangeable local CAS implementations or fault-durability guarantees.

Rust's stock serde_json may use different escaping/map order than Jolt. Compare
all recovered physical values, types and row counts rather than asserting equal
SQL hashes across different serializers; record their payload sizes. The current
fixture captures the exporter's **exact SQL prefix**, including query-local
`input_format_read_datetime_number_as_raw_value=1`. Validation rejects a missing
or different prefix before opening the writer. This keeps native parsing settings
and exact SQL WAL aligned with raw numeric nanoseconds; reusing the header is
not reusing row encoding or the complete statement.

The first Rust constructor omitted that setting and overflowed DateTime64. This
was a diagnostic construction error, **not proof that ordinary-query insertion
cannot accept raw ticks**. A temporary ISO variant round-tripped correctly;
its historical results below are retained separately from the corrected raw-tick
variant. Do not silently relabel its different timestamp wire as a same-wire run.

Rust's fixture-specific borrowing serializer does not perform SDK admission, approved
catalog installation/freshness checks or general attribute-shape fallback on
each batch. Therefore this isolates the practical native/Durable baseline for
the same data and schema; it is not complete application parity or a causal
claim about the cost of any single Jolt feature.

## Initial ISO screen, 2026-10-17 (commit ebdf7ec)

One caller, three warmups and 16 measured batches per runtime/size, local POSIX,
same 28-column physical schema and synthetic source spans. Jolt exporter fd8fa0e,
JSON4aeb24b, driver3d766fa, runtime1ae7e0d; Rust e685da9; same native library
package26.9.0/engine26.9.2.1. Both report auto(8) native thread limits and enabled
parallel input parsing. Runs are sequential, not a causal code-change experiment.
Rust used ISO timestamps in this historical screen; Jolt used raw ticks. The
corrected same-prefix/raw-tick run is a separate subsequent result.

| Batch | Runtime | Confirmed rows/s | Request p50 / p90 / p95 / p99 ms |
| --- | --- | ---: | --- |
| 5k | Jolt full exporter | 25,493 | 185.968 / 245.329 / 278.846 / 278.846 |
| 5k | Rust fixture serializer | 52,693 | 87.842 / 132.188 / 143.616 / 143.616 |
| 10k | Jolt full exporter | 32,618 | 303.949 / 331.326 / 342.298 / 342.298 |
| 10k | Rust fixture serializer | 53,890 | 161.368 / 283.716 / 303.909 / 303.909 |

All570,000 physical rows across both sizes/runtimes passed independent stock-Jolt
recovery, exact original nanoseconds and typed values. Temporary stores and input
fixtures were removed after bounded receipts were retained. Sixteen samples
make nearest-rank p99 the maximum: **not sustained/tail qualification**, full SDK
parity or a claim that every remaining difference is JSON encoding.

Rust mean encode/execute/flush times: 5k11.353/53.356/30.181ms;
10k27.105/107.304/51.155ms. Jolt calling-thread CPU p50 is99.082/176.162ms at
5k/10k; this excludes worker CPU and cannot be directly equated to a Rust wall
phase. The borrowing serializer avoids materializing both the stringified
attribute maps and a whole intermediate 28-value row vector; scalar `collect_str`
also avoids temporary formatted-number strings. Those are useful next projection
and encoding experiments, not an already-demonstrated Jolt implementation fix.

## Corrected same-prefix/raw-tick screen, 2026-10-17

Same graph, native library, schema, input, one-caller scope and 16-sample limit.
Both encode raw nanoseconds and carry the same query-local parsing setting in
the exact SQL prefix. Escaping/map order, SDK scope, native entry and local CAS
implementations still differ as described above; this is not byte-identical SQL.

| Batch | Runtime | Confirmed rows/s | Request p50 / p90 / p95 / p99 ms |
| --- | --- | ---: | --- |
| 5k | Jolt full exporter | 28,658 | 164.341 / 198.663 / 201.769 / 201.769 |
| 5k | Rust fixture serializer | 56,619 | 82.072 / 123.712 / 126.341 / 126.341 |
| 10k | Jolt full exporter | 28,959 | 342.390 / 381.764 / 389.538 / 389.538 |
| 10k | Rust fixture serializer | 55,316 | 171.804 / 205.374 / 227.859 / 227.859 |

Another570,000 physical rows, exact nanoseconds and typed values passed fresh
stock-Jolt readback across both runtimes/sizes; all synthetic stores were removed.
Rust mean encode/execute/flush:5k9.451/52.477/26.380ms;
10k20.309/109.804/50.668ms. Jolt calling-thread CPU p50:88.952/203.565ms.
The Rust~50k observation is plausible under per-batch confirmation and this
wide fixture, but neither screen qualifies sustained tails or proves a causal
cost allocation between the remaining differences. No production default change.
