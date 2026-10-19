# Stock SDK/local Durable batch geometry screen

Production sources: driver a189afb1fff8fc674348ce2118363bd5549b2d99 and
exporter4966a8b99c251d0025a8adca0477fe2c6c9f8b0c. Official stock Jolt0.8.23,
libchdb26.9.0. No production source/default/model changed in this screen.
Research wrappers select only batch geometry, distinct exporter lane count,
the existing explicit codec, and ordinary metadata JSON reader.

One serial SDK worker per distinct exporter, sharing one serialized Durable
writer. Total queue capacity stays20k in all four processes. Arrival is a
capacity-throttled saturated producer, not an open-loop arrival SLO test.
Every batch geometry retains32 measured rounds and3 warmups; eight siblings
per trace, 35copies per distinct span. Thus larger batches also measure more
rows and longer windows. This is one process per geometry, not a repeated,
controlled scaling experiment or a throughput-tail qualification.

## Results

| Batch limit | Lanes | Confirmed window rows/s | Estimated allocated B/row | Independently checked rows |
| --- | --- | --- | --- | --- |
| 5,000 | 1 | 10,963 | 20,352 | 175,000 |
| 10,000 | 1 | 11,853 | 19,968 | 350,000 |
| 10,000 | 2 | 16,837 | 19,961 | 350,000 |
| 5,000 | 2 | 16,362 | 20,379 | 175,000 |

All1,050,000 physical rows passed the old-stack independent per-field/type/
multiplicity comparison and the parameterized stats/routing/count verifier.
Source-side checks retain per-trace affinity/order and serial-per-exporter
callbacks. No failed/dropped spans, queues empty before settlement. All four
temporary stores retired only after terminal writer, reader, verifier, copied
and compared receipts, and source-manifest checks. No native stores retained.

summary.edn records callback p50/p90/p95/p99. It also reports inverse time/row
at those slow percentiles, NOT upper throughput quantiles or sustained
throughput floors. Single-lane callback p99:591ms at5k and1140ms at10k.
Two-lane callbacks include partial drain batches and connection contention;
their latency/rate quantiles cannot simply be multiplied by lane count.

Larger batches give only a modest observed single-lane change and do not
close the target gap. The two-lane windows are similar, well below25k. Do not
claim either a statistically established scaling ratio or an architectural
throughput bound from this screen. Production25k p50/20k p99 remains unmet.

## Verifier controls and review

The wrapper reuses every old verifier check by substituting exact, unique
geometry literals. It intercepts only writer/reader receipt reads with immutable
in-memory EDN; it does not write fake passing receipts. Positive tests cover a
real valid5k receipt and explicitly SYNTHETIC scaled10k geometry. Ten negatives
cover wrong geometry/lane count, dropped rows, failed callbacks, missing order
proof and incorrect recovery counts/multiplicity/full-field equality.
Synthetic controls are not claimed as actual10k readback evidence.

Claude source review found no scaling/cleanup defect, but flagged generic
Throwable catching in negative controls. Fixed: catch only AssertionError,
require intended assertion-form marker;2positive/10negative controls pass.
Follow-up confirms wrong-cause failures are rejected, but requests per-field
markers within compound assertion forms. Codex does not treat that residual
request as a demonstrated vacuity defect: each immutable reference receipt
passes, each mutant changes one named field, and the same deterministic
verifier then rejects it at the expected assertion block. An unchanged sibling
field cannot newly fail that block. The markers identify blocks, not independent
field diagnostics; neither the tests nor report claim otherwise. No verifier
invariant was weakened to satisfy the review.

## Next implementation direction

Stop treating bigger batches or byte-copy removal as the missing multiplier.
Continue the owned traversal/byte/codec layering on stock Jolt. Investigate
fusing projection/encoding and avoiding a separate WAL preparation scan inside
the serialized writer. Any cached/private prepared representation must retain
independent public results, immutable snapshot ownership, admission/policy
ordering, exact WAL bytes, and persistence acknowledgements. Existing
publication/recovery/fault gates remain required; no contract change is implied.
