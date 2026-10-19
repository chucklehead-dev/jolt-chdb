# Private prepared WAL records on stock Jolt

Implementation: driver6f854b022cd2239f0df1d4cb6d8ae8ac97cbbfc7 and
exporterafa44f80d39d497e86f0328197122b4a8610173e. Official stock Jolt0.8.23,
libchdb26.9.0. No compiler fork, new format, acknowledgement or admission policy.
This remains an opt-in explicit-codec experiment, not a default dependency repin.

## Implementation

The exporter can finish pure WAL encoding before submitting an owned statement.
The serialized writer receives an opaque byte record instead of repeating that
encoding scan. This moves work so it can overlap native execution; it does not
eliminate encoding work or make persistence asynchronous.

| Consumer | Byte storage exposed |
| --- | --- |
| Public prepared-wal | Fresh independent chunk copies |
| Qualified builtin file port | Synchronous private backing borrow, no retention |
| Custom/memory/pipe/unsupported output | Fresh copies, never cached backing |

Each prepared record belongs to its opaque statement; there is no global query
cache or native resource to close. The original snapshot remains immutable and
usable. Repeated preparation is idempotent. Eligibility conservatively bounds
worst-case encoded size6N+11 to128MiB. Larger inputs retain ordinary encoding;
the existing64MiB statement and actual128MiB segment checks remain authoritative.

No public mutable array is adopted. Custom streams cannot poison later writes
by mutating or retaining the array they receive. Size/sealed-spool checks, policy
authorization, native mutation, append accounting and confirmed publication keep
their original order. Partial output errors still require checkpoint recovery
and are never retried through another output path.

Ordinary exporters keep loading with their old driver pin. The new helper is
resolved only at opt-in startup; a missing helper rejects before storage opens.
The matching experimental driver must be selected explicitly for this path.

## Correctness and CI

- Private record suite:5tests/77assertions, including cached policy, size-budget
  and retained-sealed-WAL rejection controls reused from the existing suite.
- Owned writer:5/35. Owned statements:14/15563 plus real native snapshot/WAL parity.
- Stock stream suite:8/71. JVM and Babashka stream suites:each5/55.
- Codec/exporter final source:15/100, including startup and plan-order controls.
- Ordinary namespace loading succeeds with the old default driver and no new helper.
- Claude source review and exporter compatibility follow-up found no defects.
- Hosted driver guard CI39337777725 at6f854b0 verified both runtime checksums and
  ran13tests/135assertions. It did not yet run the private record fixture suite;
  the subsequent CI extension adds that explicit gate. Do not conflate it with
  native/app integration. Exporter isolated stock codec CI39337942210 is likewise
  not a full collector or Oscope gate.

All seven actual SDK/local Durable stores passed independent old-stack physical
field/type/multiplicity readback and lane/routing/settlement validation:1,225,000
rows total. Each owned temporary store was removed only after terminal writer,
reader, verifier, receipt comparison and source checks. No synthetic native
stores are retained. No full-app/AOT/S3 qualification or exhaustive model run.
No formal model or unchecked codec primitive changed. Immutable-byte assumptions
are enforced at the new runtime ownership boundary, not waived for performance.

## Performance evidence and limits

Initial two-lane ordinary/cached/cached/ordinary comparison:

| Arm | Individual confirmed rows/s | Pooled rows/s | Estimated allocated B/row |
| --- | --- | --- | --- |
| Ordinary |17,202 /14,388|15,670|20,416|
| Prepared |18,862 /18,318|18,586|20,373|

The observed pooled change is+18.6%, but controls drift16.35%. Both candidate
windows exceed both controls; that is promising, not stable target qualification.
Actual precompute/writer/opaque counters confirm the intended paths were used.

The final exporter changes only helper lookup/capture for old-pin compatibility.
Its separate final-source screen recovered another525k rows:

| Arm | Lanes | Confirmed rows/s |
| --- | --- | --- |
| Prepared |2|10,050|
| Ordinary |4|9,948|
| Prepared |4|13,186|

These much slower absolute rates are retained, not discarded as outliers.
Four-lane prepared beats its control in this screen, but there is only one process
per arm. The final two-lane process used36.61CPU seconds versus18.88/19.41 for
the earlier prepared processes, with similar allocation and confirmed counts.
Cause is **not established**; do not attribute it to GC, scheduling, host speed
or the compatibility fix without stronger evidence.

A same-process direct-versus-captured helper-call diagnostic does not reproduce
the2x gap:8MiB preparation median22.996 versus23.007ms. It is a single-thread
component check, not proof that whole-pipeline concurrency is unaffected.

Both summaries retain callback p50/p90/p95/p99. Those are not sustained throughput
quantiles or end-to-end span latency; partial multi-lane drain batches must not
be multiplied by lane count. The25k p50/20k p99 goal remains unmet.

## Provenance and next action

The first comparison predates the exporter lookup fix. measured-exporter.clj
reconstructs that exact source and its hash matches the initial manifest;
final source is the committed exporter reference above. The driver runtime body
is unchanged after6f854b0; a later docstring only clarifies statement-owned bytes.
Measured runner retains its old hardcoded “four arms” terminal text even for the
three-case final screen; the actual three CLI cases and three validated receipts
are authoritative. A subsequent diagnostic-only runner message fix prints count.

Do not claim final-source equivalence for the initial ABBA or a stable speedup
from short windows. Next inspect coarse projection/precompute/native/commit phase
time and CPU on the final source to localize the actual2x variation. Avoid a
shared cross-lane current-context atom; scalar interval events may be aggregated
atomically but nested/overlapping phase sums must not be double-counted.
