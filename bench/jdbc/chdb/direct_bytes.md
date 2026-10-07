# Direct byte-buffer diagnostic

This is an experiment, not an enabled JSON backend. It follows the Go
observability collector's single destination buffer and direct decimal append
pattern. Clojure persistent maps and vectors remain at the input boundary.

One invocation owns a growable, byte-budgeted buffer and a 21-byte decimal
scratch area. Strings use data.json's default Unicode, slash, and JS separator
escaping. Maps use the runtime-owned sequence-order fold. The final exact-size
buffer is copied and returned through the runtime's byte-array bridge; that
ownership copy and bridge are included in the measured section.

Only nil, Boolean, string, bounded signed/unsigned wire integers, finite
double, persistent vector, and string-keyed persistent map shapes are supported.
The guarded arm resolves the live JSONWriter method for every value and rejects
changed/custom dispatch. It does not invoke custom writers or callbacks, support
arbitrary data.json options, or implement a portable fallback. Both arms are
explicit diagnostics, never public writer replacements. Nothing changes WAL,
admission, persistence, acknowledgement, recovery, or native insertion.

## Measured first result

Pinned runtime bcb376a04f8e3d4508e9f77f4fa33b50bae7924b, Chez 10.4.1.
Prepared rows come from exporter 5d273302264cf53e3e52c39fcdb0ac5e502da4a1,
using its actual compact span/log/gauge/sum/histogram row constructors.
Maintained data.json source is 3adc8c5d3a57a30d18f62bad9d1c7a5c1f6857e0.
Baseline uses the existing guarded string-caching writer and converts its final
JSONEachRow string to UTF-8. Candidate writes bytes directly. Row preparation,
OTLP decoding, insertion, WAL, and S3 are NOT measured here.

For each signal, two samples per arm, each three encodings of 5,000 rows;
order baseline, unguarded, guarded, guarded, unguarded, baseline.
All three arms' payload SHA-256 values matched for all five signals.

| Signal | Baseline ms | Guarded bytes ms | Baseline allocation MB | Guarded allocation MB |
| --- | --- | --- | --- | --- |
| spans | 120.4–136.7 | 72.8–80.3 | 98.44 | 25.86 |
| logs | 109.7–120.5 | 69.4–78.8 | 87.61 | 25.39 |
| gauge | 101.8–106.2 | 62.8–63.0 | 85.25 | 25.84 |
| sum | 97.3–110.0 | 63.2–63.8 | 88.16 | 26.22 |
| histogram | 118.7–134.0 | 80.3–85.8 | 101.16 | 31.86 |

Allocation is estimated cumulative Scheme allocation, not retained heap.
These are component samples, not enough observations for meaningful p99.
Restoring live dispatch checks retained roughly 1.5–1.7x mean component speed
and 68–74% lower allocation. The unguarded arm was about 2x as fast as baseline.
No end-to-end throughput or tail improvement is claimed.

Full receipts are under /home/chuck/ai-src/evidence:
direct-byte-buffer-guarded-component-20261007.edn and matching log;
direct-byte-buffer-tests-20261007.log. The initial unguarded-only report is
direct-byte-buffer-component-20261007.edn; keep the guarded result distinct.

## Next bounded integration

First add an explicit, batch-owned exporter walker for stock supported DTOs,
preserving attribute normalization/collision rules and typed descriptors. Keep
unusual shapes/custom behavior on the existing path without replaying effects.
Then qualify output through the unchanged insertion/WAL contract, with writer
and fresh-reader counts and p50/p90/p95/p99. Do not treat this restricted encoder
as satisfying arbitrary JSONWriter extensibility or full collector coverage.
