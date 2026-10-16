# WAL confirmation: matched local/S3 results, 2026-10-16

The experimental checksum option worked on real AWS S3 in this matched window:
it avoided the selected file-WAL verification downloads, improved S3 throughput,
and retained independent full-row recovery. It did **not** meet the S3 targets.
Readback remains the default; this is not a production-default recommendation.

## Workload and scope

[Protected run 38962075034](https://github.com/chucklehead-dev/jolt-chdb/actions/runs/38962075034)
attempt 1 succeeded at workflow commit
`fb00089cde054ed38621cb677ad5cee72117179a`. Each arm used three warmups and
256 measured exports of 10,000 wide typed spans, one SDK caller, per-export
confirmed acknowledgement, automatic renewal and two automatic checkpoints.
Encoding, native execution and persistence are inside the export timer.

Both arms used the same frozen measured source and dependencies. Only the S3
candidate writer selected `:wal-upload-confirmation :sha256-single-put`;
baseline used full readback. The local arms are identical-behavior controls.
Both S3 readers retained the ordinary metadata parser and full verification on
the same pinned runtime: “stock reader” does not mean an all-stock ecosystem.

| Component | Exact measured revision |
|---|---|
| chDB library | `7efb62f96c8f636cfa50714067ce368c53794d4d` |
| Jolt | `1ae7e0d19aa3088789410106610beb171bcfb0ef`, binary SHA256 `06a2ccb858c072bfcd74ce94d96eb95f008d75e0fb1f46117728602572189811` |
| data.json | `b959a5625c28cc8571e04de5ca02a2c8902b8b5e` |
| ClickHouse exporter | `4f3bfd2bac88227710a3687b2a14fb5274689ef8` |
| OTel SDK | `19fc49d20b3a75906f0ccbb8b50c7e48b03e4813` |
| db fork | `cb06349c7ffe55e423021e402d4f182f4a27bcf7` |
| Upstream crypto | `5effcc89a3258499a79a2a3d69edad9e7800d1bf` |
| Native chDB | `26.9.0`, library SHA256 `a7d22ade86e1de82b187adf572b569a6034600ce932c52b0acb8c97b82430557` |

This is the measured SDK branch, **not the ordinary app's default stack**.
Some owned-byte/encoder paths still need forward-porting to main (#249).
The library's default native version was not changed by this experiment.

## Results

Times are milliseconds per confirmed 10,000-row export. Rates below are
10,000 divided by the corresponding latency, not sustained arrival capacity.
The target is at least 25k rows/s at p50 and 20k at p99: 400 ms and 500 ms.

| Arm | p50 | p90 | p95 | p99 | Inverse p50 rows/s | Inverse p99 rows/s |
|---|---:|---:|---:|---:|---:|---:|
| Local baseline | 300.570 | 352.319 | 394.005 | 450.666 | 33,270 | 22,189 |
| Local control | 290.190 | 341.277 | 402.124 | 512.397 | 34,460 | 19,516 |
| S3 readback | 1,039.003 | 1,309.513 | 1,887.281 | 2,456.418 | 9,625 | 4,071 |
| S3 checksum | 871.504 | 1,084.560 | 1,229.937 | 1,456.702 | 11,474 | 6,865 |

S3's empirical inverse-p50/p99 rates improved about 19.2%/68.6%; mean confirmed
window throughput improved 22.6%, from 8,850 to 10,850 rows/s. This is one
fixed-order comparison, not a randomized causal estimate or sustained-tail
qualification. The local controls' different tails cannot be attributed to an
S3-only option; one local control still missed 20k at p99.

All four independent reader processes verified 2,590,000 rows each, including
full physical-row and typed-value equality. Every arm retained the checkpoint,
renewal, confirmed-return, request/upload-budget and source-identity checks.

## What the counters establish

- Both S3 writers completed 257 observed file publications without publication
  errors. Baseline completed 257 file-publication/commit verification phases;
  candidate completed zero and observed 514 valid private proof checks. Proof
  checks are not unique uploads. Byte publication and checkpoints are unchanged.
- Both S3 readers made five GET requests and no uploads. They did not substitute
  upload metadata for independent object/body verification.
- Writer GET requests fell from 1,122 to 811. That includes metadata/renewal
  traffic as well as file verification, so do not attribute all 311 saved GETs
  to the 257 file publications. The shorter candidate window had fewer renewals.
- S3 allocation fell only about 1.5%, from 22.357 to 22.018 GB. Calling-thread
  CPU stayed near 40 seconds; it excludes worker CPU. Candidate recorded Scheme
  GC time was about 0.42% of its confirmed window. These are not RSS readings.
- Large-file PUT mean transfer time was 374 ms in baseline and 398 ms in
  candidate. GET means were 114 and 84 ms. These cover the process scope, not
  only measured samples; renewal requests can overlap the main writer. Curl
  milestones are cumulative, not exclusive phases or percentile lower bounds.

The option's real-provider **normal** path is demonstrated here. Bad-checksum,
ambiguous-response and conflicting-response controls still have local TLS
coverage, not new real-AWS fault qualification. Keep #278 open for that gate.

## Next performance targets

### Local DSL is a separate network limit

The local WSL connection is 50 Mbps down / **10 Mbps up**. The completed
GitHub-hosted run did not traverse that link. At 10 Mbps, upload capacity is at
most 1,250,000 bytes/s (1.25 decimal MB/s), before framing and other traffic.

The S3 candidate uploaded 1,847,311,712 bytes for 2,590,000 rows, including
metadata/checkpoint traffic: about 713.25 uploaded bytes per row. At that
observed amortization, a bandwidth-only projection for this DSL link is about
**1,753 rows/s** and **24.63 minutes of upload per writer arm**, before other
costs. Sustaining 20k–25k rows/s at the same wire volume would require roughly
114–143 Mbps of payload upload capacity. These are aggregate resource
projections, not measured DSL p50/p99 or percentile bounds from aggregate means.

Consequently, do not run this unchanged full window locally with its 20-minute
S3-writer timeout and interpret expiry as a performance regression. Use a
separate DSL-appropriate workload. Faster encoding cannot remove the bandwidth
limit. Buffered admission can absorb a bounded burst, not sustain arrivals above
remote drain capacity or turn admission into confirmed S3 persistence. A future
local-durable-first/remote-replication mode would need explicit, separate
guarantees; this report does not add one or weaken the existing S3 acknowledgement.

### Hosted pipeline and product integration

S3 still needs roughly 2.18x improvement at p50 and 2.91x at p99 in this profile.
The remote request/transfer costs are the next investigation target; another
small allocation reduction alone will not establish that gain. Measure the
batch critical path before deciding which head reads or pipeline stages can be
removed or overlapped without weakening fencing, CAS or confirmed returns.
Do not derive an architectural impossibility claim from aggregate means.

Forward-port the qualified owned-byte paths to current main, preserving newer
correctness fixes, then qualify the normal exporter/Oscope configuration.
Repeat matched measurements with explicit host/network context before claiming
stable tails or comparing directly with Rust/BB/JVM results from other hosts.

Only privacy-scanned scalar artifacts were retained: artifact `12073463467`,
85,153-byte archive, SHA256
`8a214dc29a9b49ae5e492a9b4aad4038bfe3da2b012a2f48182e3bcbe3801527`.
No raw cloud logs, credentials, telemetry or object contents were downloaded.
