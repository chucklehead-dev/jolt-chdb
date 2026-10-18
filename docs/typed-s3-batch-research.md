# Short S3 batch-size research

Use the manual **typed S3 batch amortization research** workflow on protected
`main`. Its default `preflight` mode only checks the existing CI role and bucket
retention. Select `sweep` to measure larger batches using the current typed
exporter, or `confirmation` for the fixed checksum comparison below. No
credentials or endpoint inputs are accepted by the workflow.

This asks whether larger batches amortize object-store request latency. It is
not the fixed qualification gate and does not establish stable throughput or
tail guarantees. The existing 10k-row, 256-sample qualification is unchanged.

## Fixed workload and graph

- A short local 10k baseline runs before and after the S3 sweep, so host drift
  is visible instead of being mistaken for a batch-size benefit.
- S3 uses 10k, 20k, 50k, 50k, 20k, then 10k rows per export. Every arm has
  3 warmups and 32 measured exports, two 16-reference automatic checkpoints,
  1-second lease renewal stress, native streaming and owned statement output.
- The fixture is an exact, checked derivative of `3fd96c4`. Only its batch and
  sample bounds, checkpoint cadence, explicit 64 MiB insert limit and research
  labels change. Both the exporter and its untimed comparison encoder use that
  same explicit byte bound. Fixture exceptions propagate to the outer diagnostic
  entry rather than exiting before a failure receipt can be produced.
  Captured values, typed schema, exact SQL oracle, confirmation
  and full physical/typed readback checks are preserved.
- The frozen storage helper is loaded with one checked, process-local change:
  its prefix parser accepts exactly the six `/typed-window/1` through `/6`
  arm namespaces. The original helper and qualification gate stay untouched.
  A no-network control exercises the actual parser before any cloud writes.
- Exporter `67e28b5`, SDK `d93846e`, JSON `8d557cc`, driver source `3d766fa`,
  authenticated runtime `1ae7e0d1`, and native package 26.9.0 are fixed. The
  unadopted idle-companion JSON candidate is not selected.

The optional 64 MiB limit applies only to the experiment's encoded physical
insert payload. The library's default remains 8 MiB; driver/protocol limits
still apply. Larger batches also increase memory and time-to-confirmation.

## Cloud and cleanup bounds

The S3 writer/reader pairs share **24,000 data requests and 6 GiB uploaded**,
not that allowance per arm. Each writer is additionally capped at 4,000 requests;
each fresh reader is capped at 1,000 GETs and zero uploaded bytes. A cumulative
ledger is carried between processes and checked against every retained receipt.
Any failed phase stops the sweep; there is no automatic retry/reset of an arm.
Artifact/native downloads and read-only IAM/bucket preflight are outside these
object-data request counters.

The workflow reuses the existing protected OIDC role, scoped run prefix,
preflight, request accounting, concurrency group and fail-closed privacy scan.
Normal readback verification remains enabled in `sweep`; `confirmation` changes
only the explicit writer option in its two candidate arms. No lease, CAS or
acknowledgement guarantee changes. Only bounded scalar receipts
are uploaded. Native stores, scratch directories, raw logs and credentials are
not artifacts. Per-arm scratch is removed when its process scope finishes;
cloud fixtures expire under the preflight-checked bucket lifecycle.
On failure, retain only a separate process exit receipt and, when available,
closed error categories, benchmark labels/startup stages and numeric request
progress. Unknown values collapse to `nil` or `:other`; exception messages and
arbitrary error data are never copied. These are diagnostics, not settlement or
performance results. A failure still stops the sweep without retrying an arm.

## Reading results

The sweep verifier checks all eight writer/reader pairs and prints p50, p90, p95 and
p99 from the raw samples, plus inverse-percentile rows/second and per-arm request
and upload counts. With 32 samples, p99 is the observed maximum, not a stable-tail
estimate. Every S3 budget receipt must chain from the previous phase, starting
from zero, with all reserved requests settled. Success means the experiment and
correctness checks completed, not that the application's rate target was met.

Use hosted results for S3 comparisons. A local 10 Mbps upload link cannot qualify
the same throughput. Keep all arms, including slow runs and the local bookends;
do not select only the fastest observed window.

## Matched checksum confirmation

Select `confirmation` only after the separate [provider controls](durable-checksum-provider-controls.md)
have passed for the target provider. Four S3 arms run in fixed order:
**readback, checksum, checksum, readback**, each with 50k rows per export,
3 warmups and 32 measured exports. The same 10k local bookends, source/runtime
pins, 64 MiB insert limit, checkpoints, renewal stress and global cloud budget
apply. The ordinary size sweep and fixed qualification remain separate.

Only candidate writers receive `:wal-upload-confirmation :sha256-single-put`.
The normal native transport must produce a valid confirmation witness; the
experiment never fabricates one. After normal owner settlement, selection
receipts must show all 33 file-backed WAL publications: baseline writers perform
33 verification reads, while candidate writers perform none and observe valid
proofs. An unselected or partial fast path fails the experiment even if normal
fallback safely persisted the data. Two exports publish checkpoints rather than
WAL files, so 35 total exports do not imply 35 WAL publications.

Every arm has a separate fresh reader using normal readback and the existing
full physical/typed equality oracle. Reader selection receipts must show zero
publications/proofs. The verifier checks all six writer/reader pairs, the fixed
A/B/B/A selection, raw latency quantiles and the cumulative budget chain. Only
closed scalar selection counts are retained in addition to existing receipts.
Passing this short comparison is not stable-tail qualification, permission to
enable the option by default, or evidence that arbitrary S3-compatible providers
support the same checksum contract.
