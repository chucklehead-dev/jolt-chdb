# Short S3 batch-size research

Use the manual **typed S3 batch amortization research** workflow on protected
`main`. Its default `preflight` mode only checks the existing CI role and bucket
retention. Select `sweep` to measure larger batches using the current typed
exporter. No credentials or endpoint inputs are accepted by the workflow.

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
  labels change. Captured values, typed schema, exact SQL oracle, confirmation
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

The six S3 writer/reader pairs share **24,000 data requests and 6 GiB uploaded**,
not that allowance per arm. Each writer is additionally capped at 4,000 requests;
each fresh reader is capped at 1,000 GETs and zero uploaded bytes. A cumulative
ledger is carried between processes and checked against every retained receipt.
Any failed phase stops the sweep; there is no automatic retry/reset of an arm.
Artifact/native downloads and read-only IAM/bucket preflight are outside these
object-data request counters.

The workflow reuses the existing protected OIDC role, scoped run prefix,
preflight, request accounting, concurrency group and fail-closed privacy scan.
Normal readback verification remains enabled; no checksum-confirmation shortcut,
lease, CAS or acknowledgement guarantee changes. Only bounded scalar receipts
are uploaded. Native stores, scratch directories, raw logs and credentials are
not artifacts. Per-arm scratch is removed when its process scope finishes;
cloud fixtures expire under the preflight-checked bucket lifecycle.

## Reading results

The verifier checks all eight writer/reader pairs and prints p50, p90, p95 and
p99 from the raw samples, plus inverse-percentile rows/second and per-arm request
and upload counts. With 32 samples, p99 is the observed maximum, not a stable-tail
estimate. Every S3 budget receipt must chain from the previous phase, starting
from zero, with all reserved requests settled. Success means the experiment and
correctness checks completed, not that the application's rate target was met.

Use hosted results for S3 comparisons. A local 10 Mbps upload link cannot qualify
the same throughput. Keep all arms, including slow runs and the local bookends;
do not select only the fastest observed window.
