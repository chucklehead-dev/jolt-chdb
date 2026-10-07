# Fresh versus reused S3 transport comparison

Manual profile only; not a production default change. The existing workflow
`exporter-storage-qualification.yml` accepts `reuse_comparison: true`. It runs
the same local control, then fresh-handle and scoped-reuse S3 arms sequentially,
using distinct run-id/run-attempt/mode prefixes and separate writer/reader
processes. Both S3 arms use identical library, exporter, JSON, native and
runtime code. The sole transport selection is `BENCH_REUSE_CURL=true`.

Start with `batches: 5`: attribution and custody, not p99 qualification. The
100-batch option is retained for a later qualified run. Reuse does not batch
ACKs, skip verification, omit head reads or alter checkpoint policy. The
existing OIDC role/environment, concurrency group, artifact checksum checks,
timeouts and credential-free receipts remain in place. Curl receipts report
actual connection counts by operation, and writer provenance checks loaded
curl source and records its digest. Reader mode must match its writer receipt.
No benchmark or provider options are appended to the sanitized db-spec.

`bench/s3-reuse-loopback-smoke.sh <absolute-report-directory>` runs only the
local synthetic S3 fixture, with 100 rows per signal and one measured batch;
the same exact checkout/runtime environment inputs as the existing launcher
are required. It overwrites auth inputs with synthetic fixture credentials,
does not call AWS, and removes its own temporary database/scratch directory.
The launcher now checks the exact transport-prototype library source at
`7a95fd46fd44a9255e4b5268a5cb7104d226685d`, rather than silently claiming the
older ca7fc8a6 source is unchanged. Exporter715cf815 / JSON3adc8c5 / bcb376a0
runtime and 26.7.3 native pins are preserved for the comparison.

## Verified mechanism and fixture sensitivity

Both synthetic arms completed committed writes and independent reader count
checks for 500 physical rows. Their writer made 86 successful transport calls
and reader made 12. Fresh handles opened 86 / 12 connections respectively;
reused scopes opened one in each process. Diagnostic declines were zero.

The original fixture unexpectedly made persistent GETs slower: writer GET
p50 total 42,373 microseconds, first byte 1,080 microseconds. Enabling the
Python handler's explicit TCP_NODELAY option removed the approximately 40 ms
body-delivery delay (GET p50 total 934 microseconds, first byte 803 microseconds
in that control). The ordinary fixture keeps its previous behavior. This
supports a fixture packetization/delayed-ACK interaction, not an AWS regression
or a general claim that connection reuse is always faster. Neither tiny smoke
run establishes application throughput or useful batch-latency percentiles.

Workspace receipts: `evidence/chdb-exporter-reuse-loopback-20261007/`,
`evidence/chdb-exporter-reuse-loopback-nodelay-20261007/`, and
`evidence/chdb-exporter-reuse-loopback-final-20261007/`.

Before actual dispatch: independent review of driver/workflow and added
failure/drain tests, refresh exact public head and live run state, confirm the
existing protected role/prefix budget, then run the bounded five-batch pair.
Only actual AWS counts, connections and latency receipts can support an S3
performance conclusion. Native reset semantics remain an explicit libcurl
contract; focused lifecycle gates supplement, not replace, publication models.
