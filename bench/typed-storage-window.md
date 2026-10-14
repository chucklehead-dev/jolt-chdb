# Matched typed-span storage window

This opt-in benchmark sends the same synthetic typed spans through the public
OTel SDK/exporter into a Durable writer, using local POSIX or S3 storage. It
does not change normal dependency pins or enable experimental features for apps.

The fixture, exact-SQL oracle and full reader checks come from the completed
local `sdk-269-two-callers-20261009.clj` qualification driver. The storage support
module changes ownership/routing only: local connections use `with-open`; S3
uses `jdbc.chdb.durable.s3-writer/with-connection!`. The exporter borrows the
connection and shuts down inside the callback. The scope then closes/joins the
writer before releasing either data or independent renewal curl owner. Do not
return live connections or start unjoined producers from the callback.

## Exact stack

- libchdb 26.9.0, independently checked asset checksum.
- Cumulative Jolt 0.8.21 capabilities with guarded byte writer/observable
  protocol sites. Stock Jolt 0.8.21 alone does not supply all those capabilities.
- Exporter `4f3bfd2b`, JSON `b959a562`, chDB runtime source `291e1f43`.
- Native writer metadata reader is optional and writer-scoped; startup and
  separate-process recovery retain the ordinary reader.

`run-typed-storage-window.sh` checks source/native/binary identities before
launching. Set `BENCH_EXPECT_EXPORTER_ROOT`, `BENCH_EXPECT_JSON_ROOT`,
`BENCH_JOLT_BIN`, `BENCH_JOLT_WRAPPER` and `BENCH_RUNTIME_BINARY_SHA256` from an
independently authenticated profile. Local commands use the mandatory Chez
10.4.1 wrapper. Hosted standalone binaries embed their own verified Chez.
The measured `3250c32d` binary's older baked tag is documented in the launcher;
its actual source contains 0.8.21 ancestry. Never substitute the ordinary local
Jolt installation based on its version label.

## Run

Create a private fresh report directory and select these non-secret switches:

```sh
export SDK_STORAGE_ARM=local  # or s3
export SDK_STREAM_ARM=stream
export SDK_EXPORT_CALLERS=1
export CHDB_BENCH_OWNED_OUTPUT=true
export CHDB_BENCH_SAMPLE_COUNTERS=true
export CHDB_BENCH_METADATA_READER=native

bash bench/run-typed-storage-window.sh window "$REPORT_ROOT" 10000 256 true wide16
CHDB_BENCH_METADATA_READER=stock \
  bash bench/run-typed-storage-window.sh reader "$REPORT_ROOT" 10000 256 true wide16
```

Each command is a separate process. `window` requires at least 256 batches so
two automatic 128-reference checkpoints are actually exercised. A small `writer`
smoke is useful for functional checks but is not p99 qualification.

For S3, assume the existing CI role first and supply the `JOLT_CHDB_S3_ENDPOINT`,
`BUCKET`, `REGION`, `ACCESS_KEY`, `SECRET_KEY` and `SESSION_TOKEN` environment
variables (each with the `JOLT_CHDB_S3_` prefix). `JOLT_CHDB_S3_PREFIX` must be
exactly `ci/jolt-chdb/RUN_ID-RUN_ATTEMPT/typed-window`, with numeric run/attempt.
Credentials are read only for S3 and never included in reports/errors. Do not
enable shell tracing or print backend objects, headers, responses or environment
values. The isolated writer process temporarily observes `control/renew!` to
count completed real calls; it retains results/errors and restores the Var at
scope exit. This is benchmark instrumentation, not a recommended app hook.

## Evidence and limits

Keep process exit statuses as well as `writer.edn` / `reader.edn`: the writer
receipt is produced inside the owner callback, so a later close failure must
not be ignored. Writer report includes confirmed wall time, p50/p90/p95/p99/max,
per-call runtime counters and actual stream/checkpoint/renewal witnesses. It
contains no captured telemetry values or storage credentials.

The fresh reader checks every grouped physical row, repetitions, timestamps,
typed values/statuses and physical types, not just row counts. The 256-batch
10k scenario expects 2,590,000 rows including three warmups. Throughput expressed
as inverse latency is `10000 * 1000 / percentile-ms`; report it separately from
overall wall throughput. Finite samples are not a probabilistic p99 guarantee.

Only prune local objects/scratch after both processes succeed and source/input
postguards pass. Retain small reports, logs and checksum receipts. S3 cleanup
must be scoped to the exact unique run prefix after writer settlement and
successful recovery; never delete a caller's broader namespace. A failed run
needs its bounded failure evidence preserved. Real AWS qualification and a
green authenticated compiler artifact remain separate gates.
