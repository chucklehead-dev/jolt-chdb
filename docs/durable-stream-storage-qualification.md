# Qualifying streaming exports on local storage and S3

`bench/run-durable-stream-storage-qualification.sh` uses the current confirmed
typed-span export workload with explicit exporter, JSON and compiler checkouts.
It can use local storage or the production S3 backend. It never substitutes
local storage when S3 configuration is missing or invalid.

Select `CHDB_BENCH_STORAGE=local` or `s3`. Local mode is the default and does not
read AWS credentials. S3 mode requires the existing `JOLT_CHDB_S3_*` endpoint,
bucket, region, prefix, access-key, secret-key and session-token inputs. Keep
credentials in the CI environment, not command arguments or files in the repo.
The endpoint must match the selected commercial AWS region. Prefixes are
restricted to `ci/jolt-chdb/<run-id>-<attempt>/stream-26-9`, optionally followed
by one lowercase/digit/hyphen lane name. A shared or empty prefix is rejected.

The runner requires `BENCH_EXPECT_EXPORTER_ROOT`, `BENCH_EXPECT_JSON_ROOT`,
`BENCH_JOLT_BIN`, `BENCH_JOLT_WRAPPER` and `BENCH_JOLT_SHA256`. It checks exporter
`0d1e41e`, JSON `805bb9a`, compiler version `v0.8.17-48-g20f25cf4` and the
supplied qualified runtime checksum. Library source must match chDB `368e767`;
benchmark-only changes are separate. Local commands must use the workspace's
mandatory Chez 10.4.1 wrapper. CI must authenticate the immutable runtime
artifact before exposing OIDC-assumed credentials to any benchmark process.

Set the native library to the independently verified 26.9.0 artifact and select
`SDK_STREAM_ARM=stream`, `SDK_EXPORT_CALLERS=1`, `CHDB_BENCH_OWNED_OUTPUT=true`
and `CHDB_BENCH_SAMPLE_COUNTERS=true`. Then run, in separate processes:

```sh
bash bench/run-durable-stream-storage-qualification.sh writer RUN_ROOT 10000 20 true wide16
bash bench/run-durable-stream-storage-qualification.sh reader RUN_ROOT 10000 20 true wide16
```

Use `window` instead of `writer`, with 300 samples, for the two automatic
checkpoint and renewal witnesses. Every measured export awaits its own Durable
confirmation. Reports distinguish confirmed wall throughput from inverse
per-call p50/p90/p95/p99 latency. Fresh readers verify physical fields, exact
nanosecond timestamps, typed values/statuses, duplicate counts and total rows,
not merely table counts. Both processes must select the same storage/prefix.

The driver emits only a fixed failure marker on caught errors; do not add
exception messages/causes or backend options to CI logs or uploaded receipts.
This affects diagnostics only, not persisted telemetry or the WAL. Keep only
bounded scalar receipts. Remove successful local object/scratch stores after
fresh recovery and input checks; preserve failed runs for diagnosis. S3 objects
belong to the isolated run prefix and need separately scoped cleanup.

Validation for this support: 3 tests/51 assertions exercise pure selection,
missing credentials, prefix/endpoint rejection and no-backend-on-error controls.
A local 1k-row, five-sample confirmed run recovered all 8,000 physical rows in
a fresh process. A synthetic invalid S3 prefix exited with the fixed marker,
did not print synthetic credential values and created no storage directory.
No hosted S3 request was made. The existing main S3 workflow still selects an
older stack; this support must be reviewed and wired to a qualified new runtime
artifact before it can provide hosted performance evidence.
