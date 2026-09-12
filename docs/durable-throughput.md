# Durable JSONEachRow throughput qualification

`jdbc.chdb-durable-throughput` is a manual, production-path benchmark for the
ClickStack-shaped log inserts used by `jolt-otel-clickhouse`. It does not alter
the Durable protocol or its ordering. Each accepted materialized statement is
prepared and size-checked before native mutation, appended to the pending WAL
only after native success, and published by an explicit later flush.

The primary acceptance workload is 512 rows per `FORMAT JSONEachRow` statement.
Encoding-inclusive Durable admission must have p50 batch latency no greater
than 20.48 ms (25,000 rows/s) and p99 no greater than 25.60 ms (20,000 rows/s).
The explicit `qualification` profile measures 100 batches per trial and five trials per
mode, so each trial p99 has 100 observations and the report's
`batch-latency-across-trials` summary has 500. The smaller `smoke` and `probe`
profiles are causal diagnostics only; their p99 values do not qualify the
latency target.

The `scale` profile runs about 50,000 measured rows per trial at batch sizes
512, 1,000, 5,000, and 10,000, with five trials per mode. Its 512- and
1,000-row configurations have at least 100 pooled batch observations and may
report an empirical p99. The larger configurations deliberately report
`:p99-qualification? false`; their p99 values are directional diagnostics
until a later run supplies at least 100 observations. Under the nearest-rank
calculation used here, a p99 over fewer than 100 samples degenerates to the
observed maximum; do not quote that number as a qualified tail percentile.

For process-attributable runs, use one of the validated selectors
`scale-512`, `scale-1000`, `scale-5000`, or `scale-10000`. Each selector
resolves to exactly one checked-in scale configuration and suppresses the
isolated-stage and instrumented supplementary controls, so an external peak-RSS
reading belongs to that configuration rather than to the complete sweep.
Near-miss or unsupported selectors fail before database work; they never fall
back to the full sweep.

Staged recovery diagnostics use `recovery-512-10`, `recovery-512-25`, and
`recovery-512-50`. These run one pre-encoded trial of exactly 10, 25, or 50
512-row WAL records with no warmup rows, then close the writer, open a fresh
snapshot reader, and reconcile the full aggregate oracle. Run the three stages
as separate processes in increasing order. Each process includes preparation,
writer admission/publication, and recovery; its peak RSS is not a
recovery-only measurement. These are bounded diagnostics, not a plateau
qualification.

Trial workload construction is bounded by the configured batch size. Warmup
and measured row maps are generated one batch at a time, pre-encoded modes keep
only the next statement outside the admission timer, and expected recovery
aggregates are folded into scalars as each batch is consumed. Row generation
and pre-encoding remain outside pre-encoded timing; encoding remains inside
encoding-inclusive timing. The harness therefore does not retain the complete
approximately 50,000-row workload or a second copy for reconciliation.
Aggregate admission time is the sum of those same per-batch samples, excluding
row generation, oracle folding, and counter collection between batches.

The report keeps these measurements separate:

- encoding-inclusive Durable admission before flush;
- pre-encoded Durable admission before flush;
- persisted throughput with one explicitly reported flush cadence; and
- ordinary JDBC and direct prepared/native controls.

The persisted rate amortizes one flush across the reported number of batches.
It is not a claim of per-batch durability. Single-row inserts and remote
per-batch flushes are separate ceilings and cannot qualify the primary target.

## Running a local probe

Use the workspace's pinned Chez wrapper, the repository-pinned standalone Jolt
executable, and a Durable-capable native library. Set the executable explicitly;
an unqualified `jolt` may select a different release. Metadata environment
variables make the resulting EDN self-describing:

```sh
BENCH_JOLT_BIN=/absolute/path/to/repository-pinned/jolt
BENCH_JOLT_SOURCE_SHA=full-jolt-source-commit

BENCH_JOLT_BIN="$BENCH_JOLT_BIN" \
BENCH_JOLT_SOURCE_SHA="$BENCH_JOLT_SOURCE_SHA" \
BENCH_JOLT_VERSION="$(/home/chuck/ai-src/tools/jolt-with-chez-10.4.1 "$BENCH_JOLT_BIN" --version)" \
BENCH_GIT_HEAD="$(git rev-parse HEAD)" \
BENCH_GIT_PARENT="$(git rev-parse HEAD^)" \
BENCH_GIT_TREE="$(git rev-parse HEAD^{tree})" \
BENCH_GIT_STATUS="$(test -z "$(git status --porcelain)" && printf clean || printf dirty)" \
BENCH_STARTED_AT="$(date -Iseconds)" \
JOLT_CHDB_LIB=/path/to/qualified/libchdb.so \
/home/chuck/ai-src/tools/jolt-with-chez-10.4.1 \
  "$BENCH_JOLT_BIN" -M:durable-throughput probe target/profiles/probe.edn
```

Accepted profiles are `smoke`, `probe`, `diagnostic`, `scale`, and
`qualification`, plus the four `scale-*` and three `recovery-512-*` selectors
listed above. Unknown names fail before any database work. `scale`,
`qualification`, and every isolated selector additionally reject missing Jolt
version/source/executable-digest, Git, timestamp, or native-library digest/size
provenance, and reject a dirty worktree; their reports are intended to be
comparable evidence rather than anonymous or locally modified samples.

Use `scale` for the checked-in batch sweep:

```sh
BENCH_JOLT_BIN=/absolute/path/to/repository-pinned/jolt
BENCH_JOLT_SOURCE_SHA=full-jolt-source-commit

BENCH_JOLT_BIN="$BENCH_JOLT_BIN" \
BENCH_JOLT_SOURCE_SHA="$BENCH_JOLT_SOURCE_SHA" \
BENCH_JOLT_VERSION="$(/home/chuck/ai-src/tools/jolt-with-chez-10.4.1 "$BENCH_JOLT_BIN" --version)" \
BENCH_GIT_HEAD="$(git rev-parse HEAD)" \
BENCH_GIT_PARENT="$(git rev-parse HEAD^)" \
BENCH_GIT_TREE="$(git rev-parse HEAD^{tree})" \
BENCH_GIT_STATUS="$(test -z "$(git status --porcelain)" && printf clean || printf dirty)" \
BENCH_STARTED_AT="$(date -Iseconds)" \
JOLT_CHDB_LIB=/path/to/qualified/libchdb.so \
/home/chuck/ai-src/tools/jolt-with-chez-10.4.1 \
  "$BENCH_JOLT_BIN" -M:durable-throughput scale target/profiles/scale.edn
```

For an attributable local maximum-RSS observation, substitute one exact
selector and run it in a fresh process. GNU `time` is external to Jolt, so save
its stderr beside the EDN report. For example:

```sh
BENCH_JOLT_BIN=/absolute/path/to/repository-pinned/jolt
BENCH_JOLT_SOURCE_SHA=full-jolt-source-commit

/usr/bin/time -v -o target/profiles/recovery-512-10.time \
  env BENCH_JOLT_BIN="$BENCH_JOLT_BIN" \
      BENCH_JOLT_SOURCE_SHA="$BENCH_JOLT_SOURCE_SHA" \
      BENCH_JOLT_VERSION="$(/home/chuck/ai-src/tools/jolt-with-chez-10.4.1 "$BENCH_JOLT_BIN" --version)" \
      BENCH_GIT_HEAD="$(git rev-parse HEAD)" \
      BENCH_GIT_PARENT="$(git rev-parse HEAD^)" \
      BENCH_GIT_TREE="$(git rev-parse HEAD^{tree})" \
      BENCH_GIT_STATUS="$(test -z "$(git status --porcelain)" && printf clean || printf dirty)" \
      BENCH_STARTED_AT="$(date -Iseconds)" \
      JOLT_CHDB_LIB=/path/to/qualified/libchdb.so \
      /home/chuck/ai-src/tools/jolt-with-chez-10.4.1 \
      "$BENCH_JOLT_BIN" -M:durable-throughput recovery-512-10 \
      target/profiles/recovery-512-10.edn
```

Repeat with `recovery-512-25` and `recovery-512-50`, or with one of the four
`scale-*` selectors. Retain GNU `time`'s `Maximum resident set size (kbytes)`
line, complete command/provenance, and the matching EDN as one evidence unit.
The GNU-time peak covers the entire fresh process, including writer setup and
publication. Compare the staged runs conservatively; do not subtract endpoint
allocator samples or call the result recovery-only auxiliary memory.

Reports under `target/profiles/` are ignored. The harness writes bounded phase
evidence after each uninstrumented trial, then isolated stages, then one
instrumented causal control. If a later phase times out, completed evidence is
still readable. The `diagnostic` profile is a one-batch, pre-encoded trace with
timestamps around open, DDL, warmup, admission, flush, close, and recovery.
Runtime metadata records only the Jolt executable and native library file names,
sizes, and digests; it does not expose either host filesystem path. The full
Jolt source commit is recorded separately.

Every Durable run asserts the pending statement count, commits exactly one
measured flush, opens a fresh immutable reader, and reconciles count, trace-flag
and severity sums, total body bytes, question-mark bodies, and min/max trace and
span IDs. The instrumented control additionally asserts one classification,
native execution, immutable WAL PUT, and head CAS per expected operation. It
records only operation labels, counts, byte counts, and timings.

Each recovery result includes absolute Jolt allocator observations immediately
before fresh-reader open and immediately after successful open plus aggregate
reconciliation: live Scheme heap, bytes currently reserved from the OS, and
the allocator's process-lifetime peak reserved bytes. These are not RSS. Two
endpoints cannot establish a plateau or bound transient growth, and the report
therefore records `:plateau-or-growth-oracle :not-supported` rather than
manufacturing a pass/fail threshold. A checked plateau/growth oracle requires a
later repeated-stage design with justified noise and slope bounds; current
qualification requires external GNU `time -v` maximum RSS.

The result also names cumulative pending WAL growth separately from the largest
input batch: `:wal-growth {:total-bytes ... :records ...}` is the complete
measured WAL segment before flush, while `:maximum-batch-statement-bytes` and
`:maximum-batch-payload-bytes` are maxima over one admitted batch. Do not quote
the cumulative WAL bytes as a record-size or batch-size measurement.
The paths under `:output-contract` that describe those values and recovery
memory are relative to each entry under `:configurations[*] :results[*]`, as
recorded by `:relative-trial-paths-to :configuration-result`.

The instrumented benchmark deliberately does not replace the production
`publish-wal!` operation. That closure owns the complete operation retry budget,
including the lease-aware stop predicate. Immutable WAL PUT and head-CAS timing
come from the instrumented backend instead, preserving retry and fencing
semantics while still proving that publication occurred.

## Pre-optimization diagnostic result

The September 10, 2026 probe at main `5bbe8651b6c24cd713a826a8a9a0f3fcb56fcd24`
used Jolt 0.8.3, Chez 10.4.1, chDB `26.7.2-rc.2`, and a 348,877-byte statement
containing 512 realistic OTel log rows. Two samples are causal diagnostics, not
a latency qualification. These historical measurements predate the current
Jolt 0.8.6 support floor:

| Path or isolated stage | Observed batch latency |
| --- | ---: |
| Durable, encoding included, p50 | 2,633.86 ms |
| Durable, pre-encoded, p50 | 2,028.54 ms |
| Ordinary JDBC, pre-encoded, p50 | 529.30 ms |
| Exporter encoding total, p50 | 645.64 ms |
| First placeholder rewrite, p50 | 718.15 ms |
| Generic `data.json` WAL encoding, p50 | 1,314.97 ms |
| Direct prepared/native insert, p50 | 10.32 ms |

The direct native control bypasses placeholder rewriting only inside the
benchmark and consumes/destroys the native result. It establishes that the
engine can execute this shape within the target's batch budget. At the observed
10.32 ms native p50, all row encoding, classification, WAL preparation, queue,
and wrapper work together have only 10.16 ms left to reach 25,000 rows/s. The
observed managed stages exceed that remainder independently, so running a long
20,480-row suite on this implementation would consume time without being able
to qualify the target.

The stale PR #63 result used a different 136 KB pre-encoded statement, omitted
exporter encoding, and did not commit its benchmark harness. Its 24,851-25,867
rows/s figures therefore remain hypotheses rather than a current-main result.
Its global rewrite memo and WAL/native overlap are not admissible replacements:
request data must not be retained globally, and WAL preparation and size
validation must still complete before native mutation.

The lowest-risk optimization order is:

1. Profile `clojure.data.json` on both realistic row maps and the quote-heavy
   outer WAL record. Prefer a generic bulk-run string writer if it preserves
   exact escaping and reader round trips.
2. Improve generic Jolt string scanning/bulk append primitives where the same
   evidence shows a runtime bottleneck.
3. Add a request-local materialized insert representation so validated row
   payload is not lexed twice and no SQL or parameters outlive the request.
4. Consider a narrow exact WAL encoder only if the generic JSON path cannot
   meet the budget. Preserve UTF-8 boundaries, astral/control round trips,
   preparation-before-mutation, native-failure/no-WAL, and replay evidence.

Do not overlap WAL construction with native execution under V1. That changes a
failure cut: native success followed by encoder failure could leave engine state
without replay state.

## Generic `data.json` transfer probe

A bounded paired probe compared the prior `data.json` pin
`932444043c0c06f9e295ba4963419b2481e9dd07` with merge
`95b1e6430b48ce4fb4e649656b79f7cabc4702a7`, changing no Durable code. Both
runs used the same 512-row workload, Jolt 0.8.3, Chez 10.4.1, and native chDB
digest, then reopened immutable state and reconciled the same aggregates. This
historical comparison also predates the current Jolt 0.8.6 support floor.

| Path or isolated stage | Prior pin | Bulk-run pin |
| --- | ---: | ---: |
| Durable encoding-inclusive admission | 133.08 rows/s | 178.05 rows/s |
| Durable pre-encoded admission | 171.91 rows/s | 206.30 rows/s |
| Generic `data.json` WAL encoding, p50 | 1,786.69 ms | 851.59 ms |
| Exporter encoding, p50 | 878.82 ms | 803.06 ms |
| Placeholder rewrite, p50 | 719.87 ms | 715.21 ms |

The generic WAL stage also allocated about 45 percent fewer Scheme heap bytes
in this probe. The unchanged placeholder stage acts as a useful negative
control. Each side has only two latency samples, and the ordinary native
control varied in the opposite direction, so these figures establish a causal
direction and justify the dependency update but do not qualify latency or tail
throughput. The remaining gap is still dominated by managed encoding and
placeholder handling.

## AWS S3 qualification design

S3 is a separate qualification slice. The local selectors and memory
observations above neither exercise S3 nor change its design.

The checked-in foundation keeps provider selection out of the workload. Each
Durable trial accepts an execution-only backend-context factory yielding a
namespace backend, opaque object id, fixed provider-kind label, and cleanup
callback. The factory is removed from configuration and progress reports; its
closed-over endpoint, bucket, prefix, credentials, and object id therefore
cannot enter benchmark EDN. The default factory remains `local-posix`.

`jdbc.chdb-durable-throughput-metrics` supplies two independent wrappers for
the remote runner:

- the logical wrapper records each fixed `ObjectBackend` operation; and
- the transport wrapper records every actual S3 transport attempt, so retries
  cannot disappear inside one logical call.

Both retain only fixed phase/operation labels, call counts, request-body and
response-body byte counts, and p50/p95/max/total latency. Logical results use a
closed set of keyword categories; transport results preserve numeric HTTP
statuses from 100 through 599 and collapse any other status to
`invalid-response`. These byte fields exclude HTTP framing, headers, and TLS
overhead; they are not wire-byte measurements. Sample-support flags prevent
one or a few requests from being presented as supported percentiles. Retry
amplification is reported as logical calls, transport attempts, and extra
attempts for the same fixed phase/operation pair. Opaque ETags and all
request/response objects are dropped.

The uncontended flush assertion is outcome-specific. A normally committed head
CAS must have the exact production base shape: three head reads, one immutable
WAL create, one WAL verification read, and one head CAS. A reconciled ambiguous
head CAS must retain that shape plus one to three head proof reads, bounded by
the writer's fixed four-attempt control budget, and its sole head CAS must be
classified `ambiguous`. Missing or additional logical operations fail the run.
S3 transport retries remain visible separately but cannot inflate logical-call
allowances.

The focused fake-transport contract uses canaries in credentials, URL,
headers, ETag, SQL-shaped request bytes, payload-shaped response bytes, bucket,
prefix, and object key. It quietly scans bounded EDN and captured stdout/stderr;
failure reports only a generic type and never the match. Run it without AWS or
native chDB:

```sh
jolt -M:durable-throughput-metrics-test
```

The manual OIDC workflow runs this redaction gate before assuming its AWS role.
Its opt-in `run_throughput` job then runs `s3-curve`: each target and admission
mode receives a fresh object, ordinary writer admissions continue until the
post-admission `writer/status` reports the requested `:pending-wal-bytes`, and
the unchanged production publisher performs exactly one measured flush. The
logical and transport wrappers bind setup, admission, flush, writer-close, and
fresh-reader recovery as separate phases. No benchmark publisher or alternate
retry path exists. The encoder retains its single UTF-8 payload byte array only
until that admission is recorded; the post-timing maximum-row scan walks LF
offsets in the same array without decoding, splitting, substring allocation, or
secondary encoding, and the array never enters reports or progress output.

Remote qualification should reuse the environment-protected manual OIDC lane
and unique run prefix from `durable-aws.yml`; it must not assume local AWS
credentials. The first curve uses the current single conditional PUT only. No
multipart implementation belongs in this benchmark slice.

Use the same 512-row statements and recovery aggregates as local POSIX, varying
the measured pending-WAL target for one explicit flush. Record WAL object bytes,
immutable PUT, head GET/verification, and head-CAS counts plus p50/p95/max/total
latencies. Reports must not contain credentials, signed URLs, headers, SQL,
payload bodies, bucket names, or object prefixes.

Include independent flush points at 419,430 bytes and 3, 12, 48, 64, 96, and
127 MiB of measured pending WAL. Stop below the frozen 128 MiB segment limit
using `:pending-wal-bytes`, rather than an estimated batch count. The fixed
targets are exposed as `wal-target-bytes` for the runner and contract tests.
For each point, calculate required aggregation from measured admission capacity
`A`, desired persisted rate `R`, and fixed S3 latency `L`:

```text
N >= R * L / (1 - R/A), when A > R
batches needed = ceil(N / 512)
```

If `A <= R`, no finite aggregation can reach that persisted target; the report
marks it explicitly impossible. `R*L` alone ignores admission time and must not
be presented as sufficient. Admission capacity and flush latency must both be
finite; NaN and either infinity fail with the same closed validation errors as
other invalid inputs.

Only recommend multipart upload if measured 64-128 MiB WAL or checkpoint
evidence shows single-PUT behavior is the limiting factor. Run this remote
curve after local admission meets its target; before that, local managed
serialization dominates and would make the S3 curve needlessly expensive.

The workflow artifact contains only a bounded EDN report and a canary-scanned
bounded log, retained for 14 days. The workload has no process-wide file-size
limit, so 127 MiB WAL and native temporary files can complete. Its raw log lives
only in runner temporary storage; at most 8 MiB is copied into the artifact
directory, and an oversized raw log fails the job. Both artifact files are
checked for type, symlink status, size, and every configured identity/content
canary before upload. A failed run or failed scan uploads nothing. The report
records a
sanitized GitHub-hosted runner/workflow identity, provider kind, and region,
but not the bucket, prefix, endpoint, namespace, or object keys. Setup,
writer-close/release, and recovery remain outside the persisted-rate numerator;
recovery gets its own phase and request counts.

The opt-in job requires `AWS_DURABLE_THROUGHPUT_JOLT_SHA` in the protected
environment to equal the workflow's exact compiler source pin. Leave the
variable unset until the local 512-row qualification has selected that same
pin. The job also runs the checksum-pinned chDB 26.7.2-rc.2 native gate before
assuming the AWS role. It also depends on the complete provider/redaction job,
so its own OIDC role assumption cannot begin after a failed provider contract.
A live dispatch remains prohibited until the execution gate is satisfied and
reviewed.
