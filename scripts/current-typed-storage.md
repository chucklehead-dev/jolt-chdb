# Current typed SDK storage qualification

This manual workflow measures the actual SDK/exporter path on local storage
and S3. It does not replace the separate pre-encoded Durable benchmark.
Application defaults and the existing AWS role and bucket policies are unchanged.

Dispatch `current-typed-storage.yml` from protected `main`. The default mode,
`preflight`, assumes the existing CI role and reads identity, role policies,
bucket versioning and lifecycle metadata. It does not write objects. Unsupported
policy shapes, permissions boundaries, missing metadata permissions or missing
bounded lifecycle rules fail closed; do not bypass the gate or silently change
the role to make a benchmark run.

The gate accepts a narrow known policy shape, not every valid IAM policy. It
does not prove effective permissions under bucket policies, SCPs or other
external controls, and an AWS lifecycle rule is not a promise of deletion at
an exact time. Enabled and suspended versioning both require bounded noncurrent
expiry. Raw policies, identifiers and CLI errors are not published.

`qualify` first authenticates the fixed compiler artifact and native library,
then runs a local 256-batch window of 10,000 rows plus three warmups and separate
stock-reader recovery. Only after that does it assume the role, perform the
read-only preflight and run the same S3 workload in a unique CI prefix.

The S3 wrapper reserves request count and declared upload bytes atomically
before calling the production curl transport. Its limits are 24,000 requests
and 6 GiB uploaded for the writer, and 1,000 GET-only requests for recovery.
The process deadlines are 30 minutes and four minutes respectively. These are
request/upload bounds, not a hard bound on downloaded bytes or a monetary quote.
Only method and byte counters are observed; no URLs, credentials, headers or
payloads are retained by the wrapper. Reservations include failed attempts;
completion counts indicate that transport returned, not that HTTP succeeded.

The limits cover real renewal and cleanup calls as well as publication. A budget
failure can prevent lease release; it fails the process and does not fabricate
successful shutdown or persistence. A successful writer receipt alone is not
enough: require process exit zero, the post-scope budget receipt, and independent
full-row reader equality. Keep writer and reader budget receipts separately.
Because this profile includes budget instrumentation, compare its performance
as an instrumented profile rather than an identical bare baseline.

A final receipt gate checks the exact batch count, SQL fixture, native stream
witness, checkpoint/renewal witnesses, independent recovery and budget scopes,
and rechecks pinned source and native-library parity. It reports p50/p90/p95/p99
and whether the 25k inverse-p50 / 20k inverse-p99 targets were observed. Correct
recovery and meeting throughput targets are separate results: a valid run that
misses the performance target remains evidence of an unfinished performance goal.

Local controls, without AWS:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 scripts/test_typed_s3_preflight.py
bb scripts/test_typed_storage_budget.clj
actionlint .github/workflows/current-typed-storage.yml
PYTHONDONTWRITEBYTECODE=1 python3 scripts/test_typed_receipt_privacy.py
PYTHONDONTWRITEBYTECODE=1 python3 scripts/test_current_typed_jolt.py
bb scripts/test_typed_storage_receipts.clj
```

Run the same request controls under the pinned qualified Jolt binary as well.
They exercise pre-transport rejection, transport-error identity and concurrent
reservations. They do not by themselves validate S3 or shutdown. The separate
loopback S3 fixture exercises the real scoped writer, checkpoints, renewals,
stock-reader recovery and wrapper routing with dummy credentials; it is not
AWS IAM, real SigV4 authentication or a throughput qualification.

Only seven named scalar receipt files, each at most 2 MiB, are eligible for
upload. A final gate rejects configured credentials and identifiers in raw,
JSON-escaped, URL-encoded or Base64 form before the artifact uploader runs.
This is a secondary leak check, not a universal secret detector. Process logs,
telemetry payloads, object contents and storage directories are never uploaded.
