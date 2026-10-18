# Current typed SDK storage qualification

This manual workflow measures the actual SDK/exporter path on local storage
and S3. It does not replace the separate pre-encoded Durable benchmark.
Application defaults and the existing AWS role and bucket policies are unchanged.

The current qualification uses `scripts/run_current_typed_storage.sh`, not the
fixture checkout's historical launcher. The workload remains frozen at
`3fd96c4`; only its bench/test roots are used. Driver source/resources/dependencies
must match `3d766fa`, with exporter `5be78d5`, SDK `d93846e`, and native JSON
`8d557cc`. This separates the stable measurement workload from the current
product graph without allowing user-selected source or workload sizes. Older
transport-comparison workflows retain their own frozen launcher and graph.

When driver `src`, `resources`, or `deps.edn` changes, deliberately refresh this
closed driver's source pin and rerun qualification. A source mismatch is an
intentional rejection, not permission to float to arbitrary `main` contents.
The workflow fetches full driver history so source/ancestry checks work on the
new workflow commit as well as the original source checkpoint.

Here, a stock reader means the stock JSON metadata decoder in a fresh process
on the authenticated qualified runtime. It does not imply a released stock
Jolt executable. Separate stock-runtime compatibility runs remain distinct.

`s3-transport-profile.yml` uses this same closed current graph and workload,
adding the existing aggregate native-transfer observer only for S3. It records
GET/PUT size classes, request/perform counts and cumulative curl timing
milestones, never URLs, headers, bodies or credentials. The ordinary correctness,
budget and privacy checks remain required. Milestones are not exclusive phases;
independent renewal requests can overlap data requests, so their summed times
are not a decomposition of the measured export wall time. Older profile runs
remain evidence for their own pinned graph, not for the current stack.

## Fixed S3 continue-wait comparison

`expect-continue-ab.yml` is a separate manual protected-main workflow. It first
qualifies the preserved local baseline, then runs the fixed candidate with
1000ms and10ms continue waits sequentially on the same host. Each S3 arm has a
different fixed object ID,256 measured10k batches plus3 warmups, the same forced
heartbeat/checkpoint workload, and a separate stock-reader process. This
comparison's historical baseline and source guard do not inherit the current
qualification launcher's new pins.

The candidate has its own closed source profile; it accepts only the two waits
in S3 mode. An unset profile retains old defaults, and a leaked S3 wait in local
mode is rejected. Both writer and reader receipts bind the chosen wait.
The new workflow pins the candidate checkout, compiler artifact, native library,
exporter and JSON dependencies; it does not accept user-selected source or size.

Each arm retains24000writer requests/6GiB declared upload and1000GET-only reader
requests/zero upload. The combined maximum is48000writer requests/12GiB plus
2000reader requests, not a single6GiB run. Writer deadlines are20minutes each,
readers four minutes each, and the S3 step is bounded to50minutes so both arms
fit inside the one-hour assumed-role session. Failures remain failures; deadlines
or exhausted budgets never fabricate persisted acknowledgements.

Process logs and owned scratch directories are cleaned on exit. Only the seven
existing allowlisted scalar receipt names within each of the two fixed arm
directories can be uploaded, after the unchanged privacy scanner checks each.
Partial receipts are diagnostic, not qualification. The final verifier composes
the original full workload/recovery/budget checks for both arms, then rejects
swapped wait profiles or differing SQL fixture hashes. This is one sequential
comparison, not a confidence interval, randomized study or full-app guarantee.

The10ms setting can send upload bytes before a rejection arrives. Conditional
write enforcement is preserved, but early-rejection bandwidth savings are not
promised. This experiment does not change application defaults or claim an AWS
throughput improvement before validated results exist.

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
PYTHONDONTWRITEBYTECODE=1 python3 scripts/test_current_typed_storage_launcher.py
bb scripts/test_typed_storage_receipts.clj
```

Run the same request controls under the pinned qualified Jolt binary as well.
They exercise pre-transport rejection, transport-error identity and concurrent
reservations. They do not by themselves validate S3 or shutdown. The separate
loopback S3 fixture exercises the real scoped writer, checkpoints, renewals,
stock-reader recovery and wrapper routing with dummy credentials; it is not
AWS IAM, real SigV4 authentication or a throughput qualification.

Ordinary qualification produces seven named scalar receipts. The separate
transport diagnostic adds two allowlisted aggregate timing receipts; each file
is bounded to 2 MiB. A final gate rejects configured credentials and identifiers in raw,
JSON-escaped, URL-encoded or Base64 form before the artifact uploader runs.
This is a secondary leak check, not a universal secret detector. Process logs,
telemetry payloads, object contents and storage directories are never uploaded.
