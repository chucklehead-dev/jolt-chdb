# Checking the optional S3 checksum contract

The manual **S3 checksum provider controls** workflow checks the existing,
default-off `:wal-upload-confirmation :sha256-single-put` option against the
configured AWS bucket. It does not enable that option for applications or
change normal WAL readback, the V1 format, lease fencing, CAS or reader checks.

Run it on protected `main`. The default `preflight` mode only inspects the
existing role and bucket retention. `qualify` also runs the fixed controls below.
No endpoint, role, source, payload size or credentials are workflow inputs.
The same authenticated compiler profile used by the current performance graph
is selected; this is not a stock-compiler or compiled-application gate.

| Control | Required observation |
|---|---|
| New upload, option enabled | Real native checksum evidence; no publication/commit readback; then independent full-body verification |
| Default option | Actual body readback remains required |
| Existing selected key | A real conditional PUT seeds it; the next checksum PUT returns the existing-object path and still requires readback |
| Client-injected lost acknowledgement | Discard the result of a completed real PUT; reconcile through actual body readback |
| Same-size source mutation | Change the file after digest selection; require provider HTTP 400, unchanged head sequence/references, and no object at that key |

The injected acknowledgement loss is **not** a real AWS network timeout. The
mutation rejection is an actual provider response, not a manufactured HTTP
status. The controls follow [S3 PutObject's checksum and conditional-write
contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html).
Local verified-TLS controls also exercise the same entry point with fake
credentials, require rejection before explicit test-CA trust, and compare
client request counts with independent fixture observations.

These are tiny, synthetic three-byte body controls, not valid SQL WAL replay,
throughput, native-database recovery or multipart qualification. Full native
recovery belongs to the separate SDK storage workloads. Results apply to the
tested provider/location/configuration, not every S3-compatible service. Missing
or composite evidence, corrupt-response handling and true timeout injection
remain covered separately by the existing local/TLS tests and sampled model;
this job does not claim those faults occurred inside AWS.

## Bounds and retained evidence

Five unique object namespaces sit below the current run's
`ci/jolt-chdb/<run>-<attempt>/typed-window/checksum-faults` prefix. The existing
protected OIDC role and read-only policy/lifecycle preflight are reused. No IAM,
bucket-policy, lifecycle or other administrative mutation is performed.

One native transport scope and one meter cover the real-provider process: at most **128
GET/PUT data requests and 1 MiB uploaded**, with 64 KiB response-body limits.
The process has a three-minute deadline and backend retries are disabled.
Read-only IAM/preflight calls and compiler downloads are outside that meter.
Each successful case releases its lease and rereads the released head. Fixture
objects expire under the preflight-checked bucket lifecycle; no new delete or
multipart permissions are needed. A failed case may leave an isolated lease
until its ten-minute expiry; it is not reused by another run.

Only closed, validated scalar receipts are uploaded. They contain case labels,
counts, Boolean checks and fixed statuses, never SQL, body bytes, object keys,
digests, ETags, credentials, exception messages or raw logs. Separate closed
failure receipts never count as success or proof of settlement. A success
receipt is emitted only after the transport closes and every check passes.

Current applications still use readback by default. See the [earlier matched
checksum results](durable-checksum-results-20261016.md) for normal-path evidence;
that older graph is not a new throughput result from this provider-control job.
