# Native S3 transfer attribution

This optional benchmark-only wrapper measures real libcurl transfers without
changing the transport implementation, HTTP options, retry policy, WAL or ACKs.
Use it only in an isolated benchmark process whose body closes/joins both data
and renewal owners before returning. It temporarily wraps their shared Vars;
it is not a general application instrumentation API.

`with-profile!` wraps an existing request function and `curl_easy_perform`.
After each perform, while the easy handle is still owned, it reads six DOUBLE
timings and the LONG new-connection count using the documented curl8.5.0 ABI.
Native errors are checked before reading output memory. Timing read failures
do not alter the transfer result; they explicitly invalidate the final profile.
Original request results and exceptions remain unchanged.

Only closed symbolic classes are retained: GET/no-body, or PUT/bytes-or-file/
small-medium-large size bucket. Unknown request shapes have fixed other/unknown
labels. No URLs, paths, headers, credentials, payloads, error text, pointers or
per-request records enter the aggregates. The number of classes is bounded.
Request failures count thrown request exceptions, not HTTP status outcomes.

Times are cumulative DNS/connection/TLS/pretransfer/first-byte/total milestones.
They overlap; don't sum them or call first-byte minus pretransfer pure server
processing time. Interim responses, uploads, reused connections, local scheduling
and callbacks affect interpretation. Counts sum across independent owners and
may overlap in wall time; totals are not an exclusive batch decomposition.
Profiling overhead exists and must be separated from unchanged throughput runs.

Tests cover symbolic privacy, original result/error preservation, failed samples,
typed ABI output slots and exact option IDs. Native loopback confirms3fresh
requests open3connections, whereas3requests through the scoped owner open1,
with complete measurements collected before handle reset/cleanup. This is not
TLS/AWS or durable-recovery qualification. Protected cloud wiring and review are
still required before using these counters to explain the real S3 deficit.

`s3-transport-profile.yml` provides that wiring once reviewed and merged. Its
default is read-only preflight; `mode=qualify` uses the original3fd96c4 benchmark
with no wait override, the same1ae runtime/native26.9.0 and current closed
driver/exporter/SDK/JSON graph as `current-typed-storage.yml`, selected by
`run_current_typed_storage.sh` rather than the fixture's historical launcher,
256x10k workload and independent stock recovery. `typed_storage_profile_driver`
wraps the unchanged request-budget driver, not a replacement for it. Writer and
reader processes each produce their own profile; receipt validation checks
closed metric/class shapes and exact agreement with settled request counts.
Unknown fields, nonfinite values, missing samples or failed calls reject.

Request limits remain24000/6GiB for the writer and1000GETs/zero upload for the
reader. The unchanged original workload/recovery oracle runs before profile
validation. Only the nine fixed scalar files may be uploaded, after credential/
identifier scans. Local logs and scratch are removed on exit, including failure.
Profile milestones may include first interim response rather than final headers;
they do not isolate server processing from upload or network delay.
