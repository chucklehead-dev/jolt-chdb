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
