# Scoped S3 transport reuse experiment

Not enabled by default. `with-reused-transport!` calls an operation with a
serial request function and owns one curl easy handle for that lexical scope.
An atomic admission gate rejects concurrent/reentrant requests, scope exit
closes admission and waits for admitted work before terminal cleanup, and
retained request functions reject after exit. The first request binds HTTP(S)
origin, region and auth; changes reject before native transfer. Credentials
are never printed and the retained configuration is cleared at close.

Each request uses its normal callback arena, streams and header list. Its
finalizer calls `curl_easy_reset` while the arena remains open, detaching
pointer-bearing options before releasing their memory. libcurl retains its
connection/session/DNS caches across reset. The scope alone calls terminal
cleanup. No WAL, conditional head CAS, retry, verification or ACK policy has
changed, and existing `request!`, `request-function` and `s3-backend` still
create fresh handles unless this explicit experimental scope is used.

The qualified bcb376a0 / Chez 10.4.1 synthetic native HTTP loopback lane passes:

- three fresh-handle GETs report new connections `[1 1 1]`;
- one scoped PUT and three GETs report `[1 0 0 0]` via CURLINFO_NUM_CONNECTS;
- exact body readback and PUT-to-GET method reset;
- once-only scope cleanup after an operation exception and rejection through
  a retained request function;
- reentrant rejection and reset before arena close;
- origin/auth/region changes rejected without another native perform;
- setup/perform/arena-construction failures reset the scoped handle and permit
  a subsequent successful request without allocating another handle;
- a deterministic two-fiber scope-exit gate observes the actual active-ticket
  wait, rejects new admission during close, and proves transfer completion
  precedes cleanup. Removing the wait produces three intended failures and
  zero errors; the control defers physical free rather than causing native UAF;
- all existing synthetic SigV4 transport checks and the parent cleanup
  regression suite remain green: 2 tests / 14 assertions plus 6 reuse tests /
  50 assertions.

This proves reuse of a synthetic local HTTP connection, not HTTPS/TLS reuse,
S3 throughput, collector performance or recovery conformance. Read-only review
of f4a6be5 found no logic counterexample; its two empirical coverage gaps are
addressed by the failure and drain gates above. The new tests require review
before adoption. A many-caller stress lane would be additional evidence, not a
replacement for the deterministic gate. A persistent application/backend owner and its close
integration are not implemented. Cookies/shares/alt-svc caches also survive
reset, hence the explicit one-configuration ownership fence.
STS token rotation also counts as a configuration change: close this scope and
open another rather than silently refreshing credentials in the cached handle.

References: [reset](https://curl.se/libcurl/c/curl_easy_reset.html),
[cleanup](https://curl.se/libcurl/c/curl_easy_cleanup.html),
[connection counter](https://curl.se/libcurl/c/CURLINFO_NUM_CONNECTS.html).
Workspace receipts: evidence/chdb-s3-curl-owned-reuse-failure-drain-gate-20261007.log
and evidence/chdb-s3-curl-reuse-drain-{green,red}-safe-20261007.log.
