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

## Data/renewal separation control

The native loopback suite now compares a shared scope with two independent
scopes while an owned OS thread holds an admitted data request before its native
perform. Its callback arena and options are live. Renewal runs a real signed
HTTP GET and conditional PUT against the synthetic S3 fixture, through the
existing control-plane `renew!` operation.

A shared handle rejects renewal before any native renewal request. Separate
handles complete exactly two native requests, advance wire expiry to 250, and
retain the generation while the data operation remains unfinished. Both cases
release and join the data thread, then release the lease and retire each handle
once. Routing renewal back to the data sender gives two intended assertion
failures and zero errors; the single-handle admission guard stays enabled, so
this negative control does not permit concurrent native use or a use-after-free.

This tests admitted-request overlap, not simultaneous on-wire payload transfer,
automatic heartbeat scheduling, full writer persistence, credential rotation,
or AWS throughput. Default selection and production sources are unchanged.
It supports dedicated renewal ownership as the next implementation direction;
it does not close the broader adoption/lifecycle requirements in issue #251.

## Writer heartbeat during a held HTTP transfer

The next test wires separate senders into the existing raw writer composition
seam: its data backend uses the first sender, while its `:renew!` operation
uses an independently owned backend with the same object namespace and token.
No writer API, transport default, retry, or persistence policy is changed.
The operation converts raw writer milliseconds to V1 wire seconds and back,
as required by the existing public open layer.

The actual writer worker calls a fake native query hook that starts a signed
HTTP data request. The fixture server reads that request and withholds its
response until a separate release request arrives. Only after the server
confirms entry does the test trigger one heartbeat tick. The writer's owned
heartbeat thread runs its real renewal loop and performs GET plus conditional
PUT while the data call remains blocked in native libcurl. Separate handles
advance wire expiry from 200 to 250, keep generation unchanged, and update
the writer's local millisecond expiry. One shared handle rejects the renewal.

The test then releases the data response, joins the caller, closes the writer
inside both transport scopes, checks that worker and heartbeat completed,
verifies released lease fields with the generation retained, and checks exact
init/cleanup balance. A safe wrong-routing control produces five intended
failures and zero errors; correct routing passes 29 assertions. The older
cleanup and reuse suites remain green at 14 and 62 assertions respectively.

This is real simultaneous on-wire transport activity and a real heartbeat-loop
renewal, but its tick is deterministic rather than wall-clock scheduling.
Database execution is stubbed: it does not prove native mutation, WAL/manifest
publication, persisted acknowledgements, independent database recovery, TLS,
credential rotation, interruption during close, or long-running AWS throughput.
Adoption still requires those relevant lifecycle and product integration gates.

## Real-native publication and independent recovery gate

`test/durable-s3-native-overlap.sh` is an additional opt-in lane. Supply the
exact qualified Jolt executable and set `JOLT_CHDB_LIB` to the qualified native
library. Run it through the workspace's pinned Chez wrapper:

```sh
JOLT_CHDB_LIB=/path/to/libchdb.so \
  tools/jolt-with-chez-10.4.1 bash test/durable-s3-native-overlap.sh /path/to/jolt
```

It opens a real Durable writer through the public open layer, creates a table,
and starts a confirmed 512-row insert. The synthetic provider reads the WAL
upload and holds it before applying the conditional create. The controlled
heartbeat then renews using a second scoped transport. An independent control
read verifies the generation is retained, expiry advances, and manifest sequence
has not advanced while the insert caller is unsettled. Releasing the upload
allows normal verification/head CAS and a confirmed receipt. The writer closes
inside both scopes and releases its lease. A second OS process, with stock
parsing and fresh curl handles, restores/replays and checks every sorted row.

The test now calls the experimental library owner described below; it no longer
intercepts `writer/start!` or replaces native execution/publication operations.
The public open layer retains its milliseconds/seconds conversion and retry
options. Routing renewal back to the guarded busy data backend fails without
unsafe shared native use. The original test-only composition remains recorded
at commit 295d53e as earlier evidence, not the current implementation.

This gate passes on chDB 26.7.3 and the cumulative 56bf8968 runtime. It qualifies
this deterministic native publication/recovery scenario, not sustained
throughput, TLS, real AWS, wall-clock scheduling, credential rotation or every
shutdown/interruption path. The script removes its own fixture directory and
server after both processes settle; no persistent provider or credentials are
used. Review and sustained qualification remain prerequisites to adoption.

## Experimental library owner

`jdbc.chdb.durable.s3-writer/with-writer!` owns two serial curl scopes for one
S3 namespace and object. Use it explicitly; ordinary Durable/S3 APIs still
select fresh handles. The callback receives the normal Durable writer:

```clojure
(require '[jdbc.chdb.durable.s3-writer :as s3-writer]
         '[jdbc.chdb.durable.writer :as writer])

(s3-writer/with-writer!
  s3-options
  {:object-id "telemetry" :owner "collector" :instance "run-1"
   :database "otel" :scratch-parent scratch-parent}
  (fn [w]
    ;; Run the writer's normal operations, or keep an application inside this
    ;; scope until its own producers have stopped and joined.
    (writer/execute-and-flush! w sql)))
```

The helper builds both backends from the same options, installs dedicated
renewal routing, and closes/joins the writer before retiring either transport.
The data backend retains normal recovery, publication, verification and head
CAS operations. Callback errors remain primary if close also fails; otherwise
close failure propagates. Startup failure unwinds both transport scopes.
Do not return a live writer or launch unjoined producers beyond the callback.
Manual early close is safe through the writer's existing idempotent close.

The helper reserves `:request!`, `:store`, `:namespace-backend` and the
`:operations/:renew-control!` hook so callers cannot accidentally route to a
different object. Other ordinary writer options remain available. The public
open seam validates that renewal hook before storage effects and uses it for
startup and heartbeat with protocol-seconds arguments; the open layer alone
converts raw writer milliseconds. Direct custom hook implementations are trusted
to honor the same lease/CAS contract, just as other explicit operation overrides
are trusted; the helper itself calls the existing control/renew! implementation.

There is no automatic credential rotation: configuration changes reject before
native work, and require a new owner scope. This is not a generic shared pool
or an exporter/JDBC lifetime adapter. Library composition is implemented and
the native controlled-overlap/recovery gate uses it, but AWS/TLS, long-run
performance, rotating credentials and interrupted-close qualification remain
open. Existing protocol models retain their prior publication/lease scope;
they do not prove curl resource lifetime or service availability.

### Borrowing a JDBC connection for the exporter

`s3-writer/with-connection!` uses the same owner but gives the callback an ordinary
Durable JDBC connection. Existing consumers can borrow it without knowing about
curl. For the chDB OTel exporter:

```clojure
(s3-writer/with-connection!
  s3-options writer-options
  (fn [connection]
    (let [e (chdb-exporter/exporter
              {:connection connection :durable? true
               :signals #{:spans :logs :metrics}})]
      (try
        ;; Configure/use the application's SDK and stop/join its producers here.
        (run-application e)
        (finally
          (export/shutdown-exporter! e)
          (export/shutdown-metric-exporter! e)
          (logs/shutdown-log-exporter! e))))))
```

Here `chdb-exporter` is `otel.exporter.chdb`, `export` is `otel.sdk.export`, and
`logs` is `otel.sdk.logs`. Check shutdown results according to the application's
error policy. Borrowed exporter shutdown does not close the connection; the
outer owner closes it and joins the writer after the callback finishes.
Do not let an exporter, SDK producer or live connection escape this scope.

A real five-table exporter integration using the current cumulative runtime and
JSON candidates verifies confirmed inserts, ordinary clock-driven renewal,
borrowed shutdown ownership and fresh-process counts. This remains local HTTP
qualification, not hosted S3 or Oscope lifecycle qualification. The profiling
fixture has an explicit `--tcp-nodelay` option to avoid charging Python's split
header/body writes and delayed-ACK interaction to the library; default regression
tests are unchanged. Small sample percentiles are descriptive, not p99 proof.

### Request-local upload scratch candidate

The upload callback now lazily allocates one managed scratch array, bounded at
64KiB, and reuses it for the request's serial reads. Each read is also clipped
to that callback's current capacity, which may shrink. The bytes are copied
into libcurl's buffer; this is not a native pointer loan or a cross-request
cache. Zero-capacity callbacks do not allocate/read, and EOF returns zero.
File downloads already use the pointer-to-fd path and are not changed.

Actual 196615-byte byte/file uploads each used four arrays on the parent and
one on the candidate, with exact provider readback. A changing-capacity/EOF
test uses guarded 64-byte physical backing; removing the logical capacity
limit produces three expected failures without native out-of-bounds writes.

In one matched local HTTP 100x5000 exporter run, cumulative allocation fell
28.300 -> 27.348GB (3.36%), but throughput was flat/slightly lower (36.992k ->
36.674k rows/s) and tails mixed. Keep this as an allocation candidate pending
review and further attribution, not a demonstrated throughput optimization.
Confirmation, automatic checkpoint/renewal and independent2.5M-row count
readback pass. These numbers are not AWS or stable tail qualification.

References: [reset](https://curl.se/libcurl/c/curl_easy_reset.html),
[cleanup](https://curl.se/libcurl/c/curl_easy_cleanup.html),
[connection counter](https://curl.se/libcurl/c/CURLINFO_NUM_CONNECTS.html).
Workspace receipts: evidence/chdb-s3-curl-owned-reuse-failure-drain-gate-20261007.log
and evidence/chdb-s3-curl-reuse-drain-{green,red}-safe-20261007.log.
