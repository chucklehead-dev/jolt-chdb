# jolt-chdb

An in-process chDB driver for Jolt's `jdbc.core` / `jolt-db` API. It uses
`libchdb` directly through `jolt.ffi`; there is no server or sidecar.

Add the library to `deps.edn`, pinning the exact commit you intend to deploy:

```clojure
{io.github.chucklehead-dev/jolt-chdb
 {:git/url "https://github.com/chucklehead-dev/jolt-chdb.git"
  :git/sha "<release-commit-sha>"}}
```

```clojure
(require '[db.jdbc]
         '[db.export :as db-export]
         '[jdbc.chdb]
         '[jdbc.core :as jdbc])

(with-open [conn (jdbc/connection "chdb::memory:")]
  (jdbc/execute! conn
    "create table events (id Int64, message String) engine=Memory")
  (jdbc/execute! conn ["insert into events values (?, ?)" 1 "started"])
  (jdbc/fetch conn ["select * from events where id = ?" 1]))
```

Basic HoneySQL output works because formatted SQL vectors use the same
positional parameter API:

```clojure
(jdbc/fetch conn
  (honey.sql/format {:select [:id :message]
                     :from [:events]
                     :where [:= :id 1]}))
```

Jolt 0.8.6 or newer is the base driver floor. Durable mode currently requires
the pinned `casselc/jolt` `integration/aspects` compiler at commit `120643d6`,
or a later release that contains upstream Jolt PR #957. That compiler supplies
the strict `CharsetDecoder` interop used by recovery. Reader and writer opens
functionally probe strict decoding and fail before storage or native effects if
it is unavailable. Durable WAL streaming separately probes for the compiler's
`OutputStreamWriter.append(CharSequence, start, end)` correction; writer
construction likewise fails before acquisition or native work when that
capability is absent. The repository declares only the base release floor with
`:jolt/min-version`; there is no older-Jolt compatibility lane.

`nil` has no inferable ClickHouse type. Use `(jdbc.chdb/typed-param
"Nullable(String)" nil)` when binding it. Transactions and generated keys are
reported as unsupported before executing SQL.

## Native installation

The driver pins chDB 26.7.0 release archives and their SHA-256 checksums.
Requiring the namespace never downloads code. Install explicitly:

```sh
jolt -M:setup-native
```

Set `JOLT_CHDB_LIB` to use an already installed `libchdb`, or
`JOLT_CHDB_CACHE_DIR` to choose the installer destination. Only one chDB storage
path may be active in a process at once, although multiple connections to that
same path are supported.

## Durable storage (experimental)

Durable mode stores a database checkpoint and later statement WAL segments in
an object namespace. A small `head.json` manifest chooses the current objects
with an atomic compare-and-swap. This lets a new process recover committed
writes instead of relying on one chDB data directory surviving intact.

The reader, single-writer lease, WAL flush, checkpoint, recovery, local POSIX
backend, and S3-compatible backend are implemented. The local backend has
cross-process and native WAL/checkpoint recovery tests on Linux. The S3 backend
has protocol, libcurl, pinned-MinIO, and live AWS OIDC qualification. The
serialized reader and writer workers and lease heartbeat are isolated from
Jolt's shared fiber carriers: they each own an OS thread because native chDB and
storage calls may block. During close, heartbeat remains live through queued
work and the final flush, then is stopped and joined before lease release.
Public lease and skew options are milliseconds; the interoperable
`head.json` lease expiry is Unix epoch seconds with fractional precision.
The adapter accepts only whole public milliseconds and bounds both forms to the
JavaScript-safe millisecond magnitude; this is a local cross-runtime safety
policy rather than a new Protocol V1 requirement.
Recovery downloads each WAL segment into its private scratch directory and
checks the declared size and digest before decoding. It then validates the
complete JSONL segment in one bounded streaming pass before a second bounded
pass revalidates and replays each statement. This keeps corrupt-tail recovery
atomic without retaining the whole segment or its decoded statements in memory.
Durable is still experimental: the stable 26.7.0 library installed by
`-M:setup-native` does not expose the required ABI, hosted native qualification
covers only Linux x86-64, and a broader crash/corruption and platform matrix
remain unfinished.

Choose Durable now when you can pin and qualify chDB 26.7.2-rc.2 yourself and
want to evaluate explicit persistence boundaries on one POSIX host or an
S3-compatible test deployment. Do not choose it yet when you need a stable
native dependency, broad platform/provider qualification, streaming inserts,
or a production-ready remote durability claim.

To try the local backend, first point `JOLT_CHDB_LIB` at a qualified
26.7.2-rc.2 library. The qualification script downloads the checksum-pinned
asset and runs the upstream C oracle plus the Jolt native suite:

```sh
bash scripts/qualify-durable-native.sh /tmp/jolt-chdb-durable
export JOLT_CHDB_LIB=/tmp/jolt-chdb-durable/native/libchdb.so
```

Then select the Durable JDBC driver and give the namespace a separate object
ID. `:owner` identifies the service. `writer-dbspec` generates a fresh UUIDv4
`:instance` unless the application supplies its own identity for this process
or writer attempt.

```clojure
(require '[jdbc.chdb.durable :as durable]
         '[jdbc.chdb.durable.local-posix :as durable-local]
         '[jdbc.core :as jdbc])

(def storage (durable-local/local-backend "/var/lib/my-app/chdb-objects"))

(with-open [conn (jdbc/connection
                  (durable/writer-dbspec
                   {:namespace-backend storage
                    :object-id "primary"
                    :owner "my-app"
                    :database "default"}))]
  (jdbc/execute! conn ["INSERT INTO events VALUES (?, ?)" 1 "accepted"])
  ;; Do this before acknowledging the write to another system.
  (durable/flush! conn))
```

Mutations are durable only after `flush!`, `checkpoint!`, or a successful close
has committed the new manifest. Materialized SQL uses statement WAL. A mutation
with native bound values makes that boundary publish a full checkpoint because
V1 WAL has no typed-parameter record; values are never interpolated or placed
in WAL. A fresh process can open `(durable/snapshot-dbspec
{:namespace-backend storage :object-id "primary"})`; it restores one fixed
manifest snapshot and does not take the writer lease. The constructors return
ordinary JDBC maps, reject role/configuration mistakes before opening resources,
and do not add another lifecycle abstraction.

See [Durable storage](docs/durable.md) for configuration, recovery and
acknowledgement behavior, provider status, and the modeling/testing method.
The lower-level ABI, head, backend, control, writer, open/recovery, S3, and trace
documents are linked from that guide.

A map dbspec can select an isolated logical ClickHouse database while sharing
that physical path:

```clojure
(jdbc/connection {:vendor "chdb"
                  :name "/var/lib/my-app/chdb"
                  :database "samizdat"})
```

On open, the driver creates the logical database if it is missing and selects
it for that connection. Different connections on the active physical path may
therefore use the same unqualified table names without colliding. `:database`
accepts a string or unqualified keyword containing only an ASCII letter or
underscore followed by ASCII letters, digits, or underscores (maximum 255
characters); other values are rejected before native chDB is opened. URI
dbspecs and map dbspecs without `:database` continue to use ClickHouse's
`default` database.

`jdbc.chdb/stream-insert!` is an optional chDB-specific API for incremental,
format-encoded input. It accepts `String` or byte-array chunks, defaults to
`JSONEachRow`, and exclusively occupies its connection until finalization.

The driver advertises the neutral `db.export` query-bytes v1 capability with
Arrow IPC file and Parquet formats, in-memory staging, and truthful default and
hard limits. Use `db.export/query-bytes` to export a result-bounded SELECT as an
owned byte array through the shared SPI:

```clojure
(let [{:keys [bytes content-type extension byte-count]}
      (db-export/query-bytes
       conn
       ["select * from events where id >= ? order by id" 100]
       {:format :parquet
        :max-rows 10000
        :max-bytes (* 16 1024 1024)})]
  ;; bytes is independent of the native result and remains valid after close.
  )
```

`jdbc.chdb/query-bytes` remains as a compatibility wrapper and delegates to
`db.export/query-bytes`; it has no separate execution or ownership path. Both
functions return exactly `:format`, `:content-type`, `:extension`,
`:byte-count`, and `:bytes`.

`:format` must be `:arrow` or `:parquet`. `:max-rows` defaults to, and may not
exceed, 100,000; `:max-bytes` defaults to, and may not exceed, 64 MiB. Both are
enforced by ClickHouse before it returns the materialized result and the byte
length is checked again before Jolt allocates its copy. The SQL must begin with
`SELECT` or `WITH`, have balanced lexical structure, and contain no unquoted
statement separator. It is nested inside a generated bounded `SELECT`, so DDL,
inserts, and multiple statements cannot occupy this API. Positional parameters
retain the same binary-safe binding as ordinary driver queries.

The SQL text is trusted application input, just like `jdbc/fetch`; the driver
does not expose it directly to HTTP or UI state. The caps bound returned rows
and serialized bytes, not arbitrary SELECT execution cost or access through
ClickHouse table functions. A user-facing explorer should compile its own
closed selection/filter contract to SQL rather than accepting SQL text.

The function never accepts or writes a path. An HTTP endpoint or application
must own filename, authorization, overwrite, and filesystem-root policy.

Validation and native failures are reported as `java.sql.SQLException` at the
public boundary. For a driver-originated failure, `(ex-cause error)` retains
the original chDB exception and its structured `ex-data`, including recovery
or connection-retirement diagnostics.

The pinned libchdb retains a failed Arrow/Parquet output format for one later
query. The driver destroys the failed user result, then consumes that stale
state with an internal successful zero-row result before propagating the
original encoded-query error. If recovery fails, it retires the connection
rather than exposing uncertain serializer state and reports both errors. If
destroying the failed result itself fails, the driver does not attempt recovery
while that result may remain live; it retires the connection and preserves the
query, destruction, and close diagnostics. Tests prove that ordinary JDBC
remains usable after successful row/byte overflow recovery. The recovery result
and original result are each destroyed once on the ordinary error path.

Do not use `stream-insert!` with the packaged 26.7.0 native library in a long-running
process: even a contract-compliant single-threaded C caller causes that build
to retain invalid ClickHouse `ThreadStatus` state and report a fatal diagnostic
when the connection closes. Use bounded `execute!` inserts instead; the OTel
exporter does so. Re-enable streaming only after qualifying a fixed native
release with the pseudo-terminal diagnostic probe.

## Native query statistics

`with-query-statistics` observes every completed chDB user query, successful or
failed, synchronously on the calling thread without changing its JDBC return
value:

```clojure
(chdb/with-query-statistics
  #(jdbc/fetch-one conn "select sum(number) from numbers(1000)"))
;; => {:result {...}
;;     :queries [{:elapsed-seconds ...
;;                :result-rows 1
;;                :result-bytes ...
;;                :storage-rows-read 1000
;;                :storage-bytes-read 8000
;;                :rows-written 0
;;                :bytes-written 0}]}
```

The scalar values come from libchDB's stable result API and are copied before
the native result is destroyed; no result pointer escapes. A thunk may issue
multiple queries, whose statistics are returned in execution order. Nested
collectors both observe the queries. Failed native results, including failed
Arrow/Parquet exports, retain their statistics under
`:db.chdb/query-statistics` in exception data. Internal serializer-recovery
queries are deliberately excluded. When a native query failure terminates a
collector's thunk after one or more results complete, the same exception data
also retains every result observed in that scope under
`:db.chdb/query-statistics-collected`.

These counters measure ClickHouse query work. Compare `:elapsed-seconds` with
an outer monotonic wall-time bracket to estimate Jolt/JDBC/FFI overhead. They
do not report process RSS or all native allocation; use OS/native profiling for
that. Embedded chDB also exposes cumulative `system.events`, instantaneous
`system.metrics`, and storage state in `system.parts`, but does not provide the
server's persistent `system.query_log` in the pinned 26.7.0 build.

## Development

The optional JVM-only Typed Clojure pilot checks the runtime-neutral ABI and
Durable data contracts without loading `jolt.ffi` or adding a production
dependency:

```sh
clojure -M:typed-check
```

Its exact checked/trusted boundary and mutation controls are documented in
[`docs/typed-clojure.md`](docs/typed-clojure.md).

Install the pinned native library and run the full driver suite:

```sh
jolt -M:setup-native
jolt -M:abi-test
# downloads/authenticates rc.2 and runs its C oracle plus the Jolt ABI suite
bash scripts/qualify-durable-native.sh /tmp/jolt-chdb-durable-qualification
jolt -M:durable-head-test
jolt -M:durable-backend-test
jolt -M:durable-control-test
jolt -M:durable-itf-test
jolt -M:durable-writer-test
jolt -M:durable-writer-concurrency-test
jolt -M:durable-worker-join-test
jolt -M:durable-open-test
scripts/check-durable-head-quint.sh
# static red/green controls for fast-only versus exhaustive model inputs
test/durable-model-path-classifier.sh
# CI's exhaustive-only entrypoint; do not use for ordinary fast local checks
scripts/check-durable-head-quint.sh --verify-only
jolt -M:durable-local-test
jolt durable-local-posix-test
# Linux isolated-process checkpoint RSS, live-heap, and retention-control gate
bash test/durable-large-checkpoint.sh \
  /home/chuck/ai-src/tools/jolt-with-chez-10.4.1
# manual retained-allocation comparison; supply fresh root/output and 1/32 MiB files
jolt -M:durable-file-allocation <root> <output> <small-file> <large-file>
jolt -M:test
jolt test-threadstatus
```

The installer streams the large release archive through `curl`, verifies it
with `sha256sum` on Linux or `shasum` on macOS (with `openssl` as a fallback),
and extracts it with `tar`; these standard platform tools must be available.

The tests include deterministic unit/integration coverage and shrinking
`jolt-hegel` properties. CI currently exercises Linux x86-64, one of the
platforms supported by the pinned chDB archive.

`jolt test-threadstatus` runs repeated successful and bounded-failure
Arrow/Parquet lifecycles under a pseudo-terminal, reuses the connection through
ordinary JDBC after every recovery, closes it, and fails if libchdb emits
`ThreadStatus: current_thread contains invalid address`. Set `JOLT_CHDB_LIB`
when the library is not installed at the default cache path. This diagnostic
launcher currently targets util-linux `script -qefc`; other platforms still
run the ordinary driver suite but need a platform-specific PTY wrapper before
claiming equivalent diagnostic coverage.

## License

Copyright © 2026 contributors. Distributed under the Eclipse Public License
2.0; see `LICENSE`.
