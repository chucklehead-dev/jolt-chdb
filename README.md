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

Jolt 0.8.3 or newer is required. The repository uses the released unboxed-array,
block-copy, and current FFI contracts directly and declares that floor with
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

The source also carries a versioned descriptor for the Durable V1 C ABI first
published by chDB core 26.7.2-rc.2. The stable 26.7.0 production pin does not
export that surface: `jdbc.chdb.native/durable-capability` reports a typed,
structured unsupported result while ordinary JDBC remains available. The
production pin will move only after upstream publishes a stable release with
the required symbols. See [`docs/durable-abi.md`](docs/durable-abi.md) for exact
provenance.

The first runtime-neutral control-plane component is the strict Durable V1
`head.json` codec in `jdbc.chdb.durable.head`. It preserves unknown fields,
separates read-only from writer feature compatibility, and validates immutable
reference integrity metadata before later backend or engine work. It is a
foundation, not a Durable-open API or conformance claim. See
[`docs/durable-head.md`](docs/durable-head.md).

The backend seam, in-memory atomic-CAS semantic oracle, and first local POSIX
provider are documented in
[`docs/durable-backend.md`](docs/durable-backend.md). The oracle is paired with
bounded Chiasmus/Z3 controls and a Hegel stale-ETag property. The local provider
adds real cross-process exclusion, synced atomic publication, and crash tests;
it now covers the complete six-operation storage seam, including bounded-memory
file upload/download. The public Durable reader/writer APIs below compose that
seam; applications select the local provider explicitly with
`jdbc.chdb.durable.local-posix/local-backend`, then pass it as
`:namespace-backend` together with one safe `:object-id`. Advanced callers may
still pass an already object-scoped `:store` directly.

The first state-machine integration slice is documented in
[`docs/durable-control.md`](docs/durable-control.md). It implements generation
lease acquisition/takeover, heartbeat, release, stale-writer fencing,
writer-aware bounded WAL publication, publication-before-head-CAS, and
reference/sequence-based ambiguous-CAS reconciliation over the backend
seam. Its Chiasmus models include both a SAT stale-writer mutant and a reachable
valid path; the focused Hegel state machine exercises the same contract.

`jdbc.chdb.durable/open-writer!` now composes compatibility checks, lease
acquisition, private scratch creation, verified checkpoint/WAL recovery,
post-recovery renewal, an independent heartbeat, local-expiry self-fencing,
and ordered close cleanup. Its returned `jdbc.chdb.durable.writer` owns the
bounded FIFO and implements classified single-statement query/execute,
buffered statement WAL, and confirmed flush. This is usable with a core that
exports the Durable ABI. Its queued checkpoint operation creates a full native
backup, streams and verifies immutable publication, then atomically replaces
the base and clears covered WAL. `open-reader!` restores one immutable first
head snapshot without taking a lease and admits only serialized reads. Both
paths expose bounded Arrow/Parquet `query-bytes` through the generic export
SPI. See [`docs/durable-open.md`](docs/durable-open.md)
and [`docs/durable-writer.md`](docs/durable-writer.md).

The S3-compatible namespace backend semantics, atomic preconditions, retry and
error mapping, and streaming transport contract are documented in
[`docs/durable-s3.md`](docs/durable-s3.md). Its Jolt-native default libcurl
SigV4 transport has loopback and pinned-MinIO integration gates; real AWS and
large-transfer qualification are still pending, so this is not yet a complete
production S3 conformance claim.

Its executable formal companion is the literate specification
[`formal/quint/durable-head-cas.md`](formal/quint/durable-head-cas.md).
The Quint model shares one transition engine between the corrected protocol,
the stale-ownership mutant, and deterministic boundary traces; see the
specification for its precise six-step scope and evidence limits. The check
script tangles generated `.qnt` files under `target/formal/quint/` before it
typechecks, tests, samples, and optionally model-checks them.
The implementation replay and observation-only aspect contract are
described in [`docs/durable-trace-validation.md`](docs/durable-trace-validation.md).

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
jolt -M:durable-open-test
scripts/check-durable-head-quint.sh
jolt -M:durable-local-test
jolt durable-local-posix-test
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
