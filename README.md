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

`jdbc.chdb/query-bytes` is the result-bounded SELECT counterpart for exporting
a query result as an owned Arrow IPC file or Parquet byte array:

```clojure
(let [{:keys [bytes content-type extension byte-count]}
      (jdbc.chdb/query-bytes
       conn
       ["select * from events where id >= ? order by id" 100]
       {:format :parquet
        :max-rows 10000
        :max-bytes (* 16 1024 1024)})]
  ;; bytes is independent of the native result and remains valid after close.
  )
```

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

The pinned libchdb retains a failed Arrow/Parquet output format for one later
query. The driver destroys the failed user result, then consumes that stale
state with an internal successful zero-row result before propagating the
original encoded-query error. If recovery fails, it retires the connection
rather than exposing uncertain serializer state and reports both errors. Tests
prove that ordinary JDBC remains usable after successful row/byte overflow
recovery. The recovery result and original result are each destroyed once.

Do not use `stream-insert!` with the packaged 26.7.0 native library in a long-running
process: even a contract-compliant single-threaded C caller causes that build
to retain invalid ClickHouse `ThreadStatus` state and report a fatal diagnostic
when the connection closes. Use bounded `execute!` inserts instead; the OTel
exporter does so. Re-enable streaming only after qualifying a fixed native
release with the pseudo-terminal diagnostic probe.

## Development

Install the pinned native library and run the full driver suite:

```sh
jolt -M:setup-native
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
