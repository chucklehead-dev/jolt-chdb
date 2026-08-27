# jolt-chdb

An in-process chDB driver for Jolt's `jdbc.core` / `jolt-db` API. It uses
`libchdb` directly through `jolt.ffi`; there is no server or sidecar.

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

Do not use it with the packaged 26.7.0 native library in a long-running
process: even a contract-compliant single-threaded C caller causes that build
to retain invalid ClickHouse `ThreadStatus` state and report a fatal diagnostic
when the connection closes. Use bounded `execute!` inserts instead; the OTel
exporter does so. Re-enable streaming only after qualifying a fixed native
release with the pseudo-terminal diagnostic probe.
