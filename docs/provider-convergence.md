# Database-provider convergence

jolt-chdb and Samizdat share one database/JDBC implementation. The canonical
library key is `jolt-lang/db`; jolt-chdb resolves that key to the integrated
`casselc/db` revision `9e8c82a59ec63a36e86a758ff39ca9c5a9c3d165`.

This is ancestry, not a resolver substitution. The reviewed revision extends
the original convergence baseline with the canonical time-provider repair and
the read-only JDBC context used by the Durable persistence observation:

```text
9e8c82a5  reject a nil native handle while opening a JDBC connection
  2ae7d22a  preserve false opaque driver handles
  bde1c83f  add read-only driver context lookup
  8c55d9e2  merge the canonical time-provider repair
    9aa44b7  align the canonical time provider
    96324713  merge the provider-convergence work
      6db79163  install the pinned Hegel native library in provider CI
        2de2ce8e  cover plain Statement batching and direct isolation metadata
        c700960f  type both direct next.jdbc batch failure paths
        5865955  type every host executeBatch failure path
        3974bf0  scope partial-count parity to current Jolt capability
        802ba509  retain structured causes through BatchUpdateException
        cdc69310  fix direct JDBC isolation metadata compatibility
        04c3479  merge current jolt-lang/db provider contracts
          6d6fff9  merge casselc/db and Samizdat's jolt-lang/db behavior
            a5bf25d  prior jolt-chdb JDBC/driver provider
            d85f391  Samizdat 22be90d SQLite/java.sql provider
          a54cc49  current jolt-lang/db main at integration
```

The merge retains the casselc driver SPI, native ownership, validated result
boundary, transaction capabilities, encoded-query extension, and aspect
manifest. It also retains the java.sql surface used by Samizdat and current
Jolt's `:jolt/provides` declaration. Direct `setTransactionIsolation` calls
outside a transaction keep jolt-lang/db's standard-constant metadata
round-trip. Unsupported SQLite isolation requested by `jdbc/atomic` is still
rejected while the transaction is staged, before its body or native `BEGIN`.
Every batch entry point preserves the exact modeled `BatchUpdateException`
class and original driver cause. Current Jolt does not yet model the exception's
partial update-count storage, constructors, or `getUpdateCounts`; that runtime
prerequisite is tracked in `chucklehead-dev/jolt-aspect-packs#131`.

## Consumer migration

Use only the canonical key:

```clojure
jolt-lang/db
{:git/url "https://github.com/casselc/db.git"
 :git/sha "9e8c82a59ec63a36e86a758ff39ca9c5a9c3d165"}
```

Remove `io.github.casselc/db` from a graph that already has `jolt-lang/db`.
Do not add an exclusion, `:override-deps`, or a root mapping from one historical
coordinate to the other historical revision. Those techniques choose a
classpath winner without integrating the behavior of both lineages.

Jolt 0.8.10 is the ordinary JDBC and Durable minimum. Durable writer
qualification uses the released Jolt v0.8.10 commit
`5b659b7dbb6fe09161d53a92f706cb55dee955d6` (banner `jolt v0.8.10`). The
fixture accepts that executable through `JOLT_BIN` and always runs it through
the workspace's mandatory Chez
10.4.1 wrapper.

## Qualification

Run:

```sh
JOLT_BIN=/path/to/the/pinned/jolt \
  test/fixtures/provider-convergence/qualify.sh
```

Set `PROVIDER_CONVERGENCE_EVIDENCE_DIR` to an output directory when the exact
resolved `-Stree` and `-Spath` text should be retained as build evidence.

The green graph combines Samizdat commit
`22be90ddf9b05ba8406d6ec231d2748a4da22d8e` with the current jolt-chdb
checkout. Its `jolt -Stree` must select `jolt-lang/db 9e8c82a`; its
`jolt -Spath` must expose exactly one physical source root for every shared
`db.*` and `next.jdbc.*` namespace path derived from that provider tree. Two
fresh Jolt processes then each keep a Samizdat SQLite connection open. The
first runs a production local-posix Durable chDB writer that creates and queries
telemetry, flushes, and closes; the second restores the same object through an
immutable snapshot reader. Separate processes preserve chDB's process-lifetime
native ownership policy while proving that each JDBC role coexists with the
single converged provider tree.

The red graph retains `jolt-lang/db` `d85f391c` and
`io.github.casselc/db` `a5bf25d9`. The same exact-one oracle must reject it
specifically because two roots provide `db/sqlite.clj`. This is the causal
control; dependency order is never treated as coexistence evidence.

The qualifier creates run-scoped `JOLT_CACHE_DIR` and `JOLT_GITLIBS_DIR`
trees. It derives the complete `db/**/*.clj` and `next/**/*.clj` inventory from
the resolved `9e8c82a59ec63a36e86a758ff39ca9c5a9c3d165` provider root rather
than a hand-maintained list, and its causal control proves that a newly added
namespace is checked. `-Stree` abbreviates git revisions and is only a graph
diagnostic; the full-SHA gate is the exact revision embedded in the resolved
source-root path. This assumes Jolt's gitlib layout continues to retain the
complete revision as one path segment, and fails closed if that layout changes.

## Pin rollout discipline

When replacing this provider pin, first publish the reviewed immutable
`casselc/db` commit, then verify that a fresh git dependency cache resolves its
exact SHA. Update `deps.edn`, the qualifier's `provider_sha`, and this document
together. The qualifier deliberately fails if its expected full SHA differs
from the root pin or the resolved source root, so an unreviewed or stale fixture
cannot pass as convergence evidence.

After both land, consumers remove the historical `io.github.casselc/db`
coordinate. They do not add exclusions or overrides during the rollout.
