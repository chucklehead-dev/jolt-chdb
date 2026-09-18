# Database-provider convergence

jolt-chdb and Samizdat share one database/JDBC implementation. The canonical
library key is `jolt-lang/db`; jolt-chdb resolves that key to the integrated
`casselc/db` revision `6db791634e5a4c65c24646833b2e82d3a5d7a121`.

This is ancestry, not a resolver substitution:

```text
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
 :git/sha "6db791634e5a4c65c24646833b2e82d3a5d7a121"}
```

Remove `io.github.casselc/db` from a graph that already has `jolt-lang/db`.
Do not add an exclusion, `:override-deps`, or a root mapping from one historical
coordinate to the other historical revision. Those techniques choose a
classpath winner without integrating the behavior of both lineages.

Jolt 0.8.6 remains the ordinary JDBC minimum. Durable writer qualification
currently requires the documented `casselc/jolt` compiler
`bf8a5dde7bebb5658d218e9757ab1df0aa9c3b95` (banner
`jolt v0.8.6-599-gbf8a5dde`) until a release contains its strict-decoder and
`OutputStreamWriter.append` corrections. The fixture accepts that executable
through `JOLT_BIN` and always runs it through the workspace's mandatory Chez
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
checkout. Its `jolt -Stree` must select `jolt-lang/db 6db7916`; its
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
the resolved `6db791634e5a4c65c24646833b2e82d3a5d7a121` provider root rather
than a hand-maintained list, and its causal control proves that a newly added
namespace is checked. `-Stree` abbreviates git revisions and is only a graph
diagnostic; the full-SHA gate is the exact revision embedded in the resolved
source-root path. This assumes Jolt's gitlib layout continues to retain the
complete revision as one path segment, and fails closed if that layout changes.

## Rollout order

1. Merge the `casselc/db` provider-convergence PR into `casselc/db` `main`
   with a strategy that preserves commit
   `6db791634e5a4c65c24646833b2e82d3a5d7a121`. Do not squash or rebase it away.
2. Verify that the commit is reachable from the published `casselc/db` main
   line and that a fresh git dependency cache can resolve the exact SHA.
3. Only then publish and open the jolt-chdb PR. Its `deps.edn` deliberately pins
   that immutable commit, so opening it first produces an unresolvable graph in
   clean CI rather than a meaningful convergence result.

After both land, consumers remove the historical `io.github.casselc/db`
coordinate. They do not add exclusions or overrides during the rollout.
