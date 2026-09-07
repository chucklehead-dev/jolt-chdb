# Durable ABI provenance

`resources/jdbc/chdb/abi.edn` is the canonical, runtime-neutral description of
the libchdb surface used by this repository. Its Durable V1 entries were audited
against these immutable upstream sources:

- chDB core release: `v26.7.2-rc.2`
- commit: `30488a59b2700188ee36ecbced7713081a909f56`
- header: `programs/local/chdb.h`
- C oracle: `examples/chdbDurableAbiTest.c`
- Python oracle: `tests/test_durable_backup_restore_classify.py`

At that revision, `chdb_query_analysis_v1` is four consecutive `uint32_t`
fields in this order: `struct_size`, `statement_count`, `flags`, and
`query_class`. Its size is 16 bytes. The query classes are ordered from
`READ_ONLY=0` through `UNKNOWN=4`; the V1 flags are bits 0, 1, and 2. The three
new entry points use only pointers, `size_t`, and an integer return. The analysis
structure is caller-owned and passed by pointer, never by value.

The Jolt adapter derives its literal FFI declarations and compiled layout from
that descriptor. Focused tests independently restate the pinned header contract
and include wrong-schema, wrong-size, wrong-type, wrong-enum, unknown-flag,
duplicate-symbol, and missing-symbol mutants. This makes descriptor drift and
vacuous capability tests fail locally rather than surfacing as native memory
corruption.

`scripts/qualify-durable-native.sh DIRECTORY` authenticates the matching rc.2
asset on each of the four published platforms, compiles and runs the exact
pinned upstream C oracle, and then runs the independent Jolt classification and
backup/restore suite against that same library. The hosted qualification gate
currently exercises Linux x86-64. The other asset mappings are pinned so the
same gate can run unchanged when those runners are added; they are not yet a
cross-platform conformance claim.

## Release boundary

The production installer remains pinned to stable libchdb 26.7.0. That library
does not export `chdb_backup_database_n`, `chdb_restore_database_n`, or
`chdb_classify_query_n`. `jdbc.chdb.native/durable-capability` loads the selected
library, resolves every symbol named by the versioned contract, and reports
missing symbols as `:jdbc.chdb.native/unsupported-core`. It does not invoke a
missing binding and ordinary JDBC remains usable.

This remains below the Durable control plane. It does not implement object
layout, WAL segments, storage, leases, CAS, backup policy, or replay.
The native production pin must not move to this prerelease. Once a stable chDB
release carries the ABI, its assets and checksums must be pinned and the upstream
C oracle plus independent Jolt classification and backup/restore tests must pass
before Durable is advertised as supported.
