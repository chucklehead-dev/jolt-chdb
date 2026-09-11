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

## Phase 0 Babashka and JVM characterization

The driver contract has one additional, deliberately test-only adapter that
derives `babashka.ffi/cfn` bindings from `jdbc.chdb.abi/binding-specs`. It does
not copy a symbol or C signature and is not reachable through the production
`jdbc.chdb.native` or `jdbc.chdb` namespaces. The same owned-thread smoke test
performs version and capability checks, opens an in-memory connection, binds
`42` as a server-side query parameter, copies the exact three CSV bytes, then
destroys the result and closes the connection owner exactly once.

The checked compatibility unit is recorded in
`resources/jdbc/chdb/ffi-compatibility.edn`: Jolt `v0.8.6` (a release build or
a commit derived from that release), with hosted CI pinned to the exact
`integration/aspects` compiler commit recorded alongside it; Babashka
`v1.13.220` at commit
`b98575c98a0ef4df77775ff25fd7fc7b591b1afd` with embedded `babashka.ffi`
source revision `aacb153618bc39ca1e4c397b8f30fb81c76d0c4c`, and the same FFI
revision as an explicit JVM dependency on Corretto JDK `25.0.2+10-LTS`.
Babashka does not expose the embedded FFI source revision at runtime, so the
qualification verifies the exact Babashka tag and commit while recording that
FFI revision as build-source provenance rather than a runtime-observed fact.
The hosted lane installs the checksum-pinned, dynamically linked Linux x64
Babashka release artifact explicitly. `setup-clojure` selects Babashka's static
Linux artifact, which cannot load the dynamically linked glibc libchdb release;
the compatibility guard rejects that artifact choice rather than skipping the
BB lane or falling back to a different library. Loader failures report a
bounded cause class/message chain and the selected path, without environment
contents or library search-path expansion.
Because setup-java's Corretto catalog accepts only major-version requests, the
hosted lane does not request floating Java 25 and relabel it as this unit. It
downloads Amazon's immutable `25.0.2.10.1` Linux x64 archive, verifies the
manifest-pinned SHA-256, installs it through setup-java's local-file provider,
and then checks the actual vendor and `25.0.2+10-LTS` runtime identity. A
cross-file mutant guard fails if the workflow archive version or digest drifts
from the compatibility manifest.
The only qualified Phase 0 native target is stable
libchdb `26.7.0` on Linux amd64. This is not a claim of Durable, Windows, or
unqualified-platform support.

Two controls keep this characterization meaningful. A raw C result-buffer
pointer must reject copying until it is reinterpreted to the exact native
length, and a binding constrained to the wrong or a missing selected library
must reject rather than falling back to a process-global symbol. Platform and
compatibility checks run before native loading. Library loading and `cfn`
construction are setup operations on the invoking thread; actual native calls
run on one owned OS thread, and only copied bytes and scalar evidence cross its
positive join.

The qualification resolves `jdbc.chdb.native/library-path` once under Jolt and
exports that exact selection as `JOLT_CHDB_LIB` to all three hosts. This keeps
custom `JOLT_CHDB_CACHE_DIR` and `XDG_CACHE_HOME` selections from silently
diverging between adapters.

Run all three hosts with `scripts/qualify-ffi-runtimes.sh jolt`. Local source
work in this workspace passes the mandatory compiler selector as two arguments:

```sh
scripts/qualify-ffi-runtimes.sh /home/chuck/ai-src/tools/jolt-with-chez-10.4.1 jolt
```

Phase 1 must fold the host adapter into `jdbc.chdb.native` and delete
`test/jdbc/chdb_abi_babashka_test.clj`; a second production ABI path is not an
acceptable endpoint.
