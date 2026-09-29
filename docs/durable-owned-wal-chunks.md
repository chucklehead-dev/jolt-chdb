# Internal WAL preparation experiment

This candidate changes how the writer prepares a WAL record, not what a
successful write means. It is not yet a measured throughput improvement.

The public `wal/line` function still returns one byte array. Internally, the
serialized writer may instead prepare a byte count and a sequence of owned
arrays with their used lengths. The complete record is prepared and checked
before native execution. Only the used bytes are written; padding is never
persisted. Each call owns fresh arrays, and the encoder never changes a sealed
array. Small records use smaller backings; no backing exceeds 64 KiB.

Jolt selects the codec only after matching the established portable encoder
over the full admission corpus and additional chunk-boundary cases. Missing
runtime support falls back before choosing any output. Non-Jolt hosts keep
the existing byte-array path. JVM fallback is tested; a Babashka run using
upstream data.json 2.5.2 cannot load that library's `definterface` form, so this
does not establish Babashka Durable support. The separate BB row-encoding
benchmark uses native Cheshire and is not changed here.
The loader embeds its resource during macro
expansion; standalone/AOT behavior still needs separate qualification.

## Failure and recovery contract

The existing size limit is checked against the complete prepared byte count
before native execution. After execution, record and byte counters advance
only when every chunk has been appended successfully. A failure after even
one chunk has been appended requires a checkpoint: that partial spool cannot
be published as a complete replay record. Existing sealing, immutable object
verification, head CAS, and recovery rules are unchanged.

The focused test records this concrete transition: native execution succeeds,
the first chunk is written, a later append throws, pending counters stay zero,
checkpoint-required becomes true, and flush cannot publish that partial WAL.
Another test rejects a malformed descriptor before engine calls.

This is a concrete representation obligation beneath the existing file-WAL
spool state machine. The abstract model does not establish array ownership,
exact chunk bytes, or native persistence. Independent byte-corpus tests,
mutation-isolation tests, fault traces, and fresh-process native recovery are
the evidence for those boundaries; passing the model alone is insufficient.

## Qualification

Run `jolt -M:durable-wal-chunks-test` for selector and focused failure tests.
Set `JOLT_CHDB_REQUIRE_WAL_CHUNKS=true` on a candidate qualification lane to
reject silent fallback. The native qualification parent is
`scripts/qualify-durable-native.sh DIRECTORY`; its child alias requires a
phase argument and is not a standalone aggregate command.

The ordinary pinned CI lane may qualify fallback only. A separate immutable
canonical-compiler lane requires native selection in both the focused tests
and fresh-process native recovery; failure to select is a failure, not a skip.
This functional lane is separate from the modern cumulative performance
baseline and does not change consumer compiler pins.
Performance comparisons
must use identical runtime/GC settings, confirmed commits and fresh-process
readback, and must record whether the new selector is active. Preparation-only
timings do not establish end-to-end throughput, p99, S3 or recovery targets.
