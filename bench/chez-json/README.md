# Chez JSONEachRow prototype

This benchmark asks how much of row encoding time remains when a small Chez
encoder reads the same actual Jolt values. It does not replace production
`jdbc.chdb.json-each-row` or `clojure.data.json`. The encoder itself does not
admit Durable writes, write a WAL, or confirm persistence. Custom `data.json/JSONWriter` implementations are
outside this prototype's contract.

`encoder.ss` writes into one Chez string output port per payload. Strings are
scanned once and unescaped spans are copied directly into the port. Fixnums are
written as digits; no rows, keys, or values are pre-encoded or cached. Both
output text and UTF-8 conversion belong in an end-to-end encoding measurement.

`bridge.clj` uses the existing `jolt.scheme` raw escape hatch. The direct path
traverses actual Jolt persistent vectors and maps, including nested values; it
does not first construct native rows. The map walk preserves Jolt's sequence
order directly, including collision buckets, without building an entry list.
This is required because distinct string, keyword, or symbol keys can encode
to the same JSON property name, and duplicate-property decoding depends on
their order. Row order is also preserved. Default escaping can be checked for
exact byte parity; minimal escaping uses decoded values and row order, with
byte counts and digests reported separately.

The separate `to-native` entry point converts actual Jolt values to explicit
Scheme objects and vectors. Measure this conversion separately if used. Timing
only the already converted representation measures the native ceiling, not the
cost of encoding application-owned Jolt rows.

## Contract

Native values are strings, finite flonums, exact integers of any size, Scheme
booleans, `'null`, proper lists/vectors (JSON arrays), and objects created with
`(make-chez-json-object association-list)`. Object keys are strings or named
values created with `(make-chez-json-keyword "namespace/name")`. Named values
write only their name, as `data.json` does. The empty list is an empty array;
null has its own explicit sentinel. Association lists preserve supplied order.

The Jolt adapter additionally understands Jolt nil, keywords, symbols,
persistent hash/array maps, persistent vectors, lists, and sequential values
(including lazy sequences). Map keys must be strings, keywords, or symbols.
Lazy values are realized while encoding and included in that cost. Custom
writers are not called. Ratios, BigDecimal, sets, sorted maps, characters,
Java arrays, dates, UUIDs, arbitrary objects, complex and non-finite numbers
are unsupported and raise an error. This is deliberately narrower than the
general `data.json` API. No coercion or fallback hides unsupported values.

Only `escape-unicode`, `escape-slash`, and `escape-js-separators` boolean options
are supported; all default to true. The Scheme API takes an association list
of unqualified symbols and booleans; the Jolt API takes a map of unqualified
keywords and booleans. Unknown options and non-boolean values raise an error.
Setting all three false is the separately labeled minimal-escaping mode:
required JSON escapes remain, while slashes and Unicode remain literal.
JavaScript separator handling follows `data.json`'s explicit option even when
`escape-unicode` is true. Astral Unicode escapes use UTF-16 surrogate pairs.
Finite flonums retain a decimal marker, including negative zero, and follow
the selected Jolt runtime's `Double.toString` layout.

Public Scheme APIs:

```scheme
(chez-json-write value port)              ; optional third options argument
(chez-json-write-rows rows port)          ; optional third options argument
(chez-json-encode value)                  ; optional second options argument
(chez-json-encode-rows rows)               ; optional second options argument
```

Public Jolt APIs (load once before timing):

```clojure
(load-file "bench/chez-json/bridge.clj")
(chez-json.bridge/load-encoder! "bench/chez-json/encoder.ss")
(chez-json.bridge/encode-value value)
(chez-json.bridge/encode-rows rows)
(chez-json.bridge/encode-rows rows {:escape-unicode false
                                  :escape-slash false
                                  :escape-js-separators false})
(def native-rows (chez-json.bridge/to-native rows))
(chez-json.bridge/encode-native-rows native-rows)
```

All encoding functions return a string. `to-native` returns an opaque Scheme
value that must be handed back to the native encoder. Do not mutate native
objects/vectors while they are being encoded. The adapter relies on the pinned
runtime's internal collection accessors; it is not a portable library API.
It requires the normal compiler-bearing Jolt CLI, not an arbitrary tree-shaken
application binary. Relative paths above assume the repository root.

Every local command that invokes Chez or Jolt must use
`/home/chuck/ai-src/tools/jolt-with-chez-10.4.1`. The selected comparison binary
is `/home/chuck/ai-src/evidence/jolt-v0810-writer-wal-cleanbuild-NkyUraeX/jolt`,
source `85fcb21d84509aee9c1efa665463669abcae0d5e`, SHA-256
`c66e2e57dd4b6fbdba958e5cabcf0791ba268f1d374b1155ac580076c7b3060b`.
`data.json` remains pinned at `1b0716268232a79dd2b2fdb968cca171414bd589`.

The companion `fixture.clj`, `parity.ss`, `bench.clj`, and `bench.ss` provide
varying production log fixtures, edge cases, rejection checks, and bounded
measurements. Fixture construction, oracle encoding, hashes, and readback
checks must stay outside timed loops. Throughput from this directory describes
encoding only; fresh chDB readback and Durable throughput are separate gates.

## Reproduce the bounded experiment

Run from the repository root. Keep timing jobs sequential. The existing
project dependencies include the pinned data.json; include `resources` when
using `-Sdeps` to replace paths, because native ABI descriptors live there.

```sh
probe_jolt=/home/chuck/ai-src/evidence/jolt-v0810-writer-wal-cleanbuild-NkyUraeX/jolt
probe_wrapper=/home/chuck/ai-src/tools/jolt-with-chez-10.4.1
probe_output=$(mktemp -d /tmp/chdb-chez-json.XXXXXXXX)
"$probe_wrapper" "$probe_jolt" -Srepro -Sdeps '{:paths ["src" "resources" "bench"]}' bench/chez-json/bench.clj parity
"$probe_wrapper" "$probe_jolt" -Srepro -Sdeps '{:paths ["src" "resources" "bench"]}' bench/chez-json/bench.clj export "$probe_output/fixture"
"$probe_wrapper" /home/chuck/ai-src/tools/chez-10.4.1 --script bench/chez-json/parity.ss "$probe_output/fixture"
"$probe_wrapper" "$probe_jolt" -Srepro -Sdeps '{:paths ["src" "resources" "bench"]}' bench/chez-json/bench.clj measure chez-default 512 10 50 "$probe_output"
"$probe_wrapper" /home/chuck/ai-src/tools/chez-10.4.1 --script bench/chez-json/bench.ss "$probe_output/fixture" compatible 512 10 50 "$probe_output"
bb -cp bench bench/chez-json/bench_bb.clj 512 10 50 "$probe_output"
```

Other row counts are 1024, 5000 and 10000. Jolt modes are `data-json` (serial
reference), `chez-default`, and `chez-minimal`; native modes are `compatible`
and `minimal`. Reports preserve scalar sample timings, byte counts and hashes.
`BENCH_RETAIN_PAYLOAD=1` optionally retains synthetic payloads. Native fixtures
are prepaid representations; only the Jolt bridge measures actual Jolt input
maps with no conversion or encoded-payload cache.

`durable.clj` feeds freshly encoded batches into the unchanged Durable ticket,
classification, WAL, native execution, and confirmed flush path. Each arm
needs a new output directory and a separate fresh-process readback:

```sh
BENCH_JOLT_BIN="$probe_jolt" "$probe_wrapper" "$probe_jolt" -Srepro -Sdeps '{:paths ["src" "resources" "bench"]}' bench/chez-json/durable.clj write chez 512 30 "$probe_output/durable-chez"
"$probe_wrapper" "$probe_jolt" -Srepro -Sdeps '{:paths ["src" "resources" "bench"]}' bench/chez-json/durable.clj read "$probe_output/durable-chez"
```

Use `normal` for the existing four-fiber encoder. This compares the existing
best encoder to the single-thread prototype; it is not a per-core comparison.
All input maps vary by row and batch, and are prepared before the receive/
encode window. The window includes encoding, UTF-8 materialization, all caller
loop work, backpressure, final ticket settlement, and one final confirmed
flush. Fixture construction, startup, warmups, close, and readback are outside.
No SQL payload is precomputed for measured execution. This boundary differs
from earlier benchmarks which generate fixtures during the overall window.
Each readback checks independent aggregate values including row count, flags,
severity, body byte count, and trace/span ranges; it is not an all-field proof.

## What would make this a library feature?

The experiment establishes a useful native path, not a transparent data.json
replacement. Keep the reusable emitter with data.json, and give it small
runtime-owned operations for sequence-order map traversal, writer span output,
and numeric rendering. Current private HAMT layout access and compiler-bearing
`eval-string`/external `load` are unsuitable as the library boundary. Static
Scheme resource inclusion and AOT dependency tracking need an explicit design.

Preserve custom `JSONWriter` extensions and option behavior on the same output
writer. Restarting serialization on fallback can duplicate effects from lazy
values or custom writers. Any direct built-in traversal needs guards that
detect changed protocol implementations and public writer redefinitions.
These are adoption gates, not reasons to discard the measured prototype.
