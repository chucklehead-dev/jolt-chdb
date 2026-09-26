# Native JSON encoder experiment, 2026-09-25

The working prototype substantially reduces JSON encoding cost on actual Jolt
maps. It is an experimental performance target, not a production data.json
replacement. See [README.md](README.md) for commands and the supported subset.

## Encoding, including UTF-8

Median milliseconds per batch; smaller is better:

| Rows | Direct Jolt, default escapes | Direct Jolt, minimal escapes | BB native Cheshire |
| ---: | ---: | ---: | ---: |
| 512 | 2.977 | 3.548 | 3.372 |
| 1024 | 9.462 | 7.572 | 7.212 |
| 5000 | 55.054 | 50.556 | 37.968 |
| 10000 | 102.894 | 108.038 | 78.644 |

Inputs are the existing varying ClickStack log fixture, materialized before
timing. Every sample traverses actual maps; no native preconversion, encoded
row cache, or key cache is used. Default bytes match data.json exactly;
minimal bytes match BB's native Cheshire exactly for all four fixture sizes.
The first two sizes used 10 warmups/50 samples, the larger two 5/20. These
sample counts and this single sweep do not qualify tail performance.

The serial data.json controls measured 43.198, 84.455, 428.110 and 891.952 ms.
These are not the existing best four-fiber path. A separate initial 512-row
four-fiber A/B/A measured 15.529 / 3.526 / 17.113 ms (5 warmups/20 samples).
That first prototype preceded the map-order correction; its fixture had
unique string keys and passed decoded parity. Final matrix values above use
the corrected, byte-compatible traversal. Neither comparison is a per-core
efficiency claim.

The standalone preconverted-Chez ceiling measured 4.041, 11.226, 60.116 and
119.940 ms with default escaping. Larger batches remain slower than Cheshire
even without Jolt collection traversal. This suggests investigating native
string-port growth, copying and allocation at that scale; it does not establish
which of those is responsible. Do not attribute the remaining gap solely to
Jolt interop from these measurements.

## Actual local Durable persistence

These runs use unchanged Durable classification, WAL preparation, native
execution and publication. One-ticket overlap admits the next batch after the
previous ticket settles; encoding may overlap prior execution. All varied
input maps are prepared before the timer. The timer includes loop work,
encoding, UTF-8 conversion, backpressure, final ticket drain and one final
confirmed local-POSIX flush. Startup, warmups and recovery are outside it.

| Rows/batch | Measured batches | Existing four-fiber confirmed rows/s | Prototype confirmed rows/s |
| ---: | ---: | ---: | ---: |
| 512 | 30 | 19,115 before / 18,309 after | 25,120 |
| 5000 | 10 | 18,465 | 34,501 |

Local-execution-only rates were 21,586 / 30,311 / 21,823 rows/s for 512-row
A/B/A and 20,046 / 40,237 rows/s for 5000-row A/B. They exclude final flush and
must not be presented as persistence rates. Flush times were respectively
91.970 / 104.718 / 135.065 ms and 213.487 / 206.588 ms.

Every arm returned `:committed`. Five separate fresh-reader processes matched
independently regenerated aggregates: 16,384 rows per 512-row arm and 60,000
per 5000-row arm, including two warmup batches. Encoding decoded-value checks
cover every prepared batch before timing. Persisted readback checks count,
flags, severity, body byte count and trace/span ranges; it is not an all-field
persisted-value proof. These are short local runs, not S3, Rust equivalence,
20k tail qualification, or a general 80–90% Rust-performance claim.

## Correctness and independent review

Final Jolt parity passed 149 corpus cases, including a 132-case deterministic
finite-float grid, controls/Unicode/astral characters, signed zero, subnormals,
nested values, named keys, transformed duplicate names, and two genuine
full-hash-collision groups. It asserts exact bytes and decoded values, plus
unsupported-value/option rejection and all four telemetry sizes. Standalone
Chez parity passed 166 checks. This is bounded coverage, not exhaustive proof.

Review found a real bug in the initial HAMT order: distinct keys could become
the same JSON property name and change the decoded last-wins value. The fix
walks sequence order directly, including hash-collision buckets; the regression
now tests both forms of collision.

Claude independently reviewed the source and measurement boundaries. Its
hash-collision and float-coverage requests were added and passed. The
four-fiber-versus-single-thread distinction is explicit above; serial controls
are also retained. Its possible Scheme/Jolt marshaling concern was checked
against the pinned `stdlib/jolt/scheme.clj` and `host/chez/host-contract.ss`:
`scheme/proc` returns the raw procedure, and strings cross unchanged. There
is no JVM representation conversion. The total timer includes the return and
complete UTF-8 materialization regardless of the internal phase split.

## Provenance and continuation

- chDB source base: `6569f97cc0aa2bedd172966fa7c1ae6dfe1942e8`.
- Cumulative Jolt source: `85fcb21d84509aee9c1efa665463669abcae0d5e`, version
  `v0.8.10-5-g85fcb21d`, binary SHA-256
  `c66e2e57dd4b6fbdba958e5cabcf0791ba268f1d374b1155ac580076c7b3060b`.
- Chez: 10.4.1; data.json: `1b0716268232a79dd2b2fdb968cca171414bd589`.
- libchdb: 26.7.3, SHA-256
  `36ad4e999882821ef13cf2d0f52c93f48e6ee1b4498b35a201c23ce4b8d15bf5`.
- Native WAL fast path verified active. Earlier compiler and data.json
  improvements remain in these comparisons; no compiler rebuild was needed.

Local evidence lives under `/home/chuck/ai-src/evidence/`:
`chdb-190-chez-json-matrix-20260925-Rj4miQ/` contains reports, exact hashes,
timing and augmented-parity provenance. Durable result/readback receipts live
in `chez-json-durable-512-{a1r,b,a2}-20260925/` and
`chez-json-durable-5000-{normal,chez}-20260925/`. The initial 512 `a1` directory
is a failed startup from omitting the ABI resource path, not a measured arm.

Next: preserve this performance target while extracting a library-owned native
backend with custom-writer/option compatibility and a proper AOT boundary.
Measure how much speed survives those guards before integration. Then rerun
the normal product path, extended tails and S3. At large batches, profile the
native ceiling's allocation/copying behavior rather than guessing that another
generic interop patch will remove the remaining cost.
