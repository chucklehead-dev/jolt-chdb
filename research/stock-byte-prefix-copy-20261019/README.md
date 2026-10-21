# Stock-Jolt byte-copy and pipeline research

Driver base: `abb630a1709bafdb3f7caef51c5ad4a98410e391`. Exporter:
`4966a8b99c251d0025a8adca0477fe2c6c9f8b0c`. Official Jolt 0.8.23,
libchdb 26.9.0, local storage, rich typed ended-span fixture. Source-mode only.

## Change

Stock `ffi/write-array` stages bytes in a second managed bytevector before its
synchronous native copy. The new internal helper passes an already-owned byte
backing directly to that same runtime copy primitive. This is still a copy,
not a native pointer loan. Bounds, fallback, source isolation and scoped native
ownership remain checked. No global runtime helper is replaced.

Only private statement snapshots and the digest loop's private read buffer use
the helper. Public `sha256-bytes` keeps its previous staging path. This boundary
was tightened after source review; callers of that existing API gain no new
no-race obligation. The raw runtime adapter remains a trusted, version-sensitive
boundary; tests do not constitute a proof of the runtime or collector.

This preserves earlier work rather than rediscovering it: compiler commit
`9cb6ac13` already removes whole-array FFI staging. The library makes the benefit
available on stock Jolt and handles partial prefixes. Earlier upstream proposal
`b37c6c2c` also includes bounded native zeroing; neither proposal was submitted
upstream here. Do not replace or lose those branches.

## Measurements

- Guarded component,8MiB copies: median3.85ms/~8MiB estimated allocation with
  ordinary FFI versus0.45ms/~656B with the helper.64KiB copies also improved.
  Every sample checked complete native bytes and neighboring sentinels.
- Four actual SDK/Durable processes, ordinary/prefix/prefix/ordinary,5k batches,
 32 measured callbacks plus3 warmups each. All700k rows independently recovered.
  Estimated whole-window allocation28.39KB versus22.47KB/row (~21% lower).
  Pooled throughput11.16k versus10.36k rows/s: **not a throughput win**. Candidate
  windows varied substantially; do not promote the component result to a
  pipeline speedup.64 callbacks/arm do not qualify production tails.
- The measured ABBA patch used the helper at three sites, including public
  byte hashing. Its captured patch/checksums remain authoritative. The final
  source restores the public path; no final-source ABBA rerun is claimed.
- Final-source research with two/four DISTINCT exporters, one serial worker
  each, one shared serialized writer and the same20k total queue budget:
  single windows17.56k/19.56k confirmed rows/s. Per-trace affinity/order and all
 350k physical rows passed independent recovery. These are research windows,
  not a qualified scaling ratio, same-exporter concurrency, product defaults,
  p99 throughput or end-to-end span latency.
- An earlier single-lane stock diagnostic attributed roughly half of compact
  export time to projection/encoding and half to native/persistence. This
  motivates pipeline overlap as well as copy reductions. Never sum phase
  medians or add nested hash/verification times to parent publication totals.

The25k p50/~20k p99 targets remain unmet. No architectural impossibility proved.
No WAL/lease/CAS/confirmation protocol or formal model changed. No S3, AOT,
full Oscope startup or failure-injection qualification is claimed.

## Correctness and portability

Stock focused gate:5tests/64assertions plus2owned tests/13assertions, and full
manual digest/local-object checks. Includes range/guard bytes, all byte values,
boxed fallback, no fallback after copy failure, and SHA256("abc") known answers.
The digest cleanup test now also injects copy failure and checks all four native
contexts/arenas are released. Avoid using two routes through one new copier as
the only integrity oracle.

JVM digest/local-object checks pass with Clojure1.12.4/stock data.json2.5.2.
BB stream-only digest checks pass. Full BB local-object checks with stock
data.json2.5.2 hit unsupported `definterface`; the same require fails at the
unchanged parent. This is not full BB qualification or a new copy regression.
Existing native-Cheshire benchmark work is untouched.

All seven synthetic stores (1.225M checked rows including the diagnostic)
were retired after terminal writer, independent reader, validator and
copied/compared receipts. Only bounded scalar reports/source remain.
