# Stock Jolt WAL output-prefix check

Implementation: d52ce5136918e4859f53d8d2884fb115ece903f5, based on
3658ce2e657d917cd23b50071688fe3fd511c3e3. Both comparison arms retain the
previous private FFI prefix-copy helper and explicit exporter codec. Only
`jdbc.chdb.byte-output/write-prefix!` differs: ordinary three-argument stream
write versus the production library helper. The selector counts 5,635 calls
per process. This is a research selector, not a new application option.

The helper admits only qualified built-in binary file ports. Other streams
keep ordinary dispatch. It does not change source ownership, WAL bytes,
flush/close, publication, acknowledgement, or replay. No formal model changed.
Private runtime record layouts remain a guarded, version-sensitive dependency;
source-mode checks do not establish standalone/AOT compatibility.

## Validation

- Official stock Jolt 0.8.23: 7 output tests, 66 assertions, green.
- Babashka and JVM Clojure 1.12.4: each 4 output tests, 50 assertions, green.
- Stock owned-writer suite: 5 tests, 35 assertions, green.
- Stock owned-statement suite: 14 tests, 15,563 assertions and the real-engine
  owned snapshot/WAL parity check, green.
- Legacy String WAL chunk suite: 6 tests, 18 assertions on its fallback path.
  The explicit require-native gate fails because stock lacks that old private
  conversion helper. This is not evidence for native chunk activation; the
  independently tested owned-byte statement path supplies the measured chunks.
- `git diff --check` and actionlint passed.
- First Claude invocation exited successfully but returned no usable findings.
  Follow-up captured its actual source-only review: no defects found. Review
  is advisory, not proof of persistence or complete runtime compatibility.

## Actual SDK/local Durable comparison

Official stock Jolt 0.8.23, libchdb 26.9.0, 5,000-row batches, one SDK worker,
32 measured batches plus three warmups. Exact exporter source remains
4966a8b99c251d0025a8adca0477fe2c6c9f8b0c. See source hashes and selectors.
All four processes independently recovered and validated 175,000 physical
rows each, including warmups. Total: 700,000 rows. These are synthetic spans,
not captured user telemetry. Small receipts are retained; temporary stores
were removed only after writer, independent reader, validation, receipt
comparison and source stability checks completed successfully.

| Arm | Individual confirmed rows/s | Pooled rows/s | Estimated allocated B/row |
| --- | --- | --- | --- |
| Ordinary | 10,541 / 11,011 | 10,771 | 22,464 |
| Prefix | 11,890 / 10,143 | 10,947 | 20,349 |

Allocation falls about 9.4%. Apparent pooled rate gain is only 1.64%, below
4.45% control drift: **no demonstrated throughput improvement**. Callback
percentiles are in summary.edn; these short samples do not qualify sustained
throughput tails or end-to-end span latency. The 25k/20k target remains unmet.

The final-source file-output component check also verifies full byte readback
after every sample. At 64KiB, median estimated allocation falls from 65,770
to 138 B/write and median write time from 33.9 to 28.1 microseconds. Its 8MiB
case is a component diagnostic, not the WAL chunk size. Explicit caller close
and flush are outside timing; fsync is not included. Do not call this a
durability or full-pipeline speedup.

## Next

Keep this allocation reduction available without requiring a compiler fork,
but do not spend more tuning cycles on its small measured time difference.
Next investigate remaining encoding/projection and native ingestion costs;
prepare a correctly validated 10k-batch comparison rather than extrapolating
the fixed-5k fixture. General traversal/byte/codec library extraction should
preserve these contracts and avoid introducing per-scalar bridge callbacks.
