# Clean WAL run copy: not adopted

Research branch only; do not merge or select this kernel as a performance fix.
Base driver `a752c0cd553fdce41087a261d081f390154f7b55`. The candidate replaces
per-byte writes of clean ASCII runs with one checked `bytevector-copy!`, retaining
the same escape table, flush boundary and private output ownership.

Stock Jolt 0.8.23 / required Chez 10.4.1 source component screen, one process,
8,320,000-byte synthetic SQL-shaped ASCII input, 9,120,011 encoded output bytes.
Three warmups per arm, fixed A/B/B/A order, 16 measured samples per arm:

| Arm | p50 / p90 / p95 / p99 ms |
| --- | --- |
| Original byte loop | 37.30 / 45.68 / 76.33 / 76.33 |
| Checked clean-run copy | 44.16 / 51.42 / 55.13 / 55.13 |

Median is approximately 18.4% worse. Both arms had five measured samples with
GC. Sample maximum is lower in the candidate but does not qualify a tail gain.
With 16 observations per arm, nearest-rank p95 and p99 are both the maximum;
neither is an independent tail estimate. No significance or stable causal
effect is established from this one component run.
No native Durable sweep, production adoption, compiler change or Oscope pin.
This rejects this implementation; it does not prove all block/vectorized scans
are unhelpful or establish their effect on the real rich workload.

The component controls pass 180 assertions over empty/small/chunk-boundary
inputs and all ASCII bytes. They compare complete encoded bytes, input roundtrip
through JSON parsing, total size, chunk used lengths and backing capacities.
These tests do not prove lifetime/mutation isolation or new unsafe-access bounds;
the new scan/copy uses checked primitives and existing unsafe writes are unchanged.
No formal model or protocol contract changed. More controls would be required
before adoption, which the negative timing result does not justify now.

`screen.clj` reads the original kernel from the explicit sibling worktree path
in `baseline-path`; that path is part of this workspace-only research fixture,
not a portable benchmark distribution. The baseline source SHA256 was
`f910f642732bde2d73965d735c969176eac0b78a18a0e3222e9187b4672a6b22`;
candidate SHA256 was
`bfb0040b06b4278e64cc852799e937516e4848d503ed8bd26c99cfab737c4ef0`.
Raw scalar observations are in `screen.edn`. No native store/build was created.

Claude's source/receipt review supports parking and identified the quantile
presentation caveat above. Codex does not adopt its stronger "not noise-driven"
inference: a single small ordered screen is sufficient to decline adoption,
not to prove a stable slowdown across workloads. Review ran no tests/commands.
