# Frozen-feature selection-head WAL measurement

This branch is measurement support, not a whole-feature merge proposal. The
next baseline is the measured single-head candidate1a23e21. Commit6ae42f4e57aa131cf020269aea306c196585cb9e
applies the reviewed policy/publication-read change to that graph, modifying only
control.clj (blob180d2d018c8b153ed9fc812528e909076f6ed7f1) and writer.clj
(blob16cb452d375d59418f30718d73bdfa2a2bb431bd). The writer retains its measured
owned-statement/native-admission features. No compiler/dependency repins.

Set QUALIFIED_STORAGE_SOURCE_PROFILE=wal-selection-head for this candidate and
wal-single-head for the1a23 baseline. The launcher accepts only the enumerated
frozen production graphs, not arbitrary source refs. Compiler/native/exporter/JSON pins,
workload, automatic checkpoints/renewals and stock recovery remain unchanged.
No Expect wait override or transport change. Default source profile stays
baseline and rejects candidate production unless explicitly selected.

Removing one head GET is not yet a throughput gain. Compare baseline/candidate
on the same host with independent object identities and complete readback before
attributing performance. The immutable old runtime remains a causal control,
not the new upstreaming target (which is stock Jolt0.8.22).
