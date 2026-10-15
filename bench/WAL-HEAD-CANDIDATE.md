# Frozen-feature single-head WAL measurement

This branch is measurement support, not a whole-feature merge proposal. Parent
workload3fd96c4 and production291e1f43 remain the baseline. Commit85738d512025ae056f7721c6ffd89c061046d94e
changes only control.clj to blobc198a3887fd8c4073763676e32c9945a58f34913,
identical to the reviewed production candidate in PR272 (db927eb7).

Set QUALIFIED_STORAGE_SOURCE_PROFILE=wal-single-head only for this candidate.
The launcher accepts exactly that frozen production graph or the original
baseline graph, not arbitrary source refs. Compiler/native/exporter/JSON pins,
workload, automatic checkpoints/renewals and stock recovery remain unchanged.
No Expect wait override or transport change. Default source profile stays
baseline and rejects candidate production unless explicitly selected.

Removing one head GET is not yet a throughput gain. Compare baseline/candidate
on the same host with independent object identities and complete readback before
attributing performance. The immutable old runtime remains a causal control,
not the new upstreaming target (which is stock Jolt0.8.22).
