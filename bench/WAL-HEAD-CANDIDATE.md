# Frozen-feature selection-head WAL measurement

## Canonical upload-buffer restoration

Production anchor `d3da7e3a9e0a53845caadd7dc1892be254457cdb` restores main's
already-reviewed `b59470503c42ae8f84170c614e20ee9b8cf807c5` request-owned
upload scratch helper on top of the seal-corrected feature graph. The older
connection-reuse branch had retained per-callback allocation. This is recovered
cumulative work, not a new fix missing from product main.

Select the closed `wal-selection-head-seal-upload-buffer` profile; all previous
profiles remain unchanged and reject this different production source. Control
and writer blobs stay exactly as below; curl is
`528ed7bfd3e01f31e937d01706d51ec357fa8ef1`, body helper is
`44b64e1ab7ba66ce3ae1f4f51ebc8b0f71ee580d`. Reuse ownership, callback-arena
cleanup, stored-object verification, commit fencing, SDK/JSON/compiler pins and
default options are unchanged. No arbitrary source override is accepted.

Compare future corrected-source windows against the seal-corrected profile,
not against relabeled historical receipts. An actual libcurl 6 MiB file-upload
screen removed about6.3MB managed allocation per request, with exact size/SHA256
readback. That is a loopback callback-algorithm control, not AWS or end-to-end
Durable throughput. Protected CI must authenticate the new exact source before
selecting this profile. No cloud dispatch or default change is implied here.

## Seal-error correction for future measurements

Production anchor `92526362d112c2da0aa4993f09db8c25c42cba45` adds the reviewed
pre-publication seal-error provenance correction to the previous measured graph.
It preserves all owned-statement/native-admission features and dependency pins.
Control remains blob `180d2d018c8b153ed9fc812528e909076f6ed7f1`; writer is
`6eac0599c0e33d4971305011355c3a9de3b1bcc1`. Select the new closed profile
`wal-selection-head-seal` for this graph. The historical profile below remains
unchanged and rejects this new production source.

This correction has focused writer/selection/owned-output tests; it is not a new
throughput result. Do not relabel the earlier matched receipts or assume their
timings apply to this head. Any protected comparison workflow must authenticate
the new exact source before selecting it. No cloud workload is dispatched here.

## Historical selection-head source

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
