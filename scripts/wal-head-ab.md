# Matched selection-head WAL qualification

`wal-head-ab.yml` is a protected-main, manual workflow. It compares baseline
1a23e21 with candidatec7f54fe on one host. Both use the same frozen advanced
exporter4f3/JSONb959/compiler1ae/native26.9 stack, not current app defaults.
Candidate production differs ONLY in control.clj (blob180d2d01) and writer.clj
(blob16cb452d), the reviewed policy/publication-read change. It preserves the
owned-statement/native-admission features of this measured graph. Other
production files and pins are unchanged. The older measured compiler is a causal comparison control,
not the target for new upstream work (currently stock Jolt0.8.22).

The workflow runs local baseline/candidate first, then adjacent S3
baseline/candidate windows, always with independent object identities. Both use
the same benchmark-only options wrapper to replace the frozen driver's
hard-coded object ID with one of two distinct, closed IDs. Unique temporary
directories alone would NOT isolate the S3 objects. No other option changes.
Each arm uses256measured10k batches +3warmups, automatic128-reference checkpoints,
independent heartbeat renewals and stock-reader full physical/typed recovery.
Only one local database/WAL tree lives at once, removed on success or failure.

Each S3 writer is capped at24000requests/6GiB declared upload; each reader at
1000GETs/zero upload. Across both writers the maximum is48000requests/12GiB,
not6GiB. The global50minute workload deadline plus5minute preflight fits the
existing one-hour role. Failed preflight prevents object writes. No wait override,
new transport options, credentials, IAM changes or payload logging.

The original workload/recovery/budget verifier and curl-profile verifier must
pass for BOTH arms. Source SHAs, tracked content and untracked/ignored code-path
files are checked; a tracked diff alone cannot detect dependency-shadowing
untracked source. No arbitrary source/runtime/workload input is accepted.

Only the two fixed arm directories of nine allowlisted scalar receipts each may
be uploaded after privacy checks. No process logs, objects, paths, headers,
credentials or telemetry payloads are uploaded. Small partial receipts may remain
after a failed run; they are not a passed qualification.

This is one sequential fixed-order comparison, not randomization or confidence
bounds. Track p50/p90/p95/p99 and raw sample counts, while retaining native curl
milestones as overlapping diagnostics, not exclusive phases. A saved GET is a
mechanism result; throughput improvement and sustained tail targets require
actual results. No blanket improvement claim from prior short smoke tests.
