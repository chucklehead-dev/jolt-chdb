# Confirmed-ticket main integration

Production source matches qualified `a752c0c`; this integrates it with canonical
main `b648ab4` without changing the atomic owned mutation/publication operation.
Submission is admission only. The opaque request's separate await settles through
interruption and returns the exact receipt/error; repeated waits do not resubmit.
This is not a BufferedTicket, a restart-persistent operation ID or an adversarial
security boundary against private access/reflection/redefinition.

## Focused controls

Stock Jolt 0.8.23 / mandatory Chez 10.4.1:

- Confirmed requests: 8 tests / 48 assertions.
- Capacity observation: 3 / 38; prepared records: 5 / 77; existing owned writer: 5 / 35.
- Both new test runners reject empty or incomplete test inventories. Hosted
  stock-byte CI invokes their explicit aliases and watches their source inputs.

`permit-return-control.clj` drops one permit return in the fake-worker fixture
and runs the SAME failed-request-insertion test. It requires exactly one failed
assertion (64 permits become 63), with no test error; its exit0 means the expected
negative control was detected, not that the mutant is correct. Normal source
passes the test and successfully publishes a later real request exactly once.

The first fault-injection draft incorrectly failed both request insertion and
permit return because they share a helper. That produced a false alarm on normal
source. The final fixture targets ONLY the request queue and delegates the
permit queue to the real helper; positive and mutant results above use this fix.
There was no production permit-restoration fix or encoding change.

From the repository root, set `JOLT_BIN` to the qualified stock 0.8.23 executable
and use the workspace's pinned Chez wrapper:

```sh
/home/chuck/ai-src/tools/jolt-with-chez-10.4.1 "$JOLT_BIN" -Srepro -M:confirmed-request-test
/home/chuck/ai-src/tools/jolt-with-chez-10.4.1 "$JOLT_BIN" -Srepro -M:owned-statement-budget-test
/home/chuck/ai-src/tools/jolt-with-chez-10.4.1 "$JOLT_BIN" -Srepro -Sdeps '{:paths ["src" "resources" "test"]}' research/confirmed-tickets-main-20261021/permit-return-control.clj
```

## Native integration: functional, not throughput

The existing concrete exporter cancellation fixture was run with explicit local
overrides selecting this candidate driver and the canonical SDK preparation
source. These are NOT published-graph qualification runs. Resolved classpaths
and pre/post production source hashes were checked; runtime/library checksums
were captured after the run, not as a full pre-run binary attestation.

Both owned and borrowed modes completed writer and fresh-reader processes.
Each observed: prepare A, enter A's real WAL put, prepare B, fence publication,
then release A. SDK stats: attempted2/exported1/failed1/dropped0/queue0;
shutdown=false reflects cancellation, not successful delivery of B. The owned
publisher terminates; the borrowed publisher remains usable until consumer close.
Fresh readers confirm full-row parity/exact nanoseconds for A and absence of B.
Small scalar receipts are in `native-results/`; no SQL or stored objects are
included. This is not a large-batch, live-S3, schema-freshness or throughput proof.

The statement-owner proof pin was audited for its two additive, read-only
capacity getters. The unchanged five WAL byte-access queries and controls replay
successfully; see the bounded proof README and `ticket-integration-replay-20261021.json`.
Those arithmetic checks do not establish queue ordering, lifetime or persistence.
