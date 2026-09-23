# Production-shape JSONEachRow encoding comparison

This is an encoder-only comparison, not a Durable or receiver throughput result.
It uses the same 512 ClickStack/OTel log rows as
`bench/jdbc/chdb_durable_throughput.clj` via their shared bench fixture. Its
source and result hashes let us compare the hosts without treating the older
sorted-map span fixture as production-shaped evidence.

From a **clean** chDB checkout:

```sh
scripts/benchmark-production-json-encode.sh verify /tmp/chdb-json-verify-1
BENCH_WARMUPS=5 BENCH_SAMPLES=100 \
  scripts/benchmark-production-json-encode.sh measure /tmp/chdb-json-measure-1
```

Each output directory must not already exist. If the Git dependency cannot be
fetched in a restricted environment, set `BENCH_DATA_JSON_LOCAL_ROOT` to a
**clean checkout at the pinned data.json SHA**; the launcher verifies both.
The launcher uses Chez 10.4.1 for Jolt and runs the hosts sequentially.
For a binary-level Jolt comparison, use `jolt-verify` followed by
`jolt-measure` with
`BENCH_JOLT_BIN=/absolute/path/to/jolt` and its exact
`BENCH_JOLT_EXPECT_SHA256`. That mode runs only Jolt's serial and four-fiber
profiles, validates the expected 348,836-byte fixture, and records the
executable SHA in both reports. Use a fresh directory and cache per binary;
an A/B/A schedule limits host-drift ambiguity. Do not merge those results into
the five-profile report or attribute a difference to one compiler patch merely
from a binary-level comparison.

Five profiles: Jolt's production `apply str`/data.json shape, Jolt's opt-in
four-fiber ordered encoder, JVM data.json from the same fork SHA, JVM Cheshire,
and Babashka's bundled Cheshire. The timed operation always includes payload
text construction **and one UTF-8 byte-array conversion**, including for the
serial profiles; it excludes the SQL prefix and statement construction.
Every profile checks 512 decoded JSON rows
against the source values. The three Jolt/JVM data.json profiles must have the
same UTF-8 payload hash. Cheshire profiles need the same canonical-value hash,
but byte identity is **not** asserted because their field order/escaping may
differ. `summary.edn` records the verified boundary. The reports identify the
exact fixture source, code head, codec pin, output bytes/hash, and latency
distribution. Do not compare process start, fixture construction, JSON parse,
chDB, WAL, flush, or fresh-reader work from these numbers: none is timed here.

Focused fixture/semantic test commands (these do not time samples):

```sh
bb -cp src:bench:test -m jdbc.chdb-production-json-encode-test
clojure -Srepro -Sdeps '{:paths ["src" "bench" "test"] :deps {org.clojure/data.json {:git/url "https://github.com/casselc/data.json.git" :git/sha "1b0716268232a79dd2b2fdb968cca171414bd589"}}}' -M -m jdbc.chdb-production-json-encode-test
/home/chuck/ai-src/tools/jolt-with-chez-10.4.1 jolt -Srepro -Sdeps '{:paths ["src" "bench" "test"] :deps {org.clojure/data.json {:git/url "https://github.com/casselc/data.json.git" :git/sha "1b0716268232a79dd2b2fdb968cca171414bd589"}}}' -M -m jdbc.chdb-production-json-encode-test
```
