# Typed process-exit regression

This child fixture is the reduced Linux reproduction for issue #111. It keeps
the real local-POSIX Durable writer, typed manifest installation, one direct
span export/readback, checkpoint, exporter shutdown, and explicit application
connection close. It intentionally excludes the OTel SDK, processors, HTTP,
Oscope, and an application database.

The causal red baseline is `jolt-chdb` `806092bf808c9ac8abf0af25b55b7dc8f90c93c2`
with `jolt-otel-clickhouse`
`14a2998a27f64a9bff329811461be9157a00c849`. On Jolt
`2d39e854a90926d8f8e9bd5d3ddbb109d657afe1` (launcher SHA-256
`28f6100f4db0a7d7463ae54c079cb3b406676d1b34987517636a2cc9f3301ec2`)
and chDB 26.7.3 `libchdb.so` (SHA-256
`36ad4e999882821ef13cf2d0f52c93f48e6ee1b4498b35a201c23ce4b8d15bf5`),
the child prints the main PASS marker and then exits 134 with a nonrecoverable
invalid memory reference. The same current chDB revision without the typed
workload exits cleanly, and the prior chDB revision
`3552a25778de1759d69f53c48e002324ee233ed8` runs the full dual local/remote
fixture cleanly.

The green contract requires both PASS markers and exit 0. The second marker is
emitted from a later Jolt shutdown hook after it observes that the driver hook
has claimed and closed the hidden anchor. Run through the workspace's required
Chez wrapper and an already-qualified native library:

```sh
JOLT_CHDB_LIB=/path/to/qualified/libchdb.so \
  scripts/check-native-typed-process-exit.sh /path/to/qualified/jolt
```
