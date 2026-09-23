# Ordered JSONEachRow encoder

`jdbc.chdb.json-each-row` provides a caller-owned, opt-in encoder for batches
of JSONEachRow values. It only produces text and UTF-8 bytes. It does not send
SQL, admit a Durable write, publish a WAL entry, or confirm persistence.

```clojure
(require '[jdbc.chdb.json-each-row :as json-rows])

(let [encoder (json-rows/open-encoder {:parallelism 4})]
  (try
    (let [{:keys [payload utf8 byte-count]}
          (json-rows/encode-rows! encoder [{"id" 1} {"id" 2}])]
      ;; Pass payload or utf8 to a caller-owned SQL/transport path.
      payload)
    (finally (json-rows/close! encoder))))
```

The default `:parallelism` is 1. Jolt accepts an explicit 4 to encode four
contiguous row chunks on fibers and concatenate them in original row order.
Parallel rows should be fully realized, CPU-only JSON values. Nothing in row
serialization—including custom writers, lazy value/key realization, or value
transforms—may park, block, or perform I/O inside a worker. Some Jolt builds
reject parking while realizing the row sequence under an internal counted
lock. Use serial mode for values needing blocking callbacks; this constraint
is a caller contract, not something the encoder can validate automatically.
The application must arrange adequate carrier capacity; the encoder neither
changes nor assumes a process-global carrier setting. JVM Clojure and
Babashka accept the same option but currently encode serially.

Share one context among callers to bound that context to one in-flight batch.
A competing call throws `::busy`. Once `close!` starts, new calls throw
`::closed`; close waits until its admitted batch has settled. With a
nonnegative timeout it may return `:pending`, in which case a later `close!`
can drain it. It does not cancel a serializer that blocks indefinitely.
Separate contexts can oversubscribe the machine: this is not a process-wide
worker or queue limit. A failed Jolt worker, failed partial spawn, or
interrupted join settles all started children before releasing the context.

Pass a vector of finite JSON-compatible values. Unsupported values fail the
batch without poisoning a settled context. Jolt and JVM use the configured
`clojure.data.json` writer and preserve that host's default writer bytes,
including escaping. Babashka uses its native Cheshire writer. For Babashka,
the portability guarantee is row order and decoded JSON-value parity, **not**
cross-host byte identity or canonical object-key order. For example, Cheshire
may emit raw `é` where data.json emits `\u00e9`. A caller that needs exact
wire bytes should pin the host and writer, not compare bytes across hosts.

This API is deliberately separate from JDBC and Durable acceptance. It makes
no claim that parallel encoding improves confirmed throughput or p99 latency
on a given host; those must be measured on the complete persistence path.
