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

The encoder alone is separate from JDBC and Durable acceptance. For a Durable
writer connection, `jdbc.chdb.durable.json-rows` provides an opt-in consumer:

```clojure
(require '[jdbc.chdb.durable.json-rows :as durable-rows]
         '[jdbc.chdb.durable :as durable])

(let [row-writer (durable-rows/open-writer connection {:parallelism 4})]
  (try
    ;; Local execution only: no persisted acknowledgement yet.
    (durable-rows/admit-rows!
     row-writer "events" ["id" "message"]
     [{"id" 1 "message" "accepted"}])
    ;; One barrier can cover several admitted batches.
    (durable/flush! connection)
    (finally (durable-rows/close! row-writer))))
```

`insert-rows-and-flush!` is the separately named option when a single batch
must execute and publish as one serialized writer request. It returns a
confirmed or reconciled receipt; `admit-rows!` returns only the local native
execution result. Do not use the latter as an external persistence ACK. The
context rejects competing operations through the entire encode-and-submit
call. Its close stops admission and waits for a synchronous operation before
closing the encoder; the caller retains ownership of the Durable connection.

The consumer validates simple ASCII table/column names, constructs exact SQL,
then uses the existing Durable classifier, policy, statement WAL and writer
queue. It does not use native streaming or change the Durable wire format.
The encoder's UTF-8 byte result does not directly enter the native query path:
the complete SQL string is encoded at the existing native boundary. Therefore
this wiring makes no claim that parallel encoding improves admitted or
confirmed throughput or p99 latency on a given host; measure the complete
persistence path independently.
