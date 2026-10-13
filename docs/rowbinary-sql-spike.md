# Experimental RowBinary SQL replay probe

This is a small Jolt-only experiment, not a new exporter backend. It tests
whether binary rows can travel through ordinary ASCII SQL and the existing
Durable V1 statement WAL. It does not change the protocol or the library.

The statement uses `INSERT ... SELECT * FROM format('RowBinary', '<schema>',
base64Decode('<data>'))`. Base64 expands the payload and the engine decodes it;
this is not zero-copy. The native query path is used, not compact JSON streams.
Normal query classification, writer admission, WAL verification/publication,
head CAS and snapshot recovery still apply.

Run `bench/jdbc/chdb_rowbinary_sql_probe.clj writer <fresh-root>` with the
qualified Jolt/chDB environment. Then run the same file with `reader <root>`
in a separate process. Use a fresh temporary directory for each experiment.
The writer creates two records and checks committed/WAL-confirmed settlement.
The reader requires referenced WAL objects and checks all physical values.
Each role emits only a small receipt and log; generated `objects` and scratch
directories may be retired after successful recovery and process completion.

The initial native26.9.0 run passed with two rows and two referenced WALs.
Coverage includes signed/unsigned64-bit boundaries, Bool, DateTime64(9), empty
and Unicode/NUL text, arrays, maps, Float64 and nullable integers. UInt64 and
Float64 physical types are checked. Numeric comparison uses canonical SQL
text so JSON number inference is not the oracle.

Limits: fixed1024-byte buffer, short strings/collections with one-byte VarUInt
lengths, fixed schema, two records and local POSIX storage. No performance,
S3, concurrency, malformed-data or arbitrary-schema qualification. JSONWriter
customization is not a binary-transport contract. A future explicit encoder
must enforce ranges, schema and byte budgets, validate UTF-8/length boundaries,
preserve descriptor fencing and ordered row conversion, and demonstrate full
writer/recovery performance rather than just smaller payloads.
