# Review scope and response

The initial source review flagged public caller-byte ownership ambiguity. The
final change retains original `ffi/write-array` staging in public byte hashing;
the fast helper is now used only for private statement snapshots and the
digest loop's private read buffer. Follow-up source review accepted this scope.

The exact public runtime context supplied to that review was Jolt v0.8.23,
commit `a0ac71c3f638fe55158bd01b4ecbb0dd3d7545e5`,
`host/chez/scheme-adapter-runtime.ss:931-993`. It defines ordinary, synchronous,
non-collect-safe `u8*` memcpy adapters. The review's temporary excerpt was not
an implementation dependency and is not retained as library source.

GC safety is inherited from that runtime primitive's contract, not proved by
the after-copy GC test. Full byte/readback/guard tests, known-answer SHA256,
copy-failure cleanup and old-stack Durable recovery provide separate concrete
checks. No source-only review is presented as executed tests or a formal proof.

The added CI workflow has read-only contents permission, checksum-pinned
official stock Jolt, an isolated dependency-free working directory and cleanup
of its own temporary archive/executable. It runs the copy tests only, without
chDB, credentials, S3, telemetry or model checking. Local actionlint and the
same isolated test command pass. Full native/consumer CI is a separate gate.
