# Durable V1 backend contract

`jdbc.chdb.durable.backend/ObjectBackend` implements the six-operation storage
seam from the frozen Durable V1 backend contract: `get-bytes`,
`get-with-etag`, `put-file-if-absent!`, `put-bytes-if-absent!`,
`replace-if-match!`, and `download-to-file!`.

Both conditional-create operations and conditional replace may report
`{:status :ambiguous}` when a remote provider cannot prove whether a request
landed. This is not a negative acknowledgement. The control plane reconciles a
create by rereading its unique immutable key and a replace by rereading
`head.json`; only exact operation-specific state proves success.

The protocol source is chDB commit
`db10b548a3e1e21e51c213baf863cb1050963d9c`,
`docs/durable/protocol-v1.mdx#backend-contract`. In particular, conditional
create and replace must be atomic provider operations; a `HEAD` followed by an
unconditional `PUT` is not an implementation. ETags are opaque tokens and are
only compared or returned.

## Operation context

`call-with-operation-context` carries bounded, synchronous operation metadata
through the frozen `ObjectBackend` method signatures. Writers use its
`:stopped?` predicate to expose local lease self-fencing to nested backend work;
the S3 backend checks that predicate before the first request and around retry
backoff. The binding is visible only on the calling thread and is restored when
the call returns or throws, so asynchronous backend implementations must copy
the cancellation signal into work they own before returning.

Nested contexts compose stop predicates monotonically: either the outer or
inner caller may stop the work, and an inner binding cannot weaken an outer
cancellation decision. Other context keys use ordinary inner-over-outer map
merging. This addition does not change any `ObjectBackend` method signature.

`memory-backend` is a runtime-neutral semantic oracle, not advertised durable
storage. It uses one atom CAS for each successful publication, returns owned
byte arrays, advances the ETag on every replacement, and rejects stale tokens
without changing the stored value. Its file operations intentionally
materialize bytes; only advertised providers must stream archives. Unsafe
relative keys and non-byte values fail closed without copying the supplied key
into public error data.

## Bounded formal evidence

The three models under `formal/durable-backend-*.smt2` use the same violation
query: can two writers that observed the same absent or revision-zero head both
succeed? They bound the state to two writers and exactly two ordered attempts;
they abstract provider I/O, crashes, retries, and lease ownership.

Verified through Chiasmus with Z3 before implementation:

- `durable-backend-atomic.smt2`: **UNSAT**; no double-success witness exists in
  the bounded atomic transition model.
- `durable-backend-nonatomic-mutant.smt2`: **SAT** with `initial = 0`, both
  success flags true, and the head incorrectly advancing twice.
- `durable-backend-boundary.smt2`: **SAT**; the valid reachable outcome has the
  first writer succeed, the stale writer fail, and the head advance once.

The executable companion checks reproduce the mutant witness, assert winner
retention, and run a 60-case Hegel trace property against a small ETag/value
model.

## Local filesystem provider

`jdbc.chdb.durable.local-posix/local-backend` implements all six backend
operations for a private, single-host filesystem root. It is intended for one
machine with any number of cooperating Jolt processes, not for NFS, SMB, or a
directory modified by software that ignores the provider lock.

The state transition is serialized by a kernel `flock` on `.jchdb.lock`.
Objects and their opaque generation ETag occupy one versioned binary envelope,
so a reader cannot observe a new value with an old token. A successful write
is:

1. atomically create a same-directory staging file with mode `0600`;
2. write the complete envelope and `fsync` the staging descriptor;
3. rename it over the object while still holding the provider lock; and
4. `fsync` the containing directory before reporting success.

New object directories are created with mode `0700` and synced before an object
depends on them. A newly created provider root is also mode `0700`; callers that
supply an existing root are responsible for keeping it private. Keys are
validated as safe relative paths, and existing symbolic links or non-directory
entries in the object hierarchy fail closed. Errors never disclose a key or
root path. Root creation and validation necessarily precede creation of the
lock file; the parent hierarchy is therefore part of the caller-owned trust
boundary.

The object/path/read/write/move layer is deliberately the same
`java.nio.file.Files` surface used by `babashka.fs`. A focused probe and the
shared-provider suite run unchanged on Jolt 0.8.3, JVM Clojure, and Babashka.
Only process locking, atomic private creation, and file/directory durability
barriers are host adapters. The current production adapter is Jolt POSIX;
JVM/Babashka adapters can use `FileChannel` without cloning the CAS or envelope
implementation.

Jolt's `StandardCopyOption/ATOMIC_MOVE` currently reaches its same-filesystem
rename implementation rather than checking the option independently. This
provider therefore creates staging files in the destination directory and
advertises only filesystems qualified by the process-race/crash suite. The
shared provider isolates bulk transfer behind the same host edge as locking and
durability; native POSIX I/O is therefore an adapter, not a second semantic
path.

`put-file-if-absent!` and `download-to-file!` route payload transfer through the
host adapter. Jolt POSIX reuses one 64 KiB native buffer and advances its native
address across partial `write(2)` results, avoiding the proportional managed
allocation observed through the Java-shaped stream bridge. A deterministic
adapter probe injects `EINTR` and repeated short writes and proves that the
unwritten suffix is emitted exactly once. Upload uses the same atomic
conditional publication as byte-array create. Download never overwrites its
local path and removes a partial file after a failed copy or sync. Per the
frozen protocol, the caller supplies a unique scratch path, verifies the
returned file against the manifest size and SHA-256, and only then atomically
publishes it at the final scratch location. The backend cannot do that content
verification itself because the expected digest belongs to the manifest/state
machine layer rather than the object-store operation.

The manual `:durable-file-allocation` alias measures
`jolt.host/bytes-allocated + jolt.host/gc-bytes` around isolated operations; it
is evidence rather than a GC-sensitive CI gate. Before changing the transfer
edge, the target was that a 32 MiB upload allocate less than 2 MiB more than a
1 MiB upload. The Java-shaped loop missed badly: 2,320,768 and 68,660,416 bytes.
The final POSIX adapter measured 187,712 and 264,976 bytes, a 77,264-byte
growth. Downloads measured 146,512 and 278,560 bytes, a 132,048-byte growth.
The source sizes were exactly 1,048,576 and 33,554,432 bytes on Linux x86-64;
these figures are a recorded sample, not universal performance promises.

That boundary also leaves a clean comparison point for a portable
`babashka.fs`-shaped implementation, JVM `FileChannel`, and Jolt native I/O.
Candidate adapters must pass the same byte-exact and failure-cleanup contract;
allocation, throughput, and scheduler impact can then be compared without
forking the CAS/envelope implementation.

Current executable evidence on Linux ext2/ext3-family storage proves:

- two independent processes replacing from one ETag produce exactly one
  winner and retain its bytes;
- killing a process while it owns the lock releases the kernel lock and leaves
  the last committed envelope readable;
- lock, object-directory, and object modes are private; and
- successful publication leaves no staging file; and
- a multi-buffer file upload/download round trip is byte-exact, reports the
  exact payload count, and creates the download at mode `0600`.

The public `jdbc.chdb.durable/open-writer!` and `open-reader!` APIs now compose
this storage protocol with native recovery, serialized SQL, lease renewal,
checkpointing, and cleanup. The POSIX provider is therefore the first usable
single-host Durable backend. The remote S3-compatible provider is implemented
separately and tested against semantic and loopback transports, pinned MinIO,
and live AWS S3 through a protected OIDC role. See
[Durable storage](durable.md) for the current provider matrix.
