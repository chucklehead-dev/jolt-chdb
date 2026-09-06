# Durable V1 backend contract

`jdbc.chdb.durable.backend/ObjectBackend` begins the six-operation storage seam
from the frozen Durable V1 backend contract with the four byte-oriented
operations needed to prove conditional publication:
`get-bytes`, `get-with-etag`, `put-bytes-if-absent!`, and
`replace-if-match!`. File upload and download remain part of the subsequent
local-filesystem provider slice.

The protocol source is chDB commit
`db10b548a3e1e21e51c213baf863cb1050963d9c`,
`docs/durable/protocol-v1.mdx#backend-contract`. In particular, conditional
create and replace must be atomic provider operations; a `HEAD` followed by an
unconditional `PUT` is not an implementation. ETags are opaque tokens and are
only compared or returned.

`memory-backend` is a runtime-neutral semantic oracle, not advertised durable
storage. It uses one atom CAS for each successful publication, returns owned
byte arrays, advances the ETag on every replacement, and rejects stale tokens
without changing the stored value. Unsafe relative keys and non-byte values
fail closed without copying the supplied key into public error data.

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

`jdbc.chdb.durable.local-posix/local-backend` implements the four byte-oriented
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
advertises only filesystems qualified by the process-race/crash suite. Direct
FFI bulk I/O remains a benchmark candidate, not a separate semantic path.

Current executable evidence on Linux ext2/ext3-family storage proves:

- two independent processes replacing from one ETag produce exactly one
  winner and retain its bytes;
- killing a process while it owns the lock releases the kernel lock and leaves
  the last committed envelope readable;
- lock, object-directory, and object modes are private; and
- successful publication leaves no staging file.

This is not yet the complete six-operation Durable backend: streaming file
upload/download and real object-storage provider validation remain separate
slices. The object provider will be tested against a pinned local
S3-compatible binary or container selected by capability probes, not against
the in-memory oracle.
