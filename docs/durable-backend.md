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
model. Cross-process filesystem races remain mandatory before a local backend
can be advertised.
