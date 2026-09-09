# Typed Clojure pilot

This is a bounded, opt-in development check for jolt-chdb's runtime-neutral
ABI data. It runs only on JVM Clojure with
`org.typedclojure/typed.clj.checker` 1.3.0:

```bash
clojure -M:typed-check
```

The checker, `script`, and `typed` paths exist only behind `:typed-check`.
Ordinary consumers and every Jolt alias therefore neither load nor resolve the
checker. This pilot does not add Babashka or JVM native bindings, does not load
`jdbc.chdb.native` on the JVM, and does not claim to check pointer ownership or
generated `jolt.ffi` declarations.

## Checked and trusted boundary

`typed/jdbc/chdb/typed/abi.clj` gives closed identities to the production
`jdbc.chdb.abi` function, contract, and type lookups, plus typed descriptor,
query-analysis, enum, source-provenance, and Durable capability shapes.
`validate-descriptor!` has the one deliberate `t/Any` input: it is a trusted
refinement seam whose actual job is to accept untrusted parsed EDN and either
return a `Descriptor` or throw. The released-Jolt ABI test verifies that
boundary with independent descriptor mutations, as well as layout, lookups,
typed exceptions, and real stock-library symbol negotiation. The ordinary
JDBC/Hegel property suite remains authoritative for runtime behavior.

`jdbc.chdb.typed.driver` is the real positive consumer. At JVM namespace load
it reads the production EDN resource through `jdbc.chdb.abi`, and the checker
verifies its Durable summary, stable-library unsupported result, and
status-to-state transition. The unsupported value is a shaped sample; this JVM
pilot does not call libchdb. The runner also executes that consumer and asserts
the V1 identity, exact 25-function full registry, exact eight-function Durable
registry and capability symbol set, and `:unavailable` result. Any ABI registry
change must update the external union and these exact controls in the same
commit. Merely loading the annotations is not accepted.

## Non-vacuous negatives

The runner asserts the exact two-name registry and count, then requires each
control outside its checker exception handler. A missing namespace, reader or
load failure, or checker/analyzer crash is therefore a hard failure. Only one
structured Typed Clojure type error containing every mutation-specific evidence
fragment counts as an expected rejection.

- `malformed-abi-descriptor-lookup` passes an unregistered function keyword to
  the closed `FunctionId` lookup contract.
- `durable-result-state-mismatch` assigns a known unsupported Durable result to
  the disjoint supported-result shape.

If either external signature is removed or widened enough to admit its mutant,
the corresponding negative turns green and the runner fails.

## Scope decision

Expand only when another runtime-neutral namespace JVM-loads and a distinct
mutant proves the added claim. Constrain the pilot if useful shapes require
pervasive `t/Any` or trusted declarations. Remove it if maintenance or hosted-CI
cost outweighs mutation-detected defects. Native FFI portability remains a
separate project.

## Version and cost record

One validated local run used checker 1.3.0, Clojure 1.12.5, Corretto JDK 25.0.2,
and Linux x86-64. With dependencies cached, it took 15.61 seconds and peaked at
496460 KiB RSS. The hosted job has a ten-minute timeout; record its actual
billed duration from the first CI run. Released Jolt 0.8.3 remains the
production runtime and is validated separately by `jolt -M:abi-test` and
`jolt -M:test`.
