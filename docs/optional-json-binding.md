# Optional native JSON support

The default `:configured` JSONEachRow encoder delegates to the caller's JSON
library. It must load and encode even when that library does not provide the
experimental native writer binding. The native backend is an explicit opt-in;
its private binding is resolved only when entering a selected native writer.
Missing support fails closed before calling the writer factory or operation.

The old static `binding` reference failed during namespace compilation with
ordinary data.json, before backend selection. Native-only tests could not catch
this because their dependency already supplied the missing Var. This was a
dependency-matrix gap, not a Durable state-machine or WAL-format change.

## Regression checks

Run these in separate Jolt processes. For local workspace checks, prefix the
commands with `tools/jolt-with-chez-10.4.1` and select the qualified runtime.

```sh
jolt -Srepro -M:json-optional-binding-test --qualified
jolt -Srepro -Sdeps '{:deps {org.clojure/data.json {:git/url "https://github.com/casselc/data.json.git" :git/sha "97298fd8a67a6d4ee3eb1346d5e184beb9565b90"}}}' -M:json-optional-binding-test --ordinary
```

The mode guard checks that the selected JSON really has, or lacks, the Var.
The tests cover configured encoding without optional lookup, missing binding
before factory/operation effects, and restoration after success or an exception
when the binding exists. Existing native encoder tests separately exercise real
encoding, custom writer effects, byte parity, bounds and parallelism.

No payload, native database or object-store artifacts are created by this
regression namespace. Passing it is not throughput or persistence qualification.
