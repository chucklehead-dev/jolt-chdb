# Bounded encoder row-source lifetime

The bounded encoder's protected body and unused fallback closure could retain
the original lazy sequence head while native encoding progressed. This can
keep already encoded rows reachable until the entire operation completes.

The encoder now hands the source into the protected body once through a private
cell, clearing that cell before validation/consumption. Allocate the source cell
before admission: failure to allocate it must not leave an admitted operation
without the existing `finally` release. The portable serial loop is a private
helper; its writer-bound callback is constructed only on a branch that needs
it, not on native collector branches.

Admission, validation order, row effects, newline-inclusive budgets, prefixes,
custom serialization, error mapping and `finally` release remain unchanged.
The native writer still determines its own internal retention. Removing this
codec's reference does not release a caller's eager vector or a serializer's
own reference; no constant-memory or GC-time guarantee is added.

With data.json's matching one-time source handoff, a 512-row weak-reachability
probe releases the first row at producer observations128/256/384 through both
the codec and actual exporter owned-output path. Before the codec handoff, the
exporter path retained it despite the standalone encoder fix. Consume-only
and eager-vector controls distinguish collectible and genuinely retained rows.

The regression fixture uses a synthetic consuming native-writer slot to isolate
this codec's reference independently of dependency pins. It verifies512visits,
three release observations and a positive strongly retained control. It is not
a real-native wire or persistence proof. The unchanged parent fails exactly
three release assertions while four other assertions pass. Separate existing
native/byte encoder suites verify actual serialization and budget behavior.

Full Durable performance, recovery and consumer integration require separate
checks with exact dependency/runtime/native pins. No WAL, native admission,
publication, lease, query or persistence-confirmation transition changes.
