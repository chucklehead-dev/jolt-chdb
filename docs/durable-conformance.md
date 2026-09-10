# Durable V1 conformance inventory

This repository targets Durable V1 with bounded local evidence. It does not yet
claim complete V1 conformance. The Phase 1 inventory pins upstream chDB commit
`66643e5030fb73c30ac5cdd31d4c7858ea040ed0`, the normative protocol at
`docs/durable/protocol-v1.mdx`, and all 49 ordered cases from
`tests/test_durable.py`.

The machine-readable source inventory is
`resources/jdbc/chdb/durable_conformance_inventory.edn`. Its separate mapper,
`resources/jdbc/chdb/durable_conformance_mapping.edn`, gives every discovered
case exactly one disposition: one or more existing local test anchors, an
explicit not-applicable rationale, or a blocker tracked by issue 47. A mapped
anchor means the named local assertion exists; it is not a claim that every
Python fixture, provider, or lifecycle detail has become language-neutral.

Run the focused, offline gate with the mandatory compiler selector:

```sh
/home/chuck/ai-src/tools/jolt-with-chez-10.4.1 \
  jolt -M:durable-conformance-inventory-test
```

The gate fails on changes to the pinned repository SHA, protocol/suite paths,
source digests, case count or ordered names; duplicate or unmapped entries;
invalid dispositions; and mapped paths or assertion anchors that no longer
exist. It also proves five deliberate drift mutants fail. For an externally
visible red control, set `JOLT_CHDB_CONFORMANCE_MUTANT=name-drift`; success from
that invocation is a gate defect.

This inventory intentionally leaves the remaining issue-47 obligations open:
independent Python-shaped fixture exchange, the earlier-archive/new-engine and
header/library cross-version matrices, current-source AWS OIDC qualification,
release evidence across claimed platforms/providers/runtimes, protocol
clarification and refinement-model work, the upstream renewal-failure fencing
sequence including read survival, and final review of the eventual full matrix.
Stable chDB 26.7.0 still lacks the Durable ABI, so the pinned rc.2 ABI
qualification is not an ordinary-install conformance claim.
