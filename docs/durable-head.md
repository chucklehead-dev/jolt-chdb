# Durable V1 head contract

`jdbc.chdb.durable.head` is the runtime-neutral parser and encoder for the
frozen `head.json` document. Its normative source is chDB commit
`66643e5030fb73c30ac5cdd31d4c7858ea040ed0`,
`docs/durable/protocol-v1.mdx#head`. The Python binding's head parser and
seconds-based fixtures at that revision are the cross-binding reference; no
alternate or legacy head shape is accepted.

The codec accepts UTF-8 bytes or a string and returns ordinary Clojure data
with JSON string keys. Keeping string keys is intentional: fields unknown to a
V1 reader survive `decode`, an update to a known field, and `encode` without
being renamed or discarded.

An active lease's `expires_at` is a finite nonnegative Unix timestamp in
seconds. Fractional seconds are preserved as a JSON number. The codec does not
infer or migrate legacy millisecond values from their magnitude.

```clojure
(require '[jdbc.chdb.durable.head :as head])

(let [snapshot (head/decode bytes :read-only)
      renewed (assoc-in snapshot ["lease" "expires_at"] new-expiry)
      body (head/encode renewed)]
  body)
```

`decode` defaults to `:read-only`; `validate!` accepts either `:read-only` or
`:writer`. An unknown reader feature fails both modes. An unknown writer feature
remains readable but fails writer validation, so an older implementation cannot
acquire a lease and rewrite state whose writer requirement it does not
understand. `encode` always requires writer compatibility.

The validator enforces the 1 MiB inclusive limit, BOM-free canonical UTF-8,
JSON-only values and cross-language safe integers, every required known field,
unambiguous object keys (including escaped-equivalent spellings),
active-versus-released lease shape, canonical immutable reference names,
lowercase SHA-256 digests, strict WAL replay order, manifest sequence agreement,
and reference generations no newer than the lease. Failures have stable
`:type` values:

- `:jdbc.chdb.durable.head/corrupt`
- `:jdbc.chdb.durable.head/protocol-unsupported`
- `:jdbc.chdb.durable.head/limit-exceeded`

Passing a mode other than `:read-only` or `:writer` is caller misuse and fails
with `:jdbc.chdb.durable.head/invalid-mode`; it is not a document-validation
result.

Parser exceptions and document values are deliberately absent from error data
and messages because unknown fields may contain credentials.

This slice does not compare chDB release precedence, apply `backup_format` or
`min_reader` compatibility, implement a storage backend, acquire a lease,
restore objects, or mutate `head.json`. Those gates belong after the decoder,
before any engine or backend side effect. No Durable V1 conformance claim is
made.
