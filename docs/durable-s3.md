# Durable S3-compatible backend

`jdbc.chdb.durable.s3/s3-backend` implements the Durable `ObjectBackend`
semantics over a streaming, SigV4-capable HTTP transport. It is a namespace
backend: pass it with `:namespace-backend` and a separate `:object-id`, or scope
it explicitly with `jdbc.chdb.durable.backend/object-backend`.

The backend emits path-style URLs of the form:

```text
<endpoint>/<bucket>/<optional-prefix>/<object-id>/<protocol-key>
```

Every path component is UTF-8 percent encoded. The endpoint must be HTTP(S)
without embedded credentials, query, or fragment. Credentials are passed only
to the transport in the private `:auth` field; they are never included in an
object key, exception, trace, WAL record, or `head.json`.

Conditional operations use provider-native request preconditions:

- immutable creates send `If-None-Match: *`;
- mutable `head.json` replacement sends the exact opaque ETag in `If-Match`;
- HTTP 412 maps to `:precondition-failed`, never to an emulated HEAD/PUT race;
- S3's conditional-write HTTP 409 response is retried within the same bound;
- an uncertain write outcome maps to `:ambiguous` so the control plane can
  reread and prove the exact object or manifest transition.

Authentication, permission, throttling, transport, provider-response, and
invalid-response failures have separate sanitized exception categories. Reads
and writes that are proved not sent may retry up to `:max-attempts` (default
three, hard maximum eight). Exhausted uncertain writes return `:ambiguous`.

The transport request contract uses `{:bytes ... :byte-count n}` for bounded
control/WAL bodies and `{:file path :byte-count n}` for streaming checkpoint
uploads. Downloads use `{:response-body {:file path :create-new? true}}`; the
transport must atomically create the caller's unique destination, stream into
it without payload-sized buffering, remove a partial file on failure, and
return the exact `:byte-count`.

The checked-in semantic transport test is deliberately adversarial: it checks
conditional headers, opaque ETags, retry bounds, credential redaction,
streaming descriptors, sibling scoping, timeout-after-object-create
reconciliation, and timeout-after-head-CAS reconciliation.

```sh
/home/chuck/ai-src/tools/jolt-with-chez-10.4.1 \
  jolt -M:durable-s3-test
```

This slice does **not** yet claim a production S3 backend: the default libcurl
SigV4 transport and required real AWS/MinIO conformance runs remain to be
implemented. The eventual service principal needs object read and conditional
write permission for the configured bucket/prefix; listing and deletion are
not part of Durable V1.
