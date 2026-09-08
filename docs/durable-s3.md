# Durable S3-compatible backend

`jdbc.chdb.durable.s3/s3-backend` implements the Durable `ObjectBackend`
semantics over a streaming HTTP transport. On Jolt, omitting `:request!` loads
the default `jdbc.chdb.durable.s3-curl` transport, which uses libcurl's native
SigV4 support. It is a namespace
backend: pass it with `:namespace-backend` and a separate `:object-id`, or scope
it explicitly with `jdbc.chdb.durable.backend/object-backend`.

Compiled Jolt applications should call
`jdbc.chdb.durable.s3-curl/s3-backend` directly. That wrapper supplies the same
transport but also gives the AOT reachability analysis an explicit namespace
edge; a dynamic default transport lookup alone is insufficient evidence that a
standalone executable contains the FFI bindings.

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

The default transport requires libcurl 7.75 or newer. It configures
`CURLOPT_AWS_SIGV4`, supplies access and secret keys through libcurl's private
credential option, signs an optional `x-amz-security-token`, retains only the
response ETag, and never includes credential-bearing curl messages in public
exceptions. `:connect-timeout-ms` defaults to 10000, `:timeout-ms` to 300000,
and bounded byte responses default to 128 MiB via `:max-response-bytes`.
Checkpoint file transfers are streamed without payload-sized buffering.

The checked-in semantic transport test is deliberately adversarial: it checks
conditional headers, opaque ETags, retry bounds, credential redaction,
streaming descriptors, sibling scoping, timeout-after-object-create
reconciliation, and timeout-after-head-CAS reconciliation.

```sh
jolt -M:durable-s3-test
bash test/durable-s3-curl.sh jolt
bash test/durable-s3-minio.sh jolt
```

The loopback transport gate verifies real libcurl signing, upload and download
callbacks, conditional headers, opaque ETags, response bodies, and failed-file
cleanup, including a truncated response. The MinIO gate is pinned to image
digest `sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
(`RELEASE.2025-09-07T16-13-09Z`) and checks real service authentication,
concurrent conditional creators, stale ETags, lease contention, verified WAL
commit, and streaming file transfer. This slice does **not** yet claim complete
production S3 qualification: real AWS conformance, injected real-transport
timeout boundaries, corruption cases, and large-checkpoint memory evidence
remain. The service principal needs object read and conditional write permission
for the configured bucket/prefix; listing and deletion are not part of Durable
V1.

## AWS OIDC qualification

The manual `durable-aws-qualification` workflow runs the same live-provider
checks against a pre-provisioned AWS bucket. Configure the GitHub environment
`aws-durable-ci` with these non-secret environment variables:

- `AWS_DURABLE_ROLE_ARN`
- `AWS_DURABLE_BUCKET`
- `AWS_DURABLE_REGION`

The workflow assumes the role with GitHub OIDC and maps the resulting access
key, secret key, and mandatory session token into the transport. It neither
creates nor deletes the bucket. Each attempt writes beneath the unique prefix
`ci/jolt-chdb/<run-id>-<run-attempt>` so retries cannot inherit a prior head.
It is safe to share the bucket with other CI consumers when each role is scoped
to a distinct top-level prefix. For this workflow the object permission can be
limited to:

```text
arn:aws:s3:::BUCKET/ci/jolt-chdb/*
```

Only `s3:GetObject` and `s3:PutObject` are required. `s3:ListBucket`,
`s3:DeleteObject`, and `s3:CreateBucket` are not required by the conformance
run. Configure a bucket lifecycle rule for the shared `ci/` subtree rather
than granting test jobs destructive cleanup permission.
