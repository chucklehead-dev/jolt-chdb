# Changelog

## Unreleased

- Avoid a Durable writer deadlock when a full operation queue coincides with
  worker failure or close (#219). Callers wait for queue capacity outside the
  admission lock, then recheck the writer lifecycle before enqueueing. Queued
  work and concurrent close retain their existing terminal results; confirmed
  and settled execution acknowledgements are unchanged.

- Keep the fast Durable model gate, but skip exhaustive checking for a
  committed `casselc/data.json` SHA-only repin when every other model input is
  unchanged. Other dependency edits and unavailable model fingerprints still
  select the exhaustive tier (#205).

- Repin the merged `casselc/data.json` map-entry-sequence writer optimization
  at `1b071626`. Keep the production ClickStack fixture's byte and decoded-value
  goldens as gates for the repin, and correct the throughput-metrics exact-pin
  assertion and active-pin documentation that still named the older `e7f97a9`
  revision. Existing throughput measurements remain tied to their original
  dependency and require a fresh full-path comparison.

- Add an encoder-only cross-host comparison for the exact 512-row
  ClickStack/OTel Durable log fixture, with Jolt serial/four-fiber, JVM
  data.json/Cheshire, and Babashka native Cheshire profiles. The fixture is
  shared with the Durable probe; verification requires decoded-value parity
  across hosts and UTF-8 byte parity for the Jolt/JVM data.json profiles.
  This makes no persisted-throughput claim.

- Add a manual, isolated 512-row local throughput selector for the opt-in
  Durable JSONEachRow consumer. It times 100 admission-only batches and one
  separate flush, reports both admission and flush-amortized persisted rates,
  and verifies aggregates from a fresh reader process. It does not alter the
  existing acceptance gate or claim that the target has been met.

- Add an opt-in, caller-owned JSONEachRow Durable consumer. `admit-rows!`
  executes through the existing classified raw writer/WAL path without a
  persistence acknowledgement; `insert-rows-and-flush!` uses one atomic
  execute-and-publish request. A one-operation context covers encoding and
  synchronous submission, drains on close without closing the caller's
  connection, and leaves serial encoding as the default. No native stream,
  Durable wire-format, or default JDBC behavior changes; full-path throughput
  remains to be qualified.

- Reuse one request-owned exact UTF-8 SQL buffer for default Durable JDBC
  mutation classification and prepared execution, including typed bound
  requests. Authorization still precedes native mutation, bound values remain
  outside the classifier and V1 statement WAL, and zero-parameter WAL records
  retain their original SQL bytes. Configured operation overrides keep their
  existing route. A matched local 512-row A/B/A showed a small full-path gain,
  but the encode-inclusive throughput target remains unmet. Read-only query
  execution and Durable persistence semantics are unchanged.

- Add a caller-owned ordered JSONEachRow encoder with explicit four-fiber
  opt-in on Jolt and serial JVM/Babashka fallbacks (#209). It admits one batch
  per context, settles started fibers before releasing ownership, and supports
  repeatable timed close. Jolt/JVM retain their data.json writer bytes;
  Babashka uses native Cheshire with row-order and decoded-value parity, not
  cross-host byte identity. The encoder alone is not JDBC or Durable
  admission; the separate opt-in Durable consumer above uses it without a
  confirmed-throughput claim. Parallel row
  serialization requires CPU-only, nonparking callbacks; the lifecycle tests
  gate workers before serialization rather than parking inside a JSON writer.

- Add redacted, exact-order traces for public JDBC and raw Durable writer
  preflight routes, classifier rejection, and bound-value checkpoint recovery.
  Link those concrete routes to the existing writer abstraction without changing
  Durable behavior or expanding the model state space (#207).

- Pin the merged `casselc/data.json` default writer and map-entry fast paths
  for production-shaped JSONEachRow. A matched local 512-row run (500 samples
  per mode) reduced encoding-inclusive p50 from 107.095 to 60.167 ms and p99
  from 187.387 to 172.959 ms; both still miss the 20.48/25.60 ms targets.
  All ten measured WAL objects per arm had identical SHA-256 hashes across
  arms. Broader data.json value-shape parity remains governed by the fork's
  JVM/Jolt tests; this run does not qualify S3 or collector throughput.

- Expose the existing option-bearing `publish-wal-file!` entry as an additive
  Durable aspect selector. The `publish-wal-bytes!` selector remains intact;
  this adds observation coverage for the writer's sealed-file WAL path without
  changing the runtime publication behavior or wire format.

- Keep Durable model CI exhaustive for dependency or existing-alias edits, but
  skip its exhaustive tier for validated additive `deps.edn` aliases that only
  launch newly added bench/test namespaces. The fast tier still runs for
  model-linked test paths; malformed or unsupported edits fail closed (#196).

- Reuse one request-owned exact UTF-8 SQL buffer across native Durable
  classification and execution for fully materialized writer mutations. The
  sealed-WAL check, WAL preparation, full policy gate, native mutation, and
  append-on-success order remain intact; caller-supplied operation overrides
  retain their existing path. A matched local 10,000-row public
  `execute-and-flush!` run (three warmups, 100 measured, fresh reader) measured
  p99 419.0 ms versus 443.4 ms on the clean control, with 1,030,000-row
  readback parity. This local result does not qualify S3 or collector delivery.
  The Python checkpoint fixture now checks the closed recover-stage startup
  envelope and its retained in-process corruption cause.

- Add a separately gated manual OIDC S3 qualification for 10,000-row public
  `execute-and-flush!`: three warmups, 100 measured confirmations, and a
  fresh-process readback of all 1,030,000 rows with aggregate/fingerprint
  parity. Evidence is bounded and redacted. No AWS run or throughput claim is
  included; live dispatch still requires verified IAM scope, current and
  noncurrent lifecycle expiry, and request/storage/cost bounds.

- Reuse a WAL object's completed size-and-SHA readback when committing the
  matching publication on the same backend and lease token. A private witness
  preserves the public receipt shape; absent or mismatched witnesses still
  require verification before head CAS, and checkpoint verification is
  unchanged. This removes one redundant local WAL readback per confirmed
  publication without changing the durable wire format. This does not qualify
  S3 or collector throughput.

- Wire the bounded buffered-publication literate Quint model into the sampled
  Durable CI tier. Its corrected-run witnesses and five deliberately faulty
  controls now run after extraction and typechecking; the effective-input
  fingerprint distinguishes prose-only edits from changed generated model
  content. This adds no exhaustive check or runtime behavior claim.

- Publish a closed, redacted Durable startup failure envelope for reader and
  writer operational phases. The envelope exposes only
  `::jdbc.chdb.durable/startup-failed` and a closed stage keyword; its original
  cause remains available only in-process. This covers capability, head, lease,
  scratch, native-open, recovery, renewal, and writer-start failures without
  exposing backend paths, owner data, payloads, or exception text (#157).

- Add `execute-and-flush!` at the Durable writer and JDBC extension boundaries.
  It performs one fully materialized mutation and its confirmed/reconciled V1
  publication as a single FIFO worker request, so another shared-writer caller
  cannot be admitted between those halves. Failed or ambiguous publication
  retains the existing recovery obligation. This fixes a caller-composition
  boundary and makes no batching, throughput, provider-delivery, or freshness
  claim.

- Add an opt-in positive Durable writer
  `:checkpoint-wal-reference-threshold`. A crossing `flush!` now uses the
  existing full V1 checkpoint publication/CAS before returning its persistence
  witness, atomically replacing the base and clearing manifest WAL references.
  This bounds `head.json` reference growth without introducing merged WALs,
  manifest indexes, or object garbage collection; it makes no throughput,
  recovery, or provider-qualification claim.

- Make the local Durable throughput selector reject tracked or untracked
  nonignored checkout state before claiming `BENCH_GIT_STATUS=clean`. Ignored
  generated/cache/output paths remain outside that Git-status claim, while
  runtime binary and native-library identities remain explicit inputs. The
  hosted stage-smoke lane now keeps its generated Jolt cache and gitlibs trees
  in a workspace sibling rather than the checkout.

- Add a receipt-first acceptance gate for the Durable encoding-inclusive
  512-row path. `qualification`, `scale-512`, and matched local/AWS 512 runs
  now require exactly 500 p99-qualified samples with p50 at most 20.48 ms and
  p99 at most 25.60 ms; a miss persists its bounded, redacted report and RSS
  evidence before failing. Other batch sizes and the S3 WAL curve remain
  non-gating. No new throughput result is claimed.

- Split the Durable formal CI decision into exhaustive and fast-model-linked
  outputs. Receipt-only throughput/acceptance changes now leave both model
  tiers as explicit successful stubs, while runtime, ITF, and toolchain edits
  retain fast validation and exhaustive inputs still require both tiers. Add
  the previously omitted persistence-observation literate source and generated
  modules to the effective-input fingerprint inventory.

- Add a separately opt-in, OIDC-only AWS S3 matched-batch qualification lane
  for the existing 512, 1,000, 5,000, and 10,000-row Durable selectors. It
  preserves the independent 512-row measured-WAL curve, exact compiler/native
  pin checks, unique prefixes, and bounded redacted artifacts; no AWS run or
  throughput claim is included in this change.

- Repin hosted Durable compiler provenance to merged `casselc/jolt`
  `integration/aspects` commit `57e591d4` (banner
  `jolt v0.8.6-37-g57e591d4`). The installer, cache namespace, guarded
  workflows, compatibility manifest, throughput fixtures, and user-facing
  Durable guidance now share that immutable tuple. The base driver floor
  remains Jolt 0.8.6; this enables no new behavior by itself.

- Repair the Durable throughput-metrics provenance contract and its paired
  documentation to name the actual checked-in `casselc/data.json` revision
  `e7f97a9`. The contract now rejects the immediately prior `97298fd` pin as
  well as the earlier reader pin; this is qualification metadata only and does
  not alter the benchmark workload, measurements, or Durable behavior.

- Replace the Durable WAL fallback's `OutputStreamWriter` encoding with
  managed `StringWriter` JSON followed by one UTF-8 conversion. The fallback
  is qualified against fixed, independently specified JSONL bytes plus a
  deterministic generated corpus, so stock Jolt 0.8.10 can execute writer and
  recovery paths without relying on the broken ranged-append overload. An
  optional native encoder is selected only after the same broad behavioral
  corpus matches the fallback. Byte limits, native-execute-before-append
  ordering, spool lifecycle, immutable publication, and recovery semantics
  are unchanged; this makes no throughput claim.

- Stage pending Durable WAL segments in private, scalar-accounted files below
  the recovered engine scratch directory. Sealed publication retries fence all
  new mutations before preparation or native execution, while read-only SQL
  remains available; successful head commits clear spool state before
  best-effort local deletion. The literate Quint spool lifecycle and its
  mutants now run in the Durable formal CI boundary. This does not claim local
  crash durability, fsync semantics, or S3/provider qualification.

- Attribute the existing scalar WAL prepare and WAL append observations as
  separate, causally non-overlapping categories of opt-in `stage-512` Durable
  admission timing. The enclosing admission stopwatch and explicit residual
  remain unchanged; this benchmark-only diagnostic changes no writer queue or
  persistence behavior and makes no throughput claim.

- Make the Typed Clojure CI bootstrap deterministic: install the immutable
  Clojure CLI 1.12.4.1582 archive from its checksum-attested release artifact
  with bounded download retries, then assert the installed CLI provenance
  before preserving the existing positive and mutation-control check.

- Avoid rebuilding zero-parameter SQL when every `?` is contained in a quoted
  literal, quoted identifier, or comment. Executable unbound placeholders
  retain the established validation error, while accepted literal-only SQL
  keeps the caller's exact text.

- Add internal file-WAL publication and private spool-preparation primitives
  for a later bounded writer transition. The production Durable writer retains
  its current in-memory WAL behavior; this phase neither changes its lifecycle
  nor exposes a spool API.

- Repin the owned `casselc/data.json` provider to `e7f97a9`, including its
  Jolt-specialized codepoint-decoder access and String-backed reader ordinary
  run improvements. In a current-runtime, six-fresh-slot recovery comparison,
  the two measured `a672451` slots averaged 16,028.26 rows/s and the two
  measured `e7f97a9` slots averaged 17,250.68 rows/s on the same 52,224-row,
  three-segment fixture; the remaining slots were prime controls. This is a
  directional `e7f97a9`-versus-`a672451` recovery result only, not a direct
  `97298fd` comparison or a p99, Rust-relative, S3, admission, or
  general-throughput qualification.

- Correct the pinned macOS chDB 26.7.3 archive filename to `libchdb.so` in the
  installer and native qualification. Both public macOS archives retain that
  filename, rather than using the conventional `.dylib` suffix; macOS Durable
  setup and hosted qualification can therefore reach the verified library.

- Canonicalize absolute native Durable backup and restore archive paths before
  crossing the ABI. This makes a macOS `/var` scratch alias agree with the
  canonical `/private/var` `backups.allowed_path` configuration while retaining
  native rejection of relative archive paths.

- Repin the Jolt crypto provider to the maintained `casselc/jolt-crypto`
  compatibility revision. Its Darwin selector includes Homebrew's Intel OpenSSL
  path while retaining the required Jolt 0.8.6 provider surface, avoiding the
  system `libcrypto` loader abort during hosted macOS Durable qualification.

- Make native Durable qualification failures stage-aware. The added stage lines
  name only pinned public assets/oracles and phases; they are not a general
  redaction boundary, and curl/compiler/oracle/Jolt-child stderr remains under
  ordinary CI log handling. Pinned public assets and the upstream oracle now
  download to a temporary sibling, verify before an atomic rename, and use
  bounded curl retries, connection timeout, and error output.

- Add macOS Intel and ARM64 native Durable qualification lanes for the existing
  process-lifetime anchor, host-signal, fresh-process, and typed process-exit
  probes. Make the pinned compiler install and native typed-exit timeout
  boundary runner-portable, and include lifecycle code, scripts, fixtures,
  model traces, and documentation in the native workflow trigger set. Hosted
  macOS results remain the required qualification evidence for #103.

- Preserve an ambiguous Durable WAL observation across later mutation admission
  and WAL confirmation; only a validated checkpoint can clear that uncertainty.
  Add a literate, bounded Quint lifecycle projection model for recovered,
  pending, confirmed, ambiguous, public-close, forced-teardown, reader, and
  native/public ownership boundaries. This remains observation-only: it makes
  no delivery, timing, object-store freshness, or native-persistence claim.

- Add a declarative Durable exhaustive-obligation inventory and opt-in timing/
  peak-RSS runner. It pins the existing 13 corrected Apalache invocations
  (24 positive properties) and 20 independent counterexample/ITF controls;
  a lightweight causal validator rejects any removed or altered row. The
  established checker, model bounds, classifier, fast gates, and CI behavior
  remain unchanged. This records a measurement baseline; it does not claim a
  shorter exhaustive runtime or change its timeout margin (#80).

- Qualify the native public-close failure boundary: a failed destructor marks
  that public handle unavailable, is never retried, retains the conservative
  process reference, and cannot permit a second native engine bootstrap.

- Add a separate, fail-closed release-runtime Durable recovery A'/B'/A/B/B/A
  verifier. Receipt-only invocation now reports structural consistency only;
  it cannot claim release provenance. Provenance invocation requires a reviewed
  checked-in profile selected before execution, which pins chDB source,
  runner-script, data.json/provider, native, workload shape, and generator
  identities. The profile must be a clean tracked Git blob at the current
  HEAD/tree, and its path/blob/HEAD/tree identities appear in the manifest and
  each receipt. No production profile or completed qualification receipt is
  committed yet. The generated fixture inventory remains per-run and all six
  receipts must agree. Provenance mode also verifies each actual release
  archive, checksum sidecar, declared `jolt` member, invoked binary, and
  version banner. It establishes release-reference plus archive-integrity
  provenance, not a signed source/build chain, artifact attestation,
  reproducibility, percentile, S3, or general-throughput qualification.
  The normal launcher additionally binds caller-supplied Jolt cache and
  gitlibs seed trees by recursive content/path digest, checks the one resolved
  data.json/provider source and namespace from the gitlibs seed, snapshots
  both under read-only verified inputs, and only then creates fresh writable
  per-condition caches. Every sandboxed Cargo, Jolt, and harness process uses
  an output-contained `TMPDIR`/`TEMP`/`TMP`; ambient dependency and temporary
  paths cannot participate.

- Correct the Durable data.json A/B/B/A summary to compare runtime binary,
  version, source, and revision across conditions while validating each
  dependency-sensitive `-Sdescribe` receipt against its own condition. Separate
  cache paths no longer invalidate a completed comparison; missing or changed
  receipts and actual runtime drift still fail closed.

- Add an opt-in, best-effort scalar-only Durable writer phase observer for WAL
  preparation, append/join, immutable publication/verification, and head CAS.
  Observer failures cannot alter protocol outcomes and events omit SQL,
  payloads, object references, backend identities, and exceptions. This is
  diagnostic attribution only; it makes no throughput claim.

- Partition the opt-in `stage-512` benchmark's enclosing Durable admission and
  flush timers into fixed, privacy-safe operation categories plus an explicit
  residual. The harness records a bounded causal start/finish sequence and
  fails if selected stage durations nest, overlap, or exceed their enclosing
  timer. This benchmark-only diagnostic changes neither Durable
  queue, WAL, lease, publication, receipt, nor redaction behavior and makes no
  new performance claim.

- Preserve the closed stage selector in isolated Durable benchmark worker
  requests, so the real writer child enables its existing scalar WAL/control
  observer for `stage-512` and `stage-smoke`. The worker allowlist still
  excludes provider configuration and execution closures; this corrects
  diagnostic completeness only and makes no throughput claim.

- Add an opt-in `stage-512` Durable benchmark selector for one fresh,
  instrumented 512x100 local preencoded trial. It preserves the separate
  uninstrumented scale selector and its peak-RSS interpretation, redacts raw
  batch samples from retained worker evidence after parent aggregation,
  reports stage totals only, and makes no throughput or tail-latency claim.

- Add a separate, fail-closed Jolt-only Durable recovery A/B/B/A harness for
  future exact `data.json` source comparisons. It fixes the current
  52,224-row/3-segment fixture, requires clean condition checkouts and one
  resolved canonical provider root per condition, and reports only two
  directional observations. It makes no p99, Rust, S3, or general-throughput
  qualification claim.

- Repin the exact hosted Durable compiler provenance to `casselc/jolt`
  `bf8a5dde` (`jolt v0.8.6-599-gbf8a5dde`), including the shared installer,
  guarded workflows, compatibility fixture, and fail-closed provenance
  controls. This is a compiler/provenance update only; it makes no throughput
  claim.

- Correct the Durable throughput pin contract and documentation to name the
  active `casselc/data.json` revision. This restores the fail-closed benchmark
  gate; it does not claim a new throughput result.

- Update the DB and optimized data.json pins together to their merged shared
  time-provider metadata fixes. This prepares the natural dependency graph for
  fresh-process Durable benchmarking without qualification overrides; it does
  not establish native delivery, recovery correctness or throughput results.

- Prepare matched local/AWS Durable benchmark selectors for batch sizes 512,
  1,000, 5,000 and 10,000 using the existing logical workload and schedule.
  Isolate writer and snapshot reader in separate native processes and retain
  persistent child receipts. Label admission separately from persisted flush
  and aggregate readback; this does not qualify typed-value equivalence,
  crash-safe per-admission ACK, tail performance, or BB/JVM native support.

- Include native-lifecycle ITF generator changes in both automatic Durable
  model workflow triggers, and check trigger/input closure with removal controls
  without running a solver. Unchanged exhaustive-input skips are preserved (#123).

- Add same-release 26.7.3 bidirectional native checkpoint plus suffix-WAL
  fixture cases alongside existing WAL-only exchange. Require base-only and
  final typed rows, missing/altered/truncated base and missing/altered WAL
  corruption rejection, and unchanged readonly protocol inventories.
  This does not qualify other releases, platforms, providers or performance.

- Add a serial, pinned Jolt-writer to Python-reader logical WAL fixture gate,
  complementing Python-writer to Jolt-reader coverage. Check exact rows,
  aggregates, unchanged protocol bytes, and corrupt/missing WAL controls;
  this does not qualify checkpoints, other releases or other platforms.

- Skip exhaustive Durable checks for known literate-prose-only edits only when
  exact base/head effective models and checker/tool/corpus inventories match.
  Preserve fast and manual gates; missing/unknown inputs or extraction failures
  select exhaustive checks. No invariants, bounds or mutant checks are removed.

- Clarify checkpoint cleanup error/flush retry semantics and qualify public
  retries with model-native operations and real memory-backend CAS; document
  the lifecycle model's omitted per-checkpoint exception boundary (#113).

- Retain native qualification fixtures on failure, timeout or signal; remove
  them only after successful completion, with bounded typed-child kill grace (#114).

- Preserve the first checkpoint publication or commit failure when archive
  cleanup also fails; standalone cleanup failures remain observable (#113).

- Add ordinary-only `jdbc.chdb/insert-json-rows!` for trusted encoded
  JSONEachRow payloads and ordered validated identifiers. The driver constructs
  the INSERT without lexing row data for SQL placeholders and reuses locked
  query/result ownership rather than streaming insertion. Durable preparation,
  classification and WAL admission remain unchanged. Encoder row-shape,
  numeric and size validation remain caller responsibilities.

- Close the retained native anchor exactly once during orderly process exit,
  after logical application close and before Jolt tears down the host runtime.
  This prevents the post-PASS invalid-memory abort exposed by typed ClickHouse
  export/readback while preserving same-path anchor reuse and Durable scratch
  ownership. A reduced child-process regression and an executable host-exit
  model obligation with a causal skip-close mutant cover the boundary (#111).

- Retain a private native anchor from first successful chDB bootstrap until
  process exit, making the first physical path immutable without crossing the
  upstream last-close/reinitialize boundary. Public same-path handles can close
  and reopen without another engine boot; different paths fail before native
  connect. Default Durable recovery admits one private scratch lifetime per
  process and rejects a later one before backend or lease effects, retaining
  anchor-owned scratch until an external owner cleans it after process exit.
  Canonical path identity collapses lexical and existing-symlink aliases;
  uncertain post-entry bootstrap failures make the lifecycle terminal while
  pre-native and documented null-owner failures remain retryable. Durable
  scratch cleanup uses that same canonical identity so a symlink-spelled
  scratch parent cannot remove the live anchor's data directory. A literate
  Quint model, three generated deterministic ITF implementation replays,
  causal last-close/path-switch/terminal/options mutants, exact Linux
  host-signal address checks, and two-process `:memory:` probe cover the
  policy. macOS signal/lifecycle qualification remains explicit platform work
  rather than an inferred claim.

- Converge the Durable JDBC dependency with Samizdat on the canonical
  `jolt-lang/db` library key at `casselc/db` revision `6db79163`. That revision
  descends from the prior jolt-chdb pin `a5bf25d9`, Samizdat's `d85f391c` pin,
  and current `jolt-lang/db` `a54cc49f`, retaining casselc's driver/lifecycle
  contracts, Samizdat's SQLite/java.sql behavior, and the current Jolt provider
  declaration. Consumers should remove `io.github.casselc/db`; they must not
  exclude either lineage or override an unrelated revision onto the canonical
  key. Jolt 0.8.6 remains the ordinary JDBC floor; the combined Durable fixture
  uses the documented `120643d6` compiler until its required interop fixes are
  available in a release. The exact Samizdat `22be90d` graph now has one
  physical `db.*`/`next.jdbc.*` source root, while a causal fixture retains the
  two historical coordinates and is rejected for two `db/sqlite.clj`
  providers.
  Qualification uses run-scoped compiler and gitlib caches, derives the shared
  namespace inventory from the exact full-SHA provider tree, and documents the
  SHA-preserving database-first rollout order.

- Make stable chDB core 26.7.3 the packaged driver and Durable native floor.
  The installer and qualification oracle pin all four official release-archive
  SHA-256 values plus upstream commit `7d84d719`; ABI and runtime capability
  checks reject 26.7.2 before connection/storage work even when all expected
  symbols appear present. Current Durable heads advertise 26.7.3 as producer
  and minimum reader, while the literate engine-metadata model, generated ITF
  replay, typed contracts, and red/green version controls use the same floor.
  The former rc.2 qualification matrix is retired rather than carried as a
  supported path. Applications must still keep one connection open for their
  complete chDB lifetime pending the lifecycle policy tracked in #103. A new
  focused pseudo-terminal gate confirms bounded streaming-insert lifecycle and
  exact readback without the former `ThreadStatus` diagnostic on the packaged
  Linux x86-64 library; sustained and cross-platform use remain unqualified.

- Refresh the upstream Durable conformance inventory to chDB main `c5ed925d`,
  exact test-suite Git blob `4b72fa84`, SHA-256 `b4294901`, and 50 ordered
  cases. The reviewed 49-case ledger remains historical 47/0/2 evidence; the
  newly added Python local/file namespace URL alias is explicitly outside
  jolt-chdb's ObjectBackend-based API, yielding an honest current 47 mapped,
  0 blocked, and 3 binding-level not-applicable ledger. Exact blob and missing-
  current-case mutants fail closed without claiming full conformance.

- Add a pinned Linux x86_64 Python-writer interoperability fixture. Upstream
  `chdb.durable` at commit `66643e50` and checksum-pinned `chdb-core` 26.7.3
  create, flush, close, and independently read a WAL-only object; Jolt 26.7.3
  then recovers the exact logical `head.json` and referenced WAL bytes and
  reconciles the same aggregate. The lane explicitly excludes Python's local
  provider lock file and an injected unreferenced canary, and uses a test-only
  raw read-only adapter because the Python and Jolt local providers have
  different private ETag representations.

- Map the two remaining pinned secret-bearing conformance cases at public
  Durable writer and immutable-reader boundaries. Secret-bearing mutations stop
  before native execution and WAL admission. Failed secret-bearing reads replace
  arbitrary engine messages, data, and nested causes with one fixed SQL category,
  while successful results, encoded bytes, persisted WAL SQL, and
  telemetry-shaped attribute values remain exact. Typed-placeholder, native AST,
  wrong-secret-flag, storage, cleanup, and non-secret throwable-identity controls
  move the 49-case ledger to 47 mapped, 0 blocked, and 2 binding-level not
  applicable cases.

- Tolerate a manifest-referenced, size-and-SHA-verified zero-byte Durable WAL
  as an empty JSONL sequence while continuing to omit empty references from
  writer flushes. A provenance-pinned fixture, wrong-size and wrong-digest
  controls, and privacy-shaped recovery events cover integrity-before-
  validation and the absence of replay effects. The upstream V1 prose does not
  yet state this boundary explicitly, so the local conformance ledger keeps the
  clarification pending rather than claiming normative resolution.

- Add opt-in, scalar-only Durable recovery phase observations through the
  existing open-operation seam. Fixed labels now separate base/WAL download
  and hashing, LF scanning, record buffering and copying, WAL decoding,
  JSON parsing, bounded replay-plan retention, classification, and native
  replay. The default path performs no instrumentation clock reads; observer
  failures cannot replace a recovery result or throwable, and events retain no
  path, object key, SQL, payload, or exception data. The throughput harness
  aggregates these events with its existing bounded stage report.

- Specialize Durable WAL raw-LF scanning with an explicit byte-array and
  primitive-index source contract while preserving strict UTF-8 validation,
  JSONL framing, error precedence, and verify-before-replay behavior. An
  immutable 52,224-row A/B/B/A qualification measured a 51.13% lower LF-phase
  mean and a 15.37% lower complete-recovery mean with identical inventory and
  aggregates. These two-observation results are descriptive, not percentile or
  general Durable-throughput claims; the emitted-code comparison is preserved
  separately from the source-shape regression control.

- Decode ordinary Durable WAL records through Jolt's native UTF-8 String
  constructor, falling back to the strict `CharsetDecoder` only when decoded
  text contains U+FFFD. The fallback distinguishes a legitimate encoded U+FFFD
  from replacement caused by malformed input, preserving strict rejection and
  existing termination/error/effect ordering without re-encoding every record.
  Exhaustive byte strings through length two plus targeted three/four-byte
  malformed and scalar-boundary cases match the prior decoder exactly. On one
  immutable 52,224-row A/B/B/A recovery qualification, mean open time fell from
  6.131 s to 3.536 s (42.3%) with identical inventory and aggregate results.
  The candidate source base was jolt-chdb `bafd44b`; its mandatory Jolt
  executable was built separately from compiler source `120643d6` and is not
  part of this candidate diff. This bounded comparison is not a percentile,
  allocation, RSS-plateau, or matched Rust qualification.

- Pin `casselc/data.json` to merge `3174868a`, whose String-backed reader
  decodes the eight ordinary JSON escapes from a local cursor without one
  pushback-reader call and one single-character String allocation per escape.
  Unicode, malformed escapes, EOF, generic readers, and observable positions
  retain the prior decoder. On one immutable 6,144-row, 13-record recovery
  pair, open time fell from 4.254 s to 1.464 s and runtime-accounted allocation
  from 569.8 MB to 185.5 MB. On the immutable 52,224-row fixture, a single
  same-fixture direction pass reduced open time from 35.377 s to 12.952 s and
  the JSON parse phase from 26.005 s to 2.374 s; the open-window GC-byte delta
  fell from 4.843 GB to 1.541 GB while maximum RSS rose from 721,204 KiB to
  801,668 KiB. Operation counts, byte inventory, and recovered aggregates
  reconciled exactly. These are bounded direction results; representative
  Rust/Jolt qualification and the 80% target remain open.

- Parse each ordinary Durable recovery WAL record once. Validation now retains
  an exact per-segment replay plan while cumulative record bytes stay within
  48 MiB and the segment stays within 16,384 records, then analyzes and executes
  that plan only after complete validation. Crossing either cap discards the
  partial plan and retains the prior bounded second-pass behavior. This covers
  the 36.788 MiB, 100-record representative WAL without making the protocol's
  128 MiB ceiling an unbounded managed-memory commitment. A one-pair local
  directional check reduced open time from 73.285 s to 36.299 s with identical
  52,224-row reconciliation; the matched multi-trial Rust qualification and its
  80% target remain open.

- Pin `casselc/data.json` to merge `36b19024`, whose direct long-string reader
  scan removes the dominant per-character recovery allocation while retaining
  its existing escape, surrogate, control, and EOF semantics. A source-candidate
  comparison reduced one immutable 6,144-row, 13-record recovery open from
  12.159 s to 3.806 s; a fresh-cache consumer run selected the byte-identical
  merged source and retained the allocation reduction and exact reconciliation,
  but took 9.304 s. These bounded diagnostics do not qualify a stable latency or
  representative percentile; the 80% Rust target remains open. The
  strict-decoder compiler pin remains `120643d6`.

- Avoid an unconditional statement-sized UTF-8 allocation when Durable checks
  the 64 MiB mutation limit. Conservative character-count bounds now settle
  ordinary far-from-limit statements, while the narrow uncertain band retains
  the exact UTF-8 byte count and the same rejection-before-engine ordering.

- Pin hosted Jolt lanes to `casselc/jolt` `120643d6`, whose strict
  `CharsetDecoder` support lets Durable WAL recovery reject malformed UTF-8
  directly from each buffered record. Recovery no longer re-encodes every
  decoded record solely to compare its bytes, while retaining complete
  validation before replay and existing termination, corruption, shape, and
  statement-limit precedence. Durable mode therefore requires that compiler,
  or a later Jolt release containing upstream PR #957; Jolt 0.8.6 remains the
  base driver's declared floor. Reader and writer opens probe the decoder's
  valid and malformed-input behavior before storage or native effects.

- Avoid a duplicate statement-sized UTF-8 allocation during Durable WAL
  recovery when the already-buffered JSON record proves the 64 MiB statement
  bound. Oversized records retain the exact encoded-size fallback and existing
  validation-before-replay failure ordering.

- Pin all hosted Jolt lanes and performance provenance fixtures to the merged
  `casselc/jolt` `OutputStreamWriter` fast path. Each consuming workflow now
  checks the shared install action's exact source revision and version output,
  as well as the selected binary banner, before running its gate.

- Add a manual cross-binding Durable recovery oracle that makes Jolt and a
  pinned `chdb-rust` revision recover one byte-identical immutable local
  fixture with the same native library, aggregate reconciliation, cache
  condition, per-trial provenance, peak RSS, and explicit 80% throughput / 1.25x
  elapsed acceptance ratios. Jolt trials use project-only `-Srepro` resolution
  and run-scoped caches, record the caller-asserted full source SHA separately
  from the banner-confirmed abbreviated revision, executable digest, and
  captured `-Sdescribe`, and fail if repository state changes between any prime
  or measured process.

- Add the non-live manual AWS S3 throughput qualification harness with reusable
  execution-only backend parameterization and separate redacted logical-backend
  and transport-attempt metrics. Fake-transport canaries cover credentials,
  transport metadata, object identity, SQL-shaped bytes, payload-shaped bytes,
  bounded EDN, and captured output. The opt-in OIDC job stops each 512-row
  curve at measured pending WAL targets, separates admission/flush/close/
  recovery provider phases, records sanitized hosted provenance, and uploads
  only bounded scanned artifacts with 14-day retention. A live curve remains
  gated on the exact merged `cf0b6928` runtime baseline and manual review.
   Aggregation estimates reject NaN and infinite admission or flush inputs.
   Flush controls now validate exact committed or bounded ambiguity-reconciled
   logical call shapes instead of rejecting valid reconciliation proof reads.

- Use the supported primitive byte-array equality operation for canonical
  Durable WAL UTF-8 validation, avoiding byte-at-a-time Clojure traversal while
  preserving complete validation before recovery effects and failure ordering.

- Require performance qualification to record the exact Jolt source revision
  and executable digest, and let the Durable native qualification script select
  an explicit `JOLT_BIN` instead of silently depending on ambient `PATH`.

- Add fresh-process Durable scale selectors for 512, 1,000, 5,000, and 10,000
  rows per batch plus staged 512-row recovery diagnostics at 10, 25, and 50 WAL
  records. Reports now retain exact recovery endpoint allocator observations,
  distinguish cumulative WAL growth from maximum input-batch size, and require
  external GNU `time -v` maximum RSS rather than claiming an unsupported
  two-sample plateau oracle. S3 remains a separate qualification slice.

- Validate and replay Durable WAL segments with two bounded streaming passes
  over the private, already size-and-digest-verified scratch file. Recovery now
  retains at most one JSONL record instead of materializing the complete WAL as
  bytes, text, persistent byte vectors, split lines, and decoded SQL, while
  preserving complete-segment validation before the first engine effect.

- Bound Durable scale and qualification harness retention to one row batch,
  including pre-encoded modes, and fold fresh-reader recovery aggregates
  incrementally instead of retaining the complete approximately 50,000-row
  trial workload. Per-batch admission timing boundaries, deterministic indices,
  warmup, pending-WAL checks, one-flush cadence, and recovery reconciliation are
  preserved.

- Make Durable throughput evidence reproducible across the requested 512,
  1,000, 5,000, and 10,000-row batch sweep. The manual harness now has explicit
  `scale` and `qualification` profiles, rejects unknown profile names, requires
  exact clean-worktree provenance for evidence-producing runs, and labels p99
  summaries non-qualifying when fewer than 100 batch observations exist.

- Stream Durable V1 WAL JSON through the generic `data.json` writer directly
  into UTF-8 bytes, avoiding an intermediate complete JSON string while keeping
  statement and segment validation before native mutation and WAL append after
  native success. This path requires the Jolt
  `OutputStreamWriter.append(CharSequence, start, end)` range fix tracked by
  `casselc/jolt#73`; writer construction now rejects affected runtimes before
  acquisition or native work instead of risking malformed replay bytes.
- Raise the supported Jolt floor to 0.8.6 and make every hosted workflow build
  and verify the same immutable revision from the canonical append-only
  `integration/aspects` line. Multi-runtime ABI qualification accepts the exact
  release or a git-described compiler derived from it, while the install action
  separately asserts the selected compiler commit and complete version banner;
  causal cross-file mutants prevent that hosted pin from drifting away from the
  checked compatibility manifest.
  The main CI gate also makes an unsupported WAL byte-writer capability a
  failure, so its functional and Hegel coverage cannot be reported green
  through a skip.

- Preserve the original SQL object when an empty-parameter request contains no
  question mark, avoiding placeholder output construction while retaining the
  lexical scanner whenever a question mark is present.

- Reuse one request-local prepared query for Durable mutation classification
  and native execution, eliminating a second placeholder rewrite without a
  global cache or any change to WAL-before-mutation ordering. Prepared values
  are explicitly ephemeral because typed bindings can contain secrets.

- Add a reproducible production-path Durable JSONEachRow throughput harness
  with explicit 512-row targets of at least 25,000 rows/s at p50 and 20,000
  rows/s at p99, causal instrumentation and exact recovery reconciliation.
  Current measurements are diagnostic baselines, not qualification evidence.

- Update the pinned `data.json` writer to bulk-append unescaped string runs.
  A bounded paired probe cuts the isolated outer-WAL JSON stage by about half
  and improves realistic Durable admission, while remaining diagnostic rather
  than evidence that the throughput targets are met.

- Cover the pinned public writer renewal-loss sequence end to end: tolerate one
  failed heartbeat before expiry, self-fence after the last proved lease
  expires, refuse execute/flush/checkpoint before effects, and preserve a public
  queued read on the opened local handle. A separate literate Quint module,
  deterministic ITF projection, and four causal mutants cover the same
  boundary. The conformance ledger is now 45 mapped, 2 blocked, and 2
  binding-level not applicable.

- Close four pinned Durable public-open conformance gaps with executable
  sorted-key/indented head recovery and a reader/writer matrix for missing,
  wrong-size, and wrong-digest checkpoint and WAL references. Valid controls
  prove restore/replay reachability; faulted writers release their lease and
  cleanup while retaining `corrupt` over injected secondary cleanup errors.
  Together with the merged live-force warning coverage, the 49-case ledger is
  now 44 mapped, 3 blocked, and 2 binding-level not applicable.

- Check public Durable acquisition and recovery-renewal `head.json` bytes
  against an offline Python `Decimal` epoch-seconds fixture, with a CI drift
  guard bound to the canonical protocol pin and digest, plus a precise
  wrong-unit identity-conversion mutant witness. This is a narrow adapter unit
  oracle, not a full upstream Python-writer interoperability claim.

- Bound Durable public TTL, heartbeat, skew, observed clocks, and derived lease
  comparisons to integral cross-runtime-safe milliseconds, while preserving
  bounded fractional epoch seconds on the wire. Direct open and raw head decode
  now reject truncating or extreme-time mutants. Configuration and the initial
  clock sample fail before acquisition; an invalid recovery sample cleans up
  the acquired attempt, while invalid live samples self-fence before subsequent
  data or renewal effects and defer release/cleanup to close. These checks are
  documented as local adapter policy; the independent cross-binding unit oracle
  is recorded separately above.

- Reject malformed Durable producer and minimum-reader releases at fresh
  acquisition, takeover, and checkpoint boundaries using the canonical pinned
  compatibility parser, before conditional publication, verification, or head
  replacement. Diagnostics retain neither the rejected release nor object
  identity.

- Bound Durable head JSON object/array nesting to 64 containers during both
  raw decoding and programmatic encoding, with redacted boundary diagnostics.

- Return one structured, redacted warning after a successful forced takeover of
  a live Durable lease, including reconciled ambiguous success, while keeping
  fresh and expired acquisitions silent and preserving CAS outcomes.

- Match the SHA-pinned upstream Durable V1 engine-version comparator exactly,
  including variable numeric tuples, numeric prerelease ranks, stable fallback
  suffixes, malformed fail-closed inputs, and non-lowering minimum-reader floors.
  A differential Python oracle, causal mutants, and bounded Hegel properties
  guard the compatibility boundary independently of the Quint rank projection.

- Accept RFC JSON whitespace around exactly one Durable V1 head value while
  retaining full-byte size limits and fail-closed duplicate, malformed,
  non-UTF-8, BOM, and multiple-value handling.

- Add an offline, SHA-pinned inventory of all 49 upstream Durable V1 cases, an
  obligation-by-obligation local mapping/blocker record, and a focused drift
  gate. This is Phase 1 qualification evidence, not a full V1 conformance claim.

- Record the running producer version when acquiring an existing Durable head,
  and atomically record that producer plus non-lowering backup-format and
  minimum-reader requirements with each full checkpoint.
- Keep a Durable V1 lease held through equality at
  `expires_at + clock_skew`, allowing normal takeover only after that boundary
  while preserving the current writer's conservative self-fence at expiry.
- Write and compare Durable V1 `lease.expires_at` as epoch seconds, including
  fractional seconds, while keeping the public `*-ms` configuration and
  monotonic retry budgets in milliseconds. Python-shaped fixtures cover both
  interoperability directions and the explicit force-takeover migration from
  legacy Jolt millisecond heads.
- Split Durable formal CI into an always-run fast model-linked tier and a
  diff-gated exhaustive Apalache tier. Preserve the historical
  `literate-model` check as an explicit successful no-op for byte-identical
  model inputs, with conservative base/head classification and tested path
  controls.
- Characterize the canonical libchdb driver ABI on pinned Babashka and JVM
  `babashka.ffi` compatibility units with an owned-thread parameterized-query
  lifecycle smoke, one Jolt-resolved native-library selection, typed exact-copy
  and selected-library negative controls, a checksum-pinned exact hosted
  Corretto compatibility unit, a checksum-pinned dynamic Babashka artifact with
  bounded loader diagnostics, and an explicit Phase 1 retirement boundary for
  the test-only adapter.
- Positively join each Durable reader and writer operation OS thread before a
  public close returns or rethrows. Concurrent and repeated closes retain the
  exact original failure while cleanup and worker exit remain exactly once;
  same-thread joins fail fast instead of deadlocking, and close still waits for
  actual heartbeat-thread exit before release.
- Add an opt-in JVM-only Typed Clojure development check for the runtime-neutral
  Durable ABI and capability contracts, including a real positive consumer and
  two mutation-specific negative controls. Normal Jolt consumers do not resolve
  the checker or include the external annotation sources.
- Propagate each live writer's self-fencing predicate into synchronous nested
  object-backend work without changing the Durable backend ABI. S3 stops before
  a first request and before/after transport backoff, maps writer-originated
  stopping to `lease-fenced`, and still never reissues an uncertain write.
- Advance the target-owned Durable aspect epoch to the retry-aware control
  surface and select its option-bearing terminal arities exactly once.
- Require the generated Durable Quint ITF corpus to cover every legacy action
  and semantic outcome before replay, and record the aggregate counts in a
  machine-readable coverage manifest.
- Add monotonic retry deadlines and capped exponential backoff to Durable
  control and S3 operations. Writers stop retrying when their locally proved
  lease expires; ambiguous writes are never reissued, while delayed
  reconciliation reads remain bounded and preserve `commit-ambiguous`.
- Check ambiguous head-CAS landing as its own refinement transition, model a
  rival lease takeover before reconciliation, and require the stale writer to
  receive `LeaseFenced` without losing the landed recovery reference. Exercise
  the same interleaving through a deterministic runtime barrier test.
- Add validated `writer-dbspec` and `snapshot-dbspec` constructors. Durable
  JDBC maps now reject unknown options, incomplete storage identity, invalid
  timing, and writer-only fields on snapshot readers before opening storage or
  native resources; writer instances default to UUIDv4 while lease generation
  remains the fencing authority.
- Run Durable reader and writer operation loops and writer lease heartbeat on
  owned OS threads, so blocking native or storage calls cannot starve other
  work on Jolt's shared fiber carriers.
- Keep writer heartbeat active through close queue drain and final flush, then
  require its successful termination before lease release and native cleanup.
