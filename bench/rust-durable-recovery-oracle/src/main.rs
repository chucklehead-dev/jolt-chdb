use chdb_rust::durable::{Namespace, OpenOptions};
use chdb_rust::format::OutputFormat;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const CHDB_RUST_SHA: &str = "e685da930ecf02198c0b9e72a412cbc25842deb1";
const CHDB_RUST_VERSION: &str = "1.4.0";
const BASE_NANOS: u64 = 1_700_000_000_000_000_000;
const DDL: &str = "CREATE TABLE otel_logs (\
     Timestamp DateTime64(9), TraceId String, SpanId String, TraceFlags UInt8,\
     SeverityText String, SeverityNumber UInt8, ServiceName String, Body String,\
     ResourceSchemaUrl String, ResourceAttributes Map(String, String),\
     ScopeSchemaUrl String, ScopeName String, ScopeVersion String,\
     ScopeAttributes Map(String, String), LogAttributes Map(String, String),\
     EventName String\
   ) ENGINE=MergeTree ORDER BY (toStartOfFiveMinutes(Timestamp), ServiceName, Timestamp)";
const AGGREGATE_SQL: &str = "SELECT count() n, sum(TraceFlags) flags, \
     sum(SeverityNumber) severity_sum, sum(length(Body)) body_bytes, \
     countIf(position(Body, '?') > 0) question_bodies, \
     min(TraceId) min_trace, max(TraceId) max_trace, \
     min(SpanId) min_span, max(SpanId) max_span FROM otel_logs";

type AnyResult<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct FixtureConfig {
    object_id: String,
    database: String,
    batch_size: usize,
    warmup_batches: usize,
    measured_batches: usize,
    total_rows: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct InventoryEntry {
    key: String,
    kind: String,
    bytes: Option<u64>,
    sha256: Option<String>,
    target: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct FileIdentity {
    file_name: String,
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PatchIdentity {
    file_name: String,
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct UntrackedIdentity {
    path: String,
    bytes: u64,
    mode: String,
    sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct HarnessState {
    schema_version: u32,
    head: String,
    parent: String,
    tree: String,
    status: String,
    tracked_patch: PatchIdentity,
    untracked_files: Vec<UntrackedIdentity>,
    state_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RuntimeProvenance {
    runtime: String,
    chdb_rust_git_sha: String,
    chdb_rust_crate_version: String,
    oracle_crate_version: String,
    rustc_version: String,
    cargo_version: String,
    engine_source: String,
    native_version: String,
    harness_state: HarnessState,
    native_library: FileIdentity,
    native_header: FileIdentity,
    executable: FileIdentity,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ObjectReference {
    key: String,
    size: u64,
    sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct FixtureManifest {
    db: String,
    base: Option<ObjectReference>,
    wal: Vec<ObjectReference>,
    seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ExpectedAggregate {
    n: String,
    flags: String,
    severity_sum: String,
    body_bytes: String,
    question_bodies: String,
    min_trace: String,
    max_trace: String,
    min_span: String,
    max_span: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureDescriptor {
    schema_version: u32,
    run_id: String,
    producer: RuntimeProvenance,
    config: FixtureConfig,
    expected: ExpectedAggregate,
    manifest: FixtureManifest,
    inventory: Vec<InventoryEntry>,
    inventory_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RunConfig {
    object_id: String,
    database: String,
    trials: usize,
    batch_size: usize,
    warmup_batches: usize,
    measured_batches: usize,
    total_rows: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ScheduleEntry {
    ordinal: usize,
    phase: String,
    runtime: String,
    trial: Option<usize>,
    report_file: String,
    time_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RunManifest {
    schema_version: u32,
    config: RunConfig,
    harness_state: HarnessState,
    schedule: Vec<ScheduleEntry>,
    run_id: String,
}

fn fail(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn validate_object_id(value: &str) -> AnyResult<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
    {
        return Err(fail("object id must be one safe path component").into());
    }
    Ok(())
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn sha256_file(path: &Path) -> AnyResult<String> {
    Ok(sha256_bytes(&fs::read(path)?))
}

fn collect_inventory(root: &Path) -> AnyResult<Vec<InventoryEntry>> {
    fn visit(base: &Path, path: &Path, out: &mut Vec<InventoryEntry>) -> AnyResult<()> {
        let metadata = fs::symlink_metadata(path)?;
        let key = path
            .strip_prefix(base)?
            .to_string_lossy()
            .replace('\\', "/");
        if metadata.file_type().is_symlink() {
            out.push(InventoryEntry {
                key,
                kind: "symlink".into(),
                bytes: None,
                sha256: None,
                target: Some(fs::read_link(path)?.to_string_lossy().into_owned()),
            });
        } else if metadata.is_dir() {
            let mut children = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
            children.sort_by_key(|entry| entry.file_name());
            for child in children {
                visit(base, &child.path(), out)?;
            }
        } else if metadata.is_file() {
            out.push(InventoryEntry {
                key,
                kind: "file".into(),
                bytes: Some(metadata.len()),
                sha256: Some(sha256_file(path)?),
                target: None,
            });
        } else {
            return Err(fail(format!("unsupported fixture entry: {key}")).into());
        }
        Ok(())
    }

    let mut entries = Vec::new();
    visit(root, root, &mut entries)?;
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(entries)
}

fn inventory_digest(entries: &[InventoryEntry]) -> AnyResult<String> {
    Ok(sha256_bytes(&serde_json::to_vec(entries)?))
}

fn log_row(index: u64) -> Value {
    let timestamp = BASE_NANOS + index * 1_000_000;
    let error = index % 20 == 0;
    json!({
        "Timestamp": format!("{}.{:09}", timestamp / 1_000_000_000, timestamp % 1_000_000_000),
        "TraceId": format!("{index:032x}", index = index + 1),
        "SpanId": format!("{span:016x}", span = 1_000_000 + index),
        "TraceFlags": index % 2,
        "SeverityText": if error { "ERROR" } else { "INFO" },
        "SeverityNumber": if error { 17 } else { 9 },
        "ServiceName": "oscope.benchmark",
        "Body": format!(
            "request completed route=/api/items/{} status={}",
            index % 64,
            if error { 500 } else { 200 }
        ),
        "ResourceSchemaUrl": "https://opentelemetry.io/schemas/1.27.0",
        "ResourceAttributes": {
            "service.name": "oscope.benchmark",
            "deployment.environment.name": "benchmark"
        },
        "ScopeSchemaUrl": "",
        "ScopeName": "oscope.benchmark",
        "ScopeVersion": "1.0",
        "ScopeAttributes": {"library.language": "clojure"},
        "LogAttributes": {
            "http.request.method": "GET",
            "http.response.status_code": if error { "500" } else { "200" },
            "benchmark.bucket": (index % 16).to_string()
        },
        "EventName": "benchmark.request"
    })
}

fn batch_sql(start: u64, rows: usize) -> AnyResult<String> {
    let mut sql = String::from("INSERT INTO otel_logs FORMAT JSONEachRow\n");
    for offset in 0..rows as u64 {
        sql.push_str(&serde_json::to_string(&log_row(start + offset))?);
        sql.push('\n');
    }
    Ok(sql)
}

fn query_aggregate(object: &chdb_rust::durable::DurableObject) -> AnyResult<ExpectedAggregate> {
    let bytes = object.query(AGGREGATE_SQL, OutputFormat::JSONEachRow)?;
    let text = std::str::from_utf8(&bytes)?.trim();
    if text.lines().count() != 1 {
        return Err(fail("aggregate reconciliation did not return exactly one row").into());
    }
    let row: Value = serde_json::from_str(text)?;
    let fields = row
        .as_object()
        .ok_or_else(|| fail("aggregate reconciliation row is not an object"))?;
    let normalized: serde_json::Map<String, Value> = fields
        .iter()
        .map(|(key, value)| {
            let text = value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string());
            (key.clone(), Value::String(text))
        })
        .collect();
    Ok(serde_json::from_value(Value::Object(normalized))?)
}

fn object_reference(reference: &chdb_rust::durable::ObjectRef) -> ObjectReference {
    ObjectReference {
        key: reference.key.clone(),
        size: reference.size,
        sha256: reference.sha256.clone(),
    }
}

fn fixture_manifest(object: &chdb_rust::durable::DurableObject) -> FixtureManifest {
    let manifest = object.manifest();
    FixtureManifest {
        db: manifest.db.clone(),
        base: manifest.base.as_ref().map(object_reference),
        wal: manifest.wal.iter().map(object_reference).collect(),
        seq: manifest.seq,
    }
}

fn required_env(name: &str) -> AnyResult<String> {
    env::var(name)
        .map_err(|_| fail(format!("required environment variable {name} is missing")).into())
}

fn file_identity(path: &Path) -> AnyResult<FileIdentity> {
    Ok(FileIdentity {
        file_name: path
            .file_name()
            .ok_or_else(|| fail("provenance path has no file name"))?
            .to_string_lossy()
            .into_owned(),
        bytes: fs::metadata(path)?.len(),
        sha256: sha256_file(path)?,
    })
}

fn load_harness_state() -> AnyResult<HarnessState> {
    let path = required_env("BENCH_HARNESS_STATE_FILE")?;
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn provenance() -> AnyResult<RuntimeProvenance> {
    let executable = env::current_exe()?;
    let native = PathBuf::from(required_env("BENCH_NATIVE_LIBRARY")?);
    let header = PathBuf::from(required_env("BENCH_NATIVE_HEADER")?);
    Ok(RuntimeProvenance {
        runtime: "rust".into(),
        chdb_rust_git_sha: CHDB_RUST_SHA.into(),
        chdb_rust_crate_version: CHDB_RUST_VERSION.into(),
        oracle_crate_version: env!("CARGO_PKG_VERSION").into(),
        rustc_version: required_env("BENCH_RUSTC_VERSION")?,
        cargo_version: required_env("BENCH_CARGO_VERSION")?,
        engine_source: chdb_rust::version::ENGINE_SOURCE.into(),
        native_version: chdb_rust::version::engine_version()?.to_string(),
        harness_state: load_harness_state()?,
        native_library: file_identity(&native)?,
        native_header: file_identity(&header)?,
        executable: file_identity(&executable)?,
    })
}

fn epoch_millis() -> AnyResult<u128> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
}

fn write_json(path: &Path, value: &impl Serialize) -> AnyResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| fail("output needs a parent directory"))?;
    fs::create_dir_all(parent)?;
    let mut output = fs::File::create(path)?;
    serde_json::to_writer_pretty(&mut output, value)?;
    output.write_all(b"\n")?;
    Ok(())
}

fn load_run_manifest(path: &str) -> AnyResult<RunManifest> {
    let manifest: RunManifest = serde_json::from_slice(&fs::read(path)?)?;
    if manifest.schema_version != 1 || manifest.harness_state.schema_version != 1 {
        return Err(fail("run manifest schema version is unsupported").into());
    }
    let calculated = manifest.config.batch_size as u64
        * (manifest.config.warmup_batches + manifest.config.measured_batches) as u64;
    if manifest.config.total_rows != calculated {
        return Err(fail("run manifest total rows does not match its workload").into());
    }
    let mut expected = Vec::new();
    let mut add = |phase: &str, runtime: &str, trial: Option<usize>, label: String| {
        let ordinal = expected.len();
        expected.push(ScheduleEntry {
            ordinal,
            phase: phase.into(),
            runtime: runtime.into(),
            trial,
            report_file: format!("{runtime}-{label}.json"),
            time_file: format!("{runtime}-{label}.time"),
        });
    };
    add("prime", "rust", None, "prime".into());
    add("prime", "jolt", None, "prime".into());
    for trial in 1..=manifest.config.trials {
        let runtimes = if trial % 2 == 1 {
            ["rust", "jolt"]
        } else {
            ["jolt", "rust"]
        };
        for runtime in runtimes {
            add("measured", runtime, Some(trial), format!("trial-{trial}"));
        }
    }
    if manifest.schedule != expected {
        return Err(fail("run schedule is not the exact required prime/alternating order").into());
    }
    Ok(manifest)
}

fn prepare(args: &[String]) -> AnyResult<()> {
    if args.len() != 5 {
        return Err(fail("prepare ROOT OBJECT_ID RUN_MANIFEST_JSON DESCRIPTOR_JSON").into());
    }
    let root = PathBuf::from(&args[1]);
    let object_id = &args[2];
    validate_object_id(object_id)?;
    let run_manifest = load_run_manifest(&args[3])?;
    let descriptor_path = PathBuf::from(&args[4]);
    if run_manifest.config.object_id != *object_id {
        return Err(fail("run manifest object id does not match").into());
    }
    let batch_size = run_manifest.config.batch_size;
    let warmup_batches = run_manifest.config.warmup_batches;
    let measured_batches = run_manifest.config.measured_batches;
    if root.exists() && fs::read_dir(&root)?.next().is_some() {
        return Err(fail("fixture root must be absent or empty").into());
    }
    fs::create_dir_all(&root)?;

    let namespace = Namespace::new(
        root.to_str()
            .ok_or_else(|| fail("fixture root is not UTF-8"))?,
    )?
    .with_owner("cross-binding-recovery-oracle");
    let (object, existed) = namespace.open(
        object_id,
        OpenOptions {
            database: Some(run_manifest.config.database.clone()),
            ..OpenOptions::default()
        },
    )?;
    if existed {
        return Err(fail("fixture object unexpectedly existed").into());
    }
    object.execute(DDL)?;
    object.flush()?;
    let mut next = 0u64;
    for _ in 0..warmup_batches {
        object.execute(&batch_sql(next, batch_size)?)?;
        next += batch_size as u64;
    }
    object.flush()?;
    for _ in 0..measured_batches {
        object.execute(&batch_sql(next, batch_size)?)?;
        next += batch_size as u64;
    }
    if next != run_manifest.config.total_rows {
        return Err(fail("produced row count differs from run manifest").into());
    }
    object.flush()?;
    let expected = query_aggregate(&object)?;
    let manifest = fixture_manifest(&object);
    object.close()?;

    let object_root = root.join(object_id);
    let inventory = collect_inventory(&object_root)?;
    let descriptor = FixtureDescriptor {
        schema_version: 1,
        run_id: run_manifest.run_id,
        producer: provenance()?,
        config: FixtureConfig {
            object_id: object_id.clone(),
            database: run_manifest.config.database,
            batch_size,
            warmup_batches,
            measured_batches,
            total_rows: next,
        },
        expected,
        manifest,
        inventory_sha256: inventory_digest(&inventory)?,
        inventory,
    };
    write_json(&descriptor_path, &descriptor)?;
    println!(
        "{}",
        serde_json::to_string(&json!({
        "status": "ok",
        "phase": "prepared",
        "descriptor": descriptor_path.file_name().map(|name| name.to_string_lossy().into_owned()),
            "total_rows": next,
            "inventory_sha256": descriptor.inventory_sha256
        }))?
    );
    Ok(())
}

fn recover(args: &[String]) -> AnyResult<()> {
    let process_started_epoch_ms = epoch_millis()?;
    if args.len() != 7 {
        return Err(fail(
            "recover ROOT OBJECT_ID DESCRIPTOR_JSON RUN_MANIFEST_JSON ORDINAL OUTPUT_JSON",
        )
        .into());
    }
    let root = PathBuf::from(&args[1]);
    let object_id = &args[2];
    validate_object_id(object_id)?;
    let descriptor: FixtureDescriptor = serde_json::from_slice(&fs::read(&args[3])?)?;
    let run_manifest = load_run_manifest(&args[4])?;
    if descriptor.schema_version != 1 {
        return Err(fail("fixture descriptor schema version is unsupported").into());
    }
    let ordinal = args[5]
        .parse::<usize>()
        .map_err(|_| fail("ordinal must be a non-negative integer"))?;
    let output = PathBuf::from(&args[6]);
    let schedule = run_manifest
        .schedule
        .get(ordinal)
        .ok_or_else(|| fail("schedule ordinal is out of range"))?;
    if schedule.ordinal != ordinal
        || schedule.runtime != "rust"
        || output.file_name().and_then(|name| name.to_str()) != Some(schedule.report_file.as_str())
    {
        return Err(fail("recovery invocation differs from the run schedule").into());
    }
    if descriptor.run_id != run_manifest.run_id
        || descriptor.config.object_id != run_manifest.config.object_id
    {
        return Err(fail("fixture descriptor differs from the run manifest").into());
    }
    if descriptor.config.object_id != *object_id {
        return Err(fail("descriptor object id does not match requested object").into());
    }
    let object_root = root.join(object_id);
    let before = collect_inventory(&object_root)?;
    let before_sha = inventory_digest(&before)?;
    if before != descriptor.inventory || before_sha != descriptor.inventory_sha256 {
        return Err(fail("fixture inventory differs from its descriptor before recovery").into());
    }

    let namespace = Namespace::new(
        root.to_str()
            .ok_or_else(|| fail("fixture root is not UTF-8"))?,
    )?;
    let start = Instant::now();
    let (object, existed) = namespace.open(
        object_id,
        OpenOptions {
            read_only: true,
            existing_only: true,
            ..OpenOptions::default()
        },
    )?;
    if !existed || !object.read_only() {
        return Err(fail("recovery did not open an existing read-only object").into());
    }
    let actual = query_aggregate(&object)?;
    if actual != descriptor.expected {
        return Err(fail(format!(
            "recovery aggregate mismatch: expected {:?}, got {:?}",
            descriptor.expected, actual
        ))
        .into());
    }
    object.close()?;
    let elapsed_ns = start.elapsed().as_nanos() as u64;

    let after = collect_inventory(&object_root)?;
    let after_sha = inventory_digest(&after)?;
    if before != after || before_sha != after_sha {
        return Err(fail("read-only recovery changed fixture bytes or links").into());
    }
    let rows = descriptor.config.total_rows;
    let process_finished_epoch_ms = epoch_millis()?;
    let report = json!({
        "schema_version": 1,
        "run_id": &run_manifest.run_id,
        "schedule_ordinal": ordinal,
        "phase": &schedule.phase,
        "runtime": provenance()?,
        "trial": schedule.trial,
        "process_id": std::process::id(),
        "process_started_epoch_ms": process_started_epoch_ms,
        "process_finished_epoch_ms": process_finished_epoch_ms,
        "cache_condition": "warm-provider-cache-fresh-process-engine-and-scratch",
        "fixture": {
            "inventory_sha256": before_sha,
            "batch_size": descriptor.config.batch_size,
            "warmup_batches": descriptor.config.warmup_batches,
            "measured_batches": descriptor.config.measured_batches,
            "wal_segments": descriptor.manifest.wal.len(),
            "recovered_rows": rows
        },
        "recovery": {
            "elapsed_ns": elapsed_ns,
            "rows_per_second": rows as f64 * 1_000_000_000.0 / elapsed_ns as f64,
            "expected": descriptor.expected,
            "actual": actual,
            "inventory_unchanged": true
        }
    });
    write_json(&output, &report)?;
    println!(
        "{}",
        serde_json::to_string(&json!({
            "status": "ok", "runtime": "rust", "trial": schedule.trial,
            "elapsed_ns": elapsed_ns, "recovered_rows": rows
        }))?
    );
    Ok(())
}

fn run() -> AnyResult<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        Some("prepare") => prepare(&args),
        Some("recover") => recover(&args),
        _ => Err(fail("expected prepare or recover subcommand").into()),
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("rust recovery oracle failed: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_run_config_rejects_unknown_field() {
        let value = json!({"object_id":"o","database":"d","trials":1,"batch_size":1,
            "warmup_batches":1,"measured_batches":1,"total_rows":2,"unexpected":true});
        assert!(serde_json::from_value::<RunConfig>(value).is_err());
    }

    #[test]
    fn strict_fixture_descriptor_rejects_unknown_field_before_recovery() {
        let value = json!({"schema_version":1,"unexpected":true});
        let error = serde_json::from_value::<FixtureDescriptor>(value).unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }
}
