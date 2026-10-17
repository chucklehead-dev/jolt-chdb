//! A fixture-specific projection, NOT a general OTel SDK/exporter.
//! Every timed batch projects and encodes again, then confirms its own ticket.
use chdb_rust::durable::{Namespace, OpenOptions, Tuning};
use chdb_rust::format::OutputFormat;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use serde_json::{json, Value};
use std::{
    env, fs,
    io::Write,
    path::PathBuf,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
struct Text<T: std::fmt::Display>(T);
impl<T: std::fmt::Display> Serialize for Text<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        // serde_json streams Display through its escaping adapter rather than
        // first allocating a temporary String for each numeric attribute.
        serializer.collect_str(&self.0)
    }
}
const BASE: [&str; 24] = [
    "Timestamp",
    "TraceId",
    "SpanId",
    "ParentSpanId",
    "TraceState",
    "SpanName",
    "SpanKind",
    "ServiceName",
    "ResourceAttributes",
    "ScopeName",
    "ScopeVersion",
    "SpanAttributes",
    "Duration",
    "StatusCode",
    "StatusMessage",
    "Events.Timestamp",
    "Events.Name",
    "Events.Attributes",
    "Links.TraceId",
    "Links.SpanId",
    "Links.TraceState",
    "Links.Attributes",
    "EventsJSON",
    "LinksJSON",
];

struct StringAttrs<'a>(&'a Value);
impl Serialize for StringAttrs<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let attrs = self.0.as_object().expect("validated attribute map");
        let mut map = serializer.serialize_map(Some(attrs.len()))?;
        for (key, value) in attrs {
            if let Some(text) = value.as_str() {
                map.serialize_entry(key, text)?;
            } else {
                map.serialize_entry(key, &Text(value))?;
            }
        }
        map.end()
    }
}

struct CompactRow<'a> {
    span: &'a Value,
    fields: &'a [Value],
}
impl Serialize for CompactRow<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let s = self.span;
        let mut row = serializer.serialize_seq(Some(BASE.len() + self.fields.len() * 2))?;
        // Ordinary-query JSON input interprets an integer as Unix seconds;
        // Jolt's typed stream path accepts raw ticks. ISO preserves all nine
        // fractional digits without forcing a lossy f64 seconds conversion.
        let nanos = s["start-time-unix-nano"].as_u64().unwrap();
        let timestamp = chrono::DateTime::from_timestamp(
            (nanos / 1_000_000_000) as i64,
            (nanos % 1_000_000_000) as u32,
        )
        .unwrap();
        row.serialize_element(&Text(timestamp.format("%Y-%m-%dT%H:%M:%S%.9fZ")))?;
        row.serialize_element(&s["span-context"]["trace-id"])?;
        row.serialize_element(&s["span-context"]["span-id"])?;
        row.serialize_element("")?; // Fixture has no parent or trace state.
        row.serialize_element("")?;
        row.serialize_element(&s["name"])?;
        row.serialize_element("Internal")?;
        row.serialize_element(&s["resource"]["attributes"]["service.name"])?;
        row.serialize_element(&StringAttrs(&s["resource"]["attributes"]))?;
        row.serialize_element(&s["scope"]["name"])?;
        row.serialize_element(&s["scope"]["version"])?;
        row.serialize_element(&StringAttrs(&s["attributes"]))?;
        row.serialize_element(
            &(s["end-time-unix-nano"].as_u64().unwrap()
                - s["start-time-unix-nano"].as_u64().unwrap()),
        )?;
        row.serialize_element("Unset")?;
        row.serialize_element("")?;
        for _ in 0..7 {
            row.serialize_element(&[] as &[u8])?;
        }
        row.serialize_element("[]")?;
        row.serialize_element("[]")?;
        for field in self.fields {
            row.serialize_element(&s["attributes"][field["key"].as_str().unwrap()])?;
            row.serialize_element(&3u8)?;
        }
        row.end()
    }
}

fn validate(f: &Value) -> Result<()> {
    assert_eq!(f["fixture-version"], 1);
    assert_eq!(f["scope"], "wide16-fixture-only-not-general-sdk");
    let cols = f["columns"].as_array().ok_or("columns missing")?;
    let fields = f["fields"].as_array().ok_or("fields missing")?;
    assert_eq!(fields.len(), 2);
    assert_eq!(cols.len(), BASE.len() + fields.len() * 2);
    for (i, name) in BASE.iter().enumerate() {
        assert_eq!(cols[i], *name);
    }
    for (i, field) in fields.iter().enumerate() {
        assert_eq!(cols[24 + i * 2], field["physical"]["value-column"]);
        assert_eq!(cols[25 + i * 2], field["physical"]["status-column"]);
    }
    let spans = f["spans"].as_array().ok_or("spans missing")?;
    assert!(!spans.is_empty() && spans.len() <= 10000);
    for s in spans {
        assert_eq!(s["kind"], "internal");
        assert_eq!(s["status"], json!({"code":"unset"}));
        assert_eq!(s["events"], json!([]));
        assert_eq!(s["links"], json!([]));
        assert_eq!(s["span-context"].as_object().unwrap().len(), 2);
        assert!(s["span-context"]["trace-id"].is_string());
        assert!(s["span-context"]["span-id"].is_string());
        assert!(
            s["name"].is_string()
                && s["scope"]["name"].is_string()
                && s["scope"]["version"].is_string()
        );
        assert!(s["resource"]["attributes"]["service.name"].is_string());
        assert!(s["resource"]["attributes"].is_object() && s["attributes"].is_object());
        assert!(s["resource"]["attributes"]
            .as_object()
            .unwrap()
            .values()
            .all(Value::is_string));
        assert!(s["attributes"]
            .as_object()
            .unwrap()
            .values()
            .all(|v| v.is_string() || v.is_boolean() || v.as_i64().is_some()));
        assert!(
            s["end-time-unix-nano"].as_u64().unwrap()
                >= s["start-time-unix-nano"].as_u64().unwrap()
        );
        for field in fields {
            let value = &s["attributes"][field["key"].as_str().unwrap()];
            match field["type"].as_str().unwrap() {
                "boolean" => assert!(value.is_boolean()),
                "int64" => assert!(value.as_i64().is_some()),
                _ => return Err("unsupported typed fixture field".into()),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Value {
        let fields = json!([
            {"key":"count", "type":"int64", "physical":{"value-column":"c", "status-column":"cs"}},
            {"key":"flag", "type":"boolean", "physical":{"value-column":"f", "status-column":"fs"}}
        ]);
        let mut columns: Vec<Value> = BASE.iter().map(|s| json!(s)).collect();
        columns.extend([json!("c"), json!("cs"), json!("f"), json!("fs")]);
        json!({"fixture-version":1, "scope":"wide16-fixture-only-not-general-sdk",
        "fields":fields, "columns":columns, "spans":[{
            "name":"sample", "kind":"internal", "status":{"code":"unset"},
            "start-time-unix-nano":1700000000123456789u64,
            "end-time-unix-nano":1700000000123457789u64,
            "span-context":{"trace-id":"trace", "span-id":"span"},
            "resource":{"attributes":{"service.name":"test"}},
            "scope":{"name":"scope", "version":"1"},
            "attributes":{"count":42, "flag":false, "text":"quote\"/\\\n漢字😀"},
            "events":[], "links":[]
        }]})
    }

    #[test]
    fn borrowing_projection_preserves_physical_values_and_shapes() {
        let f = fixture();
        validate(&f).unwrap();
        let bytes = encode(&f).unwrap();
        let lines: Vec<&[u8]> = bytes.split(|b| *b == b'\n').collect();
        assert_eq!(lines.len(), 3);
        let row: Value = serde_json::from_slice(lines[1]).unwrap();
        assert_eq!(row.as_array().unwrap().len(), 28);
        assert_eq!(row[0], "2023-11-14T22:13:20.123456789Z");
        assert_eq!(row[8], json!({"service.name":"test"}));
        assert_eq!(row[11]["flag"], "false");
        assert_eq!(row[11]["count"], "42");
        assert_eq!(row[11]["text"], f["spans"][0]["attributes"]["text"]);
        assert_eq!(row[12], 1000);
        for i in 15..22 {
            assert_eq!(row[i], json!([]));
        }
        assert_eq!(row[22], "[]");
        assert_eq!(row[23], "[]");
        assert_eq!(row[24], 42);
        assert_eq!(row[25], 3);
        assert_eq!(row[26], false);
        assert_eq!(row[27], 3);
    }

    #[test]
    fn each_operation_reprojects_without_cached_encoded_data() {
        let mut f = fixture();
        let before = encode(&f).unwrap();
        f["spans"][0]["attributes"]["count"] = json!(99);
        assert_ne!(before, encode(&f).unwrap());
    }

    #[test]
    #[should_panic]
    fn rejects_nonempty_events_instead_of_silently_dropping_them() {
        let mut f = fixture();
        f["spans"][0]["events"] = json!([{"name":"event"}]);
        validate(&f).unwrap();
    }

    #[test]
    #[should_panic]
    fn rejects_incompatible_typed_layout() {
        let mut f = fixture();
        f["columns"][24] = json!("another-column");
        validate(&f).unwrap();
    }

    #[test]
    fn count_accepts_both_json_integer_policies_and_rejects_other_shapes() {
        assert_eq!(row_count(&json!(512)).unwrap(), 512);
        assert_eq!(row_count(&json!("512")).unwrap(), 512);
        assert!(row_count(&json!(false)).is_err());
        assert!(row_count(&json!(-1)).is_err());
        assert!(row_count(&json!("not-count")).is_err());
    }
}

fn encode(f: &Value) -> Result<Vec<u8>> {
    let cols = f["columns"].as_array().unwrap();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"INSERT INTO otel_traces (");
    for (i, col) in cols.iter().enumerate() {
        if i != 0 {
            bytes.extend_from_slice(b", ");
        }
        write!(bytes, "`{}`", col.as_str().unwrap())?;
    }
    bytes.extend_from_slice(b") FORMAT JSONCompactEachRow\n");
    for span in f["spans"].as_array().unwrap() {
        serde_json::to_writer(
            &mut bytes,
            &CompactRow {
                span,
                fields: f["fields"].as_array().unwrap(),
            },
        )?;
        bytes.push(b'\n');
    }
    Ok(bytes)
}

fn row_count(value: &Value) -> Result<u64> {
    if let Some(n) = value.as_u64() {
        Ok(n)
    } else {
        Ok(value
            .as_str()
            .ok_or("count must be an unsigned integer or decimal string")?
            .parse()?)
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 5 {
        return Err("usage: FIXTURE_JSON EMPTY_ROOT SAMPLES REPORT_JSON".into());
    }
    let fixture: Value = serde_json::from_slice(&fs::read(&args[1])?)?;
    validate(&fixture)?;
    let root = PathBuf::from(&args[2]);
    if root.exists() && fs::read_dir(&root)?.next().is_some() {
        return Err("root is not empty".into());
    }
    let samples: usize = args[3].parse()?;
    assert!((1..=256).contains(&samples));
    fs::create_dir_all(root.join("scratch-rust"))?;
    let namespace = Namespace::new(root.join("objects").to_str().ok_or("non UTF8 root")?)?
        .with_owner("typed-compact-prefix");
    let (object, existed) = namespace.open(
        "typed-compact-prefix",
        OpenOptions {
            database: Some("otel".into()),
            scratch_root: Some(root.join("scratch-rust")),
            tuning: Tuning {
                lease_ttl: Duration::from_secs(900),
                heartbeat_interval: Duration::from_secs(300),
                ..Tuning::default()
            },
            ..OpenOptions::default()
        },
    )?;
    assert!(!existed);
    let engine = object.query("SELECT version() AS engine", OutputFormat::JSONEachRow)?;
    let engine: Value = serde_json::from_slice(&engine)?;
    assert_eq!(engine["engine"], "26.9.2.1");
    let settings = String::from_utf8(object.query(
        "SELECT name, value FROM system.settings WHERE name IN ('max_threads', 'max_insert_threads', 'max_parsing_threads', 'input_format_parallel_parsing') ORDER BY name",
        OutputFormat::JSONEachRow)?)?;
    object.execute(fixture["ddl"].as_str().ok_or("DDL missing")?)?;
    object.flush()?;
    object.checkpoint()?;
    let batch = fixture["spans"].as_array().unwrap().len();
    let mut observations = Vec::new();
    let mut payload_bytes = None;
    for i in 0..(samples + 3) {
        let start = Instant::now();
        let bytes = encode(&fixture)?; // Every batch re-projects and encodes.
        let encoded = start.elapsed().as_nanos();
        let bytes_len = bytes.len();
        assert_eq!(*payload_bytes.get_or_insert(bytes_len), bytes_len);
        let sql = std::str::from_utf8(&bytes)?;
        let ticket = object.execute(sql)?;
        let executed = start.elapsed().as_nanos();
        object.flush_through(ticket)?; // Barrier per batch, never flush once per trial.
        let confirmed = start.elapsed().as_nanos();
        if i >= 3 {
            observations.push(json!({"index":i - 3, "encode_ns":encoded,
                "execute_ns":executed - encoded, "flush_ns":confirmed - executed,
                "confirmed_ns":confirmed}));
            fs::write(
                &args[4],
                serde_json::to_vec(&json!({
                    "scope":"fixture-specific-rust-projection-confirmed-per-batch-local-posix",
                    "tail_qualified":false, "gate_status":"pending", "batch_size":batch,
                    "independent_recovery_required":true, "timestamp_wire":"iso-utc",
                    "native_settings_json_each_row":settings,
                    "warmups":3, "payload_bytes":payload_bytes, "samples":observations,
                    "native_engine":"26.9.2.1", "chdb_rust":"e685da930ecf02198c0b9e72a412cbc25842deb1"
                }))?,
            )?;
        }
    }
    let count = object.query(
        "SELECT count() AS n FROM otel_traces",
        OutputFormat::JSONEachRow,
    )?;
    let count: Value = serde_json::from_slice(&count)?;
    assert_eq!(row_count(&count["n"])?, (batch * (samples + 3)) as u64);
    object.close()?;
    // Outer runner marks the independent stock-Jolt recovery result separately.
    let mut report: Value = serde_json::from_slice(&fs::read(&args[4])?)?;
    report["gate_status"] = "writer-count-and-close-passed".into();
    fs::write(&args[4], serde_json::to_vec(&report)?)?;
    println!(
        "writer confirmed {} rows; independent recovery still required",
        batch * (samples + 3)
    );
    Ok(())
}
