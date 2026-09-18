#!/usr/bin/env python3
"""Fail-closed summary for the Jolt-only Durable recovery A/B/B/A experiment.

This deliberately reports two directional observations per condition.  It is
not a tail-latency, Rust-comparison, S3, or general throughput qualification.
"""
import hashlib
import json
import pathlib
import re
import sys

SHA256 = re.compile(r"[0-9a-f]{64}")
GIT_SHA = re.compile(r"[0-9a-f]{40}")
EXPECTED = {"n", "flags", "severity_sum", "body_bytes", "question_bodies",
            "min_trace", "max_trace", "min_span", "max_span"}
CACHE = "warm-provider-cache-fresh-process-engine-and-scratch"

def fail(message):
    raise SystemExit("data.json recovery A/B/B/A summary failed: " + message)

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()

def digest(value):
    return hashlib.sha256(value).hexdigest()

def load(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read {path.name}: {error}")

def exact(label, value, keys):
    if not isinstance(value, dict) or set(value) != set(keys):
        fail(f"{label} has missing or unknown fields")
    return value

def sha(label, value, regex=SHA256):
    if not isinstance(value, str) or not regex.fullmatch(value):
        fail(f"{label} is not an exact SHA")

def positive(label, value):
    if not isinstance(value, int) or value <= 0:
        fail(f"{label} is not positive")

def identity(label, value):
    exact(label, value, {"file_name", "bytes", "sha256"})
    if not isinstance(value["file_name"], str) or not value["file_name"]:
        fail(f"{label} file name is missing")
    positive(f"{label} bytes", value["bytes"])
    sha(f"{label} digest", value["sha256"])

def schedule():
    # Priming is outside the reported corpus.  The measured order avoids a
    # one-sided warm-up/order story while retaining fresh process boundaries.
    return [
        {"ordinal": 0, "phase": "prime", "condition": "A", "observation": None,
         "report_file": "A-prime.json", "time_file": "A-prime.time"},
        {"ordinal": 1, "phase": "prime", "condition": "B", "observation": None,
         "report_file": "B-prime.json", "time_file": "B-prime.time"},
        {"ordinal": 2, "phase": "measured", "condition": "A", "observation": 1,
         "report_file": "A-1.json", "time_file": "A-1.time"},
        {"ordinal": 3, "phase": "measured", "condition": "B", "observation": 1,
         "report_file": "B-1.json", "time_file": "B-1.time"},
        {"ordinal": 4, "phase": "measured", "condition": "B", "observation": 2,
         "report_file": "B-2.json", "time_file": "B-2.time"},
        {"ordinal": 5, "phase": "measured", "condition": "A", "observation": 2,
         "report_file": "A-2.json", "time_file": "A-2.time"},
    ]

def validate_state(label, value):
    exact(label, value, {"head", "tree", "state_sha256"})
    sha(label + " head", value["head"], GIT_SHA)
    sha(label + " tree", value["tree"], GIT_SHA)
    sha(label + " identity", value["state_sha256"])

def validate_condition(label, value, condition):
    exact(label, value, {"condition", "checkout", "data_json", "jolt", "native",
                         "cache_scope", "harness"})
    if value["condition"] != condition:
        fail(f"{label} condition differs")
    exact(label + " checkout", value["checkout"], {"root", "head", "tree"})
    if not isinstance(value["checkout"]["root"], str) or not value["checkout"]["root"]:
        fail(f"{label} checkout root is missing")
    sha(label + " checkout head", value["checkout"]["head"], GIT_SHA)
    sha(label + " checkout tree", value["checkout"]["tree"], GIT_SHA)
    exact(label + " data.json", value["data_json"], {"sha", "root", "namespace"})
    sha(label + " data.json SHA", value["data_json"]["sha"], GIT_SHA)
    if not isinstance(value["data_json"]["root"], str) or not value["data_json"]["root"]:
        fail(f"{label} data.json root is missing")
    identity(label + " data.json namespace", value["data_json"]["namespace"])
    exact(label + " Jolt", value["jolt"], {"version", "source_sha", "executable", "executable_revision", "describe"})
    if not isinstance(value["jolt"]["version"], str) or not value["jolt"]["version"].startswith("jolt v"):
        fail(f"{label} Jolt banner is invalid")
    sha(label + " Jolt source SHA", value["jolt"]["source_sha"], GIT_SHA)
    identity(label + " Jolt executable", value["jolt"]["executable"])
    if (not isinstance(value["jolt"]["executable_revision"], str) or
        not re.fullmatch(r"[0-9a-f]{8,40}", value["jolt"]["executable_revision"]) or
        not value["jolt"]["source_sha"].startswith(value["jolt"]["executable_revision"]) or
        not value["jolt"]["version"].endswith("-g" + value["jolt"]["executable_revision"])):
        fail(f"{label} Jolt banner/source binding differs")
    identity(label + " Jolt Sdescribe", value["jolt"]["describe"])
    exact(label + " native", value["native"], {"library", "header", "version"})
    identity(label + " native library", value["native"]["library"])
    identity(label + " native header", value["native"]["header"])
    if not isinstance(value["native"]["version"], str) or not value["native"]["version"]:
        fail(f"{label} native version is missing")
    if value["cache_scope"] != "condition-scoped-isolated-after-prime":
        fail(f"{label} cache scope differs")
    validate_state(label + " harness", value["harness"])
    return value

def validate_manifest(reports):
    manifest = load(reports / "run-manifest.json")
    exact("run manifest", manifest, {"schema_version", "fixture", "conditions", "schedule", "run_id"})
    if manifest["schema_version"] != 1:
        fail("unsupported run manifest schema")
    exact("fixture configuration", manifest["fixture"], {"rows", "segments", "description", "inventory_sha256", "expected", "expected_sha256"})
    if manifest["fixture"]["rows"] != 52224 or manifest["fixture"]["segments"] != 3:
        fail("fixture is not the required current 52,224-row/3-segment shape")
    if manifest["fixture"]["description"] != "current 52,224-row/3-segment fixture":
        fail("fixture description is not the current fixture description")
    sha("fixture inventory", manifest["fixture"]["inventory_sha256"])
    exact("fixture expected aggregate", manifest["fixture"]["expected"], EXPECTED)
    sha("fixture expected aggregate identity", manifest["fixture"]["expected_sha256"])
    if digest(canonical(manifest["fixture"]["expected"])) != manifest["fixture"]["expected_sha256"]:
        fail("fixture expected aggregate identity differs")
    exact("conditions", manifest["conditions"], {"A", "B"})
    for condition in ("A", "B"):
        validate_condition("manifest " + condition, manifest["conditions"][condition], condition)
    if manifest["conditions"]["A"]["data_json"]["sha"] == manifest["conditions"]["B"]["data_json"]["sha"]:
        fail("conditions must name distinct exact data.json source SHAs")
    # A and B intentionally vary only the resolved data.json provider and
    # their clean source checkouts.  The compiler and native engine cannot
    # drift between conditions without making the directional result useless.
    for field in ("jolt", "native"):
        if manifest["conditions"]["A"][field] != manifest["conditions"]["B"][field]:
            fail(f"condition compiler/native identity differs: {field}")
    if manifest["schedule"] != schedule():
        fail("schedule is not exact A-prime/B-prime/A/B/B/A order")
    unsigned = dict(manifest); claimed = unsigned.pop("run_id")
    sha("run id", claimed)
    if digest(canonical(unsigned)) != claimed:
        fail("run id differs from manifest content")
    return manifest

def validate_time(path):
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as error:
        fail(f"cannot read {path.name}: {error}")
    if not re.search(r"Exit status:\s+0\s*$", text, re.M):
        fail(f"{path.name} is incomplete/failed")

def validate_report(reports, manifest, entry, seen_pids):
    report = load(reports / entry["report_file"])
    exact("report", report, {"schema_version", "run_id", "schedule_ordinal", "phase", "condition",
                              "observation", "process_id", "process_started_epoch_ms",
                              "process_finished_epoch_ms", "cache_condition", "fixture", "provenance",
                              "recovery"})
    if report["schema_version"] != 1 or report["run_id"] != manifest["run_id"]:
        fail("report schema/run identity differs")
    expected_schedule = {"schedule_ordinal": entry["ordinal"], "phase": entry["phase"],
                         "condition": entry["condition"], "observation": entry["observation"]}
    for key, expected in expected_schedule.items():
        if report[key] != expected: fail(f"report {key} differs from schedule")
    positive("process id", report["process_id"])
    if report["process_id"] in seen_pids: fail("process id is not fresh")
    seen_pids.add(report["process_id"])
    positive("process start", report["process_started_epoch_ms"])
    positive("process finish", report["process_finished_epoch_ms"])
    if report["process_finished_epoch_ms"] < report["process_started_epoch_ms"]:
        fail("process clock order differs")
    if report["cache_condition"] != CACHE: fail("cache condition differs")
    exact("report fixture", report["fixture"], {"inventory_sha256", "rows", "segments", "inventory_unchanged"})
    if report["fixture"] != {"inventory_sha256": manifest["fixture"]["inventory_sha256"],
                             "rows": 52224, "segments": 3, "inventory_unchanged": True}:
        fail("fixture inventory/shape changed")
    provenance = validate_condition("report provenance", report["provenance"], entry["condition"])
    if provenance != manifest["conditions"][entry["condition"]]:
        fail("report provenance differs from its prepared condition")
    exact("recovery", report["recovery"], {"elapsed_ns", "rows_per_second", "expected", "actual"})
    positive("recovery elapsed", report["recovery"]["elapsed_ns"])
    for aggregate in ("expected", "actual"):
        exact("recovery " + aggregate, report["recovery"][aggregate], EXPECTED)
    if report["recovery"]["expected"] != manifest["fixture"]["expected"]:
        fail("fixture expected aggregate differs")
    if report["recovery"]["expected"] != report["recovery"]["actual"]:
        fail("nine-field aggregate mismatch")
    if report["recovery"]["expected"]["n"] != "52224": fail("recovered row count differs")
    calculated = 52224e9 / report["recovery"]["elapsed_ns"]
    if not isinstance(report["recovery"]["rows_per_second"], (int, float)) or abs(report["recovery"]["rows_per_second"] - calculated) > max(1e-6, calculated * 1e-12):
        fail("rows per second does not recompute")
    validate_time(reports / entry["time_file"])
    return report

def main():
    if len(sys.argv) != 3:
        fail("usage: summarize-durable-data-json-recovery-abba.py REPORT_DIR SUMMARY")
    reports, output = map(pathlib.Path, sys.argv[1:])
    manifest = validate_manifest(reports)
    expected_files = {"run-manifest.json"}
    measured = {"A": [], "B": []}; seen_pids = set()
    for entry in schedule():
        expected_files.update((entry["report_file"], entry["time_file"]))
        report = validate_report(reports, manifest, entry, seen_pids)
        if entry["phase"] == "measured": measured[entry["condition"]].append(report["recovery"])
    actual_files = {path.name for path in reports.iterdir() if path.is_file()}
    if actual_files != expected_files:
        fail("report/time file set is not exact")
    def average(condition, field): return sum(x[field] for x in measured[condition]) / 2
    summary = {"schema_version": 1,
               "semantics": {"metric": "in-region read-only open + nine-field aggregate reconciliation + close",
                             "observations_per_condition": 2,
                             "claim_boundary": "directional two-observation comparison only; not p99, Rust, S3, or general throughput qualification"},
               "fixture": manifest["fixture"],
               "conditions": {c: {"data_json_sha": manifest["conditions"][c]["data_json"]["sha"],
                                  "mean_elapsed_ns": average(c, "elapsed_ns"),
                                  "mean_rows_per_second": average(c, "rows_per_second")} for c in ("A", "B")}}
    summary["directional"] = {"A_elapsed_over_B": summary["conditions"]["A"]["mean_elapsed_ns"] / summary["conditions"]["B"]["mean_elapsed_ns"],
                              "A_rows_per_second_over_B": summary["conditions"]["A"]["mean_rows_per_second"] / summary["conditions"]["B"]["mean_rows_per_second"]}
    output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

if __name__ == "__main__": main()
