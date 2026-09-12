#!/usr/bin/env python3
"""Strictly validate and summarize a matched Jolt/chdb-rust recovery run."""
import hashlib, json, math, pathlib, re, sys

SHA = re.compile(r"[0-9a-f]{64}")
GIT_SHA = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")
CACHE = "warm-provider-cache-fresh-process-engine-and-scratch"
EXPECTED = {"n", "flags", "severity_sum", "body_bytes", "question_bodies",
            "min_trace", "max_trace", "min_span", "max_span"}

def fail(message): raise SystemExit(f"cross-binding recovery summary failed: {message}")
def load(path):
    try:
        with path.open(encoding="utf-8") as source: return json.load(source)
    except (OSError, json.JSONDecodeError) as error: fail(f"cannot read {path.name}: {error}")
def exact(label, value, keys):
    if not isinstance(value, dict) or set(value) != set(keys): fail(f"{label} has missing or unknown fields")
    return value
def digest(data): return hashlib.sha256(data).hexdigest()
def canonical(value): return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
def sha(label, value):
    if not isinstance(value, str) or not SHA.fullmatch(value): fail(f"{label} is not a full SHA")
def git_sha(label, value):
    if not isinstance(value, str) or not GIT_SHA.fullmatch(value): fail(f"{label} is not a full Git SHA")
def nonempty(label, value):
    if not isinstance(value, str) or not value: fail(f"{label} is missing")
def file_identity(label, value):
    exact(label, value, {"file_name", "bytes", "sha256"})
    nonempty(f"{label} file name", value["file_name"])
    if not isinstance(value["bytes"], int) or value["bytes"] <= 0: fail(f"{label} byte count is invalid")
    sha(f"{label} digest", value["sha256"])
    return value

def validate_harness(report_dir, state):
    exact("harness state", state, {"schema_version", "head", "parent", "tree", "status",
          "tracked_patch", "untracked_files", "state_sha256"})
    if state["schema_version"] != 1 or state["status"] not in {"clean", "dirty"}: fail("invalid harness schema/status")
    for name in ("head", "parent", "tree"): git_sha(f"harness {name}", state[name])
    patch = exact("tracked patch", state["tracked_patch"], {"file_name", "bytes", "sha256"})
    if patch["file_name"] != "harness-tracked.patch": fail("unexpected tracked patch name")
    try: patch_bytes = (report_dir / patch["file_name"]).read_bytes()
    except OSError as error: fail(f"cannot read tracked patch: {error}")
    if patch["bytes"] != len(patch_bytes) or patch["sha256"] != digest(patch_bytes): fail("tracked patch identity differs")
    seen = set()
    if not isinstance(state["untracked_files"], list): fail("untracked inventory is not a list")
    for item in state["untracked_files"]:
        exact("untracked file", item, {"path", "bytes", "mode", "sha256"})
        if not item["path"] or item["path"] in seen or not isinstance(item["bytes"], int) or item["bytes"] < 0: fail("invalid/duplicate untracked identity")
        seen.add(item["path"]); sha("untracked digest", item["sha256"])
        if not re.fullmatch(r"[0-7]{4}", item["mode"]): fail("invalid untracked mode")
    unsigned = dict(state); claimed = unsigned.pop("state_sha256"); sha("harness state digest", claimed)
    if digest(canonical(unsigned)) != claimed: fail("harness state digest differs")
    if (state["status"] == "dirty") != bool(patch_bytes or state["untracked_files"]): fail("harness clean/dirty claim differs")

def expected_schedule(trials):
    result = []
    def add(phase, runtime, trial, label):
        result.append({"ordinal": len(result), "phase": phase, "runtime": runtime, "trial": trial,
                       "report_file": f"{runtime}-{label}.json", "time_file": f"{runtime}-{label}.time"})
    add("prime", "rust", None, "prime"); add("prime", "jolt", None, "prime")
    for trial in range(1, trials + 1):
        for runtime in (("rust", "jolt") if trial % 2 else ("jolt", "rust")): add("measured", runtime, trial, f"trial-{trial}")
    return result

def validate_manifest(report_dir, manifest):
    exact("run manifest", manifest, {"schema_version", "config", "harness_state", "schedule", "run_id"})
    config = exact("run config", manifest["config"], {"object_id", "database", "trials", "batch_size", "warmup_batches", "measured_batches", "total_rows"})
    if manifest["schema_version"] != 1: fail("unsupported run manifest schema")
    for key in ("trials", "batch_size", "warmup_batches", "measured_batches", "total_rows"):
        if not isinstance(config[key], int) or config[key] <= 0: fail(f"run config {key} must be positive")
    if config["trials"] < 5: fail("at least five measured trials are required")
    if config["total_rows"] != config["batch_size"] * (config["warmup_batches"] + config["measured_batches"]): fail("total rows differs from workload")
    if manifest["schedule"] != expected_schedule(config["trials"]): fail("schedule is not exact prime/alternating order")
    unsigned = dict(manifest); claimed = unsigned.pop("run_id"); sha("run id", claimed)
    if digest(canonical(unsigned)) != claimed: fail("run id differs from manifest content")
    validate_harness(report_dir, manifest["harness_state"])
    return config

def validate_fixture(fixture, run):
    exact("fixture", fixture, {"schema_version", "run_id", "producer", "config", "expected", "manifest", "inventory", "inventory_sha256"})
    if fixture["schema_version"] != 1 or fixture["run_id"] != run["run_id"]: fail("fixture schema/run id differs")
    config = exact("fixture config", fixture["config"], {"object_id", "database", "batch_size", "warmup_batches", "measured_batches", "total_rows"})
    if config != {key: run["config"][key] for key in config}: fail("fixture workload differs from run")
    expected = exact("expected aggregate", fixture["expected"], EXPECTED)
    if any(not isinstance(v, str) for v in expected.values()) or int(expected["n"]) != config["total_rows"]: fail("expected count/value shape differs")
    durable = exact("Durable manifest", fixture["manifest"], {"db", "base", "wal", "seq"})
    if durable["db"] != config["database"] or durable["base"] is not None or durable["seq"] != 3 or len(durable["wal"]) != 3: fail("Durable manifest shape differs")
    inventory = {}
    for item in fixture["inventory"]:
        exact("inventory entry", item, {"key", "kind", "bytes", "sha256", "target"})
        if item["key"] in inventory: fail("duplicate inventory key")
        if item["kind"] == "file":
            if not isinstance(item["bytes"], int) or item["bytes"] <= 0 or item["target"] is not None: fail("invalid inventory file shape")
            sha("inventory file digest", item["sha256"])
        elif item["kind"] == "symlink":
            if item["bytes"] is not None or item["sha256"] is not None or not isinstance(item["target"], str): fail("invalid inventory symlink shape")
        else: fail("unknown inventory entry kind")
        inventory[item["key"]] = item
    for number, ref in enumerate(durable["wal"], 1):
        exact("WAL reference", ref, {"key", "size", "sha256"})
        if not re.fullmatch(rf"wal/[^/]*-{number}-[^/]+\.jsonl", ref["key"]): fail("WAL sequence/key shape differs")
        item = inventory.get(ref["key"])
        if item is None or item["kind"] != "file" or item["bytes"] != ref["size"] or item["sha256"] != ref["sha256"]: fail("WAL reference differs from inventory")
    head = inventory.get("head.json")
    if head is None or head["kind"] != "symlink" or head["target"] not in inventory or inventory[head["target"]]["kind"] != "file": fail("head symlink does not select an inventoried version")
    allowed = re.compile(r"(?:head\.json|\.head-versions/[1-9][0-9]*\.json|wal/[^/]+\.jsonl)")
    if any(not allowed.fullmatch(key) for key in inventory): fail("fixture inventory contains an unexpected path shape")
    sha("inventory digest", fixture["inventory_sha256"])
    if digest(json.dumps(fixture["inventory"], separators=(",", ":")).encode()) != fixture["inventory_sha256"]: fail("inventory digest differs")

def validate_provenance(label, value, runtime, harness):
    common = {"runtime", "native_version", "harness_state", "native_library", "native_header", "executable"}
    if runtime == "rust":
        keys = common | {"chdb_rust_git_sha", "chdb_rust_crate_version", "oracle_crate_version", "rustc_version", "cargo_version", "engine_source"}
        git_sha(f"{label} chdb-rust SHA", value.get("chdb_rust_git_sha"))
        for field in ("chdb_rust_crate_version", "oracle_crate_version", "rustc_version", "cargo_version", "engine_source"): nonempty(f"{label} {field}", value.get(field))
    else:
        keys = common | {"jolt_version", "jolt_source_sha", "scheme_version", "machine_type"}
        git_sha(f"{label} Jolt SHA", value.get("jolt_source_sha"))
        for field in ("jolt_version", "scheme_version", "machine_type"): nonempty(f"{label} {field}", value.get(field))
    exact(f"{label} provenance", value, keys)
    if value["runtime"] != runtime or value["harness_state"] != harness: fail(f"{label} runtime/harness differs")
    nonempty(f"{label} native version", value["native_version"])
    for field in ("native_library", "native_header", "executable"): file_identity(f"{label} {field}", value[field])
    return value

def gnu_time(path):
    text = path.read_text(encoding="utf-8")
    rss = re.search(r"Maximum resident set size \(kbytes\):\s+(\d+)", text)
    wall = re.search(r"Elapsed \(wall clock\) time \(h:mm:ss or m:ss\):\s+([^\s]+)", text)
    status = re.search(r"Exit status:\s+(\d+)", text)
    if not rss or not wall or not status or int(status.group(1)) != 0: fail(f"incomplete/failed GNU time {path.name}")
    parts = [float(part) for part in wall.group(1).split(":")]
    seconds = parts[0] * 60 + parts[1] if len(parts) == 2 else parts[0] * 3600 + parts[1] * 60 + parts[2] if len(parts) == 3 else 0
    if seconds <= 0: fail(f"invalid GNU-time wall duration {path.name}")
    return {"maximum_rss_kib": int(rss.group(1)), "wall_seconds": seconds}
def rank(values, q): return sorted(values)[max(0, math.ceil(q * len(values)) - 1)]
def distribution(values): return {"p50": rank(values, .5), "p95_exploratory": rank(values, .95), "p99_exploratory": rank(values, .99), "max": max(values)}
def runtime_summary(reports, timings):
    ns = [r["recovery"]["elapsed_ns"] for r in reports]; rows = [r["fixture"]["recovered_rows"] for r in reports]; walls = [t["wall_seconds"] for t in timings]; total = sum(rows)
    return {"trials": len(reports), "total_recovered_rows": total,
            "in_region_recovery": {"total_elapsed_ns": sum(ns), "aggregate_rows_per_second": total * 1e9 / sum(ns), "elapsed_ns": distribution(ns)},
            "whole_process": {"total_elapsed_seconds": sum(walls), "aggregate_rows_per_second": total / sum(walls), "elapsed_seconds": distribution(walls)},
            "maximum_rss_kib": {"p50": rank([t["maximum_rss_kib"] for t in timings], .5), "max": max(t["maximum_rss_kib"] for t in timings)}}

def main():
    if len(sys.argv) != 3: fail("usage: summarizer REPORT_DIR OUTPUT_JSON")
    report_dir, output = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
    run = load(report_dir / "run-manifest.json"); config = validate_manifest(report_dir, run)
    fixture = load(report_dir / "fixture.json"); validate_fixture(fixture, run)
    producer = validate_provenance("producer", fixture["producer"], "rust", run["harness_state"])
    reports, timings, pids, previous_finish = [], [], set(), None
    for entry in run["schedule"]:
        report = load(report_dir / entry["report_file"]); timing = gnu_time(report_dir / entry["time_file"])
        exact("report", report, {"schema_version", "run_id", "schedule_ordinal", "phase", "runtime", "trial", "process_id", "process_started_epoch_ms", "process_finished_epoch_ms", "cache_condition", "fixture", "recovery"})
        if (report["schema_version"], report["run_id"], report["schedule_ordinal"], report["phase"], report["trial"]) != (1, run["run_id"], entry["ordinal"], entry["phase"], entry["trial"]): fail("report differs from schedule")
        validate_provenance(entry["report_file"], report["runtime"], entry["runtime"], run["harness_state"])
        pid, started, finished = report["process_id"], report["process_started_epoch_ms"], report["process_finished_epoch_ms"]
        if not isinstance(pid, int) or pid <= 0 or pid in pids: fail("prime/measured processes are not fresh")
        if not isinstance(started, int) or not isinstance(finished, int) or finished < started or (previous_finish is not None and started < previous_finish): fail("process timestamps violate serial schedule")
        pids.add(pid); previous_finish = finished
        if report["cache_condition"] != CACHE: fail("cache condition differs")
        work = exact("trial fixture", report["fixture"], {"inventory_sha256", "batch_size", "warmup_batches", "measured_batches", "wal_segments", "recovered_rows"})
        if work != {"inventory_sha256": fixture["inventory_sha256"], "batch_size": config["batch_size"], "warmup_batches": config["warmup_batches"], "measured_batches": config["measured_batches"], "wal_segments": 3, "recovered_rows": config["total_rows"]}: fail("trial workload differs")
        recovery = exact("recovery", report["recovery"], {"elapsed_ns", "rows_per_second", "expected", "actual", "inventory_unchanged"})
        if not isinstance(recovery["elapsed_ns"], int) or recovery["elapsed_ns"] <= 0 or recovery["expected"] != fixture["expected"] or recovery["actual"] != fixture["expected"] or recovery["inventory_unchanged"] is not True: fail("recovery reconciliation differs")
        expected_rate = config["total_rows"] * 1e9 / recovery["elapsed_ns"]
        if not isinstance(recovery["rows_per_second"], (int, float)) or isinstance(recovery["rows_per_second"], bool) or not math.isfinite(recovery["rows_per_second"]) or not math.isclose(recovery["rows_per_second"], expected_rate, rel_tol=1e-12, abs_tol=1e-9): fail("raw rows_per_second does not recompute from rows and elapsed_ns")
        reports.append((entry, report)); timings.append((entry, timing))
    planned_json = {"fixture.json", "harness-state.json", "run-manifest.json"} | {e["report_file"] for e in run["schedule"]}
    actual_json = {p.name for p in report_dir.glob("*.json")} - ({output.name} if output.parent == report_dir else set())
    if actual_json != planned_json: fail("JSON file set is not exact")
    if {p.name for p in report_dir.glob("*.time")} != {e["time_file"] for e in run["schedule"]}: fail("GNU-time file set is not exact")
    identities = [producer] + [r["runtime"] for _, r in reports]
    for field in ("native_version", "native_library", "native_header"):
        if any(item[field] != identities[0][field] for item in identities[1:]): fail("native library/header provenance differs")
    rust_keys = ("chdb_rust_git_sha", "chdb_rust_crate_version", "oracle_crate_version", "rustc_version", "cargo_version", "engine_source", "executable")
    if any(any(r["runtime"][k] != producer[k] for k in rust_keys) for e, r in reports if e["runtime"] == "rust"): fail("Rust provenance differs")
    jolts = [r["runtime"] for e, r in reports if e["runtime"] == "jolt"]
    if any(any(item[k] != jolts[0][k] for k in ("jolt_source_sha", "jolt_version", "scheme_version", "machine_type", "executable")) for item in jolts[1:]): fail("Jolt provenance differs")
    summaries = {runtime: runtime_summary([r for e, r in reports if e["phase"] == "measured" and e["runtime"] == runtime], [t for e, t in timings if e["phase"] == "measured" and e["runtime"] == runtime]) for runtime in ("jolt", "rust")}
    def ratios(region, elapsed):
        j, r = summaries["jolt"][region], summaries["rust"][region]
        return {"jolt_throughput_over_rust": j["aggregate_rows_per_second"] / r["aggregate_rows_per_second"], "jolt_elapsed_over_rust": j[elapsed] / r[elapsed]}
    acceptance = ratios("in_region_recovery", "total_elapsed_ns"); acceptance.update({"metric": "in-region read-only open + aggregate reconciliation + close", "target": "Jolt throughput >= 0.80 * Rust and Jolt elapsed <= 1.25 * Rust"}); acceptance["meets_80_percent_rust_target"] = acceptance["jolt_throughput_over_rust"] >= .8 and acceptance["jolt_elapsed_over_rust"] <= 1.25
    whole = ratios("whole_process", "total_elapsed_seconds"); whole["metric"] = "whole fresh process including runtime initialization, dependency loading, inventory/provenance validation, recovery, and report emission"
    result = {"schema_version": 1, "run_id": run["run_id"], "semantics": {"fixture": "one immutable byte-identical Durable object read by both runtimes", "cache_condition": CACHE, "percentiles": f"p95 and p99 are exploratory at n={config['trials']}; acceptance uses aggregate in-region ratios"}, "fixture": fixture, "run_manifest": run, "summaries": summaries, "comparison": {"acceptance": acceptance, "whole_process_diagnostic": whole}}
    output.parent.mkdir(parents=True, exist_ok=True); output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"status": "ok", "comparison": result["comparison"]}, sort_keys=True))

if __name__ == "__main__": main()
