#!/usr/bin/env python3
"""Translate one validated legacy Jolt reader receipt into the A/B/B/A schema."""
import json, pathlib, sys
def fail(message): raise SystemExit("data.json recovery receipt normalization failed: " + message)
def load(path):
    try: return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error: fail(str(error))
def main():
    if len(sys.argv) != 12: fail("usage: normalize... RAW OUTER_MANIFEST CONDITION OBSERVATION PHASE ORDINAL LEGACY_ORDINAL LEGACY_PHASE LEGACY_TRIAL PROVENANCE OUTPUT")
    raw, outer, condition, observation, phase, ordinal, legacy_ordinal, legacy_phase, legacy_trial, provenance, output = sys.argv[1:]
    value, outer, provenance = load(pathlib.Path(raw)), load(pathlib.Path(outer)), load(pathlib.Path(provenance))
    if "conditions" in provenance:
        try: provenance = provenance["conditions"][condition]
        except (KeyError, TypeError): fail("outer condition provenance is missing")
    needed={"schema_version","run_id","schedule_ordinal","phase","runtime","trial","process_id","process_started_epoch_ms","process_finished_epoch_ms","cache_condition","fixture","recovery"}
    if set(value) != needed or value["schema_version"] != 1: fail("raw receipt schema differs")
    if value["cache_condition"] != "warm-provider-cache-fresh-process-engine-and-scratch": fail("raw cache condition differs")
    expected_trial=None if legacy_trial == "none" else int(legacy_trial)
    if (value["schedule_ordinal"] != int(legacy_ordinal) or value["phase"] != legacy_phase or
        value["trial"] != expected_trial):
        fail("raw legacy ordinal/phase/trial differs from selected Jolt slot")
    runtime=value["runtime"]
    if not isinstance(runtime, dict) or runtime.get("runtime") != "jolt": fail("raw receipt runtime differs")
    # Do not let this adapter replace a different Jolt/native execution with
    # the outer manifest's prettier names. The legacy reader has already
    # recorded file identities; bind them before translating its schema.
    expected_native=provenance.get("native", {})
    expected_jolt=provenance.get("jolt", {})
    if (runtime.get("native_library") != expected_native.get("library") or
        runtime.get("native_header") != expected_native.get("header") or
        runtime.get("native_version") != expected_native.get("version") or
        runtime.get("jolt_version") != expected_jolt.get("version") or
        runtime.get("jolt_source_sha_asserted") != expected_jolt.get("source_sha") or
        runtime.get("jolt_executable_revision") != expected_jolt.get("executable_revision") or
        runtime.get("jolt_sdescribe") != expected_jolt.get("describe") or
        runtime.get("executable") != expected_jolt.get("executable")):
        fail("raw compiler/native identity differs from outer condition")
    fixture=value["fixture"]
    if fixture.get("recovered_rows") != 52224 or fixture.get("wal_segments") != 3 or not value["recovery"].get("inventory_unchanged"):
        fail("raw receipt does not prove current fixture inventory/shape")
    if not isinstance(outer.get("run_id"), str): fail("outer manifest run identity is missing")
    out={"schema_version":1,"run_id":outer["run_id"],"schedule_ordinal":int(ordinal),"phase":phase,"condition":condition,
         "observation":None if observation == "none" else int(observation),"process_id":value["process_id"],
         "process_started_epoch_ms":value["process_started_epoch_ms"],"process_finished_epoch_ms":value["process_finished_epoch_ms"],
         "cache_condition":value["cache_condition"],"fixture":{"inventory_sha256":fixture["inventory_sha256"],"rows":52224,"segments":3,"inventory_unchanged":True},
         "provenance":provenance,"recovery":{k:value["recovery"][k] for k in ("elapsed_ns","rows_per_second","expected","actual")}}
    pathlib.Path(output).write_text(json.dumps(out, sort_keys=True) + "\n")
if __name__ == "__main__": main()
