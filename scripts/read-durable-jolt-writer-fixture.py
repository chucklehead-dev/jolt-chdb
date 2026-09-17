#!/usr/bin/env python3
"""Independently recover public Jolt WAL bytes with the pinned Python reader."""
import hashlib
import importlib.util
import json
import pathlib
import shutil
import sys
from unittest.mock import patch


def fail(message):
    raise ValueError(message)


def digest(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def inventory(root):
    result = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            fail("fixture contains a symlink")
        if path.is_file():
            result.append({"key": path.relative_to(root).as_posix(),
                           "bytes": path.stat().st_size, "sha256": digest(path)})
    return result


def validate(root, descriptor):
    if set(descriptor) != {"schema_version", "object_id", "engine", "database",
                           "inventory", "excluded"}:
        fail("descriptor fields differ")
    if (descriptor["schema_version"] != 1 or descriptor["object_id"] != "jolt-writer"
            or descriptor["engine"] != "26.7.3" or descriptor["database"] != "fixture"):
        fail("descriptor identity differs")
    actual = inventory(root)
    if actual != descriptor["inventory"]:
        fail("logical-object inventory differs")
    head = json.loads((root / "head.json").read_bytes())
    manifest = head["manifest"]
    checkpoint = manifest["base"] is not None
    excluded = descriptor["excluded"]
    if (not isinstance(excluded, list) or len(excluded) != (2 if checkpoint else 1)
            or excluded[0] != "unreferenced-provider.canary"
            or (checkpoint and (not isinstance(excluded[1], str) or not excluded[1].startswith("wal/")))):
        fail("excluded provider inventory differs")
    if (manifest["seq"] != (3 if checkpoint else 1)
            or len(manifest["wal"]) != 1 or manifest["db"] != "fixture"
            or head["lease"]["owner"] is not None
            or head["engine"]["version"] != "26.7.3"
            or head["engine"]["min_reader"] != "26.7.3"
            or head["engine"]["backup_format"] != 1):
        fail("head is not the expected released WAL-only object")
    key = manifest["wal"][0]["key"]
    parts = key.split("/")
    if "\\" in key or any(p in {"", ".", ".."} for p in parts):
        fail("unsafe WAL key")
    refs = manifest["wal"] + ([manifest["base"]] if checkpoint else [])
    for ref in refs:
        parts = ref["key"].split("/")
        if "\\" in ref["key"] or any(p in {"", ".", ".."} for p in parts):
            fail("unsafe referenced key")
        actual_ref = next((e for e in actual if e["key"] == ref["key"]), None)
        if actual_ref is None or actual_ref["bytes"] != ref["size"] or actual_ref["sha256"] != ref["sha256"]:
            fail("reference size or digest differs")
    if {e["key"] for e in actual} != {"head.json"} | {r["key"] for r in refs}:
        fail("exported inventory contains unreferenced provider data")
    return key, actual


def main(argv):
    if len(argv) != 8:
        fail("usage: SOURCE_ROOT SOURCE_ARCHIVE CORE_WHEEL LIBCHDB HEADER JOLT_BIN OUTPUT_DIR")
    source, archive, wheel, library, header, jolt, output = map(pathlib.Path, argv[1:])
    source, output = source.resolve(), output.resolve()
    spec = importlib.util.spec_from_file_location(
        "pinned_fixture", pathlib.Path(__file__).with_name("generate-durable-python-writer-fixture.py"))
    pins = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pins)
    expected = [(archive, pins.SOURCE_ARCHIVE_SHA256), (wheel, pins.CORE_WHEEL_SHA256),
                (source / "docs/durable/protocol-v1.mdx", pins.PROTOCOL_SHA256),
                (source / "tests/test_durable.py", pins.SUITE_SHA256),
                (library, "36ad4e999882821ef13cf2d0f52c93f48e6ee1b4498b35a201c23ce4b8d15bf5"),
                (header, "ec234db7e47589b3780bcd9a5508f5666cb44b0b56cc6f3079c8a24aa1b8d911")]
    for path, expected_hash in expected:
        if digest(path) != expected_hash:
            fail("pinned artifact digest differs: " + path.name)
    from chdb import durable as cd
    from chdb.durable.engine import engine_has_v1_abi, engine_version
    from chdb.durable.errors import Corrupt
    import chdb
    if source not in pathlib.Path(cd.__file__).resolve().parents:
        fail("Python Durable module is not from the pinned source root")
    if chdb.core_version != "26.7.3" or engine_version() != "26.7.3" or not engine_has_v1_abi():
        fail("Python engine is not qualified 26.7.3 Durable V1")
    if digest(chdb._chdb.__file__) != "15aae3d06f0f074ea91fe126983918ae7868fa791b999af112b3142413dc2cc0":
        fail("loaded Python extension differs from the pinned wheel extension")
    root = output / "fixture-store" / "jolt-writer"
    descriptor = json.loads((output / "fixture.json").read_bytes())
    key, before = validate(root, descriptor)
    namespace = cd.Namespace("local:" + str(root.parent), db="fixture")
    reader = namespace.open("jolt-writer", read_only=True)
    try:
        raw_rows = reader.query(
            "SELECT n, ok, label FROM fixture.events ORDER BY n SETTINGS output_format_json_quote_64bit_integers=0",
            "JSONEachRow").data()
        rows = [json.loads(line) for line in raw_rows.splitlines() if line.strip()]
        expected_rows = [{"n": 1, "ok": True, "label": "snowman ☃"},
                         {"n": 2, "ok": False, "label": "question ?"},
                         {"n": 3, "ok": True, "label": "comma,quote"}]
        if (rows != expected_rows or
                any(type(row.get("n")) is not int or type(row.get("ok")) is not bool
                    or type(row.get("label")) is not str for row in rows)):
            fail("independent Python exact row readback differs")
        observed = reader.query(
            "SELECT count(), sum(n), countIf(ok), sum(length(label)), min(n), max(n) FROM fixture.events",
            "CSV").data().strip()
        if observed != "3,6,2,32,1,3":
            fail("independent Python aggregate differs")
    finally:
        reader.close()
    if inventory(root) != before:
        fail("Python readonly recovery changed logical protocol bytes")
    bad_descriptor = dict(descriptor, engine="26.7.2")
    with patch.object(cd.Namespace, "open", side_effect=AssertionError("reader unexpectedly opened")) as opening:
        try:
            validate(root, bad_descriptor)
        except ValueError:
            opening.assert_not_called()
            print("ok descriptor mismatch rejected before reader open")
        else:
            fail("descriptor mismatch was accepted")
    head = json.loads((root / "head.json").read_bytes())
    base = head["manifest"]["base"]
    if base is not None:
        format_root = output / "controls" / "backup-format" / "jolt-writer"
        shutil.copytree(root, format_root)
        format_head = dict(head, engine=dict(head["engine"], backup_format=2))
        (format_root / "head.json").write_text(json.dumps(format_head), encoding="utf-8")
        format_descriptor = dict(descriptor, inventory=inventory(format_root))
        with patch.object(cd.Namespace, "open", side_effect=AssertionError("reader unexpectedly opened")) as opening:
            try:
                validate(format_root, format_descriptor)
            except ValueError:
                opening.assert_not_called()
            else:
                fail("backup-format mismatch was accepted")
        print("ok backup-format mismatch rejected before reader open")
        snapshot = output / "base-only-store" / "jolt-writer"
        snapshot_before = inventory(snapshot)
        snapshot_head = json.loads((snapshot / "head.json").read_bytes())
        if (snapshot_head["manifest"]["seq"] != 2 or snapshot_head["manifest"]["wal"] or
                snapshot_head["manifest"]["base"] != base or
                {e["key"] for e in snapshot_before} != {"head.json", base["key"]}):
            fail("invalid base-only snapshot")
        snapshot_reader = cd.Namespace("local:" + str(snapshot.parent)).open("jolt-writer", read_only=True)
        try:
            rows = [json.loads(line) for line in snapshot_reader.query(
                "SELECT n, ok, label FROM fixture.events ORDER BY n SETTINGS output_format_json_quote_64bit_integers=0",
                "JSONEachRow").data().splitlines() if line.strip()]
            if rows != expected_rows[:2] or any(type(r["n"]) is not int or type(r["ok"]) is not bool for r in rows):
                fail("base-only exact typed row readback differs")
        finally:
            snapshot_reader.close()
        if inventory(snapshot) != snapshot_before:
            fail("base-only recovery changed protocol bytes")
        print("ok independent Python base-only recovery contains exactly two typed rows")
    labels = ["missing-wal", "corrupt-wal"] + (["missing-base", "corrupt-base", "truncated-base"] if base else [])
    for label in labels:
        scenario = output / "controls" / label / "jolt-writer"
        shutil.copytree(root, scenario)
        wal = scenario / (base["key"] if label.endswith("base") else key)
        if label.startswith("missing"):
            wal.unlink()
        elif label.startswith("truncated"):
            wal.write_bytes(wal.read_bytes()[:-1])
        else:
            value = bytearray(wal.read_bytes())
            value[0] ^= 1
            wal.write_bytes(value)
        mutant_before = inventory(scenario)
        try:
            mutant = cd.Namespace("local:" + str(scenario.parent), db="fixture").open(
                "jolt-writer", read_only=True)
        except Corrupt as error:
            print("ok independent Python rejects", label, type(error).__name__)
        else:
            mutant.close()
            fail("independent Python accepted " + label)
        if inventory(scenario) != mutant_before:
            fail("failed readonly recovery changed mutant protocol bytes")
    receipt = {"source_commit": pins.SOURCE_COMMIT, "source_tree": pins.SOURCE_TREE,
               "artifacts": {name: pins.identity(path) for name, path in
                             [("archive", archive), ("wheel", wheel), ("library", library),
                              ("header", header), ("jolt", jolt)]},
               "python_extension": pins.identity(pathlib.Path(chdb._chdb.__file__)),
               "inventory_before": before, "inventory_after": inventory(root),
               "controls": ["descriptor-mismatch"] + (["backup-format-pre-open"] if base else []) + labels}
    (output / "python-readback.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print("all reverse Jolt-writer/Python-reader WAL-only fixture checks passed")


if __name__ == "__main__":
    main(sys.argv)
