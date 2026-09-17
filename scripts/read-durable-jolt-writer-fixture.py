#!/usr/bin/env python3
"""Independently recover public Jolt WAL bytes with the pinned Python reader."""
import hashlib
import importlib.util
import json
import pathlib
import shutil
import sys


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
            or descriptor["engine"] != "26.7.3" or descriptor["database"] != "fixture"
            or descriptor["excluded"] != ["unreferenced-provider.canary"]):
        fail("descriptor identity differs")
    actual = inventory(root)
    if actual != descriptor["inventory"]:
        fail("logical-object inventory differs")
    head = json.loads((root / "head.json").read_bytes())
    manifest = head["manifest"]
    if (manifest["base"] is not None or manifest["seq"] != 1
            or len(manifest["wal"]) != 1 or manifest["db"] != "fixture"
            or head["lease"]["owner"] is not None
            or head["engine"]["version"] != "26.7.3"
            or head["engine"]["min_reader"] != "26.7.3"):
        fail("head is not the expected released WAL-only object")
    key = manifest["wal"][0]["key"]
    parts = key.split("/")
    if "\\" in key or any(p in {"", ".", ".."} for p in parts):
        fail("unsafe WAL key")
    if {e["key"] for e in actual} != {"head.json", key}:
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
    try:
        validate(root, bad_descriptor)
    except ValueError:
        print("ok descriptor mismatch rejected before reader open")
    else:
        fail("descriptor mismatch was accepted")
    for label in ("missing-wal", "corrupt-wal"):
        scenario = output / "controls" / label / "jolt-writer"
        shutil.copytree(root, scenario)
        wal = scenario / key
        if label == "missing-wal":
            wal.unlink()
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
               "controls": ["descriptor-mismatch", "missing-wal", "corrupt-wal"]}
    (output / "python-readback.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print("all reverse Jolt-writer/Python-reader WAL-only fixture checks passed")


if __name__ == "__main__":
    main(sys.argv)
