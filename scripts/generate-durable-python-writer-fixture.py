#!/usr/bin/env python3
"""Generate one immutable, logical Durable object with the pinned Python writer."""

import hashlib
import json
import pathlib
import shutil
import sys


SOURCE_COMMIT = "66643e5030fb73c30ac5cdd31d4c7858ea040ed0"
SOURCE_TREE = "a61323f3f7246c83f9083ca8d4f733a7c4402c50"
SOURCE_ARCHIVE_SHA256 = "4269548e589fa34497c207e84e660785399b52edecb7294cc929be796b04b381"
CORE_VERSION = "26.7.3"
CORE_WHEEL_SHA256 = "b10b96f9599fab42ba51d9be80333e1819782bdc8a91b2b26979149693ba431f"
PROTOCOL_SHA256 = "82538d958d2f522bea6e4a6ccbc27c6bb6e230e19a1a386c605d43a0c02d11ba"
SUITE_SHA256 = "5b9a97a3bccc0c29b10aca6b6e9a72edceea6a79e19d9a54c900b10d24f5eee6"
OBJECT_ID = "python-writer"
DATABASE = "fixture"
EXPECTED = {
    "label_bytes": "32",
    "max_n": "3",
    "min_n": "1",
    "n": "3",
    "sum_n": "6",
    "true_count": "2",
}


def fail(message):
    raise SystemExit(f"Python Durable fixture generation failed: {message}")


def digest(path):
    value = hashlib.sha256()
    with open(path, "rb") as source:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            value.update(chunk)
    return value.hexdigest()


def identity(path):
    path = pathlib.Path(path).resolve()
    if not path.is_file():
        fail(f"artifact is not a file: {path}")
    return {"file_name": path.name, "bytes": path.stat().st_size, "sha256": digest(path)}


def inventory_digest(value):
    encoded = "".join(
        f'{entry["key"]}\0{entry["bytes"]}\0{entry["sha256"]}\n' for entry in value
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def inventory(root):
    entries = []
    for path in sorted(pathlib.Path(root).rglob("*")):
        if path.is_file():
            entries.append({
                "key": path.relative_to(root).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": digest(path),
            })
    return entries


def main(argv):
    if len(argv) not in (7, 8):
        fail("usage: SOURCE_ROOT SOURCE_ARCHIVE CORE_WHEEL LIBCHDB HEADER OUTPUT_DIR [wal|checkpoint]")
    kind = argv[7] if len(argv) == 8 else "wal"
    if kind not in ("wal", "checkpoint"):
        fail("unknown fixture kind")
    source_root, source_archive, core_wheel, native_library, native_header, output = map(
        pathlib.Path, argv[1:7]
    )
    source_root = source_root.resolve()
    output = output.resolve()
    if output.exists() and any(output.iterdir()):
        fail("output directory must be absent or empty")
    output.mkdir(parents=True, exist_ok=True)

    source_archive_id = identity(source_archive)
    wheel_id = identity(core_wheel)
    if source_archive_id["sha256"] != SOURCE_ARCHIVE_SHA256:
        fail("source archive digest differs from the pinned commit archive")
    if wheel_id["sha256"] != CORE_WHEEL_SHA256:
        fail("chdb-core wheel digest differs from the pinned Linux x86_64 wheel")
    if digest(source_root / "docs/durable/protocol-v1.mdx") != PROTOCOL_SHA256:
        fail("protocol source digest differs")
    if digest(source_root / "tests/test_durable.py") != SUITE_SHA256:
        fail("upstream Durable suite digest differs")

    import chdb
    from chdb import durable as cd
    from chdb.durable.backends import make_backend
    from chdb.durable.engine import engine_has_v1_abi, engine_version

    durable_module = pathlib.Path(cd.__file__).resolve()
    if source_root not in durable_module.parents:
        fail("Durable Python module was not loaded from the pinned source tree")
    if chdb.core_version != CORE_VERSION or engine_version() != CORE_VERSION:
        fail("Python package and running engine are not both chdb-core 26.7.3")
    if not engine_has_v1_abi():
        fail("Python engine lacks the Durable V1 ABI")

    provider_root = output / "python-local-provider"
    namespace = cd.Namespace(
        "local:" + str(provider_root), owner="issue47-python-writer", db=DATABASE
    )
    obj = namespace.open(OBJECT_ID)
    obj.execute(
        "CREATE TABLE events (n Int64, ok Bool, label String) "
        "ENGINE=MergeTree ORDER BY n"
    )
    obj.execute(
        "INSERT INTO events VALUES "
        "(1, true, 'snowman ☃'), "
        "(2, false, 'question ?')" +
        (", (3, true, 'comma,quote')" if kind == "wal" else "")
    )
    published = obj.flush()
    folded_wal = published
    base_only = None
    if kind == "checkpoint":
        base_key = obj.checkpoint()
        if make_backend("local:" + str(provider_root), OBJECT_ID).get(folded_wal) is None:
            fail("folded WAL exclusion control was not established")
        if obj.seq != 2 or obj.wal or obj.base.key != base_key:
            fail("checkpoint did not fold the initial WAL")
        snapshot_backend = make_backend("local:" + str(provider_root), OBJECT_ID)
        base_only = snapshot_backend.get("head.json")
        obj.execute("INSERT INTO events VALUES (3, true, 'comma,quote')")
        published = obj.flush()
    obj.close()

    reader = namespace.open(OBJECT_ID, read_only=True)
    observed = reader.query(
        "SELECT count(), sum(n), countIf(ok), sum(length(label)), min(n), max(n) "
        "FROM fixture.events",
        "CSV",
    ).data().strip().split(",")
    reader.close()
    python_aggregate = dict(zip(
        ["n", "sum_n", "true_count", "label_bytes", "min_n", "max_n"], observed
    ))
    if python_aggregate != EXPECTED:
        fail(f"Python readback aggregate differs: {python_aggregate!r}")

    backend = make_backend("local:" + str(provider_root), OBJECT_ID)
    head_bytes = backend.get("head.json")
    if head_bytes is None:
        fail("Python writer did not publish head.json")
    head = json.loads(head_bytes)
    manifest = head.get("manifest", {})
    if not (
        head.get("engine", {}).get("version") == CORE_VERSION
        and head.get("engine", {}).get("min_reader") == CORE_VERSION
        and head.get("lease", {}).get("owner") is None
        and manifest.get("db") == DATABASE
        and (manifest.get("base") is None if kind == "wal" else
             manifest.get("base", {}).get("key") == base_key)
        and head.get("engine", {}).get("backup_format") == 1
        and manifest.get("seq") == (1 if kind == "wal" else 3)
        and len(manifest.get("wal", [])) == 1
        and manifest["wal"][0].get("key") == published
    ):
        fail("Python writer produced an unexpected WAL-only head")

    # A causal control: an arbitrary provider file must not become part of the
    # exported protocol fixture merely because it shares the object prefix.
    (provider_root / OBJECT_ID / "unreferenced-provider.canary").write_bytes(
        b"must-not-be-exported"
    )
    logical_keys = ["head.json"] + [entry["key"] for entry in manifest["wal"]]
    if kind == "checkpoint":
        logical_keys.append(manifest["base"]["key"])
    fixture_object = output / "fixture-store" / OBJECT_ID
    for key in logical_keys:
        source = provider_root / OBJECT_ID / key
        target = fixture_object / key
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    logical_inventory = inventory(fixture_object)
    if [entry["key"] for entry in logical_inventory] != sorted(logical_keys):
        fail("logical fixture contains a provider-private or unreferenced object")
    for ref in manifest["wal"] + ([manifest["base"]] if kind == "checkpoint" else []):
        actual = next(entry for entry in logical_inventory if entry["key"] == ref["key"])
        if actual["bytes"] != ref["size"] or actual["sha256"] != ref["sha256"]:
            fail("published object bytes differ from the head reference")
    if base_only is not None:
        snapshot = output / "base-only-store" / OBJECT_ID
        snapshot.mkdir(parents=True)
        (snapshot / "head.json").write_bytes(base_only)
        target = snapshot / base_key
        target.parent.mkdir(parents=True)
        shutil.copyfile(fixture_object / base_key, target)

    provider_inventory = inventory(provider_root / OBJECT_ID)
    excluded = sorted(set(entry["key"] for entry in provider_inventory) - set(logical_keys))
    expected_excluded = sorted(["head.json.lock", "unreferenced-provider.canary"] +
                               ([folded_wal] if kind == "checkpoint" else []))
    if excluded != expected_excluded:
        fail(f"unexpected Python provider-private inventory: {excluded!r}")

    descriptor = {
        "schema_version": 1,
        "source": {
            "repository": "https://github.com/chdb-io/chdb.git",
            "commit": SOURCE_COMMIT,
            "tree": SOURCE_TREE,
            "archive": source_archive_id,
            "protocol_sha256": PROTOCOL_SHA256,
            "suite_sha256": SUITE_SHA256,
        },
        "python": {
            "implementation": sys.implementation.name,
            "version": ".".join(map(str, sys.version_info[:3])),
            "durable_module": str(durable_module.relative_to(source_root)),
            "core_version": chdb.core_version,
            "engine_version": engine_version(),
            "core_wheel": wheel_id,
            "extension": identity(chdb._chdb.__file__),
        },
        "jolt_native": {
            "expected_engine_version": CORE_VERSION,
            "library": identity(native_library),
            "header": identity(native_header),
        },
        "provider_boundary": {
            "producer": "python-local-raw-mtime-size-etag",
            "consumer": "jolt-test-only-raw-read-only",
            "direct_provider_compatibility": False,
            "excluded_provider_private_keys": excluded,
        },
        "fixture": {
            "object_id": OBJECT_ID,
            "database": DATABASE,
            "expected": EXPECTED,
            "manifest": manifest,
            "logical_inventory": logical_inventory,
            "inventory_sha256": inventory_digest(logical_inventory),
        },
    }
    with open(output / "fixture.json", "w", encoding="utf-8") as target:
        json.dump(descriptor, target, indent=2, sort_keys=True, ensure_ascii=False)
        target.write("\n")
    print(json.dumps({"fixture": str(output / "fixture.json"), "aggregate": EXPECTED}, sort_keys=True))


if __name__ == "__main__":
    main(sys.argv)
