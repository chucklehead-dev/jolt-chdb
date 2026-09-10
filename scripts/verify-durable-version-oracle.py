#!/usr/bin/env python3
"""Verify the checked-in golden corpus with the exact pinned chDB module."""

import hashlib
import importlib.util
import json
import pathlib
import sys
import types


def load_protocol(path: pathlib.Path):
    package = "_jolt_chdb_upstream.durable"
    root = types.ModuleType("_jolt_chdb_upstream")
    durable = types.ModuleType(package)
    errors = types.ModuleType(f"{package}.errors")
    errors.Corrupt = type("Corrupt", (Exception,), {})
    root.__path__ = []
    durable.__path__ = []
    sys.modules[root.__name__] = root
    sys.modules[package] = durable
    sys.modules[errors.__name__] = errors
    spec = importlib.util.spec_from_file_location(f"{package}.protocol", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def json_shape(value):
    if isinstance(value, tuple):
        return [json_shape(item) for item in value]
    return value


def main():
    if len(sys.argv) != 6:
        raise SystemExit(
            "usage: verify-durable-version-oracle.py "
            "CORPUS PROTOCOL_PY REPOSITORY COMMIT PATH"
        )
    corpus_path = pathlib.Path(sys.argv[1])
    protocol_path = pathlib.Path(sys.argv[2])
    corpus = json.loads(corpus_path.read_text(encoding="utf-8"))
    expected_provenance = {
        "repository": sys.argv[3],
        "commit": sys.argv[4],
        "path": sys.argv[5],
        "sha256": corpus["provenance"]["sha256"],
    }
    if corpus["provenance"] != expected_provenance:
        raise SystemExit(
            f"corpus provenance drift: {corpus['provenance']!r} "
            f"!= {expected_provenance!r}"
        )
    digest = hashlib.sha256(protocol_path.read_bytes()).hexdigest()
    expected_digest = corpus["provenance"]["sha256"]
    if digest != expected_digest:
        raise SystemExit(f"upstream protocol.py digest drift: {digest} != {expected_digest}")
    protocol = load_protocol(protocol_path)
    failures = []
    for case in corpus["parse_cases"]:
        actual = json_shape(protocol.parse_version(case["input"]))
        if actual != case["expected"]:
            failures.append(("parse", case, actual))
    for case in corpus["compare_cases"]:
        actual = protocol.version_lt(case["left"], case["right"])
        if actual != case["expected"]:
            failures.append(("compare", case, actual))
    if failures:
        for kind, case, actual in failures:
            print(f"FAIL {kind}: expected {case['expected']!r}, got {actual!r}: {case!r}")
        raise SystemExit(1)
    print(
        "verified",
        len(corpus["parse_cases"]),
        "parse and",
        len(corpus["compare_cases"]),
        "ordering cases against",
        expected_provenance["commit"],
    )


if __name__ == "__main__":
    main()
