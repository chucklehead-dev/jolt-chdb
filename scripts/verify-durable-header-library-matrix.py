#!/usr/bin/env python3
"""Validate the Linux x86-64 Durable header/library qualification matrix."""

import copy
import hashlib
import json
import pathlib
import subprocess
import sys


DURABLE_SYMBOLS = {
    "chdb_backup_database_n",
    "chdb_classify_query_n",
    "chdb_restore_database_n",
    "chdb_version",
}


def fail(message):
    raise RuntimeError(message)


def exact(label, expected, value):
    if not isinstance(value, dict) or set(value) != set(expected):
        fail(f"{label} has missing or unknown fields")
    return value


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
    path = pathlib.Path(path)
    if not path.is_file():
        fail(f"artifact is not a file: {path}")
    return {"file_name": path.name, "bytes": path.stat().st_size, "sha256": digest(path)}


def validate(matrix, roots):
    exact("matrix", {"schema_version", "scope", "releases", "archive_cells",
                     "header_library_cells"}, matrix)
    exact("scope", {"os", "arch", "provider_compatibility"}, matrix["scope"])
    if matrix["schema_version"] != 1 or matrix["scope"] != {
        "os": "linux", "arch": "x86_64", "provider_compatibility": False
    }:
        fail("matrix scope differs from Linux x86-64 protocol qualification")
    if set(matrix["releases"]) != {"rc2", "release"}:
        fail("matrix must contain exactly the rc2 and release artifacts")
    for name, release in matrix["releases"].items():
        exact(name, {"version", "tag", "commit", "archive", "library", "header"}, release)
        for kind in ("archive", "library", "header"):
            exact(f"{name} {kind}", {"file_name", "bytes", "sha256"}, release[kind])
        root = pathlib.Path(roots[name])
        if identity(root / "libchdb.so") != release["library"]:
            fail(f"{name} library identity differs")
        if identity(root / "chdb.h") != release["header"]:
            fail(f"{name} header identity differs")
        text = (root / "chdb.h").read_text()
        if f'#define CHDB_VERSION "{release["version"]}"' not in text:
            fail(f"{name} header version differs")
        if any(symbol not in text for symbol in DURABLE_SYMBOLS):
            fail(f"{name} header lacks a Durable V1 declaration")
        symbols = subprocess.run(
            ["nm", "-D", "--defined-only", str(root / "libchdb.so")],
            check=True, text=True, capture_output=True,
        ).stdout
        available = {line.split()[-1] for line in symbols.splitlines() if line.split()}
        if not DURABLE_SYMBOLS <= available:
            fail(f"{name} library lacks a Durable V1 symbol")
    archive_expected = {
        ("rc2", "rc2"): "accept",
        ("rc2", "release"): "accept",
        ("release", "release"): "accept",
        ("release", "rc2"): "refuse",
    }
    archive_actual = {}
    for cell in matrix["archive_cells"]:
        exact("archive cell", {"id", "producer", "reader", "expected", "reason"}, cell)
        pair = (cell["producer"], cell["reader"])
        if pair in archive_actual or pair not in archive_expected:
            fail(f'{cell["id"]} is a duplicate or unknown archive cell')
        if cell["expected"] != archive_expected[pair]:
            fail(f'{cell["id"]} has the wrong compatibility disposition')
        archive_actual[pair] = cell["expected"]
    if archive_actual != archive_expected:
        fail("archive matrix is incomplete")
    header_pairs = set()
    for cell in matrix["header_library_cells"]:
        exact("header/library cell", {"id", "header", "library", "expected", "reason"}, cell)
        pair = (cell["header"], cell["library"])
        if pair in header_pairs or not set(pair) <= {"rc2", "release"}:
            fail(f'{cell["id"]} is a duplicate or unknown header/library cell')
        header_pairs.add(pair)
        matching = cell["header"] == cell["library"]
        expected = "accept" if matching else "refuse"
        if cell["expected"] != expected:
            fail(f'{cell["id"]} expected result does not fail closed on mixed provenance')
    if len(header_pairs) != 4:
        fail("header/library matrix is incomplete")
    return matrix


def compile_matching(cell, matrix, roots, output):
    release = matrix["releases"][cell["header"]]
    root = pathlib.Path(roots[cell["header"]]).resolve()
    cell_root = output / cell["id"]
    cell_root.mkdir(parents=True)
    source = cell_root / "probe.c"
    binary = cell_root / "probe"
    source.write_text(
        '#include <stdio.h>\n#include "chdb.h"\n'
        'int main(void) { printf("%s\\n%s\\n", CHDB_VERSION, chdb_version()); return 0; }\n'
    )
    subprocess.run([
        "cc", "-std=c11", "-Wall", "-Wextra", "-Werror", f"-I{root}", str(source),
        f"-L{root}", f"-Wl,-rpath,{root}", "-lchdb", "-o", str(binary),
    ], check=True)
    lines = subprocess.run([str(binary)], check=True, text=True, capture_output=True).stdout.splitlines()
    if lines != [release["version"], release["version"]]:
        fail(f'{cell["id"]} compile-time and runtime versions differ: {lines!r}')
    return {"id": cell["id"], "expected": "accept", "actual": "accept",
            "reason": cell["reason"], "header_version": lines[0], "runtime_version": lines[1]}


def main(argv):
    if len(argv) != 5:
        raise SystemExit("usage: MATRIX RC2_DIR RELEASE_DIR OUTPUT_DIR")
    matrix_path, rc2, release, output = map(pathlib.Path, argv[1:])
    output.mkdir(parents=True, exist_ok=True)
    matrix = json.loads(matrix_path.read_text())
    roots = {"rc2": rc2.resolve(), "release": release.resolve()}
    validate(matrix, roots)

    for cell in matrix["archive_cells"]:
        print(f'PLAN archive {cell["id"]}: {cell["expected"]} - {cell["reason"]}')
    for cell in matrix["header_library_cells"]:
        print(f'PLAN header-library {cell["id"]}: {cell["expected"]} - {cell["reason"]}')
    results = []
    for cell in matrix["header_library_cells"]:
        if cell["expected"] == "refuse":
            results.append({"id": cell["id"], "expected": "refuse", "actual": "refuse",
                            "reason": cell["reason"], "native_execution": False})
        else:
            results.append(compile_matching(cell, matrix, roots, output))

    for label, mutant in (
        ("wrong-header-identity", copy.deepcopy(matrix)),
        ("wrong-library-identity", copy.deepcopy(matrix)),
    ):
        if label == "wrong-header-identity":
            mutant["releases"]["rc2"]["header"]["sha256"] = "0" * 64
        else:
            mutant["releases"]["release"]["library"]["sha256"] = "0" * 64
        try:
            validate(mutant, roots)
        except RuntimeError:
            results.append({"id": label, "expected": "refuse", "actual": "refuse",
                            "native_execution": False})
        else:
            fail(f"{label} mutant was accepted")
    report = {"schema_version": 1, "scope": matrix["scope"], "results": results}
    (output / "header-library-report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    print("all Durable header/library matrix checks passed")


if __name__ == "__main__":
    try:
        main(sys.argv)
    except (RuntimeError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"Durable header/library matrix failed: {error}")
