#!/usr/bin/env bash
# Extract data at an exact Git revision; never execute that revision's scripts.
set -euo pipefail
[[ $# == 2 ]] || exit 2
exec python3 - "$1" "$2" <<'PY'
import hashlib
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile

repo, revision = sys.argv[1:]
def git(*args):
    return subprocess.check_output(["git", "-C", repo, *args], timeout=10)

docs = ["formal/quint/durable-head-cas.md",
        "formal/quint/durable-writer-lifecycle.md",
        "formal/quint/native-process-lifecycle.md",
        "formal/quint/durable-persistence-observation.md",
        "formal/quint/durable-file-wal-spool.md",
        "formal/quint/buffered-publication.md"]
models = [name + suffix + ".qnt" for name in (
    "durableHeadCas", "durablePublicationAck", "durableWriterBoundary",
    "durableLeaseTime", "durableEngineMetadata", "durableWriterLifecycle",
    "nativeProcessLifecycle") for suffix in ("", "Test")]
models += [name + suffix + ".qnt" for name in (
    "durablePersistenceObservation",) for suffix in ("", "Test")]
models += [name + suffix + ".qnt" for name in (
    "durableFileWalSpool",) for suffix in ("", "Test")]
models += ["bufferedPublication.qnt"]
outputs = {"target/formal/quint/" + name for name in models}
required = [
    ".github/workflows/durable-head-quint.yml",
    "scripts/check-durable-head-quint.sh",
    "scripts/check-durable-file-wal-spool-quint.sh",
    "scripts/check-buffered-publication-quint.sh",
    "scripts/check-durable-head-itf-corpus.sh",
    "scripts/generate-durable-head-itf.sh",
    "scripts/generate-durable-engine-metadata-itf.sh",
    "scripts/generate-native-process-lifecycle-itf.sh",
    "scripts/durable-head-itf-commands.jq",
    "scripts/durable-head-itf-coverage.jq",
    "scripts/classify-durable-model-paths.sh",
]
# Optional only on the initial pre-implementation revision. Addition changes
# inventory/hash, requesting a conservative first exhaustive qualification.
optional = "scripts/fingerprint-durable-model-inputs.sh"
tool = shutil.which("lmt")
if not tool:
    raise RuntimeError("missing pinned tangler")
metadata = subprocess.check_output(["go", "version", "-m", tool], timeout=10).decode()
if not re.search(r"(?m)^\s*mod\s+github.com/driusan/lmt\s+v0\.0\.0-20210421124901-62fe18f2f6a6\s", metadata):
    raise RuntimeError("tangler version mismatch")
tool_hash = hashlib.sha256(pathlib.Path(tool).read_bytes()).hexdigest()
tree = git("ls-tree", "-rz", "--full-tree", revision).split(b"\0")
blobs = {}
for entry in tree:
    if not entry:
        continue
    description, path = entry.split(b"\t", 1)
    mode, kind, oid = description.decode().split()
    path = path.decode()
    if path.startswith("formal/") or path in required or path == optional:
        if mode != "100644" and mode != "100755":
            raise RuntimeError("nonregular effective input")
        if kind != "blob":
            raise RuntimeError("invalid effective input")
        blobs[path] = git("cat-file", "blob", oid)
if any(path not in blobs for path in docs + required):
    raise RuntimeError("missing effective input")
file_block = re.compile(r"^`{3,}\s?([\w\+]+)\s+([\w\.\-/]+)\s*([+][=])?$")
fingerprint = hashlib.sha256()
def include(path, data):
    fingerprint.update(path.encode() + b"\0" + str(len(data)).encode() + b"\0" + data)
include("fingerprint-schema", b"1")
include("pinned-lmt-binary", tool_hash.encode())
with tempfile.TemporaryDirectory(prefix="durable-effective-model.") as directory:
    root = pathlib.Path(directory)
    (root / "target/formal/quint").mkdir(parents=True)
    for path in docs:
        data = blobs[path]
        if len(data) > 4 * 1024 * 1024:
            raise RuntimeError("oversized literate source")
        for line in data.decode().splitlines():
            # lmt ProcessFile strips a fence's indentation; parseHeader then
            # strings.TrimSpace before applying the exact fileBlockRe below.
            match = file_block.fullmatch(line.strip())
            if match and (match[1] != "quint" or match[2] not in outputs or match[3] != "+="):
                raise RuntimeError("unknown extraction output")
        destination = root / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        subprocess.run([tool, path], cwd=root, timeout=20, check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    actual = set()
    for path in (root / "target").rglob("*"):
        if path.is_symlink():
            raise RuntimeError("symlink extraction output")
        if path.is_file():
            actual.add(path.relative_to(root).as_posix())
    if actual != outputs:
        raise RuntimeError("extraction inventory mismatch")
    for path in sorted(actual):
        data = (root / path).read_bytes()
        if len(data) > 4 * 1024 * 1024:
            raise RuntimeError("oversized extraction output")
        include(path, data)
for path in sorted(blobs):
    if path not in docs:
        include(path, blobs[path])
print(fingerprint.hexdigest())
PY
