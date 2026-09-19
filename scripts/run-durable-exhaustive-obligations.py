#!/usr/bin/env python3
"""Validate and time the established Durable exhaustive Quint obligations.

This is deliberately separate from check-durable-head-quint.sh.  It records
the current 13 corrected invocations (24 properties) and 20 independent red
controls without changing the existing CI command or its scheduling policy.
"""
import argparse
import hashlib
import json
import pathlib
import re
import shutil
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_INVENTORY = ROOT / "formal/quint/durable-exhaustive-obligations.json"
CANONICAL_ROWS_SHA256 = "f8b7473c3f58274c7ecfc3be0ad61bae668fa2991d8a38d0108611ef4483e708"
REQUIRED_TOOLS = {"quint": "0.32.0", "apalache": "0.56.1"}
REQUIRED_POSITIVE_PROPERTIES = 24
REQUIRED_MUTANTS = 20
REQUIRED_CORRECTED_INVOCATIONS = 13
LMT_MODULE_VERSION = "v0.0.0-20210421124901-62fe18f2f6a6"
LITERATE_SOURCES = (
    "formal/quint/durable-head-cas.md",
    "formal/quint/durable-writer-lifecycle.md",
    "formal/quint/native-process-lifecycle.md",
)
MUTANT_VIOLATION_RE = re.compile(r"(?m)^\[violation\] Found an issue")


def fail(message):
    raise ValueError(message)


def canonical_rows_digest(rows):
    encoded = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def load_inventory(path):
    try:
        document = json.loads(pathlib.Path(path).read_text())
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read obligation inventory: {error}")
    validate_inventory(document)
    return document


def validate_inventory(document):
    if set(document) != {"schema", "tools", "obligations"}:
        fail("inventory top-level shape changed")
    if document["schema"] != 1:
        fail("inventory schema changed")
    if document["tools"] != REQUIRED_TOOLS:
        fail("inventory tool pins changed")
    rows = document["obligations"]
    if not isinstance(rows, list):
        fail("inventory obligations must be a list")
    if canonical_rows_digest(rows) != CANONICAL_ROWS_SHA256:
        fail("corrected or mutant obligation row changed, removed, or added")
    required = {"kind", "id", "model", "main", "invariants", "max_steps", "itf"}
    identities = set()
    positive_properties = 0
    mutants = 0
    corrected = 0
    itfs = set()
    for row in rows:
        if set(row) != required:
            fail("obligation row shape changed")
        kind = row["kind"]
        if kind not in {"corrected", "mutant"}:
            fail("unknown obligation kind")
        identity = (kind, row["id"])
        if identity in identities or not isinstance(row["id"], str) or not row["id"]:
            fail("duplicate or invalid obligation id")
        identities.add(identity)
        if not (isinstance(row["model"], str) and row["model"].startswith("target/formal/quint/")
                and row["model"].endswith(".qnt")):
            fail("invalid obligation model path")
        if not isinstance(row["main"], str) or not row["main"]:
            fail("invalid obligation main")
        if not (isinstance(row["invariants"], list) and row["invariants"]
                and all(isinstance(item, str) and item for item in row["invariants"])
                and len(row["invariants"]) == len(set(row["invariants"]))):
            fail("invalid obligation invariant set")
        if not isinstance(row["max_steps"], int) or row["max_steps"] <= 0:
            fail("invalid obligation bound")
        if kind == "corrected":
            corrected += 1
            positive_properties += len(row["invariants"])
            if row["itf"] is not None:
                fail("corrected obligation must not request a counterexample ITF")
        else:
            mutants += 1
            if len(row["invariants"]) != 1:
                fail("mutant obligation must keep one independent invariant")
            if not (isinstance(row["itf"], str) and row["itf"].startswith("target/formal/quint/")
                    and row["itf"].endswith(".itf.json") and row["itf"] not in itfs):
                fail("mutant obligation has missing, malformed, or duplicate ITF requirement")
            itfs.add(row["itf"])
    if corrected != REQUIRED_CORRECTED_INVOCATIONS:
        fail("corrected invocation count changed")
    if positive_properties != REQUIRED_POSITIVE_PROPERTIES:
        fail("corrected positive-property count changed")
    if mutants != REQUIRED_MUTANTS:
        fail("independent mutant-control count changed")


def run_command(command, log_path, time_binary):
    metrics_path = log_path.with_suffix(".time")
    timed = [time_binary, "-f", "%e\\t%M", "-o", str(metrics_path), "--", *command]
    with log_path.open("w") as log:
        completed = subprocess.run(timed, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                   text=True, check=False)
    try:
        # GNU time prefixes its own nonzero-exit diagnostic before our format
        # line. Mutants are intentionally nonzero, so select the final exact
        # scalar record instead of treating a successful red control as a
        # measurement failure.
        records = [line for line in metrics_path.read_text().splitlines()
                   if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?\t[0-9]+", line)]
        if len(records) != 1:
            fail(f"cannot parse one timing/RSS result for {log_path.name}")
        wall_seconds, peak_rss_kib = records[0].split("\t")
        return completed.returncode, float(wall_seconds), int(peak_rss_kib)
    except (OSError, ValueError) as error:
        fail(f"cannot read timing/RSS result for {log_path.name}: {error}")


def require_pinned_lmt(lmt, go="go"):
    lmt_path = shutil.which(lmt) if pathlib.Path(lmt).name == lmt else lmt
    if not lmt_path:
        fail("lmt is required; install github.com/driusan/lmt@62fe18f2f6a6e11c158ff2b2209e1082a4fcd59c")
    metadata = subprocess.run([go, "version", "-m", lmt_path], cwd=ROOT, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if metadata.returncode != 0 or not re.search(
            rf"(?m)^\s*mod\s+github\.com/driusan/lmt\s+{re.escape(LMT_MODULE_VERSION)}(?:\s|$)",
            metadata.stdout):
        fail("installed lmt differs from the pinned Durable tangler")
    return lmt_path


def tangle_literate_sources(lmt):
    for source in LITERATE_SOURCES:
        if not (ROOT / source).is_file():
            fail(f"required literate source is absent: {source}")
    for source in LITERATE_SOURCES:
        completed = subprocess.run([lmt, source], cwd=ROOT, text=True, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, check=False)
        if completed.returncode != 0:
            fail(f"cannot tangle required literate source: {source}")


def run_obligations(document, output, quint, time_binary, inventory_path=DEFAULT_INVENTORY,
                    lmt="lmt"):
    version = subprocess.run([quint, "--version"], cwd=ROOT, text=True,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if version.returncode != 0 or version.stdout.strip() != document["tools"]["quint"]:
        fail("installed Quint version differs from the obligation inventory")
    tangle_literate_sources(require_pinned_lmt(lmt))
    output.mkdir(parents=True, exist_ok=False)
    results = []
    for ordinal, row in enumerate(document["obligations"], start=1):
        model = ROOT / row["model"]
        if not model.is_file():
            fail(f"tangled model is absent: {row['model']}")
        log_path = output / f"{ordinal:02d}-{row['kind']}-{row['id']}.log"
        command = [quint, "verify", str(model), "--main", row["main"], "--invariants",
                   *row["invariants"], "--max-steps", str(row["max_steps"]), "--backend",
                   "apalache", "--apalache-version", document["tools"]["apalache"], "--verbosity", "1"]
        if row["itf"] is not None:
            itf = ROOT / row["itf"]
            itf.parent.mkdir(parents=True, exist_ok=True)
            # A retained witness must never let a mutant pass without producing
            # the required counterexample in this invocation.
            if itf.exists():
                if not itf.is_file():
                    fail(f"mutant ITF output is not a file: {row['itf']}")
                itf.unlink()
            command.extend(["--out-itf", str(itf)])
        status, wall_seconds, peak_rss_kib = run_command(command, log_path, time_binary)
        log = log_path.read_text()
        expected = "success" if row["kind"] == "corrected" else "violation"
        if (expected == "success" and status != 0) or (
                expected == "violation" and (status == 0 or not MUTANT_VIOLATION_RE.search(log))):
            fail(f"{row['kind']} obligation failed its expected outcome: {row['id']}")
        if row["itf"] is not None and not (ROOT / row["itf"]).is_file():
            fail(f"mutant did not produce required ITF: {row['id']}")
        results.append({"id": row["id"], "kind": row["kind"], "model": row["model"],
                        "main": row["main"], "invariants": row["invariants"],
                        "max_steps": row["max_steps"], "itf": row["itf"],
                        "wall_seconds": wall_seconds, "peak_rss_kib": peak_rss_kib,
                        "outcome": expected, "log": log_path.name})
    try:
        inventory_reference = str(inventory_path.resolve().relative_to(ROOT))
    except ValueError:
        inventory_reference = str(inventory_path.resolve())
    report = {"schema": 1, "tools": document["tools"], "inventory": inventory_reference,
              "obligations": results}
    (output / "obligation-timings.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=pathlib.Path, default=DEFAULT_INVENTORY)
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--quint", default="quint")
    parser.add_argument("--lmt", default="lmt")
    parser.add_argument("--time", default="/usr/bin/time")
    parser.add_argument("--check", action="store_true", help="validate only; do not invoke Quint")
    args = parser.parse_args(argv)
    document = load_inventory(args.inventory)
    if args.check:
        print("Durable exhaustive obligation inventory: 24 corrected properties, 20 independent mutants")
        return 0
    if args.output is None:
        parser.error("--output is required when executing obligations")
    if args.output.exists():
        parser.error("--output must not already exist")
    report = run_obligations(document, args.output, args.quint, args.time, args.inventory, args.lmt)
    print(f"Durable exhaustive obligations: {len(report['obligations'])} executions recorded in {args.output}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
