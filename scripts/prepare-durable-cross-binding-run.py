#!/usr/bin/env python3
"""Create the immutable provenance and execution plan for one oracle run."""

import hashlib
import json
import os
import pathlib
import stat
import subprocess
import sys


def fail(message):
    raise SystemExit(f"cross-binding run preparation failed: {message}")


def git(repo, *args, text=True):
    return subprocess.check_output(
        ["git", "-C", str(repo), *args], text=text
    )


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def positive(name, raw):
    try:
        value = int(raw)
    except ValueError:
        fail(f"{name} must be a positive integer")
    if value <= 0:
        fail(f"{name} must be positive")
    return value


def current_harness_state(repo):
    head = git(repo, "rev-parse", "HEAD").strip()
    parent = git(repo, "rev-parse", "HEAD^").strip()
    tree = git(repo, "rev-parse", "HEAD^{tree}").strip()
    tracked_patch = git(repo, "diff", "--binary", "--no-ext-diff", "HEAD", "--", text=False)
    untracked = []
    for relative in sorted(
        line for line in git(repo, "ls-files", "--others", "--exclude-standard").splitlines()
        if line
    ):
        path = repo / relative
        if not path.is_file():
            fail(f"untracked harness entry is not a regular file: {relative}")
        data = path.read_bytes()
        untracked.append(
            {
                "path": relative,
                "bytes": len(data),
                "mode": format(stat.S_IMODE(path.stat().st_mode), "04o"),
                "sha256": sha256(data),
            }
        )
    status = "dirty" if tracked_patch or untracked else "clean"
    state = {
        "schema_version": 1,
        "head": head,
        "parent": parent,
        "tree": tree,
        "status": status,
        "tracked_patch": {
            "file_name": "harness-tracked.patch",
            "bytes": len(tracked_patch),
            "sha256": sha256(tracked_patch),
        },
        "untracked_files": untracked,
    }
    state["state_sha256"] = sha256(canonical(state))
    return state, tracked_patch


def write_harness_state(repo, report_dir):
    state, tracked_patch = current_harness_state(repo)
    (report_dir / "harness-tracked.patch").write_bytes(tracked_patch)
    return state


def verify_harness_state(repo, report_dir):
    try:
        expected = json.loads((report_dir / "harness-state.json").read_text())
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read prepared harness state: {error}")
    actual, _ = current_harness_state(repo)
    if actual != expected:
        fail("current harness state differs from prepared state")
    print(json.dumps({"status": "ok", "harness": actual["state_sha256"]}))


def schedule(trials):
    entries = []

    def add(phase, runtime, trial, label):
        ordinal = len(entries)
        entries.append(
            {
                "ordinal": ordinal,
                "phase": phase,
                "runtime": runtime,
                "trial": trial,
                "report_file": f"{runtime}-{label}.json",
                "time_file": f"{runtime}-{label}.time",
            }
        )

    add("prime", "rust", None, "prime")
    add("prime", "jolt", None, "prime")
    for trial in range(1, trials + 1):
        order = ("rust", "jolt") if trial % 2 else ("jolt", "rust")
        for runtime in order:
            add("measured", runtime, trial, f"trial-{trial}")
    return entries


def main():
    if len(sys.argv) == 4 and sys.argv[1] == "--verify-state":
        verify_harness_state(pathlib.Path(sys.argv[2]).resolve(),
                             pathlib.Path(sys.argv[3]).resolve())
        return
    if len(sys.argv) != 8:
        fail(
            "usage: prepare-durable-cross-binding-run.py REPO REPORT_DIR "
            "OBJECT_ID TRIALS BATCH_SIZE WARMUP_BATCHES MEASURED_BATCHES"
        )
    repo = pathlib.Path(sys.argv[1]).resolve()
    report_dir = pathlib.Path(sys.argv[2]).resolve()
    object_id = sys.argv[3]
    if not object_id or object_id in {".", ".."} or "/" in object_id or "\\" in object_id:
        fail("object id must be one safe path component")
    trials = positive("trials", sys.argv[4])
    if trials < 5:
        fail("at least five measured trials are required")
    batch_size = positive("batch size", sys.argv[5])
    warmup = positive("warmup batches", sys.argv[6])
    measured = positive("measured batches", sys.argv[7])
    report_dir.mkdir(parents=True, exist_ok=True)
    state = write_harness_state(repo, report_dir)
    state_path = report_dir / "harness-state.json"
    state_path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    plan = {
        "schema_version": 1,
        "config": {
            "object_id": object_id,
            "database": "benchmark",
            "trials": trials,
            "batch_size": batch_size,
            "warmup_batches": warmup,
            "measured_batches": measured,
            "total_rows": batch_size * (warmup + measured),
        },
        "harness_state": state,
        "schedule": schedule(trials),
    }
    plan["run_id"] = sha256(canonical(plan))
    (report_dir / "run-manifest.json").write_text(
        json.dumps(plan, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps({"status": "ok", "run_id": plan["run_id"], "harness": state["state_sha256"]}))


if __name__ == "__main__":
    main()
