#!/usr/bin/env python3
"""Run an anchored Durable recovery A'/B'/A/B/B/A release comparison.

All release archives, checksum sidecars, binaries, native artifacts, and cache
seeds are supplied by the caller.  This runner deliberately has no downloader
or installer path.  It prepares one immutable Rust fixture and launches six
fresh Jolt reader processes from the reviewed source checkout.
"""
import argparse
import copy
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
VERIFY_PATH = ROOT / "scripts" / "verify-durable-release-runtime-abba.py"
WRAPPER = pathlib.Path("/home/chuck/ai-src/tools/jolt-with-chez-10.4.1")
SCHEDULE = [
    ("A'", "A", "prime", None, 1, "A-prime.json"),
    ("B'", "B", "prime", None, 3, "B-prime.json"),
    ("A", "A", "measured", 1, 4, "A-1.json"),
    ("B", "B", "measured", 1, 7, "B-1.json"),
    ("B", "B", "measured", 2, 8, "B-2.json"),
    ("A", "A", "measured", 2, 11, "A-2.json"),
]


def fail(message):
    raise SystemExit("release-runtime Durable A'/B'/A/B/B/A runner failed: " + message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def identity(path):
    path = pathlib.Path(path)
    if not path.is_file() or path.is_symlink():
        fail(f"required regular file is missing: {path}")
    data = path.read_bytes()
    return {"file_name": path.name, "bytes": len(data), "sha256": digest(data)}


def git(checkout, *args):
    try:
        return subprocess.check_output(["git", "-C", str(checkout), *args], text=True,
                                       stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError) as error:
        fail(f"cannot inspect source checkout: {error}")


def load(path):
    try:
        return json.loads(pathlib.Path(path).read_text())
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read JSON {path}: {error}")


def write(path, value):
    pathlib.Path(path).write_text(json.dumps(value, sort_keys=True) + "\n")


def release_module():
    import importlib.util
    spec = importlib.util.spec_from_file_location("release_runtime_abba_verify", VERIFY_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def profile_and_source(profile_path, source_checkout):
    verify = release_module()
    profile_path = pathlib.Path(profile_path).resolve()
    profile = load(profile_path)
    verify.exact("reviewed production profile", profile,
                 {"schema_version", "purpose", "assurance_claim", "fixed", "conditions"})
    if profile["schema_version"] != 1 or profile["purpose"] != "reviewed release-runtime Durable profile selected before execution":
        fail("reviewed production profile schema or purpose differs")
    verify.static_fixed_identity(profile["fixed"])
    verify.exact("reviewed production profile conditions", profile["conditions"], {"A", "B"})
    verify.runtime_identity("profile condition A", profile["conditions"]["A"], "A")
    verify.runtime_identity("profile condition B", profile["conditions"]["B"], "B")
    if identity(__file__) != profile["fixed"]["runner_script"]:
        fail("runner script identity differs from reviewed production profile")
    source = pathlib.Path(source_checkout).resolve()
    claimed = profile["fixed"]["chdb"]
    if git(source, "status", "--porcelain"):
        fail("source checkout has index or working-tree dirt")
    if git(source, "rev-parse", "HEAD") != claimed["source_sha"]:
        fail("source checkout HEAD differs from reviewed production profile")
    if git(source, "rev-parse", "HEAD^{tree}") != claimed["source_tree"]:
        fail("source checkout tree differs from reviewed production profile")
    return verify, profile_path, profile, source


def verify_inputs(verify, profile, args):
    native = profile["fixed"]["native"]
    if identity(args.libchdb) != native["library"] or identity(args.native_header) != native["header"]:
        fail("supplied native library or header differs from reviewed production profile")
    for condition in ("A", "B"):
        binary = getattr(args, condition.lower() + "_binary")
        archive = getattr(args, condition.lower() + "_archive")
        sidecar = getattr(args, condition.lower() + "_sidecar")
        verify.verify_release_artifacts("condition " + condition, binary, archive, sidecar,
                                       profile["conditions"][condition])
    for seed in (args.cache_seed, args.gitlibs_seed):
        if not pathlib.Path(seed).is_dir() or pathlib.Path(seed).is_symlink():
            fail("cache seeds must be supplied real directories; this runner downloads nothing")


def clean_output(path):
    path = pathlib.Path(path).resolve()
    if path.exists() and any(path.iterdir()):
        fail("output directory must be absent or empty")
    path.mkdir(parents=True, exist_ok=True)
    return path


def copy_seed(source, destination):
    source, destination = pathlib.Path(source), pathlib.Path(destination)
    shutil.copytree(source, destination, symlinks=False)


def run(command, env=None, cwd=None):
    try:
        subprocess.run(command, check=True, env=env, cwd=cwd)
    except (OSError, subprocess.CalledProcessError) as error:
        fail(f"subprocess failed: {error}")


def verify_harness(source, reports):
    run(["python3", str(source / "scripts" / "prepare-durable-cross-binding-run.py"),
         "--verify-state", str(source), str(reports)])


def raw_receipt(raw, expected_ordinal, expected_phase):
    value = load(raw)
    required = {"schema_version", "run_id", "schedule_ordinal", "phase", "runtime", "trial",
                "process_id", "process_started_epoch_ms", "process_finished_epoch_ms", "cache_condition",
                "fixture", "recovery"}
    if set(value) != required or value["schema_version"] != 1:
        fail("Jolt recovery receipt schema differs")
    if value["schedule_ordinal"] != expected_ordinal or value["phase"] != expected_phase:
        fail("Jolt recovery receipt differs from selected source schedule")
    recovery = value["recovery"]
    if recovery.get("expected") != recovery.get("actual") or recovery.get("inventory_unchanged") is not True:
        fail("Jolt recovery did not reconcile the immutable fixture")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output")
    parser.add_argument("profile")
    parser.add_argument("source_checkout")
    parser.add_argument("libchdb")
    parser.add_argument("native_header")
    parser.add_argument("a_binary")
    parser.add_argument("a_archive")
    parser.add_argument("a_sidecar")
    parser.add_argument("b_binary")
    parser.add_argument("b_archive")
    parser.add_argument("b_sidecar")
    parser.add_argument("cache_seed")
    parser.add_argument("gitlibs_seed")
    parser.add_argument("--preflight", action="store_true")
    args = parser.parse_args()
    verify, profile_path, profile, source = profile_and_source(args.profile, args.source_checkout)
    verify_inputs(verify, profile, args)
    if args.preflight:
        print("PASS release-runtime Durable A'/B'/A/B/B/A preflight (no fixture or reader executed)")
        return
    output = clean_output(args.output)
    reports, fixture = output / "source-reports", output / "fixture-store"
    reports.mkdir()
    run(["python3", str(source / "scripts" / "prepare-durable-cross-binding-run.py"), str(source),
         str(reports), "release-runtime-abba", "5", "512", "2", "100"])
    verify_harness(source, reports)
    native_dir = pathlib.Path(args.libchdb).resolve().parent
    rust_target = output / "rust-target"
    build_env = dict(os.environ, CHDB_LIB_DIR=str(native_dir), CHDB_INCLUDE_DIR=str(native_dir),
                     BENCH_NATIVE_LIBRARY=str(pathlib.Path(args.libchdb).resolve()),
                     BENCH_NATIVE_HEADER=str(pathlib.Path(args.native_header).resolve()),
                     BENCH_HARNESS_STATE_FILE=str(reports / "harness-state.json"),
                     CARGO_TARGET_DIR=str(rust_target))
    run(["cargo", "build", "--locked", "--release", "--manifest-path",
         str(source / "bench" / "rust-durable-recovery-oracle" / "Cargo.toml")], env=build_env)
    oracle = rust_target / "release" / "jolt-chdb-rust-recovery-oracle"
    run([str(oracle), "prepare", str(fixture), "release-runtime-abba", str(reports / "run-manifest.json"),
         str(reports / "fixture.json")], env=dict(build_env, LD_LIBRARY_PATH=str(native_dir)))
    descriptor = load(reports / "fixture.json")
    fixed = copy.deepcopy(profile["fixed"])
    fixed["fixture"] = {"inventory_sha256": descriptor["inventory_sha256"],
                        "rows": descriptor["config"]["total_rows"],
                        "segments": len(descriptor["manifest"]["wal"]),
                        "expected_sha256": digest(canonical(descriptor["expected"]))}
    if fixed["fixture"]["rows"] != fixed["workload"]["rows"] or fixed["fixture"]["segments"] != fixed["workload"]["segments"]:
        fail("generated fixture differs from reviewed workload shape")
    receipt_dir, raw_dir = output / "receipts", output / "raw"
    receipt_dir.mkdir(); raw_dir.mkdir()
    provenance = verify.checked_in_profile_provenance(profile_path)
    manifest = {"schema_version": 1, "mode": "release-runtime-abba", "assurance_claim": verify.ASSURANCE,
                "fixed": fixed, "conditions": profile["conditions"], "schedule": verify.SCHEDULE,
                "profile_provenance": provenance}
    manifest["run_id"] = digest(canonical(manifest))
    write(receipt_dir / "run-manifest.json", manifest)
    seen = set()
    for outer_ordinal, (condition_label, condition, phase, observation, legacy_ordinal, receipt_name) in enumerate(SCHEDULE):
        cache, gitlibs = output / ("cache-" + condition), output / ("gitlibs-" + condition)
        if not cache.exists():
            copy_seed(args.cache_seed, cache); copy_seed(args.gitlibs_seed, gitlibs)
        binary = pathlib.Path(getattr(args, condition.lower() + "_binary")).resolve()
        describe, raw = raw_dir / (condition + "-describe.edn"), raw_dir / receipt_name
        runtime = profile["conditions"][condition]
        env = dict(os.environ, JOLT_CACHE_DIR=str(cache), JOLT_GITLIBS_DIR=str(gitlibs),
                   BENCH_JOLT_BIN=str(binary), BENCH_JOLT_SOURCE_SHA_ASSERTED=runtime["release"]["tag_commit"],
                   BENCH_JOLT_EXECUTABLE_REVISION=runtime["release"]["tag_commit"][:8], BENCH_JOLT_VERSION=runtime["version"],
                   BENCH_JOLT_DESCRIBE=str(describe), BENCH_NATIVE_LIBRARY=str(pathlib.Path(args.libchdb).resolve()),
                   BENCH_NATIVE_HEADER=str(pathlib.Path(args.native_header).resolve()), BENCH_HARNESS_STATE_FILE=str(reports / "harness-state.json"),
                   JOLT_CHDB_LIB=str(pathlib.Path(args.libchdb).resolve()), LD_LIBRARY_PATH=str(native_dir))
        with describe.open("w") as handle:
            try:
                subprocess.run([str(WRAPPER), str(binary), "-Srepro", "-Sdescribe"], check=True,
                               env=env, cwd=source, stdout=handle)
            except (OSError, subprocess.CalledProcessError) as error:
                fail(f"Jolt describe failed: {error}")
        verify_harness(source, reports)
        run([str(WRAPPER), str(binary), "-Srepro", "-M:durable-cross-binding-recovery", str(fixture),
             "release-runtime-abba", str(reports / "fixture.json"), str(reports / "run-manifest.json"),
             str(legacy_ordinal), str(raw)], env=env, cwd=source)
        verify_harness(source, reports)
        measured = raw_receipt(raw, legacy_ordinal, "prime" if phase == "prime" else "measured")
        if measured["process_id"] in seen:
            fail("Jolt recovery process was reused")
        seen.add(measured["process_id"])
        receipt = {"schema_version": 1, "run_id": manifest["run_id"], "schedule_ordinal": outer_ordinal,
                   "phase": phase, "condition": condition_label, "runtime_condition": condition,
                   "fixed": fixed, "runtime": runtime, "profile_provenance": provenance,
                   "execution": {"process_id": measured["process_id"], "started_epoch_ms": measured["process_started_epoch_ms"],
                                 "finished_epoch_ms": measured["process_finished_epoch_ms"], "outcome": "pass"}}
        receipt["receipt_id"] = digest(canonical(receipt))
        write(receipt_dir / receipt_name, receipt)
    run(["python3", str(VERIFY_PATH), str(receipt_dir), str(profile_path), args.a_binary, args.a_archive,
         args.a_sidecar, args.b_binary, args.b_archive, args.b_sidecar])
    print("PASS release-runtime Durable A'/B'/A/B/B/A run: " + str(receipt_dir))


if __name__ == "__main__":
    main()
