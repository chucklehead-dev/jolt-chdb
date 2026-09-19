#!/usr/bin/env python3
"""Run an anchored Durable recovery A'/B'/A/B/B/A release comparison.

All release archives, checksum sidecars, binaries, native artifacts, and cache
seeds are supplied by the caller.  A non-preflight run requires Bubblewrap's
network namespace and executes every dependency-resolving command inside it;
Cargo is additionally passed ``--offline --frozen``.  The runner copies the
verified executable, archive, sidecar, native library, and *actual bindgen
header* into its fresh output before running them.  It prepares one immutable
Rust fixture and launches six fresh Jolt reader processes from the reviewed
source checkout.
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
# The source reader insists on its cross-binding schedule.  The two outer
# primes therefore each use the one source Jolt-prime slot (in separate fresh
# processes), while the four outer measured observations use four real source
# Jolt-measured slots.  Do not relabel a source measured slot as a prime.
SCHEDULE = [
    ("A'", "A", "prime", None, 1, "prime", None, "jolt-prime.json", "A-prime.json"),
    ("B'", "B", "prime", None, 1, "prime", None, "jolt-prime.json", "B-prime.json"),
    ("A", "A", "measured", 1, 3, "measured", 1, "jolt-trial-1.json", "A-1.json"),
    ("B", "B", "measured", 1, 4, "measured", 2, "jolt-trial-2.json", "B-1.json"),
    ("B", "B", "measured", 2, 7, "measured", 3, "jolt-trial-3.json", "B-2.json"),
    ("A", "A", "measured", 2, 8, "measured", 4, "jolt-trial-4.json", "A-2.json"),
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


def cargo_home_identity(path):
    """Digest the offline dependency portion of a Cargo home, never its tool shims.

    A normal ``~/.cargo`` contains rustup-managed executable symlinks and may
    contain credentials.  Neither belongs in an isolated dependency seed.  The
    caller still supplies that Cargo home, but only the registry/git payload
    (plus an optional non-secret config) is copied and mounted as CARGO_HOME.
    """
    path = pathlib.Path(path)
    if not path.is_dir() or path.is_symlink():
        fail(f"required real Cargo home is missing: {path}")
    entries, total = [], 0
    for name in ("registry", "git"):
        child = path / name
        if not child.is_dir() or child.is_symlink():
            fail(f"Cargo home is missing required offline {name} seed: {child}")
        # Reconstruct the actual records so the root digest commits every path,
        # not merely two independent aggregate digests.
        for root, directories, files in os.walk(child, followlinks=False):
            root_path = pathlib.Path(root)
            directories.sort(); files.sort()
            for directory in directories:
                if (root_path / directory).is_symlink():
                    fail(f"Cargo home contains a symlink: {root_path / directory}")
            for filename in files:
                entry = root_path / filename
                if not entry.is_file() or entry.is_symlink():
                    fail(f"Cargo home contains a non-regular file: {entry}")
                data = entry.read_bytes()
                total += len(data)
                entries.append({"path": entry.relative_to(path).as_posix(),
                                "bytes": len(data), "sha256": digest(data)})
    config = path / "config.toml"
    if config.exists():
        if not config.is_file() or config.is_symlink():
            fail(f"Cargo home config is not a regular file: {config}")
        data = config.read_bytes()
        total += len(data)
        entries.append({"path": "config.toml", "bytes": len(data), "sha256": digest(data)})
    return {"files": len(entries), "bytes": total, "sha256": digest(canonical(entries))}


def verify_cargo_home_seed(seed, expected):
    actual = cargo_home_identity(seed)
    if actual != expected:
        fail("supplied Cargo home seed differs from reviewed production profile")
    return actual


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


def control_paths(profile_path):
    """Return the immutable, checked-in control inputs this runner may execute.

    The profile is the policy root for both Python programs.  Do not import the
    verifier until its byte identity and Git checkout status have been checked
    against that profile: importing a mutable verifier would make the final
    receipt claim self-referential.
    """
    profile = pathlib.Path(profile_path).resolve()
    try:
        relative = profile.relative_to(ROOT)
    except ValueError:
        fail("reviewed production profile must be checked into this repository")
    return profile, relative, pathlib.Path(__file__).resolve(), VERIFY_PATH.resolve()


def checked_in_clean(paths):
    """Require each control input to be a clean, tracked file at this HEAD."""
    relative = []
    for path in paths:
        path = pathlib.Path(path)
        if not path.is_file() or path.is_symlink():
            fail(f"release-runtime control is not a regular file: {path}")
        try:
            relative.append(path.relative_to(ROOT).as_posix())
        except ValueError:
            fail("release-runtime control is not checked into this repository")
    if git(ROOT, "status", "--porcelain"):
        fail("release-runtime control checkout has index or working-tree dirt")
    for name in relative:
        git(ROOT, "ls-files", "--error-unmatch", "--", name)
        if git(ROOT, "diff", "--name-only", "HEAD", "--", name):
            fail("release-runtime control differs from HEAD")
    return relative


def release_module(verifier_path):
    import importlib.util
    spec = importlib.util.spec_from_file_location("release_runtime_abba_verify", verifier_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def profile_and_source(profile_path, source_checkout):
    profile_path, _, runner, verifier = control_paths(profile_path)
    profile = load(profile_path)
    fixed = profile.get("fixed") if isinstance(profile, dict) else None
    if not isinstance(fixed, dict):
        fail("reviewed production profile fixed control is missing")
    if identity(runner) != fixed.get("runner_script"):
        fail("runner script identity differs from reviewed production profile")
    if identity(verifier) != fixed.get("verifier_script"):
        fail("verifier script identity differs from reviewed production profile")
    checked_in_clean((profile_path, runner, verifier))
    # Only now is code from the verifier control path imported.
    verify = release_module(verifier)
    verify.exact("reviewed production profile", profile,
                 {"schema_version", "purpose", "assurance_claim", "fixed", "conditions"})
    if profile["schema_version"] != 1 or profile["purpose"] != "reviewed release-runtime Durable profile selected before execution":
        fail("reviewed production profile schema or purpose differs")
    verify.static_fixed_identity(profile["fixed"])
    verify.exact("reviewed production profile conditions", profile["conditions"], {"A", "B"})
    verify.runtime_identity("profile condition A", profile["conditions"]["A"], "A")
    verify.runtime_identity("profile condition B", profile["conditions"]["B"], "B")
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
    header = pathlib.Path(args.native_header).resolve()
    if header.name != "chdb.h":
        fail("native header must be the exact chdb.h selected through CHDB_INCLUDE_DIR")
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
    verify_cargo_home_seed(args.cargo_home_seed, profile["fixed"]["cargo_home"])


def clean_output(path):
    path = pathlib.Path(path).resolve()
    if path.exists() and any(path.iterdir()):
        fail("output directory must be absent or empty")
    path.mkdir(parents=True, exist_ok=True)
    return path


def copy_seed(source, destination):
    source, destination = pathlib.Path(source), pathlib.Path(destination)
    shutil.copytree(source, destination, symlinks=False)


def require_offline_sandbox():
    """Require the mechanism that makes the normal run no-network, not aspirational."""
    sandbox = shutil.which("bwrap")
    if not sandbox:
        fail("Bubblewrap (bwrap) is required for a no-network normal run")
    try:
        subprocess.run([sandbox, "--unshare-net", "--die-with-parent", "--ro-bind", "/", "/",
                        "--proc", "/proc", "--dev", "/dev", "/usr/bin/true"],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except (OSError, subprocess.CalledProcessError) as error:
        fail("Bubblewrap cannot create the required no-network sandbox: " + str(error))
    return sandbox


def offline_command(sandbox, command, writable_paths, read_only_paths=()):
    """Return a no-network command with explicit writable and immutable mounts."""
    wrapped = [sandbox, "--unshare-net", "--die-with-parent", "--new-session",
               "--ro-bind", "/", "/", "--proc", "/proc", "--dev", "/dev"]
    for path in sorted({str(pathlib.Path(path).resolve()) for path in writable_paths}):
        wrapped.extend(["--bind", path, path])
    # This must follow the output bind: verified inputs live below output, and
    # a later read-only bind prevents a child from replacing them in use.
    for path in sorted({str(pathlib.Path(path).resolve()) for path in read_only_paths}):
        wrapped.extend(["--ro-bind", path, path])
    return [*wrapped, "--", *map(str, command)]


def run_offline(sandbox, command, writable_paths, read_only_paths=(), env=None, cwd=None):
    run(offline_command(sandbox, command, writable_paths, read_only_paths), env=env, cwd=cwd)


def snapshot_source(source, material, claimed, claimed_tree):
    """Clone the reviewed Git tree so the read-only runner retains Git provenance.

    ``prepare-durable-cross-binding-run.py`` deliberately reads HEAD, parent,
    and tree via Git.  A plain archive makes that contract fail (or tempts a
    future weakening).  This local, no-hardlink clone remains immutable once
    ``verified-inputs`` is remounted read-only inside Bubblewrap.
    """
    source_snapshot = material / "source"
    try:
        subprocess.run(["git", "clone", "--no-local", "--no-checkout", str(source), str(source_snapshot)],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["git", "-C", str(source_snapshot), "checkout", "--detach", claimed],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except (OSError, subprocess.CalledProcessError) as error:
        fail(f"cannot snapshot reviewed source tree: {error}")
    if (git(source, "status", "--porcelain") or git(source, "rev-parse", "HEAD") != claimed or
            git(source, "rev-parse", "HEAD^{tree}") != claimed_tree):
        fail("source checkout changed while creating immutable snapshot")
    if (git(source_snapshot, "status", "--porcelain") or
            git(source_snapshot, "rev-parse", "HEAD") != claimed or
            git(source_snapshot, "rev-parse", "HEAD^{tree}") != claimed_tree):
        fail("immutable source snapshot differs from reviewed Git provenance")
    return source_snapshot


def snapshot_cargo_home(seed, material, expected):
    cargo_home = material / "cargo-home"
    cargo_home.mkdir()
    seed = pathlib.Path(seed)
    for name in ("registry", "git"):
        shutil.copytree(seed / name, cargo_home / name, symlinks=False)
    if (seed / "config.toml").exists():
        shutil.copy2(seed / "config.toml", cargo_home / "config.toml")
    if cargo_home_identity(cargo_home) != expected:
        fail("verified Cargo home seed changed while being copied")
    return cargo_home


def snapshot_control(profile_path, material, profile):
    """Clone the verified control checkout for all post-snapshot verification.

    The benchmark's source snapshot intentionally names the older reviewed
    durable tree.  The profile, launcher and verifier instead live in this
    control checkout.  Cloning it commits the final verifier to the same clean
    HEAD as the profile, then Bubblewrap remounts it read-only.
    """
    profile_path, profile_relative, runner, verifier = control_paths(profile_path)
    control = material / "control"
    control_head, control_tree = git(ROOT, "rev-parse", "HEAD"), git(ROOT, "rev-parse", "HEAD^{tree}")
    try:
        subprocess.run(["git", "clone", "--no-local", "--no-checkout", str(ROOT), str(control)],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["git", "-C", str(control), "checkout", "--detach", control_head],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except (OSError, subprocess.CalledProcessError) as error:
        fail(f"cannot snapshot release-runtime control checkout: {error}")
    checked_in_clean((profile_path, runner, verifier))
    if git(ROOT, "rev-parse", "HEAD") != control_head or git(ROOT, "rev-parse", "HEAD^{tree}") != control_tree:
        fail("release-runtime control checkout changed while creating immutable snapshot")
    snap_profile, snap_runner, snap_verifier = (control / profile_relative,
                                                 control / "scripts" / runner.name,
                                                 control / "scripts" / verifier.name)
    if (git(control, "status", "--porcelain") or git(control, "rev-parse", "HEAD") != control_head or
            git(control, "rev-parse", "HEAD^{tree}") != control_tree):
        fail("immutable release-runtime control snapshot differs from Git provenance")
    if (identity(snap_profile) != identity(profile_path) or identity(snap_runner) != profile["fixed"]["runner_script"] or
            identity(snap_verifier) != profile["fixed"]["verifier_script"]):
        fail("immutable release-runtime control snapshot differs from reviewed profile")
    return {"root": control, "profile": snap_profile, "verifier": snap_verifier}


def execution_environment(overrides):
    """Construct a minimal process environment with no ambient credentials.

    PATH locates reviewed local tools; locale/TZ make output deterministic; a
    caller's RUSTUP_HOME is needed only for the locally installed Rust toolchain.
    Cargo configuration, tokens, wrappers, flags, JOLT overrides, and all
    unrelated variables are deliberately omitted and benchmark-specific values
    are supplied solely by ``overrides``.
    """
    allowed = ("PATH", "LANG", "LC_ALL", "TZ", "TMPDIR", "RUSTUP_HOME")
    env = {name: os.environ[name] for name in allowed if os.environ.get(name)}
    env.update(overrides)
    return env


def cargo_build_environment(output, cargo_home, native_dir, libchdb, native_header, reports, rust_target):
    """Use only the snapshotted Cargo home; ambient CARGO_HOME is never inherited."""
    return execution_environment({
        "CARGO_NET_OFFLINE": "true", "CARGO_HOME": str(cargo_home),
        "CHDB_LIB_DIR": str(native_dir), "CHDB_INCLUDE_DIR": str(native_header.parent),
        "BENCH_NATIVE_LIBRARY": str(libchdb), "BENCH_NATIVE_HEADER": str(native_header),
        "BENCH_HARNESS_STATE_FILE": str(reports / "harness-state.json"),
        "CARGO_TARGET_DIR": str(rust_target), "HOME": str(output / "home"),
    })


def snapshot_verified_inputs(args, profile, output, verify, source):
    """Copy then re-verify every byte that a subprocess may execute or bind."""
    material = output / "verified-inputs"
    material.mkdir()
    native_dir = material / "native"
    native_dir.mkdir()
    header = pathlib.Path(args.native_header).resolve()
    library = pathlib.Path(args.libchdb).resolve()
    if header.name != "chdb.h":
        fail("native header must be named chdb.h")
    copied_library, copied_header = native_dir / library.name, native_dir / "chdb.h"
    shutil.copy2(library, copied_library)
    shutil.copy2(header, copied_header)
    if (identity(copied_library) != profile["fixed"]["native"]["library"] or
            identity(copied_header) != profile["fixed"]["native"]["header"]):
        fail("verified native inputs changed while being copied")
    conditions = {}
    for condition in ("A", "B"):
        target = material / condition
        target.mkdir()
        binary = pathlib.Path(getattr(args, condition.lower() + "_binary")).resolve()
        archive = pathlib.Path(getattr(args, condition.lower() + "_archive")).resolve()
        sidecar = pathlib.Path(getattr(args, condition.lower() + "_sidecar")).resolve()
        copied_binary = target / binary.name
        copied_archive, copied_sidecar = target / archive.name, target / sidecar.name
        for source, destination in ((binary, copied_binary), (archive, copied_archive),
                                    (sidecar, copied_sidecar)):
            shutil.copy2(source, destination)
        verify.verify_release_artifacts("snapshotted condition " + condition, copied_binary,
                                       copied_archive, copied_sidecar,
                                       profile["conditions"][condition])
        conditions[condition] = {"binary": copied_binary, "archive": copied_archive,
                                 "sidecar": copied_sidecar}
    cargo_home = snapshot_cargo_home(args.cargo_home_seed, material, profile["fixed"]["cargo_home"])
    source_snapshot = snapshot_source(source, material, profile["fixed"]["chdb"]["source_sha"],
                                      profile["fixed"]["chdb"]["source_tree"])
    control = snapshot_control(args.profile, material, profile)
    return {"library": copied_library, "header": copied_header, "native_dir": native_dir,
            "source": source_snapshot, "cargo_home": cargo_home,
            "conditions": conditions, "control": control}


def run(command, env=None, cwd=None):
    try:
        subprocess.run(command, check=True, env=env, cwd=cwd)
    except (OSError, subprocess.CalledProcessError) as error:
        fail(f"subprocess failed: {error}")


def verify_harness(source, reports, sandbox=None, output=None, material=None):
    command = ["python3", str(source / "scripts" / "prepare-durable-cross-binding-run.py"),
               "--verify-state", str(source), str(reports)]
    if sandbox is None:
        run(command, env=execution_environment({}))
    else:
        run_offline(sandbox, command, [output], [material], env=execution_environment({}), cwd=source)


def raw_receipt(raw, expected_ordinal, expected_phase, expected_trial):
    value = load(raw)
    required = {"schema_version", "run_id", "schedule_ordinal", "phase", "runtime", "trial",
                "process_id", "process_started_epoch_ms", "process_finished_epoch_ms", "cache_condition",
                "fixture", "recovery"}
    if set(value) != required or value["schema_version"] != 1:
        fail("Jolt recovery receipt schema differs")
    if (value["schedule_ordinal"] != expected_ordinal or value["phase"] != expected_phase or
            value["trial"] != expected_trial):
        fail("Jolt recovery receipt differs from selected source schedule")
    recovery = value["recovery"]
    if set(recovery) != {"elapsed_ns", "rows_per_second", "expected", "actual", "inventory_unchanged"}:
        fail("Jolt recovery receipt measurement schema differs")
    rows = value["fixture"].get("recovered_rows") if isinstance(value["fixture"], dict) else None
    if not isinstance(rows, int) or rows <= 0:
        fail("Jolt recovery receipt recovered row count differs")
    if not isinstance(recovery["elapsed_ns"], int) or recovery["elapsed_ns"] <= 0:
        fail("Jolt recovery receipt elapsed measurement differs")
    calculated_rate = rows * 1e9 / recovery["elapsed_ns"]
    if (isinstance(recovery["rows_per_second"], bool) or
            not isinstance(recovery["rows_per_second"], (int, float)) or
            abs(recovery["rows_per_second"] - calculated_rate) > max(1e-9, calculated_rate * 1e-12)):
        fail("Jolt recovery receipt rows-per-second does not recompute")
    if recovery.get("expected") != recovery.get("actual") or recovery.get("inventory_unchanged") is not True:
        fail("Jolt recovery did not reconcile the immutable fixture")
    return value


def verify_observed_runtime(measured, runtime, native, binary, library, header):
    """Bind the reader's observed identities before creating an outer receipt."""
    observed = measured["runtime"]
    if not isinstance(observed, dict):
        fail("Jolt recovery receipt runtime metadata is missing")
    if (observed.get("jolt_version") != runtime["version"] or
            observed.get("jolt_source_sha_asserted") != runtime["release"]["tag_commit"] or
            observed.get("native_version") != native["version"]):
        fail("Jolt recovery observed runtime version differs from reviewed profile")
    observed_keys = {"executable": "executable", "native library": "native_library",
                     "native header": "native_header"}
    for label, actual_path, expected in (
            ("executable", binary, runtime["binary"]),
            ("native library", library, native["library"]),
            ("native header", header, native["header"])):
        observed_identity = observed.get(observed_keys[label])
        if not isinstance(observed_identity, dict):
            fail("Jolt recovery observed " + label + " identity is missing")
        actual = identity(actual_path)
        # A caller may retain a release binary under a versioned local name;
        # its byte identity, not that local basename, is what was invoked.
        if label == "executable":
            actual = {key: actual[key] for key in ("bytes", "sha256")}
            expected = {key: expected[key] for key in ("bytes", "sha256")}
            observed_identity = {key: observed_identity.get(key) for key in ("bytes", "sha256")}
        if actual != expected or observed_identity != actual:
            fail("Jolt recovery observed " + label + " identity differs from reviewed profile")


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
    parser.add_argument("cargo_home_seed", help="reviewed Cargo home seed; copied and remounted read-only")
    parser.add_argument("--preflight", action="store_true")
    args = parser.parse_args()
    verify, profile_path, profile, source = profile_and_source(args.profile, args.source_checkout)
    verify_inputs(verify, profile, args)
    if args.preflight:
        print("PASS release-runtime Durable A'/B'/A/B/B/A preflight (no fixture or reader executed)")
        return
    output = clean_output(args.output)
    sandbox = require_offline_sandbox()
    material = snapshot_verified_inputs(args, profile, output, verify, source)
    source = material["source"]
    reports, fixture = output / "source-reports", output / "fixture-store"
    reports.mkdir()
    run_offline(sandbox, ["python3", str(source / "scripts" / "prepare-durable-cross-binding-run.py"), str(source),
                          str(reports), "release-runtime-abba", "5", "512", "2", "100"],
                [output], [material], env=execution_environment({}), cwd=source)
    verify_harness(source, reports, sandbox, output, material)
    native_dir = material["native_dir"]
    libchdb, native_header = material["library"], material["header"]
    rust_target = output / "rust-target"
    build_env = cargo_build_environment(output, material["cargo_home"], native_dir, libchdb,
                                        native_header, reports, rust_target)
    (output / "home").mkdir()
    run_offline(sandbox, ["cargo", "build", "--locked", "--frozen", "--offline", "--release",
                          "--manifest-path", str(source / "bench" / "rust-durable-recovery-oracle" / "Cargo.toml")],
                [output], [material], env=build_env, cwd=source)
    oracle = rust_target / "release" / "jolt-chdb-rust-recovery-oracle"
    run_offline(sandbox, [str(oracle), "prepare", str(fixture), "release-runtime-abba",
                          str(reports / "run-manifest.json"), str(reports / "fixture.json")],
                [output], [material], env=dict(build_env, LD_LIBRARY_PATH=str(native_dir)))
    descriptor = load(reports / "fixture.json")
    fixed = copy.deepcopy(profile["fixed"])
    fixed["fixture"] = {"inventory_sha256": descriptor["inventory_sha256"],
                        "rows": descriptor["config"]["total_rows"],
                        "segments": len(descriptor["manifest"]["wal"]),
                        "expected_sha256": digest(canonical(descriptor["expected"]))}
    if fixed["fixture"]["rows"] != fixed["workload"]["rows"] or fixed["fixture"]["segments"] != fixed["workload"]["segments"]:
        fail("generated fixture differs from reviewed workload shape")
    receipt_dir, raw_dir = output / "receipts", output / "raw"
    receipt_dir.mkdir(); raw_dir.mkdir(); (receipt_dir / "raw").mkdir()
    profile_path = material["control"]["profile"]
    verifier_path = material["control"]["verifier"]
    # The snapshotted verifier owns all post-snapshot identity and receipt
    # checks; it is never reloaded from the mutable control checkout.
    verify = release_module(verifier_path)
    provenance = verify.checked_in_profile_provenance(profile_path)
    manifest = {"schema_version": 1, "mode": "release-runtime-abba", "assurance_claim": verify.ASSURANCE,
                "fixed": fixed, "conditions": profile["conditions"], "schedule": verify.SCHEDULE,
                "profile_provenance": provenance}
    manifest["run_id"] = digest(canonical(manifest))
    write(receipt_dir / "run-manifest.json", manifest)
    seen = set()
    for outer_ordinal, (condition_label, condition, phase, observation, source_ordinal,
                        source_phase, source_trial, source_report_name, receipt_name) in enumerate(SCHEDULE):
        cache, gitlibs = output / ("cache-" + condition), output / ("gitlibs-" + condition)
        if not cache.exists():
            copy_seed(args.cache_seed, cache); copy_seed(args.gitlibs_seed, gitlibs)
        binary = material["conditions"][condition]["binary"]
        describe = raw_dir / (condition + "-describe.edn")
        runtime = profile["conditions"][condition]
        env = execution_environment({
            "JOLT_CACHE_DIR": str(cache), "JOLT_GITLIBS_DIR": str(gitlibs), "BENCH_JOLT_BIN": str(binary),
            "BENCH_JOLT_SOURCE_SHA_ASSERTED": runtime["release"]["tag_commit"],
            "BENCH_JOLT_EXECUTABLE_REVISION": runtime["release"]["tag_commit"][:8],
            "BENCH_JOLT_VERSION": runtime["version"], "BENCH_JOLT_DESCRIBE": str(describe),
            "BENCH_NATIVE_LIBRARY": str(libchdb), "BENCH_NATIVE_HEADER": str(native_header),
            "BENCH_HARNESS_STATE_FILE": str(reports / "harness-state.json"), "JOLT_CHDB_LIB": str(libchdb),
            "LD_LIBRARY_PATH": str(native_dir), "HOME": str(output / "home"),
        })
        with describe.open("w") as handle:
            try:
                subprocess.run(offline_command(sandbox, [str(WRAPPER), str(binary), "-Srepro", "-Sdescribe"],
                                                [output], [material]), check=True, env=env, cwd=source, stdout=handle)
            except (OSError, subprocess.CalledProcessError) as error:
                fail(f"Jolt describe failed: {error}")
        verify_harness(source, reports, sandbox, output, material)
        raw = raw_dir / str(outer_ordinal) / source_report_name
        raw.parent.mkdir(parents=True, exist_ok=True)
        run_offline(sandbox, [str(WRAPPER), str(binary), "-Srepro", "-M:durable-cross-binding-recovery", str(fixture),
                              "release-runtime-abba", str(reports / "fixture.json"), str(reports / "run-manifest.json"),
                              str(source_ordinal), str(raw)],
                    [output], [material], env=env, cwd=source)
        verify_harness(source, reports, sandbox, output, material)
        measured = raw_receipt(raw, source_ordinal, source_phase, source_trial)
        verify_observed_runtime(measured, runtime, profile["fixed"]["native"], binary,
                                libchdb, native_header)
        if measured["process_id"] in seen:
            fail("Jolt recovery process was reused")
        seen.add(measured["process_id"])
        copied_raw = receipt_dir / "raw" / receipt_name
        shutil.copy2(raw, copied_raw)
        measurement = {"elapsed_ns": measured["recovery"]["elapsed_ns"],
                       "rows_per_second": measured["recovery"]["rows_per_second"],
                       "recovered_rows": measured["fixture"]["recovered_rows"]}
        receipt = {"schema_version": 1, "run_id": manifest["run_id"], "schedule_ordinal": outer_ordinal,
                   "phase": phase, "condition": condition_label, "runtime_condition": condition,
                   "fixed": fixed, "runtime": runtime, "profile_provenance": provenance,
                   "execution": {"process_id": measured["process_id"], "started_epoch_ms": measured["process_started_epoch_ms"],
                                 "finished_epoch_ms": measured["process_finished_epoch_ms"], "outcome": "pass"}}
        receipt["raw_receipt"] = {"path": "raw/" + receipt_name,
                                  "identity": verify.file_identity(copied_raw), "measurement": measurement}
        receipt["receipt_id"] = digest(canonical(receipt))
        write(receipt_dir / receipt_name, receipt)
    summary = verify.expected_summary(manifest, receipt_dir)
    summary["summary_id"] = digest(canonical(summary))
    write(receipt_dir / "summary.json", summary)
    # Recheck the profile and the immutable execution copies before issuing the
    # final claim.  The original caller paths cannot relabel what was invoked.
    verify.validate_production_profile(profile_path, load(receipt_dir / "run-manifest.json"))
    for condition in ("A", "B"):
        copied = material["conditions"][condition]
        verify.verify_release_artifacts("final snapshotted condition " + condition,
                                       copied["binary"], copied["archive"], copied["sidecar"],
                                       profile["conditions"][condition])
    run(["python3", str(verifier_path), str(receipt_dir), str(profile_path),
         str(material["conditions"]["A"]["binary"]), str(material["conditions"]["A"]["archive"]),
         str(material["conditions"]["A"]["sidecar"]), str(material["conditions"]["B"]["binary"]),
         str(material["conditions"]["B"]["archive"]), str(material["conditions"]["B"]["sidecar"]),
         "--sandbox-bwrap", sandbox], env=execution_environment({}))
    print("PASS release-runtime Durable A'/B'/A/B/B/A run: " + str(receipt_dir))


if __name__ == "__main__":
    main()
