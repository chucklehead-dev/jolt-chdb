#!/usr/bin/env python3
"""Fail-closed verifier for a release-runtime Durable recovery A'/B'/A/B/B/A corpus.

This is deliberately independent of the source-provider A/B/B/A harness.  It
records two named *release binary* conditions and binds every run receipt to
one of them.  It checks receipt structure offline; when binaries are supplied,
it additionally hashes each invoked file and checks its ``--version`` banner.

The receipt's assurance claim is intentionally limited to release-reference
plus archive-integrity provenance.  It is not a signed source/build chain,
attestation, reproducibility, tail-latency, S3, or general-throughput claim.
"""
import hashlib
import json
import math
import os
import pathlib
import re
import stat
import subprocess
import sys
import tarfile

SHA256 = re.compile(r"[0-9a-f]{64}")
GIT_SHA = re.compile(r"[0-9a-f]{40}")
ASSURANCE = ("github-release-reference-and-archive-integrity; no signed "
             "source/build chain, artifact attestation, or reproducibility proof")
ROOT = pathlib.Path(__file__).resolve().parents[1]
SCHEDULE = [
    {"ordinal": 0, "phase": "prime", "condition": "A'", "runtime_condition": "A", "receipt_file": "A-prime.json"},
    {"ordinal": 1, "phase": "prime", "condition": "B'", "runtime_condition": "B", "receipt_file": "B-prime.json"},
    {"ordinal": 2, "phase": "measured", "condition": "A", "runtime_condition": "A", "receipt_file": "A-1.json"},
    {"ordinal": 3, "phase": "measured", "condition": "B", "runtime_condition": "B", "receipt_file": "B-1.json"},
    {"ordinal": 4, "phase": "measured", "condition": "B", "runtime_condition": "B", "receipt_file": "B-2.json"},
    {"ordinal": 5, "phase": "measured", "condition": "A", "runtime_condition": "A", "receipt_file": "A-2.json"},
]
RAW_SOURCE_SCHEDULE = {
    0: (1, "prime", None), 1: (1, "prime", None),
    2: (3, "measured", 1), 3: (4, "measured", 2),
    4: (7, "measured", 3), 5: (8, "measured", 4),
}
RECEIPT_FILES = {entry["receipt_file"] for entry in SCHEDULE}
RAW_DIRECTORY = "raw"
SUMMARY_FILE = "summary.json"
RECEIPT_DIRECTORY_FILES = RECEIPT_FILES | {"run-manifest.json", SUMMARY_FILE, RAW_DIRECTORY}


def fail(message):
    raise SystemExit("release-runtime Durable A'/B'/A/B/B/A verification failed: " + message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def load(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read {path}: {error}")


def git_output(args, message):
    """Return one exact Git command's stdout or fail without weakening proof."""
    try:
        return subprocess.check_output(
            ["git", "-C", str(ROOT), *args], text=True, stderr=subprocess.DEVNULL
        ).strip()
    except (OSError, subprocess.CalledProcessError) as error:
        fail(f"cannot establish reviewed production profile Git identity: {message}: {error}")


def checked_in_profile_provenance(path):
    """Return the HEAD-bound identity of an unchanged tracked profile.

    The profile must name an immutable blob in the current checkout's HEAD.
    ``status --porcelain -z`` is deliberately consumed as NUL-delimited bytes:
    a pathname can contain whitespace, newlines, or a rename's second pathname,
    but any record for this path means either index or worktree dirt and is
    rejected before its bytes are used as evidence.
    """
    supplied_profile = pathlib.Path(path).absolute()
    if supplied_profile.is_symlink():
        fail("reviewed production profile must be a regular checked-in file")
    profile = supplied_profile.resolve()
    if profile.is_symlink():
        fail("reviewed production profile must be a regular checked-in file")
    try:
        relative = profile.relative_to(ROOT)
    except ValueError:
        fail("reviewed production profile must be checked into this repository")
    if not profile.is_file():
        fail("reviewed production profile is missing")
    relative_name = relative.as_posix()

    # --error-unmatch rejects both untracked and ignored-but-present files.
    git_output(["ls-files", "--error-unmatch", "--", relative_name],
               "profile is not tracked")
    try:
        porcelain = subprocess.check_output(
            ["git", "-C", str(ROOT), "status", "--porcelain=v1", "-z",
             "--untracked-files=all", "--", relative_name],
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        fail(f"cannot establish reviewed production profile Git identity: profile status: {error}")
    # Splitting rather than line parsing keeps v1 rename/copy records and unusual
    # names fail-closed.  We do not need to interpret X/Y: any record is dirt.
    if any(record for record in porcelain.split(b"\0")):
        fail("reviewed production profile has index or working-tree dirt")

    head = git_output(["rev-parse", "--verify", "HEAD^{commit}"], "HEAD is unavailable")
    tree = git_output(["rev-parse", "--verify", "HEAD^{tree}"], "HEAD tree is unavailable")
    blob = git_output(["rev-parse", "--verify", f"HEAD:{relative_name}"],
                      "profile blob is absent from HEAD")
    for label, value in (("profile HEAD", head), ("profile tree", tree),
                         ("profile blob", blob)):
        sha(label, value, re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}"))
    return {"path": relative_name, "blob_sha": blob, "head_sha": head, "tree_sha": tree}


def profile_provenance(value, label="production profile provenance"):
    exact(label, value, {"path", "blob_sha", "head_sha", "tree_sha"})
    if (not isinstance(value["path"], str) or not value["path"] or
            pathlib.PurePosixPath(value["path"]).is_absolute() or
            ".." in pathlib.PurePosixPath(value["path"]).parts):
        fail(f"{label} path is invalid")
    object_sha = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
    sha(label + " blob SHA", value["blob_sha"], object_sha)
    sha(label + " HEAD SHA", value["head_sha"], object_sha)
    sha(label + " tree SHA", value["tree_sha"], object_sha)
    return value


def validate_receipt_directory(receipts):
    """Require the manifest, summary, and six named outer/raw receipts."""
    if receipts.is_symlink() or not receipts.is_dir():
        fail("receipt directory is not a real directory")
    try:
        entries = list(receipts.iterdir())
    except OSError as error:
        fail(f"cannot inspect receipt directory: {error}")
    names = {entry.name for entry in entries}
    if names != RECEIPT_DIRECTORY_FILES:
        fail("receipt directory entry set is not exact")
    for entry in entries:
        try:
            mode = entry.lstat().st_mode
        except OSError as error:
            fail(f"cannot inspect receipt entry {entry.name}: {error}")
        if entry.name == RAW_DIRECTORY:
            if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
                fail("raw receipt corpus must be a real directory")
            raw_names = {item.name for item in entry.iterdir()}
            if raw_names != RECEIPT_FILES:
                fail("raw receipt corpus entry set is not exact")
            for raw in entry.iterdir():
                raw_mode = raw.lstat().st_mode
                if stat.S_ISLNK(raw_mode) or not stat.S_ISREG(raw_mode):
                    fail("raw receipt corpus entries must be regular named files")
        elif stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
            fail("receipt directory entries must be regular named files")


def exact(label, value, keys):
    if not isinstance(value, dict) or set(value) != set(keys):
        fail(f"{label} has missing or unknown fields")
    return value


def sha(label, value, pattern=SHA256):
    if not isinstance(value, str) or not pattern.fullmatch(value):
        fail(f"{label} is not an exact SHA")


def positive(label, value):
    if not isinstance(value, int) or value <= 0:
        fail(f"{label} is not positive")


def identity(label, value):
    exact(label, value, {"file_name", "bytes", "sha256"})
    if not isinstance(value["file_name"], str) or not value["file_name"]:
        fail(f"{label} file name is missing")
    positive(f"{label} bytes", value["bytes"])
    sha(f"{label} SHA-256", value["sha256"])
    return value


STATIC_FIXED_KEYS = {"chdb", "runner_script", "data_json", "provider", "workload", "native", "cargo_home"}


def directory_identity(label, value):
    """Validate the checked-in digest of an immutable dependency seed."""
    exact(label, value, {"files", "bytes", "sha256"})
    positive(f"{label} files", value["files"])
    positive(f"{label} bytes", value["bytes"])
    sha(f"{label} SHA-256", value["sha256"])
    return value


def static_fixed_identity(value, label="fixed workload profile"):
    exact(label, value, STATIC_FIXED_KEYS)
    exact("fixed chDB", value["chdb"], {"source_sha", "source_tree"})
    sha("fixed chDB source SHA", value["chdb"]["source_sha"], GIT_SHA)
    sha("fixed chDB source tree", value["chdb"]["source_tree"], GIT_SHA)
    identity("fixed runner script", value["runner_script"])
    exact("fixed data.json", value["data_json"], {"source_sha", "namespace"})
    sha("fixed data.json source SHA", value["data_json"]["source_sha"], GIT_SHA)
    identity("fixed data.json namespace", value["data_json"]["namespace"])
    exact("fixed provider", value["provider"], {"source_sha", "namespace"})
    sha("fixed provider source SHA", value["provider"]["source_sha"], GIT_SHA)
    identity("fixed provider namespace", value["provider"]["namespace"])
    exact("fixed workload", value["workload"], {"rows", "segments", "generator"})
    positive("fixed workload rows", value["workload"]["rows"])
    positive("fixed workload segments", value["workload"]["segments"])
    identity("fixed workload generator", value["workload"]["generator"])
    exact("fixed native", value["native"], {"version", "library", "header"})
    if not isinstance(value["native"]["version"], str) or not value["native"]["version"]:
        fail("fixed native version is missing")
    identity("fixed native library", value["native"]["library"])
    identity("fixed native header", value["native"]["header"])
    directory_identity("fixed Cargo home", value["cargo_home"])
    return value


def fixed_identity(value):
    exact("fixed identity", value, STATIC_FIXED_KEYS | {"fixture"})
    static_fixed_identity({key: value[key] for key in STATIC_FIXED_KEYS})
    exact("fixed fixture", value["fixture"], {"inventory_sha256", "rows", "segments", "expected_sha256"})
    sha("fixed fixture inventory", value["fixture"]["inventory_sha256"])
    sha("fixed fixture expected aggregate", value["fixture"]["expected_sha256"])
    positive("fixed fixture rows", value["fixture"]["rows"])
    positive("fixed fixture segments", value["fixture"]["segments"])
    if (value["fixture"]["rows"] != value["workload"]["rows"] or
            value["fixture"]["segments"] != value["workload"]["segments"]):
        fail("generated fixture shape differs from fixed workload profile")
    return value


def runtime_identity(label, value, condition):
    exact(label, value, {"condition", "version", "binary", "release", "member", "assurance_claim"})
    if value["condition"] != condition:
        fail(f"{label} condition differs")
    if not isinstance(value["version"], str) or not re.fullmatch(r"jolt v[0-9]+\.[0-9]+\.[0-9]+", value["version"]):
        fail(f"{label} version is invalid")
    identity(label + " binary", value["binary"])
    exact(label + " release", value["release"], {"repository", "tag_ref", "tag_commit", "release_api", "asset", "sidecar"})
    release = value["release"]
    if release["repository"] != "jolt-lang/jolt":
        fail(f"{label} release repository differs")
    if release["tag_ref"] != "refs/tags/v" + value["version"].split(" v", 1)[1]:
        fail(f"{label} tag ref does not bind version")
    sha(label + " release tag commit", release["tag_commit"], GIT_SHA)
    expected_api = "https://api.github.com/repos/jolt-lang/jolt/releases/tags/v" + value["version"].split(" v", 1)[1]
    if release["release_api"] != expected_api:
        fail(f"{label} release API reference differs")
    asset = identity(label + " release asset", release["asset"])
    sidecar = release["sidecar"]
    exact(label + " release sidecar", sidecar, {"identity", "content", "declared_asset_sha256"})
    sidecar_identity = identity(label + " release sidecar identity", sidecar["identity"])
    if not isinstance(sidecar["content"], str) or sidecar_identity["bytes"] != len(sidecar["content"].encode()):
        fail(f"{label} release sidecar bytes differ")
    if sidecar_identity["sha256"] != digest(sidecar["content"].encode()):
        fail(f"{label} release sidecar digest differs")
    if sidecar["declared_asset_sha256"] != asset["sha256"]:
        fail(f"{label} release sidecar declared asset digest differs")
    if sidecar["content"] != f"{asset['sha256']}  {asset['file_name']}\n":
        fail(f"{label} release sidecar content differs")
    member = value["member"]
    exact(label + " extracted member", member, {"path", "identity"})
    member_path = pathlib.PurePosixPath(member["path"])
    if (not member["path"] or member_path.is_absolute() or
            ".." in member_path.parts or member_path.name != "jolt"):
        fail(f"{label} extracted member path differs")
    member_identity = identity(label + " extracted member identity", member["identity"])
    if member_identity["file_name"] != member_path.name:
        fail(f"{label} extracted member file name differs")
    if member_identity != value["binary"]:
        fail(f"{label} invoked binary differs from declared extracted member")
    if value["assurance_claim"] != ASSURANCE:
        fail(f"{label} assurance claim differs")
    return value


def validate_manifest(receipts):
    manifest = load(receipts / "run-manifest.json")
    manifest_keys = {"schema_version", "mode", "assurance_claim", "fixed", "conditions", "schedule", "run_id"}
    if "profile_provenance" in manifest:
        manifest_keys.add("profile_provenance")
    exact("run manifest", manifest, manifest_keys)
    if manifest["schema_version"] != 1 or manifest["mode"] != "release-runtime-abba":
        fail("run manifest schema or mode differs")
    if manifest["assurance_claim"] != ASSURANCE:
        fail("run manifest assurance claim differs")
    fixed_identity(manifest["fixed"])
    exact("run manifest conditions", manifest["conditions"], {"A", "B"})
    a = runtime_identity("condition A runtime", manifest["conditions"]["A"], "A")
    b = runtime_identity("condition B runtime", manifest["conditions"]["B"], "B")
    if a["binary"] == b["binary"] or a["version"] == b["version"]:
        fail("named release binary conditions are not distinct")
    if manifest["schedule"] != SCHEDULE:
        fail("schedule is not exact A'/B'/A/B/B/A order")
    unsigned = dict(manifest)
    claimed = unsigned.pop("run_id")
    sha("run manifest ID", claimed)
    if digest(canonical(unsigned)) != claimed:
        fail("run manifest ID differs from content")
    if "profile_provenance" in manifest:
        profile_provenance(manifest["profile_provenance"], "run manifest production profile provenance")
    return manifest


def validate_production_profile(path, manifest):
    """Bind a claimed release run to a reviewed profile chosen before the run.

    Receipt hashes authenticate internal consistency only: a party able to
    rewrite every receipt can recompute them.  A release-provenance result must
    therefore compare runtime release metadata and the invariant workload
    profile with a checked-in profile outside the receipt directory.  The
    generated fixture inventory is deliberately not in that profile: it is
    unique to a run and all six receipts bind to it through the manifest.
    """
    supplied_profile = pathlib.Path(path).absolute()
    if supplied_profile.is_symlink():
        fail("reviewed production profile must be a regular checked-in file")
    profile = supplied_profile.resolve()
    try:
        relative = profile.relative_to(ROOT)
    except ValueError:
        fail("reviewed production profile must be checked into this repository")
    if relative.parts[:2] == ("test", "fixtures"):
        fail("synthetic test fixture cannot be used as a production profile")
    if "profile_provenance" not in manifest:
        fail("run manifest is missing production profile provenance")
    expected_provenance = profile_provenance(
        manifest["profile_provenance"], "run manifest production profile provenance"
    )
    actual_provenance = checked_in_profile_provenance(profile)
    if actual_provenance != expected_provenance:
        fail("reviewed production profile Git provenance differs from manifest")
    value = load(profile)
    exact("reviewed production profile", value,
          {"schema_version", "purpose", "assurance_claim", "fixed", "conditions"})
    if value["schema_version"] != 1:
        fail("reviewed production profile schema differs")
    if value["purpose"] != "reviewed release-runtime Durable profile selected before execution":
        fail("reviewed production profile purpose differs")
    if value["assurance_claim"] != ASSURANCE:
        fail("reviewed production profile assurance claim differs")
    static_fixed_identity(value["fixed"])
    exact("reviewed production profile conditions", value["conditions"], {"A", "B"})
    runtime_identity("profile condition A runtime", value["conditions"]["A"], "A")
    runtime_identity("profile condition B runtime", value["conditions"]["B"], "B")
    static = {key: manifest["fixed"][key] for key in STATIC_FIXED_KEYS}
    if static != value["fixed"]:
        fail("run fixed workload identity differs from reviewed production profile")
    if manifest["conditions"] != value["conditions"]:
        fail("run release runtime metadata differs from reviewed production profile")
    return value


def validate_receipt(receipts, manifest, entry):
    receipt = load(receipts / entry["receipt_file"])
    receipt_keys = {"schema_version", "run_id", "schedule_ordinal", "phase", "condition", "runtime_condition", "fixed", "runtime", "execution", "raw_receipt", "receipt_id"}
    if "profile_provenance" in manifest:
        receipt_keys.add("profile_provenance")
    exact("run receipt", receipt, receipt_keys)
    if receipt["schema_version"] != 1 or receipt["run_id"] != manifest["run_id"]:
        fail("run receipt schema or manifest binding differs")
    expected_schedule = {"schedule_ordinal": entry["ordinal"], "phase": entry["phase"],
                         "condition": entry["condition"], "runtime_condition": entry["runtime_condition"]}
    for key, expected in expected_schedule.items():
        if receipt[key] != expected:
            fail(f"run receipt {key} differs from schedule")
    if receipt["fixed"] != manifest["fixed"]:
        fail("run receipt fixed chDB/data.json/provider/fixture/native identity differs")
    if receipt["runtime"] != manifest["conditions"][entry["runtime_condition"]]:
        fail("run receipt release runtime identity differs")
    if "profile_provenance" in manifest:
        if receipt["profile_provenance"] != manifest["profile_provenance"]:
            fail("run receipt production profile provenance differs")
        profile_provenance(receipt["profile_provenance"], "run receipt production profile provenance")
    exact("run receipt execution", receipt["execution"], {"process_id", "started_epoch_ms", "finished_epoch_ms", "outcome"})
    positive("run receipt process ID", receipt["execution"]["process_id"])
    positive("run receipt start", receipt["execution"]["started_epoch_ms"])
    positive("run receipt finish", receipt["execution"]["finished_epoch_ms"])
    if receipt["execution"]["finished_epoch_ms"] < receipt["execution"]["started_epoch_ms"]:
        fail("run receipt clock order differs")
    if receipt["execution"]["outcome"] != "pass":
        fail("run receipt outcome differs")
    raw = validate_raw_receipt(receipts / RAW_DIRECTORY / entry["receipt_file"], entry, manifest)
    if {"process_id": raw["process_id"], "started_epoch_ms": raw["process_started_epoch_ms"],
        "finished_epoch_ms": raw["process_finished_epoch_ms"]} != {
            "process_id": receipt["execution"]["process_id"],
            "started_epoch_ms": receipt["execution"]["started_epoch_ms"],
            "finished_epoch_ms": receipt["execution"]["finished_epoch_ms"]}:
        fail("run receipt execution does not bind raw reader process")
    exact("run receipt raw evidence", receipt["raw_receipt"], {"path", "identity", "measurement"})
    if receipt["raw_receipt"]["path"] != RAW_DIRECTORY + "/" + entry["receipt_file"]:
        fail("run receipt raw evidence path differs")
    if receipt["raw_receipt"]["identity"] != file_identity(receipts / RAW_DIRECTORY / entry["receipt_file"]):
        fail("run receipt raw evidence identity differs")
    if receipt["raw_receipt"]["measurement"] != raw_measurement(raw):
        fail("run receipt raw evidence measurement differs")
    unsigned = dict(receipt)
    claimed = unsigned.pop("receipt_id")
    sha("run receipt ID", claimed)
    if digest(canonical(unsigned)) != claimed:
        fail("run receipt ID differs from content")
    return receipt


def file_identity(path):
    path = pathlib.Path(path)
    try:
        data = path.read_bytes()
    except OSError as error:
        fail(f"cannot read raw receipt {path}: {error}")
    return {"file_name": path.name, "bytes": len(data), "sha256": digest(data)}


def validate_raw_receipt(path, entry, manifest):
    raw = load(path)
    exact("raw Jolt reader receipt", raw,
          {"schema_version", "run_id", "schedule_ordinal", "phase", "runtime", "trial",
           "process_id", "process_started_epoch_ms", "process_finished_epoch_ms", "cache_condition",
           "fixture", "recovery"})
    if raw["schema_version"] != 1:
        fail("raw Jolt reader receipt schema differs")
    if (raw["schedule_ordinal"], raw["phase"], raw["trial"]) != RAW_SOURCE_SCHEDULE[entry["ordinal"]]:
        fail("raw Jolt reader source schedule differs")
    positive("raw Jolt reader process ID", raw["process_id"])
    positive("raw Jolt reader start", raw["process_started_epoch_ms"])
    positive("raw Jolt reader finish", raw["process_finished_epoch_ms"])
    if raw["process_finished_epoch_ms"] < raw["process_started_epoch_ms"]:
        fail("raw Jolt reader clock order differs")
    if not isinstance(raw["fixture"], dict) or not isinstance(raw["fixture"].get("recovered_rows"), int):
        fail("raw Jolt reader recovered row count is missing")
    positive("raw Jolt reader recovered rows", raw["fixture"]["recovered_rows"])
    if (raw["fixture"].get("inventory_sha256") != manifest["fixed"]["fixture"]["inventory_sha256"] or
            raw["fixture"]["recovered_rows"] != manifest["fixed"]["fixture"]["rows"]):
        fail("raw Jolt reader fixture differs from run manifest")
    exact("raw Jolt reader recovery", raw["recovery"],
          {"elapsed_ns", "rows_per_second", "expected", "actual", "inventory_unchanged"})
    positive("raw Jolt reader elapsed", raw["recovery"]["elapsed_ns"])
    rate = raw["recovery"]["rows_per_second"]
    if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate):
        fail("raw Jolt reader rows-per-second is invalid")
    expected_rate = raw["fixture"]["recovered_rows"] * 1e9 / raw["recovery"]["elapsed_ns"]
    if not math.isclose(rate, expected_rate, rel_tol=1e-12, abs_tol=1e-9):
        fail("raw Jolt reader rows-per-second does not recompute")
    if raw["recovery"]["expected"] != raw["recovery"]["actual"] or raw["recovery"]["inventory_unchanged"] is not True:
        fail("raw Jolt reader recovery reconciliation differs")
    if digest(canonical(raw["recovery"]["expected"])) != manifest["fixed"]["fixture"]["expected_sha256"]:
        fail("raw Jolt reader expected aggregate differs from run manifest")
    return raw


def raw_measurement(raw):
    return {"elapsed_ns": raw["recovery"]["elapsed_ns"],
            "rows_per_second": raw["recovery"]["rows_per_second"],
            "recovered_rows": raw["fixture"]["recovered_rows"]}


def expected_summary(manifest, receipts):
    raw_entries = []
    by_condition = {"A": [], "B": []}
    for entry in SCHEDULE:
        receipt = load(receipts / entry["receipt_file"])
        evidence = receipt["raw_receipt"]
        raw_entries.append({"receipt_file": entry["receipt_file"], "path": evidence["path"],
                            "identity": evidence["identity"], "measurement": evidence["measurement"]})
        if entry["phase"] == "measured":
            by_condition[entry["runtime_condition"]].append(evidence["measurement"])
    conditions = {}
    for condition, values in by_condition.items():
        if len(values) != 2:
            fail("summary measured observation count differs")
        conditions[condition] = {
            "measurements": values,
            "mean_elapsed_ns": sum(value["elapsed_ns"] for value in values) / len(values),
            "mean_rows_per_second": sum(value["rows_per_second"] for value in values) / len(values),
        }
    return {"schema_version": 1, "run_id": manifest["run_id"], "raw_receipts": raw_entries,
            "conditions": conditions,
            "directional": {
                "A_elapsed_over_B": conditions["A"]["mean_elapsed_ns"] / conditions["B"]["mean_elapsed_ns"],
                "A_rows_per_second_over_B": conditions["A"]["mean_rows_per_second"] / conditions["B"]["mean_rows_per_second"],
            }}


def validate_summary(receipts, manifest):
    summary = load(receipts / SUMMARY_FILE)
    exact("final release-runtime summary", summary,
          {"schema_version", "run_id", "raw_receipts", "conditions", "directional", "summary_id"})
    expected = expected_summary(manifest, receipts)
    for key in expected:
        if summary[key] != expected[key]:
            fail("final release-runtime summary statistics or raw evidence differs")
    unsigned = dict(summary)
    claimed = unsigned.pop("summary_id")
    sha("final release-runtime summary ID", claimed)
    if digest(canonical(unsigned)) != claimed:
        fail("final release-runtime summary ID differs from content")
    return summary


def sandboxed_version_command(sandbox, path):
    if sandbox is None:
        return None
    sandbox = pathlib.Path(sandbox)
    if not sandbox.is_file() or not os.access(sandbox, os.X_OK):
        fail("Bubblewrap sandbox executable is unavailable")
    return [str(sandbox), "--unshare-net", "--die-with-parent", "--new-session",
            "--ro-bind", "/", "/", "--proc", "/proc", "--dev", "/dev", "--", str(path), "--version"]


def verify_binary(label, path, expected, sandbox=None):
    path = pathlib.Path(path)
    if not path.is_file():
        fail(f"{label} binary is missing")
    actual = {"file_name": path.name, "bytes": path.stat().st_size,
              "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    # Archive-member identity includes its canonical member name (``jolt``),
    # while a caller may retain that verified member under a versioned local
    # filename such as ``jolt-0.8.6``.  The executable bytes and banner are the
    # identity relevant to what is invoked; the path basename is not.
    if {key: actual[key] for key in ("bytes", "sha256")} != {
            key: expected["binary"][key] for key in ("bytes", "sha256")}:
        fail(f"{label} invoked binary identity differs from receipt")
    command = sandboxed_version_command(sandbox, path)
    if command is None:
        return
    try:
        banner = subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip()
    except (OSError, subprocess.CalledProcessError) as error:
        fail(f"{label} binary cannot report version: {error}")
    if banner != expected["version"]:
        fail(f"{label} invoked binary version differs from receipt")


def verify_release_artifacts(label, binary_path, archive_path, sidecar_path, expected, sandbox=None):
    """Verify the downloaded archive, checksum sidecar, and extracted member.

    This checks the actual bytes passed by the caller.  It deliberately does
    not trust a receipt's description of an earlier download or extraction.
    """
    release = expected["release"]
    archive = pathlib.Path(archive_path)
    sidecar = pathlib.Path(sidecar_path)
    if not archive.is_file():
        fail(f"{label} release archive is missing")
    if not sidecar.is_file():
        fail(f"{label} release checksum sidecar is missing")
    actual_archive = {"file_name": archive.name, "bytes": archive.stat().st_size,
                      "sha256": hashlib.sha256(archive.read_bytes()).hexdigest()}
    if actual_archive != release["asset"]:
        fail(f"{label} release archive identity differs from reviewed production profile")
    actual_sidecar = {"file_name": sidecar.name, "bytes": sidecar.stat().st_size,
                      "sha256": hashlib.sha256(sidecar.read_bytes()).hexdigest()}
    if actual_sidecar != release["sidecar"]["identity"]:
        fail(f"{label} release checksum sidecar identity differs from reviewed production profile")
    if sidecar.read_text(encoding="utf-8") != release["sidecar"]["content"]:
        fail(f"{label} release checksum sidecar content differs from reviewed production profile")
    try:
        with tarfile.open(archive, mode="r:gz") as handle:
            members = [member for member in handle.getmembers() if member.name == expected["member"]["path"]]
            if len(members) != 1 or not members[0].isfile():
                fail(f"{label} release archive declared executable member differs")
            stream = handle.extractfile(members[0])
            if stream is None:
                fail(f"{label} release archive declared executable member is unreadable")
            member_bytes = stream.read()
    except (OSError, tarfile.TarError) as error:
        fail(f"{label} release archive cannot be read: {error}")
    actual_member = {"file_name": pathlib.PurePosixPath(expected["member"]["path"]).name, "bytes": len(member_bytes),
                     "sha256": hashlib.sha256(member_bytes).hexdigest()}
    if actual_member != expected["member"]["identity"]:
        fail(f"{label} release archive declared executable member identity differs from reviewed production profile")
    binary = pathlib.Path(binary_path)
    if not binary.is_file() or binary.read_bytes() != member_bytes:
        fail(f"{label} invoked binary differs from actual declared release archive member")
    verify_binary(label, binary, expected, sandbox)


def main():
    argv = sys.argv[1:]
    sandbox = None
    if len(argv) >= 2 and argv[-2] == "--sandbox-bwrap":
        sandbox = argv[-1]
        argv = argv[:-2]
    if len(argv) not in (1, 8):
        fail("usage: verify-durable-release-runtime-abba.py RECEIPT_DIR [PROFILE_ANCHOR A_BINARY A_ARCHIVE A_SIDECAR B_BINARY B_ARCHIVE B_SIDECAR]")
    receipts = pathlib.Path(argv[0])
    validate_receipt_directory(receipts)
    manifest = validate_manifest(receipts)
    seen_pids = set()
    for entry in SCHEDULE:
        validate_receipt(receipts, manifest, entry)
        value = load(receipts / entry["receipt_file"])["execution"]["process_id"]
        if value in seen_pids:
            fail("run receipt process ID is not fresh")
        seen_pids.add(value)
    validate_summary(receipts, manifest)
    if len(argv) == 8:
        if sandbox is None:
            fail("release-provenance verification requires --sandbox-bwrap for release binary banner checks")
        validate_production_profile(argv[1], manifest)
        verify_release_artifacts("condition A", argv[2], argv[3], argv[4], manifest["conditions"]["A"], sandbox)
        verify_release_artifacts("condition B", argv[5], argv[6], argv[7], manifest["conditions"]["B"], sandbox)
        print("PASS anchored release-provenance Durable A'/B'/A/B/B/A verification")
    else:
        print("PASS structural-consistency Durable A'/B'/A/B/B/A receipt verification (not release provenance)")


if __name__ == "__main__":
    main()
