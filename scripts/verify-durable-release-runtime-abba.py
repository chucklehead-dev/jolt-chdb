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
import pathlib
import re
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


STATIC_FIXED_KEYS = {"chdb", "runner_script", "data_json", "provider", "workload", "native"}


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
    if member["path"] != "jolt":
        fail(f"{label} extracted member path differs")
    member_identity = identity(label + " extracted member identity", member["identity"])
    if member_identity["file_name"] != member["path"]:
        fail(f"{label} extracted member file name differs")
    if member_identity != value["binary"]:
        fail(f"{label} invoked binary differs from declared extracted member")
    if value["assurance_claim"] != ASSURANCE:
        fail(f"{label} assurance claim differs")
    return value


def validate_manifest(receipts):
    manifest = load(receipts / "run-manifest.json")
    exact("run manifest", manifest, {"schema_version", "mode", "assurance_claim", "fixed", "conditions", "schedule", "run_id"})
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
    profile = pathlib.Path(path).resolve()
    try:
        relative = profile.relative_to(ROOT)
    except ValueError:
        fail("reviewed production profile must be checked into this repository")
    if relative.parts[:2] == ("test", "fixtures"):
        fail("synthetic test fixture cannot be used as a production profile")
    if not profile.is_file():
        fail("reviewed production profile is missing")
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
    exact("run receipt", receipt, {"schema_version", "run_id", "schedule_ordinal", "phase", "condition", "runtime_condition", "fixed", "runtime", "execution", "receipt_id"})
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
    exact("run receipt execution", receipt["execution"], {"process_id", "started_epoch_ms", "finished_epoch_ms", "outcome"})
    positive("run receipt process ID", receipt["execution"]["process_id"])
    positive("run receipt start", receipt["execution"]["started_epoch_ms"])
    positive("run receipt finish", receipt["execution"]["finished_epoch_ms"])
    if receipt["execution"]["finished_epoch_ms"] < receipt["execution"]["started_epoch_ms"]:
        fail("run receipt clock order differs")
    if receipt["execution"]["outcome"] != "pass":
        fail("run receipt outcome differs")
    unsigned = dict(receipt)
    claimed = unsigned.pop("receipt_id")
    sha("run receipt ID", claimed)
    if digest(canonical(unsigned)) != claimed:
        fail("run receipt ID differs from content")


def verify_binary(label, path, expected):
    path = pathlib.Path(path)
    if not path.is_file():
        fail(f"{label} binary is missing")
    actual = {"file_name": path.name, "bytes": path.stat().st_size,
              "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    if actual != expected["binary"]:
        fail(f"{label} invoked binary identity differs from receipt")
    try:
        banner = subprocess.check_output([str(path), "--version"], text=True, stderr=subprocess.STDOUT).strip()
    except (OSError, subprocess.CalledProcessError) as error:
        fail(f"{label} binary cannot report version: {error}")
    if banner != expected["version"]:
        fail(f"{label} invoked binary version differs from receipt")


def verify_release_artifacts(label, binary_path, archive_path, sidecar_path, expected):
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
    actual_member = {"file_name": expected["member"]["path"], "bytes": len(member_bytes),
                     "sha256": hashlib.sha256(member_bytes).hexdigest()}
    if actual_member != expected["member"]["identity"]:
        fail(f"{label} release archive declared executable member identity differs from reviewed production profile")
    binary = pathlib.Path(binary_path)
    if not binary.is_file() or binary.read_bytes() != member_bytes:
        fail(f"{label} invoked binary differs from actual declared release archive member")
    verify_binary(label, binary, expected)


def main():
    if len(sys.argv) not in (2, 9):
        fail("usage: verify-durable-release-runtime-abba.py RECEIPT_DIR [PROFILE_ANCHOR A_BINARY A_ARCHIVE A_SIDECAR B_BINARY B_ARCHIVE B_SIDECAR]")
    receipts = pathlib.Path(sys.argv[1])
    manifest = validate_manifest(receipts)
    expected_files = {"run-manifest.json"}
    seen_pids = set()
    for entry in SCHEDULE:
        expected_files.add(entry["receipt_file"])
        validate_receipt(receipts, manifest, entry)
        value = load(receipts / entry["receipt_file"])["execution"]["process_id"]
        if value in seen_pids:
            fail("run receipt process ID is not fresh")
        seen_pids.add(value)
    actual_files = {path.name for path in receipts.iterdir() if path.is_file()}
    if actual_files != expected_files:
        fail("receipt file set is not exact")
    if len(sys.argv) == 9:
        validate_production_profile(sys.argv[2], manifest)
        verify_release_artifacts("condition A", sys.argv[3], sys.argv[4], sys.argv[5], manifest["conditions"]["A"])
        verify_release_artifacts("condition B", sys.argv[6], sys.argv[7], sys.argv[8], manifest["conditions"]["B"])
        print("PASS anchored release-provenance Durable A'/B'/A/B/B/A verification")
    else:
        print("PASS structural-consistency Durable A'/B'/A/B/B/A receipt verification (not release provenance)")


if __name__ == "__main__":
    main()
