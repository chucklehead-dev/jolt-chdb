#!/usr/bin/env python3
"""Focused offline controls for release-runtime Durable A'/B'/A/B/B/A receipts."""
import copy
import hashlib
import importlib.util
import json
import pathlib
import subprocess
import tarfile
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
VERIFY = ROOT / "scripts/verify-durable-release-runtime-abba.py"
OFFICIAL = ROOT / "test/fixtures/durable-release-runtime-abba/official-release-runtime-fixture.json"
SPEC = importlib.util.spec_from_file_location("release_runtime_abba_verify", VERIFY)
VERIFY_MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFY_MODULE)
ASSURANCE = ("github-release-reference-and-archive-integrity; no signed "
             "source/build chain, artifact attestation, or reproducibility proof")
SCHEDULE = [
    {"ordinal": 0, "phase": "prime", "condition": "A'", "runtime_condition": "A", "receipt_file": "A-prime.json"},
    {"ordinal": 1, "phase": "prime", "condition": "B'", "runtime_condition": "B", "receipt_file": "B-prime.json"},
    {"ordinal": 2, "phase": "measured", "condition": "A", "runtime_condition": "A", "receipt_file": "A-1.json"},
    {"ordinal": 3, "phase": "measured", "condition": "B", "runtime_condition": "B", "receipt_file": "B-1.json"},
    {"ordinal": 4, "phase": "measured", "condition": "B", "runtime_condition": "B", "receipt_file": "B-2.json"},
    {"ordinal": 5, "phase": "measured", "condition": "A", "runtime_condition": "A", "receipt_file": "A-2.json"},
]


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def identity(path):
    data = path.read_bytes()
    return {"file_name": path.name, "bytes": len(data), "sha256": digest(data)}


def write_json(path, value):
    path.write_text(json.dumps(value, sort_keys=True) + "\n")


class TestReleaseRuntimeAbba(unittest.TestCase):
    def verify(self, receipts, *binaries):
        return subprocess.run(["python3", str(VERIFY), str(receipts), *map(str, binaries)],
                              text=True, capture_output=True)

    def write_binary(self, root, directory, version):
        target = root / directory
        target.mkdir(exist_ok=True)
        binary = target / "jolt"
        binary.write_text("#!/usr/bin/env sh\n[ \"$1\" = --version ] || exit 64\nprintf '%s\\n' '" + version + "'\n")
        binary.chmod(0o755)
        return binary

    def fixed(self):
        ident = lambda name, char: {"file_name": name, "bytes": 1, "sha256": char * 64}
        return {
            "chdb": {"source_sha": "a" * 40, "source_tree": "b" * 40},
            "runner_script": ident("run-release-abba.sh", "c"),
            "verifier_script": ident("verify-release-abba.py", "8"),
            "data_json": {"source_sha": "d" * 40, "namespace": ident("json.cljc", "e")},
            "provider": {"source_sha": "f" * 40, "namespace": ident("provider.json", "1")},
            "workload": {"rows": 52224, "segments": 3, "generator": ident("generate.clj", "2")},
            "fixture": {"inventory_sha256": "3" * 64, "rows": 52224, "segments": 3, "expected_sha256": "4" * 64},
            "native": {"version": "26.7.3", "library": ident("libchdb.so", "5"), "header": ident("chdb.h", "6")},
            "cargo_home": {"files": 1, "bytes": 1, "sha256": "7" * 64},
            "jolt_cache": {"files": 1, "bytes": 1, "sha256": "9" * 64},
            "jolt_gitlibs": {"files": 1, "bytes": 1, "sha256": "a" * 64},
        }

    def runtime(self, official, condition, binary):
        value = copy.deepcopy(official["conditions"][condition])
        value["binary"] = identity(binary)
        value["member"]["identity"] = copy.deepcopy(value["binary"])
        return value

    def rebind_manifest(self, receipts, manifest):
        unsigned = dict(manifest)
        unsigned.pop("run_id", None)
        manifest["run_id"] = digest(canonical(unsigned))
        write_json(receipts / "run-manifest.json", manifest)
        self.refresh_summary(receipts)

    def rebind_receipt(self, path, receipt):
        unsigned = dict(receipt)
        unsigned.pop("receipt_id", None)
        receipt["receipt_id"] = digest(canonical(unsigned))
        write_json(path, receipt)
        self.refresh_summary(path.parent)

    def refresh_summary(self, receipts):
        if not (receipts / "summary.json").exists():
            return
        if not all((receipts / entry["receipt_file"]).is_file() for entry in SCHEDULE):
            return
        manifest = json.loads((receipts / "run-manifest.json").read_text())
        summary = VERIFY_MODULE.expected_summary(manifest, receipts)
        summary["summary_id"] = digest(canonical(summary))
        write_json(receipts / "summary.json", summary)

    def corpus(self, root):
        official = json.loads(OFFICIAL.read_text())
        a = self.write_binary(root, "a", "jolt v0.8.6")
        b = self.write_binary(root, "b", "jolt v0.8.9")
        conditions = {"A": self.runtime(official, "A", a), "B": self.runtime(official, "B", b)}
        receipts = root / "receipts"
        receipts.mkdir()
        raw_dir = receipts / "raw"
        raw_dir.mkdir()
        manifest = {"schema_version": 1, "mode": "release-runtime-abba", "assurance_claim": ASSURANCE,
                    "fixed": self.fixed(), "conditions": conditions, "schedule": SCHEDULE}
        manifest["fixed"]["fixture"]["inventory_sha256"] = digest(b"fixture-inventory")
        manifest["fixed"]["fixture"]["expected_sha256"] = digest(canonical({}))
        self.rebind_manifest(receipts, manifest)
        for entry in SCHEDULE:
            raw = {"schema_version": 1, "run_id": "source-run",
                   "schedule_ordinal": {0: 1, 1: 1, 2: 3, 3: 4, 4: 7, 5: 8}[entry["ordinal"]],
                   "phase": "prime" if entry["phase"] == "prime" else "measured",
                   "runtime": {}, "trial": None if entry["phase"] == "prime" else entry["ordinal"] - 1,
                   "process_id": 2000 + entry["ordinal"],
                   "process_started_epoch_ms": 3000 + entry["ordinal"] * 2,
                   "process_finished_epoch_ms": 3001 + entry["ordinal"] * 2,
                   "cache_condition": "fresh", "fixture": {"recovered_rows": 52224,
                                                               "inventory_sha256": manifest["fixed"]["fixture"]["inventory_sha256"]},
                   "recovery": {"elapsed_ns": 1000 + entry["ordinal"],
                                "rows_per_second": 52224e9 / (1000 + entry["ordinal"]),
                                "expected": {}, "actual": {}, "inventory_unchanged": True}}
            raw_path = raw_dir / entry["receipt_file"]
            write_json(raw_path, raw)
            receipt = {"schema_version": 1, "run_id": manifest["run_id"],
                       "schedule_ordinal": entry["ordinal"], "phase": entry["phase"],
                       "condition": entry["condition"], "runtime_condition": entry["runtime_condition"],
                       "fixed": copy.deepcopy(manifest["fixed"]),
                       "runtime": copy.deepcopy(conditions[entry["runtime_condition"]]),
                       "execution": {"process_id": raw["process_id"],
                                     "started_epoch_ms": raw["process_started_epoch_ms"],
                                     "finished_epoch_ms": raw["process_finished_epoch_ms"],
                                     "outcome": "pass"},
                       "raw_receipt": {"path": "raw/" + entry["receipt_file"],
                                       "identity": identity(raw_path),
                                       "measurement": {"elapsed_ns": raw["recovery"]["elapsed_ns"],
                                                       "rows_per_second": raw["recovery"]["rows_per_second"],
                                                       "recovered_rows": raw["fixture"]["recovered_rows"]}}}
            self.rebind_receipt(receipts / entry["receipt_file"], receipt)
        summary = VERIFY_MODULE.expected_summary(manifest, receipts)
        summary["summary_id"] = digest(canonical(summary))
        write_json(receipts / "summary.json", summary)
        return receipts, a, b

    def write_archive(self, root, name, member, member_path="jolt"):
        archive = root / name
        with tarfile.open(archive, "w:gz") as handle:
            info = tarfile.TarInfo(member_path)
            info.mode = 0o755
            info.size = len(member)
            info.mtime = 0
            import io
            handle.addfile(info, io.BytesIO(member))
        return archive

    def release_artifacts(self, root, condition="A"):
        official = json.loads(OFFICIAL.read_text())
        expected = copy.deepcopy(official["conditions"][condition])
        binary = self.write_binary(root, "runtime", official["conditions"][condition]["version"])
        archive = self.write_archive(root, "release.tar.gz", binary.read_bytes(),
                                     expected["member"]["path"])
        sidecar = root / "release.tar.gz.sha256"
        sidecar.write_text(identity(archive)["sha256"] + "  " + archive.name + "\n")
        expected["binary"] = identity(binary)
        expected["member"]["identity"] = copy.deepcopy(expected["binary"])
        expected["release"]["asset"] = identity(archive)
        expected["release"]["sidecar"]["content"] = sidecar.read_text()
        expected["release"]["sidecar"]["identity"] = identity(sidecar)
        expected["release"]["sidecar"]["declared_asset_sha256"] = expected["release"]["asset"]["sha256"]
        return binary, archive, sidecar, expected

    def profile(self, path, manifest):
        static_keys = ("chdb", "runner_script", "verifier_script", "data_json", "provider", "workload", "native", "cargo_home", "jolt_cache", "jolt_gitlibs")
        value = {"schema_version": 1,
                 "purpose": "reviewed release-runtime Durable profile selected before execution",
                 "assurance_claim": ASSURANCE,
                 "fixed": {key: copy.deepcopy(manifest["fixed"][key]) for key in static_keys},
                 "conditions": copy.deepcopy(manifest["conditions"])}
        write_json(path, value)
        return value

    def commit_profile(self, root, manifest):
        subprocess.check_call(["git", "init", "-q", str(root)])
        profile = root / "profiles" / "release-runtime.json"
        profile.parent.mkdir()
        self.profile(profile, manifest)
        subprocess.check_call(["git", "-C", str(root), "add", "--", profile.relative_to(root).as_posix()])
        subprocess.check_call(["git", "-C", str(root), "-c", "user.name=Test", "-c",
                               "user.email=test@example.invalid", "commit", "-q", "-m", "profile"])
        return profile

    def bind_profile_provenance(self, receipts, manifest, profile):
        provenance = VERIFY_MODULE.checked_in_profile_provenance(profile)
        manifest["profile_provenance"] = provenance
        self.rebind_manifest(receipts, manifest)
        for entry in SCHEDULE:
            path = receipts / entry["receipt_file"]
            receipt = json.loads(path.read_text())
            receipt["run_id"] = manifest["run_id"]
            receipt["profile_provenance"] = copy.deepcopy(provenance)
            self.rebind_receipt(path, receipt)
        return provenance

    def test_official_release_research_fixture_is_exact_and_limited(self):
        fixture = json.loads(OFFICIAL.read_text())
        self.assertEqual("test fixture only; this is not a completed qualification receipt", fixture["purpose"])
        self.assertEqual(ASSURANCE, fixture["assurance_claim"])
        self.assertEqual({"A", "B"}, set(fixture["conditions"]))
        self.assertEqual("c2a494272cf82785ec8803426b495366000de572", fixture["conditions"]["A"]["release"]["tag_commit"])
        self.assertEqual("3dab6373f2028efaf7a0e41231fc316de866e55454023c037e9d5981be6358c2", fixture["conditions"]["A"]["release"]["asset"]["sha256"])
        self.assertEqual("abdb1250bd76d31a5c4b372eb0c2a5eb29d0797c1253c20225726f16da178cba", fixture["conditions"]["A"]["binary"]["sha256"])
        self.assertEqual("dce33c779692d26956b9e61418978e945382f2ac", fixture["conditions"]["B"]["release"]["tag_commit"])
        self.assertEqual("0984f7f954589d7aa40feab4748ad7c521a9de94d32afe30fd1be8d1ee92442b", fixture["conditions"]["B"]["release"]["asset"]["sha256"])
        self.assertEqual("62cf6239907465cbc08d723ed7541a78394c2a3c29d075e15220f7e286b3992c", fixture["conditions"]["B"]["binary"]["sha256"])

    def test_accepts_exact_six_structurally_consistent_receipts(self):
        with tempfile.TemporaryDirectory() as directory:
            receipts, _, _ = self.corpus(pathlib.Path(directory))
            result = self.verify(receipts)
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertIn("PASS structural-consistency", result.stdout)
            self.assertIn("not release provenance", result.stdout)

    def test_receipt_only_mode_is_explicitly_offline(self):
        with tempfile.TemporaryDirectory() as directory:
            receipts, _, _ = self.corpus(pathlib.Path(directory))
            result = self.verify(receipts)
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertIn("not release provenance", result.stdout)

    def test_rejects_missing_or_changed_runtime_receipt_fields(self):
        for mutation in ("missing", "sidecar", "assurance"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                receipts, _, _ = self.corpus(pathlib.Path(directory))
                manifest_path = receipts / "run-manifest.json"
                manifest = json.loads(manifest_path.read_text())
                runtime = manifest["conditions"]["A"]
                if mutation == "missing":
                    del runtime["release"]["sidecar"]
                elif mutation == "sidecar":
                    runtime["release"]["sidecar"]["content"] = "changed\n"
                else:
                    runtime["assurance_claim"] = "stronger than evidence"
                self.rebind_manifest(receipts, manifest)
                result = self.verify(receipts)
                self.assertNotEqual(0, result.returncode)
                self.assertIn("missing or unknown fields" if mutation == "missing" else "differ", result.stderr)

    def test_release_artifact_verifier_rejects_wrong_archive_sidecar_and_member(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary, archive, sidecar, expected = self.release_artifacts(root)
            VERIFY_MODULE.verify_release_artifacts("test", binary, archive, sidecar, expected)
            archive.write_bytes(b"not a gzip archive")
            with self.assertRaisesRegex(SystemExit, "release archive identity differs"):
                VERIFY_MODULE.verify_release_artifacts("test", binary, archive, sidecar, expected)
            binary, archive, sidecar, expected = self.release_artifacts(root)
            sidecar.write_text("wrong sidecar\n")
            with self.assertRaisesRegex(SystemExit, "checksum sidecar identity differs"):
                VERIFY_MODULE.verify_release_artifacts("test", binary, archive, sidecar, expected)
            binary, archive, sidecar, expected = self.release_artifacts(root)
            changed_archive = self.write_archive(root, archive.name, b"different member",
                                                 expected["member"]["path"])
            expected["release"]["asset"] = identity(changed_archive)
            sidecar.write_text(expected["release"]["asset"]["sha256"] + "  " + changed_archive.name + "\n")
            expected["release"]["sidecar"]["content"] = sidecar.read_text()
            expected["release"]["sidecar"]["identity"] = identity(sidecar)
            expected["release"]["sidecar"]["declared_asset_sha256"] = expected["release"]["asset"]["sha256"]
            with self.assertRaisesRegex(SystemExit, "declared executable member identity differs"):
                VERIFY_MODULE.verify_release_artifacts("test", binary, changed_archive, sidecar, expected)

    def test_release_artifact_verifier_accepts_a_versioned_local_binary_name(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary, archive, sidecar, expected = self.release_artifacts(root)
            renamed = binary.with_name("jolt-0.8.6")
            binary.rename(renamed)
            VERIFY_MODULE.verify_release_artifacts("test", renamed, archive, sidecar, expected)

    def test_self_consistent_mutated_receipts_are_not_anchored_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            receipts, a, b = self.corpus(root)
            manifest = json.loads((receipts / "run-manifest.json").read_text())
            original_manifest = copy.deepcopy(manifest)
            runtime = manifest["conditions"]["A"]
            runtime["version"] = "jolt v9.9.9"
            runtime["release"]["tag_ref"] = "refs/tags/v9.9.9"
            runtime["release"]["release_api"] = "https://api.github.com/repos/jolt-lang/jolt/releases/tags/v9.9.9"
            runtime["binary"] = {"file_name": "jolt", "bytes": 1, "sha256": "9" * 64}
            runtime["member"]["identity"] = copy.deepcopy(runtime["binary"])
            runtime["release"]["asset"] = {"file_name": "jolt-v9.9.9-x86_64-linux.tar.gz", "bytes": 1, "sha256": "8" * 64}
            runtime["release"]["sidecar"]["declared_asset_sha256"] = "8" * 64
            runtime["release"]["sidecar"]["identity"]["file_name"] = "jolt-v9.9.9-x86_64-linux.tar.gz.sha256"
            runtime["release"]["sidecar"]["content"] = runtime["release"]["asset"]["sha256"] + "  jolt-v9.9.9-x86_64-linux.tar.gz\n"
            runtime["release"]["sidecar"]["identity"]["bytes"] = len(runtime["release"]["sidecar"]["content"].encode())
            runtime["release"]["sidecar"]["identity"]["sha256"] = digest(runtime["release"]["sidecar"]["content"].encode())
            manifest["fixed"]["fixture"]["inventory_sha256"] = "7" * 64
            self.rebind_manifest(receipts, manifest)
            for entry in SCHEDULE:
                path = receipts / entry["receipt_file"]
                receipt = json.loads(path.read_text())
                raw_path = receipts / "raw" / entry["receipt_file"]
                raw = json.loads(raw_path.read_text())
                raw["fixture"]["inventory_sha256"] = manifest["fixed"]["fixture"]["inventory_sha256"]
                write_json(raw_path, raw)
                receipt["run_id"] = manifest["run_id"]
                receipt["fixed"] = copy.deepcopy(manifest["fixed"])
                receipt["raw_receipt"]["identity"] = identity(raw_path)
                if entry["runtime_condition"] == "A": receipt["runtime"] = copy.deepcopy(runtime)
                self.rebind_receipt(path, receipt)
            structural = self.verify(receipts)
            self.assertEqual(0, structural.returncode, structural.stderr)
            original_root = VERIFY_MODULE.ROOT
            try:
                VERIFY_MODULE.ROOT = root
                profile = self.commit_profile(root, original_manifest)
                self.bind_profile_provenance(receipts, manifest, profile)
                with self.assertRaisesRegex(SystemExit, "reviewed production profile"):
                    VERIFY_MODULE.validate_production_profile(profile, manifest)
            finally:
                VERIFY_MODULE.ROOT = original_root

    def test_rejects_duplicate_processes_stray_files_and_manifest_schedule_tamper(self):
        for mutation in ("duplicate-process", "stray-file", "nested-directory", "schedule"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                receipts, _, _ = self.corpus(pathlib.Path(directory))
                if mutation == "duplicate-process":
                    path = receipts / "A-1.json"
                    receipt = json.loads(path.read_text())
                    receipt["execution"]["process_id"] = 1000
                    self.rebind_receipt(path, receipt)
                elif mutation == "stray-file":
                    (receipts / "extra.json").write_text("{}\n")
                elif mutation == "nested-directory":
                    (receipts / "nested").mkdir()
                else:
                    path = receipts / "run-manifest.json"
                    manifest = json.loads(path.read_text())
                    manifest["schedule"] = list(reversed(manifest["schedule"]))
                    self.rebind_manifest(receipts, manifest)
                result = self.verify(receipts)
                self.assertNotEqual(0, result.returncode)
                expected = {"duplicate-process": "execution does not bind raw reader process",
                            "stray-file": "receipt directory entry set is not exact",
                            "nested-directory": "receipt directory entry set is not exact",
                            "schedule": "schedule is not exact"}[mutation]
                self.assertIn(expected, result.stderr)

    def test_rejects_symlinked_expected_receipt_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            receipts, _, _ = self.corpus(pathlib.Path(directory))
            target = receipts / "A-1.json"
            target.unlink()
            try:
                target.symlink_to(receipts / "B-1.json")
            except (NotImplementedError, OSError) as error:
                self.skipTest(f"symlinks are unavailable: {error}")
            result = self.verify(receipts)
            self.assertNotEqual(0, result.returncode)
            self.assertIn("entries must be regular named files", result.stderr)

    def test_final_summary_is_bound_to_copied_raw_receipts_and_recomputed_statistics(self):
        for mutation in ("delete-raw", "mutate-raw", "mutate-summary"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                receipts, _, _ = self.corpus(pathlib.Path(directory))
                if mutation == "delete-raw":
                    (receipts / "raw" / "A-1.json").unlink()
                    expected = "raw receipt corpus entry set is not exact"
                elif mutation == "mutate-raw":
                    raw = receipts / "raw" / "A-1.json"
                    value = json.loads(raw.read_text())
                    value["recovery"]["elapsed_ns"] += 1
                    value["recovery"]["rows_per_second"] = 52224e9 / value["recovery"]["elapsed_ns"]
                    write_json(raw, value)
                    expected = "raw evidence identity differs"
                else:
                    summary_path = receipts / "summary.json"
                    summary = json.loads(summary_path.read_text())
                    summary["conditions"]["A"]["mean_elapsed_ns"] = 1
                    unsigned = dict(summary)
                    unsigned.pop("summary_id")
                    summary["summary_id"] = digest(canonical(unsigned))
                    write_json(summary_path, summary)
                    expected = "summary statistics or raw evidence differs"
                result = self.verify(receipts)
                self.assertNotEqual(0, result.returncode)
                self.assertIn(expected, result.stderr)

    def test_release_binary_banner_command_is_sandboxed_when_execution_is_requested(self):
        command = VERIFY_MODULE.sandboxed_version_command("/usr/bin/bwrap", "/tmp/jolt")
        self.assertEqual("/usr/bin/bwrap", command[0])
        self.assertIn("--unshare-net", command)
        self.assertIn("--ro-bind", command)
        self.assertEqual(["/tmp/jolt", "--version"], command[command.index("--") + 1:])

    def test_anchored_profile_requires_clean_tracked_head_blob_and_matches_receipts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            original_root = VERIFY_MODULE.ROOT
            try:
                VERIFY_MODULE.ROOT = root
                receipts, _, _ = self.corpus(root)
                manifest = json.loads((receipts / "run-manifest.json").read_text())
                profile = self.commit_profile(root, manifest)
                provenance = self.bind_profile_provenance(receipts, manifest, profile)
                VERIFY_MODULE.validate_production_profile(profile, manifest)
                self.assertEqual("profiles/release-runtime.json", provenance["path"])
                self.assertRegex(provenance["blob_sha"], r"^[0-9a-f]{40}$")
                self.assertRegex(provenance["head_sha"], r"^[0-9a-f]{40}$")
                self.assertRegex(provenance["tree_sha"], r"^[0-9a-f]{40}$")
                result = self.verify(receipts)
                self.assertEqual(0, result.returncode, result.stderr)
            finally:
                VERIFY_MODULE.ROOT = original_root

    def test_anchored_profile_rejects_untracked_worktree_and_index_dirt(self):
        for mutation in ("untracked", "worktree", "index"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                original_root = VERIFY_MODULE.ROOT
                try:
                    VERIFY_MODULE.ROOT = root
                    receipts, _, _ = self.corpus(root)
                    manifest = json.loads((receipts / "run-manifest.json").read_text())
                    profile = self.commit_profile(root, manifest)
                    self.bind_profile_provenance(receipts, manifest, profile)
                    if mutation == "untracked":
                        candidate = root / "profiles" / "untracked.json"
                        candidate.write_text(profile.read_text())
                        expected = "profile is not tracked"
                    else:
                        candidate = profile
                        candidate.write_text(profile.read_text() + "\n")
                        if mutation == "index":
                            subprocess.check_call(["git", "-C", str(root), "add", "--",
                                                   candidate.relative_to(root).as_posix()])
                        expected = "index or working-tree dirt"
                    with self.assertRaisesRegex(SystemExit, expected):
                        VERIFY_MODULE.validate_production_profile(candidate, manifest)
                finally:
                    VERIFY_MODULE.ROOT = original_root

    def test_anchored_profile_rejects_manifest_or_receipt_provenance_drift(self):
        for mutation in ("manifest", "receipt"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                original_root = VERIFY_MODULE.ROOT
                try:
                    VERIFY_MODULE.ROOT = root
                    receipts, _, _ = self.corpus(root)
                    manifest = json.loads((receipts / "run-manifest.json").read_text())
                    profile = self.commit_profile(root, manifest)
                    self.bind_profile_provenance(receipts, manifest, profile)
                    if mutation == "manifest":
                        manifest["profile_provenance"]["tree_sha"] = "0" * 40
                        self.rebind_manifest(receipts, manifest)
                        with self.assertRaisesRegex(SystemExit, "Git provenance differs"):
                            VERIFY_MODULE.validate_production_profile(profile, manifest)
                    else:
                        path = receipts / "A-1.json"
                        receipt = json.loads(path.read_text())
                        receipt["profile_provenance"]["head_sha"] = "0" * 40
                        self.rebind_receipt(path, receipt)
                        result = self.verify(receipts)
                        self.assertNotEqual(0, result.returncode)
                        self.assertIn("production profile provenance differs", result.stderr)
                finally:
                    VERIFY_MODULE.ROOT = original_root

    def test_anchored_mode_rejects_profile_outside_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            receipts, a, b = self.corpus(root)
            result = self.verify(receipts, root / "missing-profile.json",
                                 a, a, a, b, b, b)
            self.assertNotEqual(0, result.returncode)
            self.assertIn("requires --sandbox-bwrap", result.stderr)

    def test_provenance_mode_rejects_missing_checked_in_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            original_root = VERIFY_MODULE.ROOT
            try:
                VERIFY_MODULE.ROOT = pathlib.Path(directory)
                manifest = {"profile_provenance": {"path": "missing-profile.json",
                                                    "blob_sha": "0" * 40,
                                                    "head_sha": "0" * 40,
                                                    "tree_sha": "0" * 40}}
                with self.assertRaisesRegex(SystemExit, "production profile is missing"):
                    VERIFY_MODULE.validate_production_profile(pathlib.Path(directory) / "missing-profile.json", manifest)
            finally:
                VERIFY_MODULE.ROOT = original_root

    def test_rejects_fixed_identity_or_schedule_receipt_drift(self):
        for mutation in ("fixed", "schedule", "missing"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                receipts, _, _ = self.corpus(pathlib.Path(directory))
                path = receipts / "A-1.json"
                receipt = json.loads(path.read_text())
                if mutation == "fixed":
                    receipt["fixed"]["native"]["version"] = "other"
                    self.rebind_receipt(path, receipt)
                elif mutation == "schedule":
                    receipt["condition"] = "B"
                    self.rebind_receipt(path, receipt)
                else:
                    path.unlink()
                result = self.verify(receipts)
                self.assertNotEqual(0, result.returncode)
                self.assertIn("fixed chDB/data.json/provider/fixture/native identity differs" if mutation == "fixed" else ("condition differs from schedule" if mutation == "schedule" else "receipt directory entry set is not exact"), result.stderr)


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(TestReleaseRuntimeAbba))
    raise SystemExit(not result.wasSuccessful())
