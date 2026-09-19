#!/usr/bin/env python3
"""Controls for the release-runtime launcher, without running a benchmark."""
import importlib.util
import json
import os
import pathlib
import subprocess
import tempfile
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[1]
LAUNCHER_PATH = ROOT / "scripts" / "profile-durable-release-runtime-abba.py"
spec = importlib.util.spec_from_file_location("release_runtime_launcher", LAUNCHER_PATH)
LAUNCHER = importlib.util.module_from_spec(spec)
spec.loader.exec_module(LAUNCHER)


class TestReleaseRuntimeLauncher(unittest.TestCase):
    def test_outer_schedule_only_uses_matching_source_slots(self):
        expected = [
            ("A'", "A", "prime", None, 1, "prime", None, "jolt-prime.json", "A-prime.json"),
            ("B'", "B", "prime", None, 1, "prime", None, "jolt-prime.json", "B-prime.json"),
            ("A", "A", "measured", 1, 3, "measured", 1, "jolt-trial-1.json", "A-1.json"),
            ("B", "B", "measured", 1, 4, "measured", 2, "jolt-trial-2.json", "B-1.json"),
            ("B", "B", "measured", 2, 7, "measured", 3, "jolt-trial-3.json", "B-2.json"),
            ("A", "A", "measured", 2, 8, "measured", 4, "jolt-trial-4.json", "A-2.json"),
        ]
        self.assertEqual(expected, LAUNCHER.SCHEDULE)

    def test_offline_command_is_network_isolated_and_remounts_verified_inputs_read_only(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)
            material = output / "verified-inputs"
            material.mkdir()
            command = LAUNCHER.offline_command("/usr/bin/bwrap", ["cargo", "build"], [output], [material])
            self.assertEqual("/usr/bin/bwrap", command[0])
            self.assertIn("--unshare-net", command)
            self.assertIn("--ro-bind", command)
            self.assertIn("--", command)
            bind = command.index("--bind")
            self.assertEqual(str(output.resolve()), command[bind + 1])
            self.assertEqual(str(output.resolve()), command[bind + 2])
            readonly = [index for index, value in enumerate(command) if value == "--ro-bind"]
            self.assertGreaterEqual(len(readonly), 2)
            material_bind = readonly[-1]
            self.assertEqual(str(material.resolve()), command[material_bind + 1])
            self.assertEqual(str(material.resolve()), command[material_bind + 2])
            self.assertGreater(material_bind, bind)
            self.assertEqual(["cargo", "build"], command[command.index("--") + 1:])

    def test_sandbox_environment_uses_an_output_contained_temp_for_every_child(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory) / "output"
            output.mkdir()
            with mock.patch.dict(os.environ, {"TMPDIR": "/ambient/tmp", "TEMP": "/ambient/temp",
                                              "TMP": "/ambient/TMP"}, clear=False):
                env = LAUNCHER.sandbox_environment(output, {"JOLT_CACHE_DIR": str(output / "cache")})
            expected = str((output / "tmp").resolve())
            self.assertTrue((output / "tmp").is_dir())
            self.assertEqual(expected, env["TMPDIR"])
            self.assertEqual(expected, env["TEMP"])
            self.assertEqual(expected, env["TMP"])
            self.assertEqual(str(output / "cache"), env["JOLT_CACHE_DIR"])
            command = LAUNCHER.offline_command("/usr/bin/bwrap", ["jolt", "--version"], [output])
            self.assertEqual(str(output.resolve()), command[command.index("--bind") + 1])

    def test_raw_receipt_rejects_relabelled_measured_slot_as_prime(self):
        value = {
            "schema_version": 1, "run_id": "run", "schedule_ordinal": 3,
            "phase": "measured", "runtime": {}, "trial": 1, "process_id": 1,
            "process_started_epoch_ms": 1, "process_finished_epoch_ms": 2,
            "cache_condition": "fresh", "fixture": {},
            "recovery": {"expected": {}, "actual": {}, "inventory_unchanged": True},
        }
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "jolt-trial-1.json"
            path.write_text(json.dumps(value))
            with self.assertRaisesRegex(SystemExit, "selected source schedule"):
                LAUNCHER.raw_receipt(path, 3, "prime", None)

    def test_source_reader_command_uses_prepared_source_plan_not_outer_receipt_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            reports, receipts = root / "source-reports", root / "receipts"
            reports.mkdir(); receipts.mkdir()
            outer_manifest = receipts / "run-manifest.json"
            outer_manifest.write_text('{"mode":"release-runtime-abba"}\n')
            command = LAUNCHER.source_reader_command("/reviewed/jolt", root / "fixture-store",
                                                      reports, 3, root / "raw" / "trial.json")
            manifest_index = command.index("release-runtime-abba") + 2
            self.assertEqual(str(reports / "run-manifest.json"), command[manifest_index])
            self.assertNotIn(str(outer_manifest), command)

    def test_observed_runtime_must_match_the_snapshotted_profile_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary, library, header = root / "jolt", root / "libchdb.so", root / "chdb.h"
            binary.write_bytes(b"jolt"); library.write_bytes(b"library"); header.write_bytes(b"header")
            runtime = {"version": "jolt v0.8.9", "release": {"tag_commit": "a" * 40},
                       "binary": LAUNCHER.identity(binary)}
            native = {"version": "26.7.3", "library": LAUNCHER.identity(library),
                      "header": LAUNCHER.identity(header)}
            measured = {"runtime": {"jolt_version": runtime["version"],
                                     "jolt_source_sha_asserted": runtime["release"]["tag_commit"],
                                     "native_version": native["version"],
                                     "executable": LAUNCHER.identity(binary),
                                     "native_library": LAUNCHER.identity(library),
                                     "native_header": LAUNCHER.identity(header)}}
            LAUNCHER.verify_observed_runtime(measured, runtime, native, binary, library, header)
            measured["runtime"]["native_header"]["sha256"] = "0" * 64
            with self.assertRaisesRegex(SystemExit, "native header identity differs"):
                LAUNCHER.verify_observed_runtime(measured, runtime, native, binary, library, header)

    def test_launcher_hardens_cargo_and_bindgen_header_selection(self):
        text = LAUNCHER_PATH.read_text()
        self.assertIn('"--offline", "--release"', text)
        self.assertIn('"CARGO_NET_OFFLINE": "true"', text)
        self.assertIn('"CHDB_INCLUDE_DIR": str(native_header.parent)', text)
        self.assertIn('header.name != "chdb.h"', text)
        self.assertIn("snapshot_verified_inputs", text)
        self.assertIn("snapshot_source", text)
        self.assertIn("snapshot_control", text)
        self.assertIn('"verifier_script"', text)
        self.assertIn("[output], [material]", text)

    def test_execution_environment_drops_ambient_credentials_wrappers_and_flags(self):
        with mock.patch.dict(os.environ, {
            "PATH": "/reviewed/bin", "LANG": "C.UTF-8", "RUSTUP_HOME": "/toolchain",
            "CARGO_REGISTRIES_PRIVATE_TOKEN": "must-not-leak", "CARGO_HOME": "/ambient/cargo",
            "CARGO_TARGET_DIR": "/ambient/target", "RUSTFLAGS": "-Ctarget-cpu=native",
            "RUSTC_WRAPPER": "/tmp/wrapper", "JOLT_CACHE_DIR": "/ambient/jolt",
            "JOLT_GITLIBS_DIR": "/ambient/gitlibs", "TMPDIR": "/ambient/tmp",
            "AWS_SECRET_ACCESS_KEY": "must-not-leak",
        }, clear=True):
            env = LAUNCHER.execution_environment({"HOME": "/isolated/home", "CARGO_HOME": "/isolated/cargo"})
        self.assertEqual("/reviewed/bin", env["PATH"])
        self.assertEqual("/toolchain", env["RUSTUP_HOME"])
        self.assertEqual("/isolated/cargo", env["CARGO_HOME"])
        self.assertNotIn("CARGO_REGISTRIES_PRIVATE_TOKEN", env)
        self.assertNotIn("CARGO_TARGET_DIR", env)
        self.assertNotIn("RUSTFLAGS", env)
        self.assertNotIn("RUSTC_WRAPPER", env)
        self.assertNotIn("JOLT_CACHE_DIR", env)
        self.assertNotIn("JOLT_GITLIBS_DIR", env)
        self.assertNotIn("TMPDIR", env)
        self.assertNotIn("AWS_SECRET_ACCESS_KEY", env)

    def test_profile_binds_both_control_scripts(self):
        profile = json.loads((ROOT / "profiles/durable-release-runtime-abba.json").read_text())
        self.assertEqual(LAUNCHER.identity(LAUNCHER_PATH), profile["fixed"]["runner_script"])
        self.assertEqual(LAUNCHER.identity(LAUNCHER.VERIFY_PATH), profile["fixed"]["verifier_script"])

    def test_profile_and_source_rejects_a_replaced_verifier_before_import(self):
        profile_path = ROOT / "profiles/durable-release-runtime-abba.json"
        original_identity = LAUNCHER.identity

        def replaced_identity(path):
            value = original_identity(path)
            if pathlib.Path(path).resolve() == LAUNCHER.VERIFY_PATH.resolve():
                value = dict(value, sha256="0" * 64)
            return value

        with mock.patch.object(LAUNCHER, "identity", side_effect=replaced_identity):
            with self.assertRaisesRegex(SystemExit, "verifier script identity differs"):
                LAUNCHER.profile_and_source(profile_path, ROOT)

    def test_git_aware_snapshot_is_exact_and_rejects_the_wrong_source_tree(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source, material = root / "source", root / "material"
            source.mkdir(); material.mkdir()
            subprocess.check_call(["git", "init", "-q", str(source)])
            (source / "tracked.txt").write_text("reviewed\n")
            subprocess.check_call(["git", "-C", str(source), "add", "tracked.txt"])
            subprocess.check_call(["git", "-C", str(source), "-c", "user.name=Test", "-c",
                                   "user.email=test@example.invalid", "commit", "-q", "-m", "source"])
            head = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
            tree = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD^{tree}"], text=True).strip()
            snapshot = LAUNCHER.snapshot_source(source, material, head, tree)
            self.assertTrue((snapshot / ".git").is_dir())
            self.assertEqual(head, subprocess.check_output(["git", "-C", str(snapshot), "rev-parse", "HEAD"], text=True).strip())
            self.assertEqual("", subprocess.check_output(["git", "-C", str(snapshot), "status", "--porcelain"], text=True))
            with self.assertRaisesRegex(SystemExit, "source checkout changed"):
                LAUNCHER.snapshot_source(source, root / "wrong-material", head, "0" * 40)

    def test_cargo_home_seed_is_mandatory_bound_and_not_inherited(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            seed, material = root / "seed", root / "material"
            (seed / "registry").mkdir(parents=True)
            (seed / "git").mkdir()
            (seed / "registry" / "crate").write_bytes(b"offline crate")
            (seed / "git" / "checkout").write_bytes(b"offline git crate")
            expected = LAUNCHER.cargo_home_identity(seed)
            self.assertEqual(expected, LAUNCHER.verify_cargo_home_seed(seed, expected))
            (seed / "registry" / "crate").write_bytes(b"changed")
            with self.assertRaisesRegex(SystemExit, "Cargo home seed differs"):
                LAUNCHER.verify_cargo_home_seed(seed, expected)
            (seed / "registry" / "crate").write_bytes(b"offline crate")
            material.mkdir()
            copied = LAUNCHER.snapshot_cargo_home(seed, material, expected)
            self.assertEqual(expected, LAUNCHER.cargo_home_identity(copied))
            with mock.patch.dict(os.environ, {"CARGO_HOME": "/ambient/cargo"}, clear=False):
                env = LAUNCHER.cargo_build_environment(root, copied, root / "native", root / "libchdb.so",
                                                       root / "chdb.h", root / "reports", root / "target")
            self.assertEqual(str(copied), env["CARGO_HOME"])
            self.assertNotEqual("/ambient/cargo", env["CARGO_HOME"])
            self.assertEqual("true", env["CARGO_NET_OFFLINE"])
            self.assertEqual(str((root / "tmp").resolve()), env["TMPDIR"])

    def test_jolt_seed_is_digest_bound_snapshotted_and_provider_resolved_without_ambient_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            cache, gitlibs, material = root / "cache", root / "gitlibs", root / "material"
            (cache / "compiled").mkdir(parents=True)
            (cache / "compiled" / "unit.so").write_bytes(b"compiled cache")
            sha = "a" * 40
            namespace_path = gitlibs / "https___example.invalid_data.json.git" / sha / "src/main/clojure/clojure/data/json.clj"
            namespace_path.parent.mkdir(parents=True)
            namespace_path.write_bytes(b"(ns clojure.data.json)")
            namespace = LAUNCHER.identity(namespace_path)
            profile = {"fixed": {"jolt_cache": LAUNCHER.dependency_seed_identity(cache, "Jolt cache"),
                                  "jolt_gitlibs": LAUNCHER.dependency_seed_identity(gitlibs, "Jolt gitlibs"),
                                  "data_json": {"source_sha": sha, "namespace": namespace},
                                  "provider": {"source_sha": sha, "namespace": namespace}}}
            self.assertEqual(profile["fixed"]["jolt_cache"],
                             LAUNCHER.verify_dependency_seed(cache, profile["fixed"]["jolt_cache"], "Jolt cache"))
            self.assertTrue(LAUNCHER.verify_resolved_provider_seed(gitlibs, profile))
            material.mkdir()
            copied_cache = LAUNCHER.snapshot_dependency_seed(cache, material, "jolt-cache-seed",
                                                              profile["fixed"]["jolt_cache"], "Jolt cache")
            copied_gitlibs = LAUNCHER.snapshot_dependency_seed(gitlibs, material, "jolt-gitlibs-seed",
                                                                profile["fixed"]["jolt_gitlibs"], "Jolt gitlibs")
            self.assertEqual(profile["fixed"]["jolt_cache"], LAUNCHER.dependency_seed_identity(copied_cache, "Jolt cache"))
            self.assertTrue(LAUNCHER.verify_resolved_provider_seed(copied_gitlibs, profile))
            (cache / "compiled" / "unit.so").write_bytes(b"mutated")
            with self.assertRaisesRegex(SystemExit, "Jolt cache seed differs"):
                LAUNCHER.verify_dependency_seed(cache, profile["fixed"]["jolt_cache"], "Jolt cache")
            (namespace_path).write_bytes(b"mutated provider")
            with self.assertRaisesRegex(SystemExit, "Jolt gitlibs seed differs"):
                LAUNCHER.verify_dependency_seed(gitlibs, profile["fixed"]["jolt_gitlibs"], "Jolt gitlibs")


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(TestReleaseRuntimeLauncher))
    raise SystemExit(not result.wasSuccessful())
