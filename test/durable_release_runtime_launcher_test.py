#!/usr/bin/env python3
"""Controls for the release-runtime launcher, without running a benchmark."""
import importlib.util
import json
import pathlib
import tempfile
import unittest

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
        self.assertIn('CARGO_NET_OFFLINE="true"', text)
        self.assertIn('CHDB_INCLUDE_DIR=str(native_header.parent)', text)
        self.assertIn('header.name != "chdb.h"', text)
        self.assertIn("snapshot_verified_inputs", text)
        self.assertIn("snapshot_source", text)
        self.assertIn("[output], [material]", text)


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(TestReleaseRuntimeLauncher))
    raise SystemExit(not result.wasSuccessful())
