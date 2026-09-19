#!/usr/bin/env python3
"""Lightweight red/green controls for the exhaustive-obligation inventory."""
import importlib.util
import json
import pathlib
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts/run-durable-exhaustive-obligations.py"
SPEC = importlib.util.spec_from_file_location("durable_exhaustive_obligations", SCRIPT_PATH)
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)
INVENTORY_PATH = ROOT / "formal/quint/durable-exhaustive-obligations.json"


class TestDurableExhaustiveObligations(unittest.TestCase):
    def inventory(self):
        return json.loads(INVENTORY_PATH.read_text())

    def test_checked_inventory_covers_current_positive_and_mutant_contract(self):
        document = self.inventory()
        RUNNER.validate_inventory(document)
        corrected = [row for row in document["obligations"] if row["kind"] == "corrected"]
        mutants = [row for row in document["obligations"] if row["kind"] == "mutant"]
        self.assertEqual(13, len(corrected))
        self.assertEqual(24, sum(len(row["invariants"]) for row in corrected))
        self.assertEqual(20, len(mutants))
        self.assertTrue(all(row["itf"] for row in mutants))

    def test_removed_or_changed_positive_row_is_rejected(self):
        removed = self.inventory()
        removed["obligations"] = [row for row in removed["obligations"]
                                if row["id"] != "head-stale-writer"]
        with self.assertRaisesRegex(ValueError, "changed, removed, or added"):
            RUNNER.validate_inventory(removed)
        changed = self.inventory()
        changed["obligations"][0]["max_steps"] = 99
        with self.assertRaisesRegex(ValueError, "changed, removed, or added"):
            RUNNER.validate_inventory(changed)

    def test_removed_or_changed_mutant_row_is_rejected(self):
        removed = self.inventory()
        removed["obligations"] = [row for row in removed["obligations"]
                                if row["id"] != "head-stale-ownership"]
        with self.assertRaisesRegex(ValueError, "changed, removed, or added"):
            RUNNER.validate_inventory(removed)
        changed = self.inventory()
        row = next(row for row in changed["obligations"] if row["id"] == "writer-boundary")
        row["itf"] = "target/formal/quint/not-the-reviewed-witness.itf.json"
        with self.assertRaisesRegex(ValueError, "changed, removed, or added"):
            RUNNER.validate_inventory(changed)

    def write_literate_sources(self, root):
        for source in RUNNER.LITERATE_SOURCES:
            path = root / source
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# placeholder\n")

    def write_fake_lmt(self, root):
        fake = root / "lmt"
        fake.write_text("""#!/usr/bin/env python3
import pathlib
import sys
outputs = {
    'formal/quint/durable-head-cas.md': [
        'durableHeadCas.qnt', 'durableHeadCasTest.qnt',
        'durablePublicationAck.qnt', 'durablePublicationAckTest.qnt',
        'durableWriterBoundary.qnt', 'durableWriterBoundaryTest.qnt',
        'durableLeaseTime.qnt', 'durableLeaseTimeTest.qnt',
        'durableEngineMetadata.qnt', 'durableEngineMetadataTest.qnt'],
    'formal/quint/durable-writer-lifecycle.md': [
        'durableWriterLifecycle.qnt', 'durableWriterLifecycleTest.qnt'],
    'formal/quint/native-process-lifecycle.md': [
        'nativeProcessLifecycle.qnt', 'nativeProcessLifecycleTest.qnt']}
for output in outputs[sys.argv[1]]:
    path = pathlib.Path('target/formal/quint') / output
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('module placeholder {}')
""")
        fake.chmod(0o755)
        return fake

    def run_with_fake_tools(self, root, fake_quint, expectation=None):
        document = self.inventory()
        self.write_literate_sources(root)
        fake_lmt = self.write_fake_lmt(root)
        old_root, old_inventory, old_preflight = (
            RUNNER.ROOT, RUNNER.DEFAULT_INVENTORY, RUNNER.require_pinned_lmt)
        try:
            RUNNER.ROOT = root
            RUNNER.DEFAULT_INVENTORY = root / "formal/quint/durable-exhaustive-obligations.json"
            RUNNER.require_pinned_lmt = lambda lmt: str(fake_lmt)
            if expectation is None:
                return RUNNER.run_obligations(document, root / "report", str(fake_quint), "/usr/bin/time",
                                               RUNNER.DEFAULT_INVENTORY, str(fake_lmt))
            with self.assertRaisesRegex(ValueError, expectation):
                RUNNER.run_obligations(document, root / "report", str(fake_quint), "/usr/bin/time",
                                       RUNNER.DEFAULT_INVENTORY, str(fake_lmt))
        finally:
            RUNNER.ROOT, RUNNER.DEFAULT_INVENTORY, RUNNER.require_pinned_lmt = (
                old_root, old_inventory, old_preflight)

    def test_runner_tangles_required_literate_sources_in_clean_checkout(self):
        document = self.inventory()
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            fake = root / "quint"
            fake.write_text("""#!/usr/bin/env python3
import pathlib
import sys
if sys.argv[1:] == ['--version']:
    print('0.32.0')
    raise SystemExit(0)
if '--out-itf' in sys.argv:
    path = pathlib.Path(sys.argv[sys.argv.index('--out-itf') + 1])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{}')
    print('[violation] Found an issue')
    raise SystemExit(1)
print('checked')
""")
            fake.chmod(0o755)
            self.assertFalse((root / "target").exists())
            report = self.run_with_fake_tools(root, fake)
            self.assertEqual(33, len(report["obligations"]))
            self.assertTrue((root / "report/obligation-timings.json").is_file())
            self.assertTrue((root / "target/formal/quint/durableHeadCas.qnt").is_file())
            self.assertEqual("success", report["obligations"][0]["outcome"])
            self.assertEqual("violation", report["obligations"][-1]["outcome"])

    def test_runner_rejects_a_mutant_that_only_reuses_a_stale_itf(self):
        document = self.inventory()
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            stale = root / next(row["itf"] for row in document["obligations"] if row["itf"])
            stale.parent.mkdir(parents=True, exist_ok=True)
            stale.write_text("stale witness")
            fake = root / "quint-no-itf"
            fake.write_text("""#!/usr/bin/env python3
import sys
if sys.argv[1:] == ['--version']:
    print('0.32.0')
    raise SystemExit(0)
if '--out-itf' in sys.argv:
    print('[violation] Found an issue')
    raise SystemExit(1)
print('checked')
""")
            fake.chmod(0o755)
            self.run_with_fake_tools(root, fake, "did not produce required ITF")
            self.assertFalse(stale.exists())

    def test_runner_rejects_unanchored_mutant_violation_text(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            fake = root / "quint-unanchored-violation"
            fake.write_text("""#!/usr/bin/env python3
import pathlib
import sys
if sys.argv[1:] == ['--version']:
    print('0.32.0')
    raise SystemExit(0)
if '--out-itf' in sys.argv:
    path = pathlib.Path(sys.argv[sys.argv.index('--out-itf') + 1])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{}')
    print('noise [violation] Found an issue')
    raise SystemExit(1)
print('checked')
""")
            fake.chmod(0o755)
            self.run_with_fake_tools(root, fake, "mutant obligation failed its expected outcome")

    def test_pinned_lmt_preflight_rejects_wrong_module_version(self):
        class Completed:
            returncode = 0
            stdout = "mod\tgithub.com/driusan/lmt\tv0.0.0-wrong\n"
            stderr = ""

        original_run = RUNNER.subprocess.run
        try:
            RUNNER.subprocess.run = lambda *args, **kwargs: Completed()
            with self.assertRaisesRegex(ValueError, "pinned Durable tangler"):
                RUNNER.require_pinned_lmt("/tmp/lmt")
        finally:
            RUNNER.subprocess.run = original_run


if __name__ == "__main__":
    raise SystemExit(not unittest.main(verbosity=2))
