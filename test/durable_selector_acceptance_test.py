#!/usr/bin/env python3
"""Black-box controls for the local 512-row selector receipt boundary."""
from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"


class DurableSelectorAcceptanceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.repo = self.root / "repo"
        scripts = self.repo / "scripts"
        scripts.mkdir(parents=True)
        for name in ("run-durable-throughput-selector.sh",
                     "check-durable-throughput-artifacts.sh",
                     "check-durable-512-acceptance.py"):
            source = SCRIPTS / name
            if source.exists():
                shutil.copy2(source, scripts / name)
        self.selector = scripts / "run-durable-throughput-selector.sh"
        subprocess.run(["git", "init", "-q", str(self.repo)], check=True)
        subprocess.run(["git", "-C", str(self.repo), "-c", "user.name=test", "-c",
                        "user.email=test@example.invalid", "commit", "--allow-empty",
                        "-qm", "ancestor"], check=True)
        subprocess.run(["git", "-C", str(self.repo), "add", "scripts"], check=True)
        subprocess.run(["git", "-C", str(self.repo), "-c", "user.name=test", "-c",
                        "user.email=test@example.invalid", "commit", "-qm", "fixture"], check=True)
        self.fake = self.root / "fake-jolt"
        self.fake.write_text(
            '#!/usr/bin/env bash\n'
            'if [[ ${1:-} == --version ]]; then echo jolt-fixture; exit 0; fi\n'
            'if [[ ${FAKE_MISSING_REPORT:-0} != 1 ]]; then '
            'printf "%s\\n" "$FAKE_REPORT" > "$3"; fi\n'
            'printf "worker-complete\\n" > "$BENCH_PERSISTENT_RECEIPT_ROOT/worker.txt"\n'
            'if [[ ${FAKE_CATEGORY:-0} == 1 ]]; then '
            'echo "ExceptionInfo {:type :jdbc.chdb-durable-throughput/acceptance-target-missed}" >&2; fi\n'
            'exit "$FAKE_STATUS"\n'
        )
        self.fake.chmod(0o755)
        self.wrapper = self.root / "wrapper"
        self.wrapper.write_text('#!/usr/bin/env bash\nexec "$@"\n')
        self.wrapper.chmod(0o755)
        self.library = self.root / "libchdb.so"
        self.library.write_bytes(b"fixture")
        self.acceptance = (
            ':encoding-inclusive-512-acceptance '
            '{:status %s :target {:sample-count 500 :p50-max-ms 20.48 :p99-max-ms 25.60} '
            ':observed {:count 500 :p99-qualification? true :p50-ms 19.0 :p99-ms 24.0}}'
        )

    def invoke(self, *, status: int, category: bool, acceptance_status: str,
               report: str | None = None, missing: bool = False,
               canary: str | None = None, selector: str = "scale-512"):
        output = self.root / f"output-{len(list(self.root.glob('output-*')))}"
        receipt = self.acceptance % acceptance_status
        if acceptance_status == ":failed":
            receipt = receipt.replace(":status :failed", ":status :failed :reason :latency-target-missed")
            receipt = receipt.replace(":p50-ms 19.0 :p99-ms 24.0",
                                      ":p50-ms 107.095 :p99-ms 187.387")
        body = report if report is not None else (
            '{:schema-version 2, :profile :scale-512, '
            ':configurations [{:configuration {:selector :scale-512, :batch-size 512}, '
            ':results [{}], :summaries {:durable-encode-included {}}}], :phase-log [] '
            + receipt + '}')
        env = os.environ | {
            "JOLT_WRAPPER": str(self.wrapper), "BENCH_JOLT_BIN": str(self.fake),
            "BENCH_JOLT_SOURCE_SHA": "0" * 40, "JOLT_CHDB_LIB": str(self.library),
            "FAKE_REPORT": body, "FAKE_STATUS": str(status),
            "FAKE_CATEGORY": "1" if category else "0",
            "FAKE_MISSING_REPORT": "1" if missing else "0",
        }
        if canary is not None:
            env["BENCH_PAYLOAD_CANARY"] = canary
        result = subprocess.run([str(self.selector), selector, str(output)],
                                cwd=self.repo, env=env, capture_output=True, text=True)
        return result, output

    def test_passing_512_target(self) -> None:
        result, output = self.invoke(status=0, category=False, acceptance_status=":passed")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("selector completed", result.stdout)
        self.assertTrue((output / "time-v.txt").is_file())
        self.assertEqual("worker-complete\n", (output / "receipts" / "worker.txt").read_text())

    def test_known_miss_retains_valid_artifacts_and_reports_scalars(self) -> None:
        result, output = self.invoke(status=1, category=True, acceptance_status=":failed")
        self.assertEqual(1, result.returncode)
        self.assertIn("acceptance target missed", result.stderr)
        self.assertIn(str(output / "report.edn"), result.stderr)
        self.assertIn("107.095", result.stderr)
        self.assertIn("20.48", result.stderr)
        self.assertIn("187.387", result.stderr)
        self.assertIn("25.60", result.stderr)
        for name in ("report.edn", "run.log", "time-v.txt", "receipts/worker.txt"):
            self.assertTrue((output / name).is_file())
        self.assertIn("Exit status: 1", (output / "time-v.txt").read_text())

    def test_unrelated_failure_and_bad_receipts_are_rejected(self) -> None:
        cases = (
            {"status": 1, "category": False, "acceptance_status": ":failed"},
            {"status": 2, "category": True, "acceptance_status": ":failed"},
            {"status": 1, "category": True, "acceptance_status": ":passed"},
            {"status": 1, "category": True, "acceptance_status": ":failed", "missing": True},
            {"status": 1, "category": True, "acceptance_status": ":failed", "report": "not-edn"},
            {"status": 1, "category": True, "acceptance_status": ":failed",
             "canary": "latency-target-missed"},
        )
        for case in cases:
            with self.subTest(case=case):
                result, _ = self.invoke(**case)
                self.assertNotEqual(0, result.returncode)
                self.assertNotIn("acceptance target missed: report=", result.stderr)

    def test_diagnostic_selector_does_not_qualify(self) -> None:
        result, _ = self.invoke(status=0, category=False, acceptance_status=":passed",
                                selector="stage-512")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertNotIn("acceptance target missed", result.stderr)


if __name__ == "__main__":
    unittest.main()
