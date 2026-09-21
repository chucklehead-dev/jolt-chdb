#!/usr/bin/env python3
"""Focused shell-contract checks for the restart-safe local A/B/B/A runner."""
from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "run-durable-throughput-pair.sh"
PAIR_CHECKER = ROOT / "scripts" / "check-durable-throughput-pair-artifacts.sh"
SINGLE_CHECKER = ROOT / "scripts" / "check-durable-throughput-artifacts.sh"


class DurableThroughputPairLauncherTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        self.trace = self.root / "schedule.txt"
        self.a = self.make_repo("a", 19.0, 24.0, 26000.0)
        self.b = self.make_repo("b", 20.0, 25.0, 24000.0)
        self.output = self.root / "pair"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def make_repo(self, name: str, p50: float, p99: float, rate: float) -> pathlib.Path:
        repo = self.root / name
        scripts = repo / "scripts"
        (repo / "bench" / "jdbc").mkdir(parents=True)
        scripts.mkdir()
        (repo / "bench" / "jdbc" / "chdb_durable_throughput.clj").write_text(";; fixture workload\n")
        (repo / "fixture-values").write_text(f"{name} {p50} {p99} {rate}\n")
        for source in [RUNNER, PAIR_CHECKER, SINGLE_CHECKER]:
            target = scripts / source.name
            shutil.copy2(source, target)
            target.chmod(0o755)
        selector = scripts / "run-durable-throughput-selector.sh"
        selector.write_text(
            "#!/usr/bin/env bash\nset -euo pipefail\n"
            "[ \"$1\" = scale-512 ] || exit 2\n"
            "out=$2; [ ! -e \"$out\" ] || exit 2; mkdir -p \"$out\"\n"
            "read -r name p50 p99 rate < fixture-values\n"
            "printf '%s\\n' \"$name\" >> \"$PAIR_TRACE\"\n"
            "printf '{:configurations [{:summaries {:durable-encode-included {:admission {:aggregate-rows-per-second 30000.0} :persisted {:aggregate-rows-per-second %s} :batch-latency-across-trials {:count 500 :p99-qualification? true :p50-ms %s :p99-ms %s}}}}]}\\n' \"$rate\" \"$p50\" \"$p99\" > \"$out/report.edn\"\n"
            "printf 'fixture log\\n' > \"$out/run.log\"\n"
            "printf '\\tMaximum resident set size (kbytes): 12345\\n\\tExit status: 0\\n' > \"$out/time-v.txt\"\n"
        )
        selector.chmod(0o755)
        subprocess.run(["git", "init", "-q", str(repo)], check=True)
        subprocess.run(["git", "-C", str(repo), "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-qm", "parent"], check=True)
        subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
        subprocess.run(["git", "-C", str(repo), "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "fixture"], check=True)
        return repo

    def invoke(self):
        return subprocess.run([str(self.a / "scripts" / RUNNER.name), str(self.output), str(self.a), str(self.b)],
                              text=True, capture_output=True,
                              env=os.environ | {"PAIR_TRACE": str(self.trace)})

    def test_runs_fixed_abba_and_resume_only_skips_validated_arms(self) -> None:
        result = self.invoke()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(["a", "b", "b", "a"], self.trace.read_text().splitlines())
        summary = (self.output / "summary.tsv").read_text()
        self.assertIn("p50_rows_per_second", summary)
        self.assertEqual(4, summary.count("\tpass\n"))
        self.assertIn("condition\t-\tA\t500,500\ttrue,true\tnot-computable-from-retained-quantiles", summary)
        self.assertIn("both_independent_arms_pass", summary)
        rerun = self.invoke()
        self.assertEqual(0, rerun.returncode, rerun.stderr)
        self.assertEqual(["a", "b", "b", "a"], self.trace.read_text().splitlines())
        self.assertIn("resume: validated A1", rerun.stdout)

    def test_rejects_partial_arm_instead_of_rerunning_it(self) -> None:
        partial = self.output / "arms" / "A1"
        partial.mkdir(parents=True)
        result = self.invoke()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("lacks a manifest", result.stderr)
        self.assertFalse(self.trace.exists())

    def test_rejects_changed_completed_artifact_on_resume(self) -> None:
        self.assertEqual(0, self.invoke().returncode)
        report = self.output / "arms" / "A1" / "selector" / "report.edn"
        report.write_text(report.read_text() + "\n")
        result = self.invoke()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("invalid completed evidence", result.stderr)

    def test_rejects_untracked_and_mismatched_harnesses_before_running(self) -> None:
        (self.a / "stray").write_text("untracked\n")
        result = self.invoke()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("untracked files", result.stderr)
        (self.a / "stray").unlink()
        selector = self.b / "scripts" / "run-durable-throughput-selector.sh"
        selector.write_text(selector.read_text() + "# mismatch\n")
        subprocess.run(["git", "-C", str(self.b), "add", "scripts/run-durable-throughput-selector.sh"], check=True)
        subprocess.run(["git", "-C", str(self.b), "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "mismatch"], check=True)
        result = self.invoke()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("selector sources differ", result.stderr)

    def test_condition_requires_both_independent_target_passes(self) -> None:
        (self.b / "fixture-values").write_text("b 20.0 26.0 24000.0\n")
        subprocess.run(["git", "-C", str(self.b), "add", "fixture-values"], check=True)
        subprocess.run(["git", "-C", str(self.b), "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "slow-tail"], check=True)
        result = self.invoke()
        self.assertEqual(0, result.returncode, result.stderr)
        summary = (self.output / "summary.tsv").read_text()
        self.assertIn("condition\t-\tB\t500,500\ttrue,true\tnot-computable-from-retained-quantiles", summary)
        self.assertTrue(summary.rstrip().endswith("\tfail"))


if __name__ == "__main__":
    unittest.main()
