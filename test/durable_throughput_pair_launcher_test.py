#!/usr/bin/env python3
"""Focused shell-contract checks for the restart-safe local A/B/B/A runner."""
from __future__ import annotations

import os
import pathlib
import shlex
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
        for source in [RUNNER, PAIR_CHECKER, SINGLE_CHECKER]:
            target = scripts / source.name
            shutil.copy2(source, target)
            target.chmod(0o755)
        selector = scripts / "run-durable-throughput-selector.sh"
        report = ("{:configurations [{:summaries {:durable-encode-included "
                  "{:admission {:aggregate-rows-per-second 30000.0} "
                  f":persisted {{:aggregate-rows-per-second {rate}}} "
                  f":batch-latency-across-trials {{:count 500 :p50-ms {p50} :p99-ms {p99}}}}}}}]}}")
        selector.write_text(
            "#!/usr/bin/env bash\nset -euo pipefail\n"
            "[ \"$1\" = scale-512 ] || exit 2\n"
            "out=$2; [ ! -e \"$out\" ] || exit 2; mkdir -p \"$out\"\n"
            f"printf '%s\\n' {name} >> \"$PAIR_TRACE\"\n"
            f"printf '%s\\n' {shlex.quote(report)} > \"$out/report.edn\"\n"
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


if __name__ == "__main__":
    unittest.main()
