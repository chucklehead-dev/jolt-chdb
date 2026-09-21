#!/usr/bin/env python3
"""Focused black-box checks for the opt-in selector launch forensics."""
from __future__ import annotations

import os
import pathlib
import signal
import shutil
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
SELECTOR = ROOT / "scripts" / "run-durable-throughput-selector.sh"
DIAGNOSE = ROOT / "scripts" / "diagnose-durable-throughput-selector.sh"


def strace_usable() -> bool:
    """PATH presence is insufficient under ptrace-restricted containers."""
    if not shutil.which("strace"):
        return False
    with tempfile.TemporaryDirectory() as tmp:
        probe = pathlib.Path(tmp) / "probe"
        return subprocess.run(
            ["strace", "-qq", "-e", "trace=none", "-o", str(probe), "true"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode == 0


class DurableSelectorForensicsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.tmp.name)
        # The selector correctly rejects a dirty checkout. Exercise it in a
        # disposable clean repository rather than weakening that invariant for
        # the source checkout containing this test.
        self.repo = root / "repo"
        scripts = self.repo / "scripts"
        scripts.mkdir(parents=True)
        for source in [SELECTOR, DIAGNOSE, ROOT / "scripts" / "check-durable-throughput-artifacts.sh"]:
            target = scripts / source.name
            shutil.copy2(source, target)
            target.chmod(0o755)
        self.selector = scripts / SELECTOR.name
        self.diagnose = scripts / DIAGNOSE.name
        subprocess.run(["git", "init", "-q", str(self.repo)], check=True)
        subprocess.run(
            ["git", "-C", str(self.repo), "-c", "user.name=test", "-c",
             "user.email=test@example.invalid", "commit", "--allow-empty", "-qm", "ancestor"],
            check=True,
        )
        subprocess.run(["git", "-C", str(self.repo), "add", "scripts"], check=True)
        subprocess.run(
            ["git", "-C", str(self.repo), "-c", "user.name=test", "-c",
             "user.email=test@example.invalid", "commit", "-qm", "fixture"],
            check=True,
        )
        self.fake_bin = root / "fake-jolt"
        self.fake_bin.write_text(
            "#!/usr/bin/env bash\n"
            "if [[ ${1:-} == --version ]]; then echo 'jolt vtest'; exit 0; fi\n"
            "if [[ -n ${FAKE_JOLT_CREATE_CACHES:-} ]]; then\n"
            "  mkdir -p \"$JOLT_CACHE_DIR\" \"$JOLT_GITLIBS_DIR\"\n"
            "fi\n"
            "if [[ -n ${FAKE_JOLT_PID_FILE:-} ]]; then\n"
            "  printf '%s\\n' \"$$\" > \"$FAKE_JOLT_PID_FILE\"\n"
            "  trap 'exit 42' TERM HUP INT\n"
            "  while true; do sleep 1; done\n"
            "fi\n"
            "exit ${FAKE_JOLT_STATUS:-17}\n"
        )
        self.fake_bin.chmod(0o755)
        self.wrapper = root / "wrapper"
        self.wrapper.write_text("#!/usr/bin/env bash\nexec \"$@\"\n")
        self.wrapper.chmod(0o755)
        self.library = root / "libchdb.so"
        self.library.write_bytes(b"not-loaded")
        self.env = os.environ | {
            "JOLT_WRAPPER": str(self.wrapper),
            "BENCH_JOLT_BIN": str(self.fake_bin),
            "BENCH_JOLT_SOURCE_SHA": "0" * 40,
            "JOLT_CHDB_LIB": str(self.library),
            "DURABLE_SELECTOR_FORENSICS_STRACE": "0",
        }

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_selector_records_preflight_child_and_nonzero_return(self) -> None:
        root = pathlib.Path(self.tmp.name)
        output = root / "selector-output"
        forensic = root / "forensics"
        forensic.mkdir()
        result = subprocess.run(
            [str(self.selector), "scale-512", str(output)],
            cwd=self.repo,
            env=self.env | {"DURABLE_SELECTOR_FORENSICS_DIR": str(forensic)},
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)
        events = (forensic / "selector.events").read_text()
        self.assertIn("selector-started", events)
        self.assertIn("jolt-command-about-to-start", events)
        self.assertIn("time-child-session-started pid=", events)
        self.assertIn("time-child-returned status=17", events)
        self.assertIn("artifact-validation-about-to-start", events)
        self.assertIn("selector-exit status=", events)
        self.assertTrue((output / "run.log").is_file())
        self.assertTrue((output / "time-v.txt").is_file())

    def test_selector_default_path_does_not_create_forensic_sidecar(self) -> None:
        root = pathlib.Path(self.tmp.name)
        output = root / "ordinary-output"
        result = subprocess.run(
            [str(self.selector), "scale-512", str(output)],
            cwd=self.repo,
            env=self.env,
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue((output / "run.log").is_file())
        self.assertTrue((output / "time-v.txt").is_file())
        self.assertFalse((root / "ordinary-output.launch-forensics").exists())

    def test_selector_allows_runtime_caches_outside_the_checkout(self) -> None:
        root = pathlib.Path(self.tmp.name)
        output = root / "cache-output"
        cache = root / "runtime-cache"
        gitlibs = root / "runtime-gitlibs"
        result = subprocess.run(
            [str(self.selector), "stage-smoke", str(output)],
            cwd=self.repo,
            env=self.env | {
                "FAKE_JOLT_CREATE_CACHES": "1",
                "JOLT_CACHE_DIR": str(cache),
                "JOLT_GITLIBS_DIR": str(gitlibs),
            },
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(cache.is_dir())
        self.assertTrue(gitlibs.is_dir())
        self.assertEqual(
            "", subprocess.check_output(
                ["git", "-C", str(self.repo), "status", "--porcelain"], text=True
            ),
        )
        self.assertTrue((output / "run.log").is_file())
        self.assertTrue((output / "time-v.txt").is_file())

    def test_selector_rejects_tracked_and_untracked_state_before_artifacts(self) -> None:
        root = pathlib.Path(self.tmp.name)
        original = self.selector.read_text()

        with self.subTest("tracked modification"):
            self.selector.write_text(original + "# dirty tracked fixture\n")
            output = root / "tracked-dirty-output"
            result = subprocess.run(
                [str(self.selector), "scale-512", str(output)],
                cwd=self.repo,
                env=self.env,
                text=True,
                capture_output=True,
            )
            self.assertEqual(2, result.returncode)
            self.assertIn("benchmark checkout must be clean", result.stderr)
            self.assertFalse(output.exists())
            self.selector.write_text(original)

        with self.subTest("untracked helper"):
            untracked = self.repo / "untracked-benchmark-helper"
            untracked.write_text("not reviewed\n")
            output = root / "untracked-dirty-output"
            result = subprocess.run(
                [str(self.selector), "scale-512", str(output)],
                cwd=self.repo,
                env=self.env,
                text=True,
                capture_output=True,
            )
            self.assertEqual(2, result.returncode)
            self.assertIn("benchmark checkout must be clean", result.stderr)
            self.assertFalse(output.exists())
            untracked.unlink()

    def test_diagnostic_launcher_keeps_outer_lifecycle_and_inventory(self) -> None:
        root = pathlib.Path(self.tmp.name)
        output = root / "diagnostic-output"
        sentinel = "credential-sentinel-must-not-be-serialized"
        result = subprocess.run(
            [str(self.diagnose), "scale-512", str(output)],
            cwd=self.repo,
            env=self.env | {"CREDENTIAL_SENTINEL": sentinel},
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)
        forensic = pathlib.Path(f"{output}.launch-forensics")
        events = (forensic / "launcher.events").read_text()
        inventory = (forensic / "output-inventory.txt").read_text()
        self.assertIn("launcher-started", events)
        self.assertIn("strace-disabled", events)
        self.assertIn("selector-child-started pid=", events)
        self.assertIn("selector-child-returned status=", events)
        self.assertIn("output-inventory-written", events)
        self.assertIn("run.log file bytes=", inventory)
        self.assertIn("time-v.txt file bytes=", inventory)
        self.assertNotIn("not-loaded", inventory)
        self.assertNotIn(sentinel, events + inventory)

    @unittest.skipUnless(strace_usable(), "real strace cannot trace a child on this runner")
    def test_trace_enabled_branch_does_not_serialize_credential_sentinel(self) -> None:
        root = pathlib.Path(self.tmp.name)
        output = root / "trace-output"
        sentinel = "credential-sentinel-must-not-be-serialized"
        env = self.env | {
            "DURABLE_SELECTOR_FORENSICS_STRACE": "1",
            "CREDENTIAL_SENTINEL": sentinel,
        }
        result = subprocess.run(
            [str(self.diagnose), "scale-512", str(output)],
            cwd=self.repo,
            env=env,
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)
        forensic = pathlib.Path(f"{output}.launch-forensics")
        retained = "\n".join(
            path.read_text()
            for path in [forensic / "launcher.events", forensic / "selector.events",
                         forensic / "output-inventory.txt", *forensic.glob("execve.*")]
        )
        self.assertIn("strace-enabled", retained)
        self.assertNotIn(sentinel, retained)

    def test_requested_strace_fails_closed_when_unavailable_or_unusable(self) -> None:
        root = pathlib.Path(self.tmp.name)
        output = root / "required-trace-output"
        fake_bin = root / "fake-bin"
        fake_bin.mkdir()
        fake_strace = fake_bin / "strace"
        fake_strace.write_text("#!/usr/bin/env bash\nexit 1\n")
        fake_strace.chmod(0o755)
        result = subprocess.run(
            [str(self.diagnose), "scale-512", str(output)],
            cwd=self.repo,
            env=self.env | {
                "DURABLE_SELECTOR_FORENSICS_STRACE": "1",
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
            },
            text=True,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 2)
        forensic = pathlib.Path(f"{output}.launch-forensics")
        self.assertIn("launcher-started", (forensic / "launcher.events").read_text())
        self.assertIn("strace-unusable", (forensic / "launcher.events").read_text())
        self.assertFalse(output.exists())
        self.assertFalse((forensic / "strace-usability-probe").exists())

    def test_auto_strace_unusable_falls_back_without_probe_or_secrets(self) -> None:
        root = pathlib.Path(self.tmp.name)
        output = root / "auto-unusable-output"
        fake_bin = root / "fake-bin"
        fake_bin.mkdir()
        fake_strace = fake_bin / "strace"
        fake_strace.write_text("#!/usr/bin/env bash\nexit 1\n")
        fake_strace.chmod(0o755)
        sentinel = "credential-sentinel-must-not-be-serialized"
        result = subprocess.run(
            [str(self.diagnose), "scale-512", str(output)],
            cwd=self.repo,
            env=self.env | {
                "DURABLE_SELECTOR_FORENSICS_STRACE": "auto",
                "CREDENTIAL_SENTINEL": sentinel,
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
            },
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)
        forensic = pathlib.Path(f"{output}.launch-forensics")
        launcher = (forensic / "launcher.events").read_text()
        selector = (forensic / "selector.events").read_text()
        self.assertIn("strace-unusable", launcher)
        self.assertIn("strace-unusable-fallback", launcher)
        self.assertIn("selector-started", selector)
        self.assertFalse((forensic / "strace-usability-probe").exists())
        self.assertNotIn(sentinel, launcher + selector)

    @unittest.skipUnless(
        shutil.which("strace") and not strace_usable(),
        "requires strace on PATH with ptrace denied",
    )
    def test_real_unusable_strace_auto_falls_back_to_selector_lifecycle(self) -> None:
        root = pathlib.Path(self.tmp.name)
        output = root / "real-auto-unusable-output"
        sentinel = "credential-sentinel-must-not-be-serialized"
        result = subprocess.run(
            [str(self.diagnose), "scale-512", str(output)],
            cwd=self.repo,
            env=self.env | {
                "DURABLE_SELECTOR_FORENSICS_STRACE": "auto",
                "CREDENTIAL_SENTINEL": sentinel,
            },
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)
        forensic = pathlib.Path(f"{output}.launch-forensics")
        launcher = (forensic / "launcher.events").read_text()
        selector = (forensic / "selector.events").read_text()
        inventory = (forensic / "output-inventory.txt").read_text()
        self.assertIn("strace-unusable", launcher)
        self.assertIn("strace-unusable-fallback", launcher)
        self.assertIn("selector-child-started pid=", launcher)
        self.assertIn("selector-child-returned status=", launcher)
        self.assertIn("selector-started", selector)
        self.assertIn("time-child-returned status=17", selector)
        self.assertFalse((forensic / "strace-usability-probe").exists())
        self.assertEqual(list(forensic.glob("execve.*")), [])
        self.assertNotIn(sentinel, launcher + selector + inventory)

    @unittest.skipUnless(
        shutil.which("strace") and not strace_usable(),
        "requires strace on PATH with ptrace denied",
    )
    def test_real_unusable_strace_explicit_fails_before_selector_output(self) -> None:
        root = pathlib.Path(self.tmp.name)
        output = root / "real-explicit-unusable-output"
        sentinel = "credential-sentinel-must-not-be-serialized"
        result = subprocess.run(
            [str(self.diagnose), "scale-512", str(output)],
            cwd=self.repo,
            env=self.env | {
                "DURABLE_SELECTOR_FORENSICS_STRACE": "1",
                "CREDENTIAL_SENTINEL": sentinel,
            },
            text=True,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 2)
        forensic = pathlib.Path(f"{output}.launch-forensics")
        launcher = (forensic / "launcher.events").read_text()
        self.assertIn("launcher-started", launcher)
        self.assertIn("strace-unusable", launcher)
        self.assertFalse(output.exists())
        self.assertFalse((forensic / "selector.events").exists())
        self.assertFalse((forensic / "strace-usability-probe").exists())
        self.assertEqual(list(forensic.glob("execve.*")), [])
        self.assertNotIn(sentinel, launcher + result.stdout + result.stderr)

    def test_launcher_signal_retires_selector_and_timed_workload(self) -> None:
        root = pathlib.Path(self.tmp.name)
        output = root / "signal-output"
        pid_file = root / "jolt.pid"
        process = subprocess.Popen(
            [str(self.diagnose), "scale-512", str(output)],
            cwd=self.repo,
            env=self.env | {"FAKE_JOLT_PID_FILE": str(pid_file)},
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for _ in range(100):
            if pid_file.exists():
                break
            import time
            time.sleep(0.02)
        self.assertTrue(pid_file.exists(), "fake Jolt child did not start")
        fake_pid = int(pid_file.read_text().strip())
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10)
        self.assertEqual(process.returncode, 143, (stdout, stderr))
        forensic = pathlib.Path(f"{output}.launch-forensics")
        launcher = (forensic / "launcher.events").read_text()
        selector = (forensic / "selector.events").read_text()
        self.assertIn("launcher-signal signal=15", launcher)
        self.assertIn("selector-child-signal-forwarded signal=15", launcher)
        self.assertIn("selector-child-reaped status=143", launcher)
        self.assertIn("selector-signal signal=15", selector)
        self.assertIn("time-child-signal-forwarded signal=15", selector)
        self.assertIn("time-child-reaped status=143", selector)
        with self.assertRaises(ProcessLookupError):
            os.kill(fake_pid, 0)


if __name__ == "__main__":
    unittest.main()
