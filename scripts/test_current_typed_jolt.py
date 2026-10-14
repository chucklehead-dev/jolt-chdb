"""Routing controls with a local fake executable, never a cloud request."""
import hashlib
import os
import pathlib
import subprocess
import sys
import tempfile

wrapper = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else pathlib.Path(__file__).with_name("current_typed_jolt.sh")
with tempfile.TemporaryDirectory(prefix="typed-jolt-routing-") as directory:
    root = pathlib.Path(directory)
    binary = root / "fake-jolt"
    binary.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$@"\n')
    binary.chmod(0o700)
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    driver = str(root / "original driver.clj")
    budget = str(root / "budget driver.clj")
    env = dict(os.environ, BENCH_JOLT_BIN=str(binary),
               QUALIFIED_RUNTIME_BINARY_SHA256=digest,
               TASK_ORIGINAL_TYPED_DRIVER=driver, TASK_TYPED_BUDGET_DRIVER=budget)

    def run(arm, args, sha=digest):
        return subprocess.run([str(wrapper), str(binary), *args],
                              env=dict(env, SDK_STORAGE_ARM=arm,
                                       QUALIFIED_RUNTIME_BINARY_SHA256=sha),
                              capture_output=True, text=True, timeout=5)

    result = run("s3", ["-Sdeps", '{:paths ["space path"]}', driver, "window"])
    assert result.returncode == 0
    assert result.stdout.splitlines() == ["-Sdeps", '{:paths ["space path"]}', budget, "window"]
    result = run("local", [driver, "window"])
    assert result.returncode == 0 and result.stdout.splitlines() == [driver, "window"]
    result = run("s3", ["--version"])
    assert result.returncode == 0 and result.stdout == "--version\n"
    for args in [["relative-driver.clj", "window"], [driver, driver], ["-e", "1"],
                 ["--version", "extra"]]:
        result = run("s3", args)
        assert result.returncode != 0 and result.stdout == ""
    result = run("s3", [driver, "window"], sha="0" * 64)
    assert result.returncode != 0 and result.stdout == ""
print("typed Jolt routing controls: 8 passed; path drift rejects before exec")
