"""Closed-graph launcher controls; fake processes only, never cloud effects.

The synthetic copy substitutes only binary/native digests for tiny fixtures.
Production digests and source revisions remain unchanged. This checks routing
and rejection logic, not authentication of the real compiler or native asset.
"""
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import textwrap

source = pathlib.Path(__file__).with_name("run_current_typed_storage.sh").read_text()
checks = 0
with tempfile.TemporaryDirectory(prefix="current-typed-launcher-") as directory:
    root = pathlib.Path(directory)
    scripts = root / "driver" / "scripts"
    scripts.mkdir(parents=True)
    for name in ["benchmark", "exporter", "json", "bin"]:
        (root / name).mkdir()
    calls = root / "calls.jsonl"
    binary = root / "bin" / "fake-jolt"
    binary.write_text("#!/usr/bin/env python3\n" + textwrap.dedent('''
        import json, os, sys
        with open(os.environ["TEST_CALLS"], "a") as output:
            output.write(json.dumps(sys.argv[1:]) + "\\n")
        print(os.environ.get("TEST_RUNTIME_VERSION", "jolt v0.8.21-58-g1ae7e0d1") if sys.argv[1:] == ["--version"]
              else json.dumps(sys.argv[1:]))
    '''))
    binary.chmod(0o700)
    native = root / "native"
    native.write_bytes(b"synthetic-native-only")
    binary_digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    native_digest = hashlib.sha256(native.read_bytes()).hexdigest()
    launcher = scripts / "run_current_typed_storage.sh"
    synthetic = source.replace(
        "06a2ccb858c072bfcd74ce94d96eb95f008d75e0fb1f46117728602572189811", binary_digest
    ).replace("a7d22ade86e1de82b187adf572b569a6034600ce932c52b0acb8c97b82430557", native_digest)
    launcher.write_text(synthetic)
    wrapper = root / "wrapper"
    wrapper.write_text('#!/usr/bin/env bash\nexec "$@"\n')
    wrapper.chmod(0o700)
    git = root / "bin" / "git"
    git.write_text("#!/usr/bin/env python3\n" + textwrap.dedent('''
        import os, pathlib, sys
        name = pathlib.Path(sys.argv[2]).name
        command = sys.argv[3:]
        refs = {"benchmark": "3fd96c4691c5db49ed4d3ef55622aaa6b9074127",
                "exporter": "5be78d565be0a7a929d108be929540c1b4f19524",
                "json": "8d557cc18eb9526ed29ae5613089c3793d5973fa"}
        mode = os.environ.get("TEST_DRIFT", "")
        if command == ["rev-parse", "HEAD"]:
            print("0" * 40 if mode == "head-" + name else refs[name])
        elif command == ["diff", "--quiet", "HEAD"]:
            sys.exit(1 if mode == "dirty-" + name else 0)
        elif command[:2] == ["merge-base", "--is-ancestor"]:
            assert command[2:] == ["3d766fa295b0e98c30f275d0a334db92e6dd998c", "HEAD"]
            sys.exit(1 if mode == "driver-ancestry" else 0)
        elif command[:2] == ["diff", "--quiet"]:
            assert command[2:] == ["3d766fa295b0e98c30f275d0a334db92e6dd998c",
                                   "--", "src", "resources", "deps.edn"]
            sys.exit(1 if mode == "driver-source" else 0)
        else:
            raise AssertionError(command)
    '''))
    git.chmod(0o700)
    env = dict(os.environ, PATH=str(root / "bin") + os.pathsep + os.environ["PATH"],
               TEST_CALLS=str(calls), TEST_DRIFT="", BENCH_EXPECT_BENCHMARK_ROOT=str(root / "benchmark"),
               BENCH_EXPECT_EXPORTER_ROOT=str(root / "exporter"),
               BENCH_EXPECT_JSON_ROOT=str(root / "json"), BENCH_JOLT_BIN=str(binary),
               BENCH_JOLT_WRAPPER=str(wrapper), BENCH_RUNTIME_BINARY_SHA256=binary_digest,
               QUALIFIED_RUNTIME_BINARY_SHA256=binary_digest,
               QUALIFIED_RUNTIME_PROFILE="current-0821-1ae7", JOLT_CHDB_LIB=str(native))
    args = ["window", str(root / "store"), "10000", "256", "true", "wide16"]

    def run(arguments=args, **overrides):
        calls.unlink(missing_ok=True)
        return subprocess.run(["bash", str(launcher), *arguments],
                              env=dict(env, **overrides), capture_output=True,
                              text=True, timeout=10)

    for phase in ["window", "reader"]:
        result = run([phase, *args[1:]])
        assert result.returncode == 0, result.stderr
        actual = json.loads(result.stdout)
        assert actual[:2] == ["-Srepro", "-Sdeps"]
        deps = actual[2]
        assert '"' + str(root / "benchmark" / "bench") + '"' in deps
        assert ':local/root "' + str(root / "driver") + '"' in deps
        assert "d93846e40b837dcecb546fb59f6cb05ca639f054" in deps
        assert "19fc49d20b3a75906f0ccbb8b50c7e48b03e4813" not in deps
        assert actual[3:] == [str(root / "benchmark" / "bench" / "typed-storage-window.clj"),
                              phase, *args[1:]]
        assert len(calls.read_text().splitlines()) == 2
        checks += 1
    for drift in ["head-benchmark", "head-exporter", "head-json", "dirty-benchmark",
                  "dirty-exporter", "dirty-json", "driver-ancestry", "driver-source"]:
        result = run(TEST_DRIFT=drift)
        assert result.returncode != 0 and not calls.exists(), drift
        checks += 1
    # A causal mutant proves the driver-source comparison is load-bearing.
    guard = 'git -C "$task_driver" diff --quiet 3d766fa295b0e98c30f275d0a334db92e6dd998c -- src resources deps.edn\n'
    assert synthetic.count(guard) == 1
    launcher.write_text(synthetic.replace(guard, ""))
    assert run(TEST_DRIFT="driver-source").returncode == 0
    launcher.write_text(synthetic)
    checks += 1
    for override in [dict(QUALIFIED_RUNTIME_PROFILE="old"),
                     dict(BENCH_RUNTIME_BINARY_SHA256="0" * 64),
                     dict(QUALIFIED_RUNTIME_BINARY_SHA256="0" * 64)]:
        assert run(**override).returncode != 0 and not calls.exists()
        checks += 1
    native.write_bytes(b"changed")
    assert run().returncode != 0 and not calls.exists()
    checks += 1
    native.write_bytes(b"synthetic-native-only")
    assert run().returncode == 0
    checks += 1
    assert run(TEST_RUNTIME_VERSION="wrong").returncode != 0
    assert [json.loads(line) for line in calls.read_text().splitlines()] == [["--version"]]
    checks += 1
    for bad in [["writer", *args[1:]], [*args[:2], "5000", *args[3:]],
                [*args[:3], "128", *args[4:]], [*args[:4], "false", args[5]],
                [*args[:5], "small"], args[:-1]]:
        assert run(bad).returncode != 0 and not calls.exists()
        checks += 1
    # Real Git smoke: reproduce the shallow ancestor failure, then prove a
    # complete history permits the exact same ancestry check. No network use.
    real_git = shutil.which("git")
    history = root / "real-history"
    history.mkdir()

    def git_run(*arguments, ok=True):
        result = subprocess.run([real_git, *map(str, arguments)],
                                capture_output=True, text=True, timeout=10)
        if ok:
            assert result.returncode == 0, result.stderr
        return result

    git_run("init", history)
    (history / "source").write_text("first")
    git_run("-C", history, "add", "source")
    commit_args = ["-C", history, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid"]
    git_run(*commit_args, "commit", "-m", "first")
    ancestor = git_run("-C", history, "rev-parse", "HEAD").stdout.strip()
    (history / "source").write_text("second")
    git_run("-C", history, "add", "source")
    git_run(*commit_args, "commit", "-m", "second")
    shallow = root / "shallow"
    git_run("clone", "--depth=1", history.as_uri(), shallow)
    assert git_run("-C", shallow, "merge-base", "--is-ancestor", ancestor, "HEAD", ok=False).returncode != 0
    git_run("-C", shallow, "fetch", "--unshallow")
    git_run("-C", shallow, "merge-base", "--is-ancestor", ancestor, "HEAD")
    workflow = pathlib.Path(__file__).parent.parent / ".github/workflows/current-typed-storage.yml"
    checkout = workflow.read_text().split("- name: Require closed mode and workload bounds", 1)[0]
    assert "fetch-depth: 0" in checkout
    checks += 1
print(f"current typed launcher: {checks} controls passed (includes real local Git history); no cloud requests")
