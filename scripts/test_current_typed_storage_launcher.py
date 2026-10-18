"""Closed-graph launcher controls; fake processes only, never cloud effects.

The synthetic copy substitutes only binary/native digests for tiny fixtures.
Production digests and source revisions remain unchanged. This checks routing
and rejection logic, not authentication of the real compiler or native asset.
"""
import hashlib
import contextlib
import importlib.util
import io
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import textwrap
from unittest.mock import patch

batch = sys.argv[1:] == ["--batch"]
assert sys.argv[1:] in ([], ["--batch"])
launcher_name = "run_typed_batch_research.sh" if batch else "run_current_typed_storage.sh"
exporter_sha = "67e28b59bfd215da982682dd9c59d51d52d460ed" if batch else "5be78d565be0a7a929d108be929540c1b4f19524"
source = pathlib.Path(__file__).with_name(launcher_name).read_text()
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
    launcher = scripts / launcher_name
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
                "exporter": os.environ["TEST_EXPORTER_SHA"],
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
               TEST_CALLS=str(calls), TEST_DRIFT="", TEST_EXPORTER_SHA=exporter_sha,
               BENCH_EXPECT_BENCHMARK_ROOT=str(root / "benchmark"),
               BENCH_EXPECT_EXPORTER_ROOT=str(root / "exporter"),
               BENCH_EXPECT_JSON_ROOT=str(root / "json"), BENCH_JOLT_BIN=str(binary),
               BENCH_JOLT_WRAPPER=str(wrapper), BENCH_RUNTIME_BINARY_SHA256=binary_digest,
               QUALIFIED_RUNTIME_BINARY_SHA256=binary_digest,
               QUALIFIED_RUNTIME_PROFILE="current-0821-1ae7", JOLT_CHDB_LIB=str(native))
    env.pop("TASK_BATCH_EXPERIMENT", None)
    env.pop("TASK_CONFIRMATION_ARM", None)
    env.pop("SDK_STORAGE_ARM", None)
    args = ["window", str(root / "store"), "10000", "32" if batch else "256", "true", "wide16"]

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
        expected_driver = (root / "driver" / "scripts" / "typed_storage_batch_driver.clj") if batch else (root / "benchmark" / "bench" / "typed-storage-window.clj")
        assert actual[3:] == [str(expected_driver),
                              phase, *args[1:]]
        assert len(calls.read_text().splitlines()) == 2
        checks += 1
    if batch:
        # Compose the REAL unchanged S3 wrapper with this launcher. A fake
        # executable records the selected budget entry; no cloud request runs.
        expected_entry = str(root / "driver" / "scripts" / "typed_storage_batch_driver.clj")
        expected_budget = str(root / "driver" / "scripts" / "typed_storage_batch_budget_driver.clj")
        shared_wrapper = pathlib.Path(__file__).with_name("current_typed_jolt.sh").resolve()
        routing = dict(SDK_STORAGE_ARM="s3", BENCH_JOLT_WRAPPER=str(shared_wrapper),
                       TASK_ORIGINAL_TYPED_DRIVER=expected_entry, TASK_TYPED_BUDGET_DRIVER=expected_budget)
        routed = run(**routing)
        assert routed.returncode == 0, routed.stderr
        actual = json.loads(routed.stdout)
        assert actual[3] == expected_budget
        assert str(shared_wrapper.parent) in actual[2]
        checks += 1
        for arm in ["baseline", "candidate"]:
            result = run([*args[:2], "50000", *args[3:]], **dict(routing,
                         TASK_BATCH_EXPERIMENT="confirmation", TASK_CONFIRMATION_ARM=arm))
            assert result.returncode == 0, result.stderr
            assert json.loads(result.stdout)[3] == expected_budget
            checks += 1
        result = run(TASK_BATCH_EXPERIMENT="confirmation", TASK_CONFIRMATION_ARM="baseline", SDK_STORAGE_ARM="local")
        assert result.returncode == 0
        checks += 1
        long_args = [*args[:2], "50000", "100", *args[4:]]
        result = run(long_args, **dict(routing, TASK_BATCH_EXPERIMENT="confirmation-long", TASK_CONFIRMATION_ARM="candidate"))
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)[-4:] == ["50000", "100", "true", "wide16"]
        checks += 1
        assert run(TASK_BATCH_EXPERIMENT="confirmation-long", TASK_CONFIRMATION_ARM="baseline", SDK_STORAGE_ARM="local").returncode == 0
        checks += 1
        for overrides in [dict(TASK_BATCH_EXPERIMENT="sweep"),
                          dict(TASK_BATCH_EXPERIMENT="confirmation", TASK_CONFIRMATION_ARM="candidate"),
                          dict(TASK_BATCH_EXPERIMENT="confirmation-long", TASK_CONFIRMATION_ARM="baseline"),
                          dict(TASK_BATCH_EXPERIMENT="confirmation-long", TASK_CONFIRMATION_ARM="candidate", SDK_STORAGE_ARM="local")]:
            result = run(long_args, **dict(routing, **overrides))
            assert result.returncode != 0 and not calls.exists(), overrides
            checks += 1
        assert run(**dict(routing, TASK_BATCH_EXPERIMENT="confirmation-long", TASK_CONFIRMATION_ARM="candidate")).returncode != 0
        assert not calls.exists()
        checks += 1
        assert run([*args[:2], "20000", *args[3:]], TASK_BATCH_EXPERIMENT="confirmation", TASK_CONFIRMATION_ARM="baseline", SDK_STORAGE_ARM="local").returncode != 0
        assert not calls.exists()
        checks += 1
        for overrides in [dict(TASK_BATCH_EXPERIMENT="unknown"),
                          dict(TASK_CONFIRMATION_ARM="candidate"),
                          dict(TASK_BATCH_EXPERIMENT="confirmation", TASK_CONFIRMATION_ARM="unknown"),
                          dict(TASK_BATCH_EXPERIMENT="confirmation", TASK_CONFIRMATION_ARM="baseline", SDK_STORAGE_ARM="s3"),
                          dict(TASK_BATCH_EXPERIMENT="confirmation", TASK_CONFIRMATION_ARM="candidate", SDK_STORAGE_ARM="local")]:
            result = run(**overrides)
            assert result.returncode != 0 and not calls.exists()
            checks += 1
        rejected = run(**dict(routing, TASK_ORIGINAL_TYPED_DRIVER=expected_entry + ".wrong"))
        assert rejected.returncode != 0
        assert [json.loads(line) for line in calls.read_text().splitlines()] == [["--version"]]
        checks += 1
        for size in ["20000", "50000"]:
            result = run([*args[:2], size, *args[3:]])
            assert result.returncode == 0
            assert json.loads(result.stdout)[-4:] == [size, "32", "true", "wide16"]
            checks += 1
        for size in ["0", "9999", "10001", "20001", "50001"]:
            assert run([*args[:2], size, *args[3:]]).returncode != 0 and not calls.exists()
            checks += 1
        bounds = 'case "$3" in 10000|20000|50000) ;; *) exit 64 ;; esac\n'
        assert synthetic.count(bounds) == 1
        launcher.write_text(synthetic.replace(bounds, ""))
        assert run([*args[:2], "10001", *args[3:]]).returncode == 0
        launcher.write_text(synthetic)
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
    checks += 1

    def validate_workflow(text):
        # Narrow source/pin drift controls, not a general YAML or IAM proof.
        # actionlint and the actual protected hosted run remain separate gates.
        marker = "- name: Require closed mode and workload bounds"
        assert marker in text
        assert "fetch-depth: 0" in text.split(marker, 1)[0]
        for sha in ["3fd96c4691c5db49ed4d3ef55622aaa6b9074127",
                    exporter_sha,
                    "8d557cc18eb9526ed29ae5613089c3793d5973fa"]:
            assert text.count(sha) == 2  # checkout and post-run check
        assert text.count(f'driver="$GITHUB_WORKSPACE/scripts/{launcher_name}"') == (3 if batch else 2)
        assert 'BENCH_EXPECT_BENCHMARK_ROOT=$GITHUB_WORKSPACE/.qualification/benchmark' in text
        assert '.qualification/benchmark/bench/run-typed-storage-window.sh' not in text
        assert 'git merge-base --is-ancestor 3d766fa295b0e98c30f275d0a334db92e6dd998c HEAD' in text
        assert 'git diff --quiet 3d766fa295b0e98c30f275d0a334db92e6dd998c -- src resources deps.edn' in text
        if batch:
            # Research installs the authenticated Jolt artifact, not Babashka.
            progress_step = text.split('- name: Test real budget entry without cloud access', 1)[1].split('\n      - name:', 1)[0]
            assert 'bb -cp scripts scripts/test_typed_batch_progress.clj' not in progress_step
            assert '"$BENCH_JOLT_WRAPPER" "$BENCH_JOLT_BIN" -Srepro \\\n' in progress_step
            assert '"$GITHUB_WORKSPACE/scripts/test_typed_batch_progress.clj"' in progress_step
            assert "if: github.ref == 'refs/heads/main'" in text
            assert 'TASK_ORIGINAL_TYPED_DRIVER=$GITHUB_WORKSPACE/scripts/typed_storage_batch_driver.clj' in text
            assert 'TASK_TYPED_BUDGET_DRIVER=$GITHUB_WORKSPACE/scripts/typed_storage_batch_budget_driver.clj' in text
            prefix = 'ci/jolt-chdb/${{ github.run_id }}-${{ github.run_attempt }}/typed-window'
            assert f'TASK_PREFIX: {prefix}' in text
            assert f'TASK_BASE_PREFIX: {prefix}' in text
            assert "options: [preflight, sweep, confirmation, confirmation-long]" in text
            assert "task_schedule='1:10000 2:20000 3:50000 4:50000 5:20000 6:10000'" in text
            assert "task_schedule='1:baseline 2:candidate 3:candidate 4:baseline'" in text
            assert "task_schedule='1:candidate'" in text
            assert 'task_samples=100' in text
            assert '"$task_size" "$task_samples" true wide16' in text
            assert 'for task_pair in $task_schedule; do' in text
            assert 'export TASK_CONFIRMATION_ARM="$task_size"' in text
            assert 'task_size=50000' in text
            assert 'scripts/verify_typed_batch_confirmation.clj' in text
            assert 'scripts/verify_typed_batch_long_confirmation.clj' in text
            assert text.index('> "$TASK_BATCH_BUDGET_FILE"') < text.index('for task_pair in ')
            assert text.count('> "$TASK_BATCH_BUDGET_FILE"') == 1
            assert 'for task_receipt_dir in target/typed-batch-research/*; do' in text
            assert 'shopt -s dotglob' in text
            assert 'test ! -L "$task_receipt_dir"' in text

    for name in (["typed-batch-research.yml"] if batch else ["current-typed-storage.yml", "s3-transport-profile.yml"]):
        workflow = pathlib.Path(__file__).parent.parent / ".github/workflows" / name
        text = workflow.read_text()
        validate_workflow(text)
        checks += 1
        mutants = [text.replace("fetch-depth: 0", "fetch-depth: 1", 1),
                   text.replace(exporter_sha, "0" * 40, 1),
                   text.replace('git merge-base --is-ancestor 3d766fa295b0e98c30f275d0a334db92e6dd998c HEAD', '', 1),
                   text.replace(f'driver="$GITHUB_WORKSPACE/scripts/{launcher_name}"',
                                'driver="$GITHUB_WORKSPACE/.qualification/benchmark/bench/run-typed-storage-window.sh"', 1)]
        if batch:
            mutants.extend([
                text.replace('scripts/typed_storage_batch_budget_driver.clj', 'scripts/typed_storage_batch_driver.clj'),
                text.replace("if: github.ref == 'refs/heads/main'", "if: true"),
                text.replace('test ! -L "$task_receipt_dir"', 'true'),
                text.replace('shopt -s dotglob', 'true'),
                text.replace('/typed-window', '/batch-sweep'),
                text.replace('1:baseline 2:candidate 3:candidate 4:baseline', '1:candidate'),
                text.replace('task_size=50000', 'task_size=10000'),
                text.replace('task_samples=100', 'task_samples=32'),
                text.replace('"$GITHUB_WORKSPACE/scripts/test_typed_batch_progress.clj"',
                             '"$GITHUB_WORKSPACE/scripts/missing-progress-test.clj"'),
            ])
        for mutant in mutants:
            rejected = False
            try:
                validate_workflow(mutant)
            except AssertionError:
                rejected = True
            assert rejected
            checks += 1
        if batch:
            # Run the actual preflight main with synthetic metadata, not just
            # duplicated prefix assertions. No AWS process/network is invoked.
            preflight_path = pathlib.Path(__file__).with_name("typed_s3_preflight.py")
            spec = importlib.util.spec_from_file_location("batch_preflight", preflight_path)
            preflight = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(preflight)
            selected_prefix = next(line.strip().split(": ", 1)[1] for line in text.splitlines()
                                   if line.strip().startswith("TASK_PREFIX: "))
            selected_prefix = selected_prefix.replace("${{ github.run_id }}", "123").replace("${{ github.run_attempt }}", "1")
            role = "arn:aws:iam::123456789012:role/fixture-role"
            metadata = {
                ("sts", "get-caller-identity"): {"Account": "123456789012", "Arn": "arn:aws:sts::123456789012:assumed-role/fixture-role/session"},
                ("iam", "get-role"): {"Role": {"Arn": role}},
                ("iam", "list-role-policies"): {"PolicyNames": ["fixture"]},
                ("iam", "list-attached-role-policies"): {"AttachedPolicies": []},
                ("iam", "get-role-policy"): {"PolicyDocument": {"Statement": [{"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"], "Resource": "arn:aws:s3:::fixture-bucket/ci/jolt-chdb/*"}]}},
                ("s3api", "get-bucket-lifecycle-configuration"): {"Rules": [{"Status": "Enabled", "Filter": {"Prefix": "ci/jolt-chdb/"}, "Expiration": {"Days": 7}}]},
                ("s3api", "get-bucket-versioning"): {},
            }
            with patch.dict(os.environ, TASK_ROLE_ARN=role, TASK_BUCKET="fixture-bucket", TASK_PREFIX=selected_prefix):
                with patch.object(preflight, "aws", side_effect=lambda service, operation, *a: metadata[(service, operation)]) as cloud:
                    output = io.StringIO()
                    with contextlib.redirect_stdout(output):
                        preflight.main()
                    assert json.loads(output.getvalue())["preflight"] == "passed"
                    assert cloud.call_count == 7
            checks += 1
            privacy = root / "privacy"
            (privacy / "preflight").mkdir(parents=True)
            (privacy / "preflight" / "preflight.json").write_text("{}")
            step = text.split("- name: Reject identifiers and credentials from receipt uploads", 1)[1]
            step = step.split("run: |\n", 1)[1].split("      - name: Upload only bounded scalar receipts", 1)[0]
            script = textwrap.dedent(step).replace("target/typed-batch-research", str(privacy))

            def privacy_run():
                return subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=10)

            assert privacy_run().returncode == 0
            for name in ["unexpected", ".hidden"]:
                unexpected = privacy / name
                unexpected.write_text("synthetic-private-marker")
                assert privacy_run().returncode != 0
                unexpected.unlink()
                checks += 1
            link = privacy / "local"
            link.symlink_to(privacy / "preflight", target_is_directory=True)
            assert privacy_run().returncode != 0
            link.unlink()
            checks += 1
print(f"current typed launcher: {checks} controls passed (includes real local Git history); no cloud requests")
