"""Pure orchestration controls: fake Git/timeout, no Jolt/native/cloud effects."""
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile

runner = pathlib.Path(__file__).with_name("run_wal_confirmation_ab.sh").resolve()
expected_refs = {
    "baseline": "fd8f961364e73bac370286d5481407b23120e9cd",
    "candidate": "fd8f961364e73bac370286d5481407b23120e9cd",
}
workflow = runner.parents[1] / ".github/workflows/wal-confirmation-ab.yml"


def workflow_tuple_valid(text):
    checkouts = re.findall(
        r"ref: ([a-f0-9]{40})\n\s+path: \.qualification/(baseline|candidate)\n", text)
    validations = re.findall(
        r'test "\$\(git -C \.qualification/(baseline|candidate) rev-parse HEAD\)" = ([a-f0-9]{40})', text)
    return (len(checkouts) == len(validations) == 2
            and {arm: ref for ref, arm in checkouts} == expected_refs
            and dict(validations) == expected_refs)


workflow_text = workflow.read_text()
assert workflow_tuple_valid(workflow_text), "workflow checkout/validation tuple drift"
for arm, stale in {"baseline": "1a23e211dda170e0d927728d8035d1f823e0d0be",
                   "candidate": "c7f54fe1d346cccd41fd421332e8cf46bebbc736"}.items():
    # Change checkout only, then validation only: both controls must reject.
    current = expected_refs[arm]
    assert not workflow_tuple_valid(workflow_text.replace(current, stale, 1))
    marker = f'git -C .qualification/{arm} rev-parse HEAD)" = '
    assert not workflow_tuple_valid(workflow_text.replace(marker + current, marker + stale))

with tempfile.TemporaryDirectory(prefix="wal-head-orchestration-") as directory:
    root = pathlib.Path(directory)
    fake_bin = root / "bin"
    fake_bin.mkdir()
    sources = {name: root / name for name in ["baseline", "candidate", "exporter", "json"]}
    for path in sources.values():
        path.mkdir()
    git = fake_bin / "git"
    git.write_text("#!" + sys.executable + "\n" + '''
import os, pathlib, sys
source = pathlib.Path(sys.argv[2]).name
args = sys.argv[3:]
if args == ["rev-parse", "HEAD"]:
    refs = {"baseline": "fd8f961364e73bac370286d5481407b23120e9cd",
            "candidate": "fd8f961364e73bac370286d5481407b23120e9cd"}
    print("wrong" if os.environ.get("FAKE_WRONG") == source else refs[source])
elif args == ["diff", "--quiet", "HEAD"]:
    sys.exit(1 if os.environ.get("FAKE_DIRTY") == source else 0)
elif args[:2] == ["ls-files", "--others"]:
    if os.environ.get("FAKE_UNTRACKED") == source:
        print("src/shadow.clj")
else:
    raise RuntimeError("unexpected fake Git call")
''')
    git.chmod(0o700)
    timeout = fake_bin / "timeout"
    timeout.write_text("#!" + sys.executable + "\n" + '''
import json, os, pathlib, sys
args = sys.argv[1:]
assert args[0] == "--kill-after=5s" and args[2] == "bash"
assert args[6:] == ["10000", "256", "true", "wide16"]
source = pathlib.Path(args[3]).parents[1]
phase, root = args[4], pathlib.Path(args[5])
storage = os.environ["SDK_STORAGE_ARM"]
assert os.environ["SDK_WAL_HEAD_ARM"] == source.name
assert os.environ["TASK_ORIGINAL_TYPED_DRIVER"] == str(source / "bench/typed-storage-window.clj")
assert args[1] == ("240s" if phase == "reader" else "1200s" if storage == "s3" else "360s")
entry = {"arm": source.name, "storage": storage, "phase": phase,
         "root": str(root), "profile": os.environ.get("QUALIFIED_STORAGE_SOURCE_PROFILE"),
         "reader": os.environ.get("CHDB_BENCH_METADATA_READER")}
with open(os.environ["FAKE_TRACE"], "a") as trace:
    trace.write(json.dumps(entry) + "\\n")
if os.environ.get("FAKE_FAIL") and storage == "s3" and phase == "window":
    (root / "partial-scratch").write_bytes(b"temporary")
    sys.exit(7)
(root / ("writer.edn" if phase == "window" else "reader.edn")).write_text("scalar-receipt")
if storage == "s3":
    (root / "http-budget.edn").write_text("scalar-budget")
    (root / "curl-profile.edn").write_text("scalar-profile")
    (root / "confirmation-selection.edn").write_text("scalar-confirmation")
''')
    timeout.chmod(0o700)
    base_env = dict(os.environ, PATH=str(fake_bin) + os.pathsep + os.environ["PATH"],
                    BENCH_EXPECT_EXPORTER_ROOT=str(sources["exporter"]),
                    BENCH_EXPECT_JSON_ROOT=str(sources["json"]),
                    QUALIFIED_STORAGE_SOURCE_PROFILE="must-not-leak-to-either-arm",
                    CHDB_BENCH_METADATA_READER="native")

    def run(number, **changes):
        output = root / ("receipts-" + str(number))
        output.mkdir()
        (output / "preflight.json").write_text("{}")
        trace = root / ("trace-" + str(number))
        result = subprocess.run(["bash", str(runner), str(sources["baseline"]),
                                 str(sources["candidate"]), str(output)],
                                env=dict(base_env, FAKE_TRACE=str(trace), **changes),
                                capture_output=True, text=True, timeout=10)
        entries = [json.loads(line) for line in trace.read_text().splitlines()] if trace.exists() else []
        assert all(not pathlib.Path(entry["root"]).exists() for entry in entries)
        return result, entries, output

    result, entries, output = run(0)
    assert result.returncode == 0, result.stderr
    assert [(e["storage"], e["arm"], e["phase"]) for e in entries] == [
        (storage, arm, phase) for storage in ["local", "s3"]
        for arm in ["baseline", "candidate"] for phase in ["window", "reader"]]
    assert all(e["profile"] == "wal-checksum-confirmation" for e in entries)
    assert all(e["reader"] == "stock" for e in entries if e["phase"] == "reader")
    assert len({e["root"] for e in entries}) == 4
    expected = {"preflight.json", "local-writer.edn", "local-reader.edn",
                "s3-writer.edn", "s3-reader.edn", "s3-writer-budget.edn",
                "s3-reader-budget.edn", "s3-writer-curl-profile.edn", "s3-reader-curl-profile.edn",
                "s3-writer-confirmation.edn", "s3-reader-confirmation.edn"}
    assert all({p.name for p in (output / arm).iterdir()} == expected for arm in ["baseline", "candidate"])
    result, entries, _ = run(1, FAKE_FAIL="1")
    assert result.returncode == 7 and len(entries) == 5
    for number, failure in enumerate([{"FAKE_DIRTY": "candidate"},
                                      {"FAKE_UNTRACKED": "exporter"},
                                      {"FAKE_WRONG": "candidate"},
                                      {"FAKE_WRONG": "baseline"}], 2):
        result, entries, _ = run(number, **failure)
        assert result.returncode != 0 and entries == []
print("checksum-confirmation orchestration: 6 scenarios and workflow tuple controls passed; no cloud or performance claim")
