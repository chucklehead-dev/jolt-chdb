"""Actual verifier entry with synthetic scalar fixtures; never cloud/rate evidence."""
import pathlib
import subprocess
import tempfile

scripts = pathlib.Path(__file__).resolve().parent
verifier = scripts / "verify_typed_batch_confirmation.clj"
digests = {10000: "d3afed99dac4a86aa35e34beb7933c1e84f6fdbf8ddc0cb603fc9fe2a6c25b11",
           50000: "b89c21707f1a37357ddd0367a1ed50c0611548bc7e02e5dc7833f4d4452999dd"}


def counts(n, b):
    return f"{{:requests {n} :completed {n} :upload-bytes {b}}}"


with tempfile.TemporaryDirectory(prefix="batch-confirmation-verifier-") as temporary:
    root = pathlib.Path(temporary)
    for folder, storage, n in [("local", "local", 10000), ("local-after", "local", 10000)] + [
            (f"s3-{i}", "s3", 50000) for i in range(1, 5)]:
        target = root / folder
        target.mkdir()
        scope = "local-posix" if storage == "local" else "s3-scoped"
        writer = (f'{{:gate-status :passed :research-only? true :scope :{scope}-typed-public-export '
                  f':batch-size {n} :samples 32 :warmups 3 :attribute-shape "wide16" '
                  ':export-callers 1 :native-execution-arm "stream" :stream-calls 35 '
                  f':sql-sha256 "{digests[n]}" :max-insert-bytes 67108864 '
                  ':owned-statement-output? true :timestamp-wire :raw-ticks '
                  ':metadata-reader {:mode "native" :startup-reader :stock :recovery-reader :stock} '
                  ':checkpoint-renewal-window {:renewals 2 :checkpoints [{} {}]} '
                  ':samples-nanos [' + ' '.join(['1000000'] * 32) + '] '
                  ':latency {:p50-ms 1.0 :p95-ms 1.0 :p99-ms 1.0}}')
        (target / f"{storage}-writer.edn").write_text(writer)
        (target / f"{storage}-reader.edn").write_text(
            f'{{:passed true :groups {n} :rows {n * 35} :full-physical-rows-equal true :typed-values-status true}}')
    (root / "preflight").mkdir()
    requests, uploaded = 0, 0
    for i, arm in enumerate(["baseline", "candidate", "candidate", "baseline"], 1):
        target = root / f"s3-{i}"
        for phase in ["window", "reader"]:
            delta, body = (10, 100) if phase == "window" else (1, 0)
            receipt = (f'{{:phase "{phase}" :scope-returned true :before {counts(requests, uploaded)} '
                       f':after {counts(requests + delta, uploaded + body)} :phase-counts {counts(delta, body)} '
                       ':request-limit 24000 :upload-limit 6442450944 '
                       f':phase-request-limit {4000 if phase == "window" else 1000} '
                       f':phase-upload-limit {6442450944 if phase == "window" else 0}}}')
            name = "writer" if phase == "window" else "reader"
            (target / f"s3-{name}-budget.edn").write_text(receipt)
            requests += delta
            uploaded += body
            active = phase == "window"
            candidate = active and arm == "candidate"
            (target / f"s3-{name}-confirmation.edn").write_text(
                f'{{:scope :wal-upload-confirmation-selection :scope-returned true :arm "{arm}" :phase "{phase}" '
                f':mode {":sha256-single-put" if candidate else ":readback"} '
                f':file-publications {33 if active else 0} :completed-publications {33 if active else 0} '
                f':put-completions {33 if active else 0} :publication-errors 0 '
                f':verification-completions {33 if active and not candidate else 0} '
                f':valid-proof-observations {66 if candidate else 0}}}')

    def run():
        return subprocess.run(["bb", "-cp", str(scripts), str(verifier), str(root)],
                              capture_output=True, text=True, timeout=15)

    good = run()
    assert good.returncode == 0, good.stderr
    assert ":global-budget-valid true" in good.stdout
    mutants = [("s3-2/s3-writer-confirmation.edn", ":valid-proof-observations 66", ":valid-proof-observations 0"),
               ("s3-2/s3-writer-confirmation.edn", ":verification-completions 0", ":verification-completions 1"),
               ("s3-2/s3-reader-confirmation.edn", ":valid-proof-observations 0", ":valid-proof-observations 1"),
               ("s3-2/s3-writer-budget.edn", ":before " + counts(11, 100), ":before " + counts(0, 0)),
               ("s3-3/s3-reader.edn", ":full-physical-rows-equal true", ":full-physical-rows-equal false")]
    for path, old, new in mutants:
        file = root / path
        original = file.read_text()
        assert original.count(old) == 1
        file.write_text(original.replace(old, new))
        assert run().returncode != 0, path
        file.write_text(original)
    (root / "unexpected").mkdir()
    assert run().returncode != 0
print("confirmation verifier: valid entry and6 rejected mutants; synthetic receipts only")
