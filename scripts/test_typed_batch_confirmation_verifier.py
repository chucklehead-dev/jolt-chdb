"""Actual verifier entry with synthetic scalar fixtures; never cloud/rate evidence."""
import pathlib
import subprocess
import sys
import tempfile

scripts = pathlib.Path(__file__).resolve().parent
assert sys.argv[1:] in ([], ["--long"])
long = sys.argv[1:] == ["--long"]
verifier = scripts / ("verify_typed_batch_long_confirmation.clj" if long else "verify_typed_batch_confirmation.clj")
schedule = ["candidate"] if long else ["baseline", "candidate", "candidate", "baseline"]
publications = 97 if long else 33
proofs = publications * 2
digests = {10000: "d3afed99dac4a86aa35e34beb7933c1e84f6fdbf8ddc0cb603fc9fe2a6c25b11",
           50000: "b89c21707f1a37357ddd0367a1ed50c0611548bc7e02e5dc7833f4d4452999dd"}


def counts(n, b):
    return f"{{:requests {n} :completed {n} :upload-bytes {b}}}"


with tempfile.TemporaryDirectory(prefix="batch-confirmation-verifier-") as temporary:
    root = pathlib.Path(temporary)
    for folder, storage, n in [("local", "local", 10000), ("local-after", "local", 10000)] + [
            (f"s3-{i}", "s3", 50000) for i in range(1, len(schedule) + 1)]:
        target = root / folder
        target.mkdir()
        scope = "local-posix" if storage == "local" else "s3-scoped"
        samples = 100 if long and storage == "s3" else 32
        raw = [1000000] * samples
        if samples == 100:
            raw[-2:] = [2000000, 3000000]
        writer = (f'{{:gate-status :passed :research-only? true :scope :{scope}-typed-public-export '
                  f':batch-size {n} :samples {samples} :warmups 3 :attribute-shape "wide16" '
                  f':export-callers 1 :native-execution-arm "stream" :stream-calls {samples + 3} '
                  f':sql-sha256 "{digests[n]}" :max-insert-bytes 67108864 '
                  ':owned-statement-output? true :timestamp-wire :raw-ticks '
                  ':metadata-reader {:mode "native" :startup-reader :stock :recovery-reader :stock} '
                  ':checkpoint-renewal-window {:renewals 2 :checkpoints [' + ' '.join(['{}'] * ((samples + 3) // 16)) + ']} '
                  ':samples-nanos [' + ' '.join(map(str, raw)) + '] '
                  f':latency {{:p50-ms 1.0 :p95-ms 1.0 :p99-ms {2.0 if samples == 100 else 1.0}}}}}')
        (target / f"{storage}-writer.edn").write_text(writer)
        (target / f"{storage}-reader.edn").write_text(
            f'{{:passed true :groups {n} :rows {n * (samples + 3)} :full-physical-rows-equal true :typed-values-status true}}')
    (root / "preflight").mkdir()
    requests, uploaded = 0, 0
    for i, arm in enumerate(schedule, 1):
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
                f':file-publications {publications if active else 0} :completed-publications {publications if active else 0} '
                f':put-completions {publications if active else 0} :publication-errors 0 '
                f':verification-completions {publications if active and not candidate else 0} '
                f':valid-proof-observations {proofs if candidate else 0}}}')

    def run():
        return subprocess.run(["bb", "-cp", str(scripts), str(verifier), str(root)],
                              capture_output=True, text=True, timeout=15)

    good = run()
    assert good.returncode == 0, good.stderr
    assert ":global-budget-valid true" in good.stdout
    index = 1 if long else 2
    folder = f"s3-{index}"
    mutants = [(f"{folder}/s3-writer-confirmation.edn", f":valid-proof-observations {proofs}", ":valid-proof-observations 0"),
               (f"{folder}/s3-writer-confirmation.edn", ":verification-completions 0", ":verification-completions 1"),
               (f"{folder}/s3-reader-confirmation.edn", ":valid-proof-observations 0", ":valid-proof-observations 1"),
               (f"{folder}/s3-reader-budget.edn", ":before " + counts((index - 1) * 11 + 10, index * 100), ":before " + counts(0, 0)),
               (f"{folder}/s3-reader.edn", ":full-physical-rows-equal true", ":full-physical-rows-equal false")]
    if long:
        mutants.append((f"{folder}/s3-writer.edn", ":p99-ms 2.0", ":p99-ms 3.0"))
    for path, old, new in mutants:
        file = root / path
        original = file.read_text()
        assert original.count(old) == 1
        file.write_text(original.replace(old, new))
        assert run().returncode != 0, path
        file.write_text(original)
    (root / "unexpected").mkdir()
    assert run().returncode != 0
print(f"confirmation verifier ({'100' if long else '32'} samples): valid entry and {len(mutants) + 1} rejected mutants; synthetic receipts only")
