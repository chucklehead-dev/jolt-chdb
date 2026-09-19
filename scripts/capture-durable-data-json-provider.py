#!/usr/bin/env python3
"""Capture one canonical resolved data.json provider without trusting a pin."""
import hashlib, json, os, pathlib, subprocess, sys

def fail(message): raise SystemExit("data.json provider capture failed: " + message)
def ident(path):
    data = path.read_bytes()
    return {"file_name": path.name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}

def provider_root_is_clean(root):
    """Accept a clean provider root and its one harness-owned marker only.

    The A/B/B/A harness may leave ``.jolt-git-ok`` at a provider checkout's
    root as a local liveness marker.  Porcelain v1 with NUL termination keeps
    that exceptional admission unambiguous: do not accept quoting, a nested
    name, another untracked file, a tracked modification, or a rename's
    second pathname record.
    """
    status = subprocess.check_output(
        ["git", "-C", str(root), "status", "--porcelain=v1", "-z",
         "--untracked-files=all", "--ignored"]
    )
    return status in (b"", b"?? .jolt-git-ok\0")

def canonical_namespace(root):
    """Return the one supported data.json source path below a provider root.

    data.json has shipped both Clojure-only and portable namespace forms.  The
    extension therefore is not part of provider identity, but accepting an
    arbitrary classpath file would make the capture ambiguous.  A provider
    must expose exactly one of the two canonical source locations.
    """
    base=root/"src/main/clojure/clojure/data/json"
    matches=[base.with_suffix(extension) for extension in (".clj", ".cljc") if base.with_suffix(extension).is_file()]
    if len(matches) != 1:
        return None
    return matches[0]

def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--verify":
        value=load=json.loads(pathlib.Path(sys.argv[2]).read_text())
        root=pathlib.Path(value.get("root", ""))
        if not root.is_dir() or not provider_root_is_clean(root):
            fail("resolved data.json provider root is not clean")
        actual=subprocess.check_output(["git","-C",str(root),"rev-parse","HEAD"],text=True).strip()
        if actual != value.get("sha"): fail("resolved data.json provider SHA changed")
        namespace=canonical_namespace(root)
        if namespace is None or ident(namespace) != value.get("namespace"):
            fail("resolved data.json provider namespace identity changed")
        return
    if len(sys.argv) != 5:
        fail("usage: capture-durable-data-json-provider.py CHECKOUT JOLT_BIN EXPECTED_SHA OUTPUT")
    checkout, jolt, expected, output = map(pathlib.Path, sys.argv[1:])
    # expected is deliberately kept as text below; pathlib only supplied a safe argument boundary.
    expected = sys.argv[3]
    if not checkout.is_dir() or not jolt.is_file() or len(expected) != 40 or any(c not in "0123456789abcdef" for c in expected):
        fail("checkout, executable, or expected full source SHA is invalid")
    try:
        classpath = subprocess.check_output([str(jolt), "-Srepro", "-Spath"], cwd=checkout, text=True)
    except subprocess.CalledProcessError as error:
        fail("Jolt -Srepro -Spath failed: " + str(error.returncode))
    candidates = []
    for text in classpath.strip().split(os.pathsep):
        entry = pathlib.Path(text).resolve()
        if entry.name == "clojure" and entry.parent.name == "main" and entry.parent.parent.name == "src":
            root = entry.parent.parent.parent
            namespace=canonical_namespace(root)
            if namespace is None:
                continue
            try:
                actual = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
            except subprocess.CalledProcessError:
                fail("candidate data.json provider is not a Git checkout")
            candidates.append((root, namespace, actual))
    if len(candidates) != 1:
        fail("resolved classpath does not contain exactly one canonical data.json provider/root")
    root, namespace, actual = candidates[0]
    if actual != expected:
        fail("resolved data.json provider SHA differs from caller assertion")
    if not provider_root_is_clean(root):
        fail("resolved data.json provider root is not clean")
    output.write_text(json.dumps({"sha": actual, "root": str(root), "namespace": ident(namespace)}, sort_keys=True) + "\n")

if __name__ == "__main__": main()
