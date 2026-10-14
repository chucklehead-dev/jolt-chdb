"""Pure privacy controls; no real secrets or report contents."""
import base64
import importlib.util
import pathlib
import urllib.parse

spec = importlib.util.spec_from_file_location(
    "privacy", pathlib.Path(__file__).with_name("check_typed_receipt_privacy.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
secret = "fixture/secret+value"
assert module.safe("preflight.json", b'{"preflight":"passed"}', [secret, ""])
assert not module.safe("writer.log", b"log", [])
assert not module.safe("local-writer.edn", b"x" * (2 * 1024 * 1024 + 1), [])
for value in [secret.encode(), urllib.parse.quote(secret, safe="").encode(),
              base64.b64encode(secret.encode())]:
    assert not module.safe("s3-writer.edn", value, [secret])
print("receipt privacy pure controls: 6 passed")
