"""Real AWS CLI formatting against unsigned loopback XML; no cloud/credentials."""
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
import shutil
import subprocess
import threading

assert shutil.which("aws"), "AWS CLI required for metadata-format boundary gate"


class Handler(BaseHTTPRequestHandler):
    status = ""

    def do_GET(self):
        assert self.path == "/fixture-bucket?versioning"
        selection = f"<Status>{self.status}</Status>" if self.status else ""
        body = ("<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                + selection + "</VersioningConfiguration>").encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


server = HTTPServer(("127.0.0.1", 0), Handler)
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
env.update(AWS_EC2_METADATA_DISABLED="true", AWS_CONFIG_FILE="/dev/null",
           AWS_SHARED_CREDENTIALS_FILE="/dev/null", NO_PROXY="127.0.0.1", no_proxy="127.0.0.1")
base = ["aws", "s3api", "get-bucket-versioning", "--bucket", "fixture-bucket",
        "--endpoint-url", f"http://127.0.0.1:{server.server_port}", "--no-sign-request",
        "--region", "us-east-2", "--output", "json", "--no-cli-pager"]
try:
    parent = subprocess.run(base, capture_output=True, timeout=20, env=env)
    assert parent.returncode == 0 and not parent.stdout.strip(), "parent empty-result control"
    for status in ("", "Enabled", "Suspended"):
        Handler.status = status
        candidate = subprocess.run(base + ["--query", "{metadata: @}"],
                                   capture_output=True, timeout=20, env=env)
        assert candidate.returncode == 0, "CLI metadata request failed"
        expected = {"Status": status} if status else {}
        assert json.loads(candidate.stdout) == {"metadata": expected}, "CLI envelope changed metadata"
    print("real AWS CLI loopback controls: 4 passed; no cloud calls or credentials")
finally:
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)
