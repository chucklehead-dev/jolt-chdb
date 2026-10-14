"""Fail closed before uploading scalar receipts; never print matched content."""
import base64
import json
import os
import pathlib
import sys
import urllib.parse

ALLOWED = {"preflight.json", "local-writer.edn", "local-reader.edn",
           "s3-writer.edn", "s3-reader.edn", "s3-writer-budget.edn",
           "s3-reader-budget.edn"}
FIELDS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
          "JOLT_CHDB_S3_ACCESS_KEY", "JOLT_CHDB_S3_SECRET_KEY",
          "JOLT_CHDB_S3_SESSION_TOKEN", "TASK_ROLE_ARN", "TASK_BUCKET")


def safe(name, data, values):
    if name not in ALLOWED or len(data) > 2 * 1024 * 1024:
        return False
    for value in values:
        if not value:
            continue
        forms = (value.encode(), urllib.parse.quote(value, safe="").encode(),
                 json.dumps(value)[1:-1].encode(), base64.b64encode(value.encode()))
        if any(form in data for form in forms):
            return False
    return True


def main():
    root = pathlib.Path(sys.argv[1])
    values = [os.environ.get(field, "") for field in FIELDS]
    for path in root.iterdir():
        if not path.is_file() or path.is_symlink() or not safe(path.name, path.read_bytes(), values):
            raise ValueError("receipt rejected")
    print("scalar receipt privacy controls passed")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("scalar receipt privacy controls failed")
        sys.exit(1)
