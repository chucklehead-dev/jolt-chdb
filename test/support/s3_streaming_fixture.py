#!/usr/bin/env python3
"""Disk-backed loopback S3 fixture for large-transfer qualification.

This fixture deliberately moves request and response bodies in fixed-size chunks.
It is not an S3 emulator; it implements only the signed GET and conditional PUT
surface exercised by the production Durable libcurl transport.
"""

import hashlib
import http.server
import json
import os
import pathlib
import sys
import threading
import urllib.parse


CHUNK_BYTES = 64 * 1024
LOCK = threading.Lock()
OBJECTS = {}
STATS = {
    "get_count": 0,
    "put_count": 0,
    "uploaded_bytes": 0,
    "downloaded_bytes": 0,
    "max_request_chunk_bytes": 0,
    "max_response_chunk_bytes": 0,
}


def object_path(root, key):
    return root / hashlib.sha256(key.encode("utf-8")).hexdigest()


def quoted_etag(digest):
    return '"' + digest + '"'


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    root = None

    def log_message(self, _format, *_args):
        pass

    def authenticated(self):
        authorization = self.headers.get("Authorization", "")
        return (
            authorization.startswith("AWS4-HMAC-SHA256 ")
            and "Credential=ACCESS/" in authorization
            and self.headers.get("x-amz-content-sha256") is not None
            and self.headers.get("x-amz-security-token") == "SESSION"
        )

    def answer(self, status, body=b"", etag=None, content_type=None):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        if etag is not None:
            self.send_header("ETag", etag)
        if content_type is not None:
            self.send_header("Content-Type", content_type)
        self.end_headers()
        if body:
            self.wfile.write(body)

    def key(self):
        return urllib.parse.urlsplit(self.path).path

    def do_GET(self):
        if self.key() == "/__fixture_stats__":
            with LOCK:
                body = json.dumps(STATS, sort_keys=True).encode("ascii")
            self.answer(200, body, content_type="application/json")
            return
        if not self.authenticated():
            self.answer(403)
            return
        key = self.key()
        with LOCK:
            metadata = OBJECTS.get(key)
        if metadata is None:
            self.answer(404, b"not found")
            return
        path, size, etag = metadata
        self.send_response(200)
        self.send_header("Content-Length", str(size))
        self.send_header("ETag", etag)
        self.end_headers()
        sent = 0
        with open(path, "rb") as source:
            while True:
                chunk = source.read(CHUNK_BYTES)
                if not chunk:
                    break
                self.wfile.write(chunk)
                sent += len(chunk)
                with LOCK:
                    STATS["max_response_chunk_bytes"] = max(
                        STATS["max_response_chunk_bytes"], len(chunk)
                    )
        with LOCK:
            STATS["get_count"] += 1
            STATS["downloaded_bytes"] += sent

    def do_PUT(self):
        if not self.authenticated():
            self.answer(403)
            return
        key = self.key()
        length = int(self.headers.get("Content-Length", "0"))
        path = object_path(self.root, key)
        temporary = path.with_suffix(".part-" + str(threading.get_ident()))
        digest = hashlib.sha256()
        remaining = length
        received = 0
        with open(temporary, "xb") as destination:
            while remaining:
                chunk = self.rfile.read(min(CHUNK_BYTES, remaining))
                if not chunk:
                    break
                destination.write(chunk)
                digest.update(chunk)
                received += len(chunk)
                remaining -= len(chunk)
                with LOCK:
                    STATS["max_request_chunk_bytes"] = max(
                        STATS["max_request_chunk_bytes"], len(chunk)
                    )
        if remaining:
            temporary.unlink(missing_ok=True)
            self.close_connection = True
            return
        etag = quoted_etag(digest.hexdigest())
        with LOCK:
            current = OBJECTS.get(key)
            if self.headers.get("If-None-Match") == "*" and current is not None:
                status = 412
            elif self.headers.get("If-Match") is not None and (
                current is None or self.headers.get("If-Match") != current[2]
            ):
                status = 412
            else:
                os.replace(temporary, path)
                OBJECTS[key] = (path, received, etag)
                STATS["put_count"] += 1
                STATS["uploaded_bytes"] += received
                status = 200
        if temporary.exists():
            temporary.unlink()
        self.answer(status, etag=etag if status == 200 else None)


if __name__ == "__main__":
    port_file = pathlib.Path(sys.argv[1])
    Handler.root = pathlib.Path(sys.argv[2])
    Handler.root.mkdir(parents=True, exist_ok=True)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port_file.write_text(str(server.server_port), encoding="ascii")
    server.serve_forever()
