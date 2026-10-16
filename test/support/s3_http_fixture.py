#!/usr/bin/env python3
import base64
import hashlib
import http.server
import json
import re
import sys
import threading
import time
import urllib.parse


OBJECTS = {}
AFTER_CAS_TIMEOUTS = set()
LOCK = threading.Lock()
REQUEST_COUNTS = {"authenticated_requests": 0, "gets": 0, "puts": 0}
ITF_NAMESPACES = set()


def record_request(method, path):
    match = re.match(r"^/bucket/durable-itf/(trace-[0-9]{3})/object/", path)
    with LOCK:
        REQUEST_COUNTS["authenticated_requests"] += 1
        REQUEST_COUNTS[method] += 1
        if match:
            ITF_NAMESPACES.add(match.group(1))


def etag(body):
    return '"' + hashlib.sha256(body).hexdigest() + '"'


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, _format, *_args):
        pass

    def authenticated(self):
        authorization = self.headers.get("Authorization", "")
        known_access_key = (
            "Credential=ACCESS/" in authorization
            or "Credential=JOLT-ACCESS-KEY-CANARY-9A4C/" in authorization
        )
        known_session_token = self.headers.get("x-amz-security-token") in {
            "SESSION",
            "JOLT-SESSION-TOKEN-CANARY-B813",
        }
        return (
            authorization.startswith("AWS4-HMAC-SHA256 ")
            and known_access_key
            and self.headers.get("x-amz-content-sha256") is not None
            and known_session_token
        )

    def answer(self, status, body=b"", object_etag=None, extra_headers=()):
        try:
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            if object_etag is not None:
                self.send_header("ETag", object_etag)
            for name, value in extra_headers:
                self.send_header(name, value)
            self.end_headers()
            if body:
                self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # Timeout tests deliberately close the client side before the
            # fixture is allowed to acknowledge a committed write.
            pass

    def key(self):
        return urllib.parse.urlsplit(self.path).path

    def do_GET(self):
        if not self.authenticated():
            self.answer(403)
            return
        if self.key() == "/__fixture__/interim-response-headers":
            self.send_response_only(100)
            self.send_header("ETag", '"interim-only"')
            self.send_header("x-amz-checksum-sha256", "interim-only")
            self.end_headers()
            self.answer(200)
            return
        if self.key() == "/__fixture__/selected-response-headers":
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.send_header("ETag", '"final"')
            self.send_header("X-Amz-Checksum-Sha256", "fixture-checksum")
            self.send_header("x-amz-checksum-type", "FULL_OBJECT")
            self.send_header("X-Unselected-Provider-Value", "NOT-RETAINED")
            self.end_headers()
            return
        if self.key() == "/__fixture__/duplicate-response-checksum":
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.send_header("x-amz-checksum-sha256", "FIRST-CANARY")
            self.send_header("X-Amz-Checksum-Sha256", "SECOND-CANARY")
            self.end_headers()
            return
        if self.key() == "/__fixture__/response-trailers":
            self.send_response(200)
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("ETag", '"final"')
            self.send_header("Trailer", "ETag, x-amz-checksum-sha256")
            self.end_headers()
            self.wfile.write(b'1\r\nx\r\n0\r\nETag: "trailer"\r\n'
                             b'x-amz-checksum-sha256: trailer-only\r\n\r\n')
            return
        if self.key() == "/__fixture__/stats":
            with LOCK:
                report = {
                    "authenticated-requests": REQUEST_COUNTS["authenticated_requests"],
                    "gets": REQUEST_COUNTS["gets"],
                    "puts": REQUEST_COUNTS["puts"],
                    "namespaces": len(ITF_NAMESPACES),
                }
            self.answer(200, json.dumps(report, sort_keys=True).encode("ascii"))
            return
        record_request("gets", self.key())
        if self.key().endswith("/truncated"):
            self.send_response(200)
            self.send_header("Content-Length", "10")
            self.end_headers()
            self.wfile.write(b"bad")
            self.wfile.flush()
            self.close_connection = True
            return
        with LOCK:
            value = OBJECTS.get(self.key())
        if value is None:
            self.answer(404, b"not found")
        else:
            self.answer(200, value[0], value[1])

    def do_PUT(self):
        if not self.authenticated():
            self.answer(403)
            return
        record_request("puts", self.key())
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        key = self.key()
        checksum = self.headers.get("x-amz-checksum-sha256")
        if checksum is not None and checksum != base64.b64encode(hashlib.sha256(body).digest()).decode("ascii"):
            self.answer(400, b"BadDigest")
            return
        if (
            "/timeout-before-cas/object/head.json" in key
            and self.headers.get("If-Match") is not None
        ):
            # The request reached the provider, but this controlled branch
            # never applies its conditional replacement. Withholding any
            # response still leaves the client with an ambiguous timeout.
            marker_key = key.rsplit("/", 1)[0] + "/fixture-timeout-before-cas-hit"
            marker_body = b"request-reached-before-cas"
            with LOCK:
                OBJECTS[marker_key] = (marker_body, etag(marker_body))
            time.sleep(0.4)
            self.close_connection = True
            return
        delay_after_cas = False
        with LOCK:
            current = OBJECTS.get(key)
            if self.headers.get("If-None-Match") == "*" and current is not None:
                status = 412
                current_etag = None
            elif self.headers.get("If-Match") is not None and (
                current is None or self.headers.get("If-Match") != current[1]
            ):
                status = 412
                current_etag = None
            else:
                current_etag = etag(body)
                OBJECTS[key] = (body, current_etag)
                status = 200
                if (
                    "/timeout-after-cas/object/head.json" in key
                    and self.headers.get("If-Match") is not None
                    and key not in AFTER_CAS_TIMEOUTS
                ):
                    AFTER_CAS_TIMEOUTS.add(key)
                    delay_after_cas = True
        if "/timeout-after-commit/object/wal/" in key and status == 200:
            # The object is visible before the acknowledgement is withheld.
            # ThreadingHTTPServer lets the client's reconciliation GET proceed
            # while this request remains blocked past its libcurl deadline.
            time.sleep(0.4)
        if delay_after_cas:
            # The matching replacement and its new ETag are already visible;
            # only the acknowledgement is lost, and only once for this head.
            time.sleep(0.4)
        response_headers = []
        if checksum is not None and status == 200 and "/checksum-missing/" not in key:
            response_headers.append(("x-amz-checksum-sha256",
                                     "INVALID-CHECKSUM" if "/checksum-conflict/" in key else checksum))
            response_headers.append(("x-amz-checksum-type",
                                     "COMPOSITE" if "/checksum-composite/" in key else "FULL_OBJECT"))
        self.answer(status, object_etag=current_etag, extra_headers=response_headers)


if __name__ == "__main__":
    port_file = sys.argv[1]
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    with open(port_file, "w", encoding="ascii") as stream:
        stream.write(str(server.server_port))
    server.serve_forever()
