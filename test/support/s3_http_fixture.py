#!/usr/bin/env python3
import hashlib
import http.server
import sys
import threading
import time
import urllib.parse


OBJECTS = {}
AFTER_CAS_TIMEOUTS = set()
LOCK = threading.Lock()


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

    def answer(self, status, body=b"", object_etag=None):
        try:
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            if object_etag is not None:
                self.send_header("ETag", object_etag)
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
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        key = self.key()
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
        self.answer(status, object_etag=current_etag)


if __name__ == "__main__":
    port_file = sys.argv[1]
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    with open(port_file, "w", encoding="ascii") as stream:
        stream.write(str(server.server_port))
    server.serve_forever()
