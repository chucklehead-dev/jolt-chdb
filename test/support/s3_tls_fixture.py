"""Wrap the ordinary fake-credential S3 fixture in an ephemeral test TLS server."""
import http.server
import runpy
import ssl
import sys

port_file, certificate, private_key = sys.argv[1:4]
plain_server = http.server.ThreadingHTTPServer


def tls_server(address, handler):
    server = plain_server(address, handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, private_key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    return server


http.server.ThreadingHTTPServer = tls_server
sys.argv = ["test/support/s3_http_fixture.py", port_file]
runpy.run_path("test/support/s3_http_fixture.py", run_name="__main__")
