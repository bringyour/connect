# minimal HTTP origin streaming zeros: GET /download/<bytes>; threaded
import http.server, socketserver, sys
CHUNK = b"\0" * (1 << 20)
class H(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def do_GET(self):
        try: n = int(self.path.rsplit("/", 1)[-1])
        except ValueError: n = 1 << 30
        self.send_response(200); self.send_header("Content-Length", str(n)); self.end_headers()
        while n > 0:
            k = min(n, len(CHUNK))
            try: self.wfile.write(CHUNK[:k])
            except (BrokenPipeError, ConnectionResetError): return
            n -= k
    def log_message(self, *a): pass
class S(socketserver.ThreadingMixIn, http.server.HTTPServer): daemon_threads = True; allow_reuse_address = True
S(("0.0.0.0", int(sys.argv[1])), H).serve_forever()
