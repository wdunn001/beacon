"""Tiny HTTP surface for Beacon: /healthz and /stats (for monitoring + a future
dashboard). The search API + UI are later stages; Stage 1 is crawler + registry."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import crawler, db


def start(conn_factory, port):
    conn = conn_factory()

    class Handler(BaseHTTPRequestHandler):
        def _send(self, code, obj):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            route = self.path.rstrip("/") or "/"
            try:
                if route in ("/healthz", "/"):
                    s = db.stats(conn)                        # a query proves DB is reachable
                    self._send(200, {"ok": True, "db": True, **s, "crawler": crawler.stats()})
                elif route == "/stats":
                    self._send(200, {**db.stats(conn), "crawler": crawler.stats()})
                else:
                    self._send(404, {"ok": False, "err": "not_found"})
            except Exception as e:  # noqa: BLE001
                self._send(503, {"ok": False, "db": False, "err": str(e)})

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv
