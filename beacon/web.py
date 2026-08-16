"""Beacon HTTP surface (private, operator-only, served behind auth at
crawler.quasarke.net):
  GET  /            -> crawler + Beacon-Analytics dashboard
  GET  /healthz     -> monitoring JSON (Gatus)
  GET  /stats       -> stats JSON
  POST /ev          -> Beacon-Analytics RUM ingest (page-view events from our
                       NomadNet nodes over fast localhost HTTP)
The public search API/UI are the mesh search service (searchd), not this.
"""
import hashlib
import html
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import crawler, db, searchd


def _dashboard_html(conn):
    s = db.stats(conn)
    cr = crawler.stats()
    cats = db.categories(conn)
    recent = db.recent_pages(conn, 25)
    nodes = db.top_nodes(conn, 15)
    rum = db.rum_stats(conn)
    rum_nodes = db.rum_by_node(conn, 15)
    rum_pages = db.rum_top_pages(conn, 15)
    rum_days = db.rum_by_day(conn, 14)
    up_h = int((time.time() - cr.get("started", 0)) // 3600)

    def esc(x):
        return html.escape(str(x))

    cards = "".join(
        f'<div class=c><div class=n>{esc(v)}</div><div class=l>{esc(k)}</div></div>'
        for k, v in [("nodes", s["nodes"]), ("pages", s["pages"]),
                     ("with MeshData", s.get("md_declared", 0)), ("links", s["links"]),
                     ("queue due", s["queue_due"]), ("queue total", s["queue_total"]),
                     ("crawled ok", cr.get("ok", 0)), ("failed", cr.get("failed", 0))])
    rcards = "".join(
        f'<div class=c><div class=n>{esc(v)}</div><div class=l>{esc(k)}</div></div>'
        for k, v in [("page views", rum["events"]), ("unique visitors", rum["visitors"]),
                     ("views 24h", rum["events_24h"])])
    catrows = "".join(f"<tr><td>{esc(c['type'])}</td><td>{esc(c['count'])}</td></tr>" for c in cats)
    recrows = "".join(
        f"<tr><td>{esc(p['type'])}{' *' if p['declared'] else ''}</td>"
        f"<td>{esc(p['title'][:60])}</td><td class=m>{esc(p['url'][:52])}</td></tr>"
        for p in recent)
    noderows = "".join(
        f"<tr><td>{esc(n['name'][:34])}</td><td>{esc(n['announces'])}</td>"
        f"<td class=m>{esc(n['hash'][:16])}</td></tr>" for n in nodes)
    rnoderows = "".join(
        f"<tr><td>{esc(n['node'])}</td><td>{esc(n['views'])}</td><td>{esc(n['visitors'])}</td></tr>"
        for n in rum_nodes)
    rpagerows = "".join(
        f"<tr><td>{esc(p['node'])}</td><td class=m>{esc(p['path'][:40])}</td>"
        f"<td>{esc(p['views'])}</td><td>{esc(p['visitors'])}</td></tr>" for p in rum_pages)
    rdayrows = "".join(
        f"<tr><td>{esc(d['day'])}</td><td>{esc(d['views'])}</td><td>{esc(d['visitors'])}</td></tr>"
        for d in rum_days)

    return f"""<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width, initial-scale=1">
<meta http-equiv=refresh content=30><title>Beacon crawler</title>
<style>
:root{{--bg:#0a0a0a;--fg:#00ff41;--dim:#00cc33;--mut:#3a6a3a;--line:#123;--card:#0d140d}}
*{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--fg);
font:14px/1.5 ui-monospace,Menlo,Consolas,monospace;padding:22px}}
h1{{font-size:1.3rem;margin:0 0 2px}} .sub{{color:var(--mut);margin:0 0 18px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px;margin-bottom:22px}}
.c{{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:12px}}
.n{{font-size:1.7rem;font-weight:700}} .l{{color:var(--mut);font-size:.72rem;text-transform:uppercase;letter-spacing:.06em}}
h2{{font-size:.8rem;text-transform:uppercase;letter-spacing:.08em;color:var(--dim);border-bottom:1px solid var(--line);padding-bottom:6px;margin-top:26px}}
table{{width:100%;border-collapse:collapse;font-size:.82rem;margin-bottom:22px}}
td{{padding:4px 8px;border-bottom:1px solid #0f1a0f}} .m{{color:var(--mut)}}
.cols{{display:grid;grid-template-columns:1fr 1fr;gap:26px}} @media(max-width:720px){{.cols{{grid-template-columns:1fr}}}}
</style></head><body>
<h1>&#9673; Beacon mesh crawler</h1>
<p class=sub>private operator view &middot; uptime ~{up_h}h &middot; auto-refresh 30s</p>
<div class=grid>{cards}</div>
<div class=cols>
<div><h2>Page types</h2><table>{catrows or '<tr><td class=m>none yet</td></tr>'}</table>
<h2>Top nodes (by announces)</h2><table>{noderows or '<tr><td class=m>none yet</td></tr>'}</table></div>
<div><h2>Recently crawled (* = MeshData)</h2><table>{recrows or '<tr><td class=m>none yet</td></tr>'}</table></div>
</div>
<h1 style="margin-top:34px">&#128225; Beacon-Analytics</h1>
<p class=sub>RUM page views for our NomadNet nodes &middot; visitor ids are hashed (no identities stored)</p>
<div class=grid>{rcards}</div>
<div class=cols>
<div><h2>Views by node</h2><table><tr class=m><td>node</td><td>views</td><td>uniq</td></tr>{rnoderows or '<tr><td class=m>no views yet</td></tr>'}</table>
<h2>By day (14d)</h2><table><tr class=m><td>day</td><td>views</td><td>uniq</td></tr>{rdayrows or '<tr><td class=m>no views yet</td></tr>'}</table></div>
<div><h2>Top pages</h2><table><tr class=m><td>node</td><td>path</td><td>views</td><td>uniq</td></tr>{rpagerows or '<tr><td class=m>no views yet</td></tr>'}</table></div>
</div></body></html>"""


def start(conn_factory, port):
    conn = conn_factory()
    lock = threading.Lock()   # one shared conn; serialize access across request threads

    class Handler(BaseHTTPRequestHandler):
        def _json(self, code, obj):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _html(self, code, s):
            body = s.encode()
            self.send_response(code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            # Beacon-Analytics RUM ingest: a page-view event from a NomadNet node.
            if self.path.rstrip("/") != "/ev":
                self.send_response(404)
                self.end_headers()
                return
            try:
                n = int(self.headers.get("Content-Length", 0) or 0)
                ev = json.loads(self.rfile.read(n) or b"{}") if n else {}
                node = str(ev.get("node") or "").strip()
                path = str(ev.get("path") or "").strip()
                raw = str(ev.get("rid") or ev.get("link") or "").strip()
                # hash the visitor identity -> opaque id (never store the raw identity)
                vid = hashlib.sha256(("beaconrum:" + raw).encode()).hexdigest()[:16] if raw else None
                if node and path:
                    with lock:
                        db.record_event(conn, node, path, vid)
                self.send_response(204)
                self.end_headers()
            except Exception:  # noqa: BLE001
                self.send_response(400)
                self.end_headers()

        def do_GET(self):
            route = self.path.rstrip("/") or "/"
            try:
                with lock:
                    if route == "/healthz":
                        self._json(200, {"ok": True, "db": True, **db.stats(conn),
                                         "crawler": crawler.stats(),
                                         "search_dest": searchd.dest_hash(),
                                         "rum": db.rum_stats(conn)})
                    elif route == "/stats":
                        self._json(200, {**db.stats(conn), "crawler": crawler.stats(),
                                         "categories": db.categories(conn),
                                         "rum": db.rum_stats(conn)})
                    elif route == "/":
                        self._html(200, _dashboard_html(conn))
                    else:
                        self._json(404, {"ok": False, "err": "not_found"})
            except Exception as e:  # noqa: BLE001
                self._json(503, {"ok": False, "db": False, "err": str(e)})

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv
