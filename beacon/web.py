"""Beacon HTTP surface (private, operator-only, internal-only, authentik-gated):

  main server  (BEACON_HTTP_PORT, default 8214) -> crawler.quasarke.net
    GET  /            -> crawler + Beacon-Analytics dashboard
    GET  /healthz     -> monitoring JSON (Gatus)
    GET  /stats       -> stats JSON
    POST /ev          -> Beacon-Analytics RUM ingest (page-view events from our
                         NomadNet nodes over fast localhost HTTP)

  analytics server (BEACON_ANALYTICS_PORT, default 8218) -> analytics.quasarke.net
    GET  /            -> Beacon-Analytics dashboard (charts, analytics only)
    GET  /healthz     -> RUM JSON

Both listen on the host (network_mode: host on .229). The public search API/UI are
the mesh search service (searchd), not this.
"""
import hashlib
import html
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import crawler, db, searchd

# ---------------------------------------------------------------------------
# shared style + tiny inline-SVG chart helpers (no external libs; the page is
# fully self-contained so it renders behind the auth proxy with no asset fetches)
# ---------------------------------------------------------------------------

_CSS = """
:root{--bg:#0a0a0a;--fg:#00ff41;--dim:#00cc33;--mut:#3a6a3a;--line:#123;--card:#0d140d;--bar:#00ff41;--bar2:#0a5a22}
*{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--fg);
font:14px/1.5 ui-monospace,Menlo,Consolas,monospace;padding:22px;max-width:1100px;margin:0 auto}
h1{font-size:1.35rem;margin:0 0 2px} .sub{color:var(--mut);margin:0 0 20px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:26px}
.c{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
.n{font-size:2rem;font-weight:700;line-height:1.1} .l{color:var(--mut);font-size:.72rem;text-transform:uppercase;letter-spacing:.06em;margin-top:4px}
h2{font-size:.8rem;text-transform:uppercase;letter-spacing:.08em;color:var(--dim);border-bottom:1px solid var(--line);padding-bottom:6px;margin:30px 0 14px}
.panel{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px 18px;margin-bottom:22px}
table{width:100%;border-collapse:collapse;font-size:.82rem}
td,th{padding:5px 8px;border-bottom:1px solid #0f1a0f;text-align:left} th{color:var(--mut);font-weight:400;font-size:.72rem;text-transform:uppercase;letter-spacing:.05em}
.m{color:var(--mut)} .r{text-align:right} .cols{display:grid;grid-template-columns:1fr 1fr;gap:26px}
@media(max-width:720px){.cols{grid-template-columns:1fr}}
.tbar{display:inline-block;height:8px;background:var(--bar2);border-radius:3px;vertical-align:middle}
a{color:var(--dim)}
"""


def _esc(x):
    return html.escape(str(x))


def _svg_hbars(rows, label_key, value_key, sub_key=None, height_each=26, width=560):
    """Horizontal bar chart as inline SVG. rows already sorted desc by value."""
    if not rows:
        return '<p class=m>no data yet</p>'
    maxv = max((r[value_key] for r in rows), default=0) or 1
    lblw, gap, barw = 150, 10, width - 150 - 70
    h = len(rows) * height_each + 8
    parts = [f'<svg viewBox="0 0 {width} {h}" width="100%" height="{h}" '
             f'font-family="ui-monospace,monospace" font-size="12">']
    for i, r in enumerate(rows):
        y = i * height_each + 6
        v = r[value_key]
        bw = max(1, int((v / maxv) * barw))
        label = _esc(str(r[label_key])[:22])
        val = f'{v:,}'
        if sub_key is not None:
            val += f'  ({r[sub_key]:,}u)'
        parts.append(
            f'<text x="0" y="{y+13}" fill="#3a6a3a">{label}</text>'
            f'<rect x="{lblw}" y="{y+4}" width="{bw}" height="14" rx="3" fill="#00ff41" opacity="0.85"/>'
            f'<text x="{lblw+bw+8}" y="{y+13}" fill="#00cc33">{_esc(val)}</text>')
    parts.append('</svg>')
    return "".join(parts)


def _svg_area(days, width=1040, height=180):
    """Views-over-time area+line chart from rum_by_day rows [{day,views,visitors}]."""
    if not days:
        return '<p class=m>no data yet</p>'
    pad_l, pad_r, pad_t, pad_b = 40, 12, 12, 26
    iw, ih = width - pad_l - pad_r, height - pad_t - pad_b
    maxv = max((d["views"] for d in days), default=0) or 1
    n = len(days)
    step = iw / max(1, n - 1) if n > 1 else 0

    def pt(i, v):
        x = pad_l + (i * step if n > 1 else iw / 2)
        y = pad_t + ih - (v / maxv) * ih
        return x, y

    line = " ".join(f'{x:.1f},{y:.1f}' for i, d in enumerate(days) for x, y in [pt(i, d["views"])])
    x0, _ = pt(0, 0)
    xN, _ = pt(n - 1, 0)
    baseline = pad_t + ih
    area = f'{x0:.1f},{baseline:.1f} ' + line + f' {xN:.1f},{baseline:.1f}'
    grid = "".join(
        f'<line x1="{pad_l}" y1="{pad_t+ih-(f*ih):.1f}" x2="{width-pad_r}" y2="{pad_t+ih-(f*ih):.1f}" '
        f'stroke="#0f1a0f"/><text x="0" y="{pad_t+ih-(f*ih)+4:.1f}" fill="#3a6a3a" font-size="10">'
        f'{int(maxv*f)}</text>' for f in (0, 0.5, 1.0))
    dots = "".join(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="2.4" fill="#00ff41"/>'
                   for i, d in enumerate(days) for x, y in [pt(i, d["views"])])
    # sparse x labels (first, mid, last)
    labs = []
    for i in (0, n // 2, n - 1):
        if 0 <= i < n:
            x, _ = pt(i, 0)
            labs.append(f'<text x="{x:.1f}" y="{height-8}" fill="#3a6a3a" font-size="10" '
                        f'text-anchor="middle">{_esc(days[i]["day"][5:])}</text>')
    return (f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" '
            f'font-family="ui-monospace,monospace">{grid}'
            f'<polygon points="{area}" fill="#00ff41" opacity="0.12"/>'
            f'<polyline points="{line}" fill="none" stroke="#00ff41" stroke-width="1.8"/>'
            f'{dots}{"".join(labs)}</svg>')


def _analytics_html(conn):
    """Standalone Beacon-Analytics dashboard (analytics.quasarke.net) -- charts."""
    rum = db.rum_stats(conn)
    nodes = db.rum_by_node(conn, 100)
    pages = db.rum_top_pages(conn, 20)
    days = db.rum_by_day(conn, 14)

    cards = "".join(
        f'<div class=c><div class=n>{_esc(f"{v:,}")}</div><div class=l>{_esc(k)}</div></div>'
        for k, v in [("page views", rum["events"]), ("unique visitors", rum["visitors"]),
                     ("views 24h", rum["events_24h"]), ("active nodes", len(nodes))])

    maxp = max((p["views"] for p in pages), default=0) or 1
    pagerows = "".join(
        f'<tr><td class=m>{_esc(p["node"])}</td><td>{_esc(p["path"][:44])}</td>'
        f'<td class=r>{p["views"]:,}</td><td class=r m>{p["visitors"]:,}</td>'
        f'<td><span class=tbar style="width:{max(3,int(p["views"]/maxp*120))}px"></span></td></tr>'
        for p in pages)

    return f"""<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width, initial-scale=1">
<meta http-equiv=refresh content=60><title>Beacon-Analytics</title>
<style>{_CSS}</style></head><body>
<h1>&#128225; Beacon-Analytics</h1>
<p class=sub>page-view analytics for the quasarke mesh nodes &middot; visitor ids are
hashed &mdash; no identities stored &middot; auto-refresh 60s</p>
<div class=grid>{cards}</div>

<h2>Views over time &middot; last 14 days</h2>
<div class=panel>{_svg_area(days)}</div>

<div class=cols>
<div><h2>Views by node</h2><div class=panel>{_svg_hbars(nodes[:15], "node", "views", "visitors")}</div></div>
<div><h2>Top pages</h2><div class=panel><table>
<tr><th>node</th><th>path</th><th class=r>views</th><th class=r>uniq</th><th></th></tr>
{pagerows or '<tr><td class=m colspan=5>no views yet</td></tr>'}
</table></div></div>
</div>
<p class=sub style="margin-top:24px">&#9673; Beacon &middot; <a href="//crawler.quasarke.net">crawler dashboard</a></p>
</body></html>"""


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
a{{color:var(--dim)}}
</style></head><body>
<h1>&#9673; Beacon mesh crawler</h1>
<p class=sub>private operator view &middot; uptime ~{up_h}h &middot; auto-refresh 30s
&middot; <a href="//analytics.quasarke.net">Beacon-Analytics &rarr;</a></p>
<div class=grid>{cards}</div>
<div class=cols>
<div><h2>Page types</h2><table>{catrows or '<tr><td class=m>none yet</td></tr>'}</table>
<h2>Top nodes (by announces)</h2><table>{noderows or '<tr><td class=m>none yet</td></tr>'}</table></div>
<div><h2>Recently crawled (* = MeshData)</h2><table>{recrows or '<tr><td class=m>none yet</td></tr>'}</table></div>
</div>
<h1 style="margin-top:34px">&#128225; Beacon-Analytics</h1>
<p class=sub>RUM page views for our NomadNet nodes &middot; visitor ids are hashed (no identities stored)
&middot; <a href="//analytics.quasarke.net">full analytics dashboard &rarr;</a></p>
<div class=grid>{rcards}</div>
<div class=cols>
<div><h2>Views by node</h2><table><tr class=m><td>node</td><td>views</td><td>uniq</td></tr>{rnoderows or '<tr><td class=m>no views yet</td></tr>'}</table>
<h2>By day (14d)</h2><table><tr class=m><td>day</td><td>views</td><td>uniq</td></tr>{rdayrows or '<tr><td class=m>no views yet</td></tr>'}</table></div>
<div><h2>Top pages</h2><table><tr class=m><td>node</td><td>path</td><td>views</td><td>uniq</td></tr>{rpagerows or '<tr><td class=m>no views yet</td></tr>'}</table></div>
</div></body></html>"""


def start(conn_factory, port, analytics_port=None):
    """Start the main operator server on `port`, and (if given) a second server on
    `analytics_port` whose root is the standalone Beacon-Analytics dashboard.
    Both share one connection + lock (requests are light and infrequent)."""
    conn = conn_factory()
    lock = threading.Lock()   # one shared conn; serialize access across request threads

    def make_handler(root_renderer, allow_ingest):
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
                if not allow_ingest or self.path.rstrip("/") != "/ev":
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
                            base = {"ok": True, "db": True, "rum": db.rum_stats(conn)}
                            if allow_ingest:
                                base.update({**db.stats(conn), "crawler": crawler.stats(),
                                             "search_dest": searchd.dest_hash()})
                            self._json(200, base)
                        elif route == "/stats" and allow_ingest:
                            self._json(200, {**db.stats(conn), "crawler": crawler.stats(),
                                             "categories": db.categories(conn),
                                             "rum": db.rum_stats(conn)})
                        elif route == "/":
                            self._html(200, root_renderer(conn))
                        else:
                            self._json(404, {"ok": False, "err": "not_found"})
                except Exception as e:  # noqa: BLE001
                    self._json(503, {"ok": False, "db": False, "err": str(e)})

            def log_message(self, *a):
                pass

        return Handler

    servers = []
    main = ThreadingHTTPServer(("0.0.0.0", port), make_handler(_dashboard_html, True))
    threading.Thread(target=main.serve_forever, daemon=True).start()
    servers.append(main)
    if analytics_port:
        an = ThreadingHTTPServer(("0.0.0.0", analytics_port),
                                 make_handler(_analytics_html, False))
        threading.Thread(target=an.serve_forever, daemon=True).start()
        servers.append(an)
    return servers
