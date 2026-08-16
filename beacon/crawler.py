"""Beacon crawler: discover NomadNet nodes from announces, fetch their pages over
Reticulum Links, extract text + links, and fill the registry + page store.

Polite by design: one fetch at a time, a configurable delay between fetches (LoRa
nodes are slow and intermittent), index only text micron pages (skip binaries),
and recrawl each node's index at least daily.
"""
import hashlib
import os
import threading
import time

import RNS
import meshdata

from . import dates as _dates
from . import db, micron

NODE_APP = "nomadnetwork"
NODE_ASPECT = "node"
FETCH_DELAY = float(os.environ.get("BEACON_FETCH_DELAY", "2"))    # seconds between fetches (politeness)
LINK_TIMEOUT = float(os.environ.get("BEACON_LINK_TIMEOUT", "15"))  # fast-fail unreachable pages
RECRAWL_HOURS = int(os.environ.get("BEACON_RECRAWL_HOURS", "24"))
MAX_PAGE_BYTES = int(os.environ.get("BEACON_MAX_PAGE_BYTES", str(512 * 1024)))
BINARY_EXT = (".pdf", ".epub", ".zip", ".gz", ".png", ".jpg", ".jpeg", ".gif",
              ".webp", ".mp3", ".ogg", ".opus", ".mp4", ".bin", ".tar")

_stats = {"fetched": 0, "ok": 0, "failed": 0, "started": time.time()}

# Our own first-party nodes: crawl these AND their deep pages at top priority so
# station content (Mild Take articles, service pages) indexes immediately instead
# of behind the ~900-node external backlog. Set via env (comma-separated hashes).
SEED_NODES = set(h for h in os.environ.get("BEACON_SEED_NODES", "").lower().split(",") if h)
SEED_INDEX_PRIO = 1
SEED_DEEP_PRIO = 2
INDEX_PRIO = 3
DEEP_PRIO = 6


class _NodeAnnounceHandler:
    aspect_filter = f"{NODE_APP}.{NODE_ASPECT}"

    def __init__(self, conn_factory):
        self._conn_factory = conn_factory
        self._conn = conn_factory()
        # RNS calls received_announce from multiple threads; a single psycopg2
        # connection is NOT reentrant ("connection cannot be re-entered
        # recursively"). Serialize all access to the shared connection.
        self._lock = threading.Lock()

    def received_announce(self, destination_hash, announced_identity, app_data):
        name = None
        if app_data:
            try:
                name = app_data.decode("utf-8", "replace")[:200]
            except Exception:
                name = None
        h = RNS.hexrep(destination_hash, delimit=False)
        prio = SEED_INDEX_PRIO if h in SEED_NODES else INDEX_PRIO
        with self._lock:
            try:
                db.upsert_node(self._conn, h, name)
                db.enqueue(self._conn, h, "/page/index.mu", priority=prio,
                           fresh_hours=RECRAWL_HOURS)
                RNS.log(f"[beacon] announce: {h} ({name})", RNS.LOG_DEBUG)
            except Exception as e:  # noqa: BLE001
                RNS.log(f"[beacon] announce handler error: {e}", RNS.LOG_ERROR)
                try:
                    self._conn = self._conn_factory()
                except Exception:
                    pass


def _is_binary(path):
    p = path.lower()
    return p.startswith("/file/") or any(p.endswith(e) for e in BINARY_EXT)


def fetch_page(node_hash, path, timeout=LINK_TIMEOUT):
    """Open a Link to a NomadNet node and request a page path. Returns bytes or None."""
    dest_hash = bytes.fromhex(node_hash)
    if not RNS.Transport.has_path(dest_hash):
        RNS.Transport.request_path(dest_hash)
        deadline = time.time() + timeout
        while not RNS.Transport.has_path(dest_hash) and time.time() < deadline:
            time.sleep(0.5)
    if not RNS.Transport.has_path(dest_hash):
        return None
    identity = RNS.Identity.recall(dest_hash)
    if identity is None:
        return None
    dest = RNS.Destination(identity, RNS.Destination.OUT, RNS.Destination.SINGLE,
                           NODE_APP, NODE_ASPECT)
    up = threading.Event()
    link = RNS.Link(dest, established_callback=lambda l: up.set())
    if not up.wait(timeout):
        try:
            link.teardown()
        except Exception:
            pass
        return None
    out = {}
    done = threading.Event()
    link.request(
        path, data=None,
        response_callback=lambda r: (out.__setitem__("d", r.response), done.set()),
        failed_callback=lambda r: (out.__setitem__("f", True), done.set()),
        timeout=timeout,
    )
    got = done.wait(timeout + 5)
    try:
        link.teardown()
    except Exception:
        pass
    if not got or "d" not in out:
        return None
    data = out["d"]
    if isinstance(data, str):
        data = data.encode("utf-8", "replace")
    return data


def _process(conn, item):
    node_hash, path, url = item["node_hash"], item["path"], item["url"]
    if _is_binary(path):
        db.drop_queue(conn, url)
        return
    _stats["fetched"] += 1
    _t0 = time.time()
    data = fetch_page(node_hash, path)
    fetch_ms = int((time.time() - _t0) * 1000)   # responsiveness -> ranking signal
    if data is None:
        _stats["failed"] += 1
        attempts = item.get("attempts", 0)
        if attempts >= 4:
            db.drop_queue(conn, url)          # give up after repeated failures
        else:
            db.reschedule(conn, url, delay_s=3600 * (attempts + 1), ok=False)
        db.mark_node_crawled(conn, node_hash, reachable=False)
        return
    data = data[:MAX_PAGE_BYTES]
    raw = data.decode("utf-8", "replace")
    # Declared MeshData vs the STRUCTURAL fallback are computed independently
    # (not just meshdata.describe's declared-or-inferred merge) so ranking can
    # cross-check one against the other -- a declared type that wildly
    # contradicts the page's actual shape is an adversarial-surface concern
    # now that MeshData feeds ranking (see db.search's _rank_type).
    declared = meshdata.parse(raw)
    structural = meshdata.infer(raw, path)
    md = declared if declared else structural
    md.setdefault("type", "index")
    inferred_type = structural.get("type")
    title = md.get("title") or micron.title_of(raw)
    text = micron.to_text(raw)
    heads = micron.headings_of(raw)
    chash = hashlib.sha256(data).hexdigest()
    md_date = _dates.effective_declared_date(md)
    canonical = (md.get("canonical") or "").strip() or None
    db.record_page(conn, url, node_hash, path, title, text, chash, len(data), ok=True,
                   ptype=md.get("type"), description=md.get("description"),
                   lang=md.get("lang"), tags=md.get("tags"),
                   md_declared=bool(declared), fetch_ms=fetch_ms,
                   md_date=md_date, canonical=canonical, headings=heads,
                   inferred_type=inferred_type)
    edges = micron.extract_links(raw, node_hash)
    db.record_links(conn, url, edges)
    # Deep pages of our own nodes crawl right after our indexes (prio 2), ahead of
    # the external index backlog (prio 3); external deep pages stay at prio 6.
    deep_prio = SEED_DEEP_PRIO if node_hash in SEED_NODES else DEEP_PRIO
    for to_url, nh, p in edges:
        # Record every edge (link-graph ranking), but don't ENQUEUE dynamic
        # reader pages -- read.mu needs a ref/field, so crawling it bare is junk.
        if not _is_binary(p) and not p.endswith("/read.mu"):
            if nh != node_hash:
                db.upsert_node(conn, nh, None)
            db.enqueue(conn, nh, p, priority=(SEED_DEEP_PRIO if nh in SEED_NODES else deep_prio),
                       fresh_hours=RECRAWL_HOURS)
    db.drop_queue(conn, url)
    db.mark_node_crawled(conn, node_hash, reachable=True)
    _stats["ok"] += 1
    RNS.log(f"[beacon] crawled {url} ({len(data)}B, {len(edges)} links)", RNS.LOG_VERBOSE)


CRAWL_WORKERS = int(os.environ.get("BEACON_CRAWL_WORKERS", "6"))


def _recrawl_loop(conn_factory):
    conn = conn_factory()
    while True:
        try:
            db.enqueue_recrawl(conn, RECRAWL_HOURS)
            db.refresh_lexicon(conn)      # keep the "did you mean?" lexicon current
        except Exception as e:  # noqa: BLE001
            RNS.log(f"[beacon] recrawl error: {e}", RNS.LOG_DEBUG)
            try:
                conn = conn_factory()
            except Exception:
                pass
        time.sleep(3600)


def _worker(conn_factory, wid):
    conn = conn_factory()
    while True:
        try:
            item = db.claim_item(conn, lease_s=int(LINK_TIMEOUT * 3 + 30))
            if not item:
                time.sleep(3)
                continue
            _process(conn, item)
            time.sleep(FETCH_DELAY)      # per-worker politeness
        except Exception as e:  # noqa: BLE001
            RNS.log(f"[beacon] worker {wid} error: {e}", RNS.LOG_ERROR)
            time.sleep(5)
            try:
                conn = conn_factory()
            except Exception:
                pass


def crawl_loop(conn_factory):
    """Concurrent crawler: a pool of workers each atomically leases due items
    (SKIP LOCKED), so a slow/unreachable page no longer stalls the whole crawl."""
    threading.Thread(target=_recrawl_loop, args=(conn_factory,), daemon=True).start()
    workers = []
    for i in range(max(1, CRAWL_WORKERS)):
        t = threading.Thread(target=_worker, args=(conn_factory, i), daemon=True)
        t.start()
        workers.append(t)
    RNS.log(f"[beacon] crawl pool: {len(workers)} workers, delay {FETCH_DELAY}s, timeout {LINK_TIMEOUT}s")
    for t in workers:
        t.join()


def stats():
    return dict(_stats)
