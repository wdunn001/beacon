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

from . import db, micron

NODE_APP = "nomadnetwork"
NODE_ASPECT = "node"
FETCH_DELAY = float(os.environ.get("BEACON_FETCH_DELAY", "30"))   # seconds between fetches
LINK_TIMEOUT = float(os.environ.get("BEACON_LINK_TIMEOUT", "45"))
RECRAWL_HOURS = int(os.environ.get("BEACON_RECRAWL_HOURS", "24"))
MAX_PAGE_BYTES = int(os.environ.get("BEACON_MAX_PAGE_BYTES", str(512 * 1024)))
BINARY_EXT = (".pdf", ".epub", ".zip", ".gz", ".png", ".jpg", ".jpeg", ".gif",
              ".webp", ".mp3", ".ogg", ".opus", ".mp4", ".bin", ".tar")

_stats = {"fetched": 0, "ok": 0, "failed": 0, "started": time.time()}


class _NodeAnnounceHandler:
    aspect_filter = f"{NODE_APP}.{NODE_ASPECT}"

    def __init__(self, conn_factory):
        self._conn_factory = conn_factory
        self._conn = conn_factory()

    def received_announce(self, destination_hash, announced_identity, app_data):
        try:
            name = None
            if app_data:
                try:
                    name = app_data.decode("utf-8", "replace")[:200]
                except Exception:
                    name = None
            h = RNS.hexrep(destination_hash, delimit=False)
            db.upsert_node(self._conn, h, name)
            db.enqueue(self._conn, h, "/page/index.mu", priority=3)
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
    data = fetch_page(node_hash, path)
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
    title = micron.title_of(raw)
    text = micron.to_text(raw)
    chash = hashlib.sha256(data).hexdigest()
    db.record_page(conn, url, node_hash, path, title, text, chash, len(data), ok=True)
    edges = micron.extract_links(raw, node_hash)
    db.record_links(conn, url, edges)
    for to_url, nh, p in edges:
        if not _is_binary(p):
            db.upsert_node(conn, nh, None) if nh != node_hash else None
            db.enqueue(conn, nh, p, priority=6)
    db.drop_queue(conn, url)
    db.mark_node_crawled(conn, node_hash, reachable=True)
    _stats["ok"] += 1
    RNS.log(f"[beacon] crawled {url} ({len(data)}B, {len(edges)} links)", RNS.LOG_VERBOSE)


def crawl_loop(conn_factory):
    conn = conn_factory()
    last_recrawl = 0
    while True:
        try:
            if time.time() - last_recrawl > 3600:
                db.enqueue_recrawl(conn, RECRAWL_HOURS)
                last_recrawl = time.time()
            items = db.due_items(conn, limit=1)
            if not items:
                time.sleep(5)
                continue
            _process(conn, items[0])
            time.sleep(FETCH_DELAY)
        except Exception as e:  # noqa: BLE001
            RNS.log(f"[beacon] crawl loop error: {e}", RNS.LOG_ERROR)
            time.sleep(10)
            try:
                conn = conn_factory()
            except Exception:
                pass


def stats():
    return dict(_stats)
