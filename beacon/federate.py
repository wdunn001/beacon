"""Federated search: fold results from sibling MeshAPI services (currently
rns-wiki's Xapian search over the offline encyclopedia) into Beacon's own
page-index results. This surfaces content Beacon can't crawl -- the Kiwix wiki
is dynamic (read.mu?ref=…, millions of pages) -- without indexing it.

Beacon already runs an RNS instance (the crawler's), so we just open a Link to
the sibling service and call its search op. Failures degrade to [] -- federation
never blocks a local search.
"""
import base64
import os
import threading
import time

import RNS
import umsgpack

# rns-wiki: SERVICE dest answers search; NODE dest hosts read.mu (where results link).
WIKI_SERVICE = os.environ.get("BEACON_WIKI_DEST", "d4d6be6286955a56acaa66d60dfc5431")
WIKI_NODE = os.environ.get("BEACON_WIKI_NODE", "af9186e72ae0f7b89c2f8a105da59dbe")
WIKI_APP, WIKI_ASPECT, WIKI_PATH = "rnswiki", "query", "q"


def _encode_ref(book, path):
    raw = (book + "\n" + path).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _call(dest_hex, app, aspect, path, body, timeout=12):
    dh = bytes.fromhex(dest_hex)
    if not RNS.Transport.has_path(dh):
        RNS.Transport.request_path(dh)
        deadline = time.time() + timeout
        while not RNS.Transport.has_path(dh) and time.time() < deadline:
            time.sleep(0.3)
    idn = RNS.Identity.recall(dh)
    if not idn:
        return None
    dest = RNS.Destination(idn, RNS.Destination.OUT, RNS.Destination.SINGLE, app, aspect)
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
    link.request(path, data=umsgpack.packb(body),
                 response_callback=lambda r: (out.__setitem__("d", r.response), done.set()),
                 failed_callback=lambda r: done.set(), timeout=timeout)
    done.wait(timeout + 3)
    try:
        link.teardown()
    except Exception:
        pass
    if "d" not in out:
        return None
    try:
        return umsgpack.unpackb(out["d"])
    except Exception:
        return None


def wiki_search(q, limit=6, timeout=12):
    """Return rns-wiki hits mapped to Beacon result dicts (empty on any failure)."""
    try:
        resp = _call(WIKI_SERVICE, WIKI_APP, WIKI_ASPECT, WIKI_PATH,
                     {"v": 1, "op": "search", "q": q, "limit": limit}, timeout)
    except Exception as e:  # noqa: BLE001
        RNS.log(f"[beacon] wiki federation error: {e}", RNS.LOG_DEBUG)
        return []
    if not resp or not resp.get("ok"):
        return []
    hits = resp.get("res", []) or []
    out = []
    n = max(1, len(hits))
    for i, h in enumerate(hits):
        book, path = h.get("book", ""), h.get("path", "")
        if not book or not path:
            continue
        ref = _encode_ref(book, path)
        out.append({
            "url": f"{WIKI_NODE}:/page/read.mu#{ref}",   # stable id for click ranking
            "node_hash": WIKI_NODE, "path": "/page/read.mu", "ref": ref,
            "title": h.get("title", "(untitled)"), "type": "wiki",
            "node_name": h.get("book_title", "encyclopedia"),
            "description": h.get("snippet", ""), "snippet": h.get("snippet", ""),
            "md": True, "source": "wiki",
            # base relevance descending by rns-wiki's own ranking, scaled to sit
            # among Postgres ts_rank scores; click bonus is layered on in db.search's caller.
            "score": round(0.8 * (n - i) / n, 5),
        })
    return out
