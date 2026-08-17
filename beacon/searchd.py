"""Beacon search service over Reticulum.

Registers a MeshAPI search destination on Beacon's ALREADY-RUNNING RNS instance
(the crawler's) -- no separate process or mesh join. A stable identity persists
under /data so the destination hash is durable across restarts. Open-read
(ALLOW_ALL): sharing the index is the point; abuse is bounded by a per-link token
bucket and DB LIMIT.
"""
import os
import threading
import time
from collections import deque

import RNS

from . import db, federate, manifest, protocol

ANNOUNCE_INTERVAL = 900
PER_LINK_RPS = 2.0
PER_LINK_BURST = 6
# Max results retained per query; pages are sliced from this ranked set so `total`
# and prev/next stay stable. Plenty for mesh-scale search.
SEARCH_CAP = 60
WIKI_CAP = 18       # federated encyclopedia hits folded into the ranked set

_dest_hash_hex = None
_buckets = {}
_buckets_lock = threading.Lock()


def _allow(link_id):
    key = bytes(link_id) if link_id is not None else b"anon"
    now = time.time()
    with _buckets_lock:
        tokens, ts = _buckets.get(key, (PER_LINK_BURST, now))
        tokens = min(PER_LINK_BURST, tokens + (now - ts) * PER_LINK_RPS)
        if tokens >= 1.0:
            _buckets[key] = (tokens - 1.0, now)
            if len(_buckets) > 512:
                for k, (_, t) in list(_buckets.items()):
                    if now - t > 300:
                        _buckets.pop(k, None)
            return True
        _buckets[key] = (tokens, now)
        return False


NODE_CROWD_CAP = 3
_INTERNAL_ONLY_FIELDS = ("content_hash", "canonical")   # server-side dedup keys, never sent over the wire


def _apply_node_crowding(rows, cap=NODE_CROWD_CAP, window=10):
    """Greedily reorder so no `window`-sized SLIDING span of the output has
    more than `cap` items from the same node_hash -- a stronger guarantee
    than per-bucket capping (which breaks once a bucket under-fills: the next
    bucket's items land at the wrong absolute offset and a fixed-size page
    slice can straddle two crowded buckets). At each output position, take
    the highest-ranked remaining item whose node_hash count in the trailing
    `window` PLACED items is still under `cap`; only when literally nothing
    remaining qualifies (every remaining item is from an already-capped node
    -- i.e. fewer than `cap`+1 distinct nodes have any relevant content left)
    does it fall back to the next-best item anyway. Never drops anything, so
    `total` stays accurate. Diverse content is pulled to the FRONT of the
    list; if it runs out entirely, later pages legitimately fall back to the
    crowded node (there's nothing else to show) -- but page 1, the one
    anyone actually reads, gets the spread as long as it exists anywhere.

    This is what makes "recent news" stop being ~all Wikipedia: every wiki
    hit shares the SAME node_hash, so as long as a handful of OTHER nodes
    have anything relevant, page 1 can never hold more than `cap` of them."""
    remaining = list(rows)     # already rank-sorted desc
    trailing = deque()         # node_hashes of the last `window` PLACED items
    trailing_counts = {}
    result = []
    while remaining:
        pick = 0
        for i, r in enumerate(remaining):
            if trailing_counts.get(r.get("node_hash"), 0) < cap:
                pick = i
                break
        item = remaining.pop(pick)
        nh = item.get("node_hash")
        result.append(item)
        trailing.append(nh)
        trailing_counts[nh] = trailing_counts.get(nh, 0) + 1
        if len(trailing) > window:
            old = trailing.popleft()
            trailing_counts[old] -= 1
            if trailing_counts[old] <= 0:
                del trailing_counts[old]
    return result


def _annotate_more_from_node(merged, page_rows, page_end):
    """Tag the last row of each node shown on THIS page with how many more
    results from that same node exist further down the (fully-preserved)
    ranked list -- a cheap "+N more from X" note, no extra query needed."""
    after_counts = {}
    for r in merged[page_end:]:
        nh = r.get("node_hash")
        after_counts[nh] = after_counts.get(nh, 0) + 1
    out = [dict(r) for r in page_rows]
    last_idx = {}
    for i, r in enumerate(out):
        last_idx[r.get("node_hash")] = i
    for nh, cnt in after_counts.items():
        idx = last_idx.get(nh)
        if idx is not None:
            out[idx]["more_from_node"] = cnt
    return out


def _public_fields(r):
    """Strip server-only dedup keys before a result crosses the wire."""
    return {k: v for k, v in r.items() if k not in _INTERNAL_ONLY_FIELDS}


def _make_handler(conn_factory):
    def handler(path, data, request_id, link_id, remote_identity, requested_at):
        try:
            req = protocol.unpack(data)
        except Exception:
            return protocol.pack(protocol.err("bad_encoding"))
        if not isinstance(req, dict):
            return protocol.pack(protocol.err("bad_encoding"))
        if not _allow(link_id):
            return protocol.pack(protocol.err("rate_limited", req))
        if req.get("op") == protocol.MANIFEST_OP:
            return protocol.pack({"v": protocol.VERSION, "ok": True,
                                  "manifest": manifest.MANIFEST})
        if req.get("v") != protocol.VERSION:
            return protocol.pack(protocol.err("bad_version", req))
        op = req.get("op")

        # Click promotion: log an opened result so it ranks up over time.
        if op == protocol.OP_CLICK:
            url = req.get("url")
            if not url:
                return protocol.pack(protocol.err("missing_field", req, "url"))
            conn = None
            try:
                conn = conn_factory()
                db.record_click(conn, url)
            except Exception as e:  # noqa: BLE001
                RNS.log(f"[beacon] click error: {e}", RNS.LOG_ERROR)
                return protocol.pack(protocol.err("backend_error", req))
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
            return protocol.pack(protocol.ok({}, req))

        if op != protocol.OP_SEARCH:
            return protocol.pack(protocol.err("bad_op", req))
        q = req.get("q", "")
        if not q:
            return protocol.pack(protocol.err("missing_field", req, "q"))
        page_size = min(int(req.get("limit", 10) or 10), 20)   # results per page
        offset = max(int(req.get("offset", 0) or 0), 0)
        ptype = req.get("type")
        conn = None
        try:
            conn = conn_factory()
            # Rank the full result set once (capped), then paginate over it -- so
            # `total` is accurate and prev/next are stable across page requests.
            res = db.search(conn, q, SEARCH_CAP, ptype)
            # Federate the offline encyclopedia (unless a non-wiki type filter is
            # set). Wiki hits get the same link-graph + click promotion by url, so
            # a linked/opened article graduates into the ranked data.
            if not ptype or ptype == "wiki":
                for w in federate.wiki_search(q, WIKI_CAP):
                    w["score"] = round(w.get("score", 0) + db.rank_bonus(conn, w["url"]), 5)
                    res.append(w)
            # Dedup: collapse rows sharing a MeshData `canonical` URL (author-
            # declared "this is the same page") OR an exact content_hash (a
            # mirror re-serving byte-identical content on another node) into
            # one result -- highest score wins. Falls back to a bare-url key
            # (the old behaviour) for rows with neither, e.g. federated wiki
            # hits, which carry no content_hash.
            groups = {}
            for r in res:
                u = r.get("url")
                if u is None:
                    continue
                key = r.get("canonical") or r.get("content_hash") or u
                groups.setdefault(key, []).append(r)
            deduped = []
            for rows in groups.values():
                rows.sort(key=lambda r: r.get("score", 0), reverse=True)
                winner = dict(rows[0])
                if len(rows) > 1:
                    winner["also_on"] = len(rows) - 1   # dim "also on N other nodes" note
                deduped.append(winner)
            merged = sorted(deduped, key=lambda r: r.get("score", 0), reverse=True)
            # Node crowding cap: at most NODE_CROWD_CAP results per source node
            # in any page_size-sized window (bucketed per THIS request's page
            # size -- see _apply_node_crowding's docstring for why a single
            # global reorder isn't enough). Never drops anything -- `total`
            # stays accurate and later pages still reach the deferred items.
            # This is what makes "recent news" stop being ~all Wikipedia:
            # every wiki hit shares the SAME node_hash, so at most 3 of them
            # can ever land in one page's window.
            merged = _apply_node_crowding(merged, window=page_size)
            total = len(merged)
            page_end = offset + page_size
            page_rows = _annotate_more_from_node(merged, merged[offset:page_end], page_end)
            page = [_public_fields(r) for r in page_rows]
            # "Did you mean?": when the whole result set is thin, offer a spell-
            # corrected query (pg_trgm nearest corpus term). Only on the first page.
            suggestion = None
            if total < 3 and offset == 0:
                try:
                    suggestion = db.suggest(conn, q)
                except Exception as e:  # noqa: BLE001
                    RNS.log(f"[beacon] suggest error: {e}", RNS.LOG_DEBUG)
                if suggestion and suggestion.strip().lower() == q.strip().lower():
                    suggestion = None
            # Search analytics: log the query + how many results it found (only the
            # first page request, so paging through results isn't double-counted).
            # `urls` = the impressions -- the actual result set SHOWN for this
            # query -- which is the mesh-side half of lever 2's click-through
            # story (see db.record_search's docstring for why mesh can't log
            # clicks the way beacon.web._go does for the web).
            if offset == 0:
                try:
                    db.record_search(conn, q, total, bool(suggestion), urls=[r.get("url") for r in page])
                except Exception as e:  # noqa: BLE001
                    RNS.log(f"[beacon] record_search error: {e}", RNS.LOG_DEBUG)
        except Exception as e:  # noqa: BLE001
            RNS.log(f"[beacon] search error: {e}", RNS.LOG_ERROR)
            return protocol.pack(protocol.err("backend_error", req))
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
        payload = {"res": page, "total": total, "offset": offset, "limit": page_size}
        if suggestion:
            payload["suggestion"] = suggestion
        return protocol.pack(protocol.ok(payload, req))
    return handler


def start(conn_factory, identity_path):
    """Register the search destination on the running RNS instance. Returns dest hex."""
    global _dest_hash_hex
    os.makedirs(os.path.dirname(identity_path), exist_ok=True)
    if os.path.isfile(identity_path):
        identity = RNS.Identity.from_file(identity_path)
    else:
        identity = RNS.Identity()
        identity.to_file(identity_path)
    dest = RNS.Destination(identity, RNS.Destination.IN, RNS.Destination.SINGLE,
                           protocol.APP_NAME, *protocol.ASPECTS)
    dest.register_request_handler(protocol.PATH, response_generator=_make_handler(conn_factory),
                                  allow=RNS.Destination.ALLOW_ALL)
    _dest_hash_hex = RNS.hexrep(dest.hash, delimit=False)
    manifest.MANIFEST["service"]["dest"] = _dest_hash_hex
    RNS.log(f"[beacon] search serving as {RNS.prettyhexrep(dest.hash)}")
    print(f"beacon search destination: {_dest_hash_hex}", flush=True)

    def announce_loop():
        while True:
            try:
                dest.announce()
                RNS.log("[beacon] search announced")
            except Exception as e:  # noqa: BLE001
                RNS.log(f"[beacon] search announce failed: {e}", RNS.LOG_DEBUG)
            time.sleep(ANNOUNCE_INTERVAL)

    threading.Thread(target=announce_loop, daemon=True).start()
    return _dest_hash_hex


def dest_hash():
    return _dest_hash_hex
