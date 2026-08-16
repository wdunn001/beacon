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

import RNS

from . import db, federate, manifest, protocol

ANNOUNCE_INTERVAL = 900
PER_LINK_RPS = 2.0
PER_LINK_BURST = 6

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
        limit = min(int(req.get("limit", 15) or 15), 30)
        ptype = req.get("type")
        conn = None
        try:
            conn = conn_factory()
            res = db.search(conn, q, limit, ptype)
            # Federate the offline encyclopedia (unless a non-wiki type filter is
            # set). Wiki hits get the same link-graph + click promotion by url, so
            # a linked/opened article graduates into the ranked data.
            if not ptype or ptype == "wiki":
                for w in federate.wiki_search(q, min(limit, 6)):
                    w["score"] = round(w.get("score", 0) + db.rank_bonus(conn, w["url"]), 5)
                    res.append(w)
            res.sort(key=lambda r: r.get("score", 0), reverse=True)
            res = res[:limit]
            # "Did you mean?": when results are thin, offer a spell-corrected query
            # (pg_trgm nearest corpus term). Cheap, and only when it would help.
            suggestion = None
            if len(res) < 3:
                try:
                    suggestion = db.suggest(conn, q)
                except Exception as e:  # noqa: BLE001
                    RNS.log(f"[beacon] suggest error: {e}", RNS.LOG_DEBUG)
                if suggestion and suggestion.strip().lower() == q.strip().lower():
                    suggestion = None
        except Exception as e:  # noqa: BLE001
            RNS.log(f"[beacon] search error: {e}", RNS.LOG_ERROR)
            return protocol.pack(protocol.err("backend_error", req))
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
        payload = {"res": res}
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
