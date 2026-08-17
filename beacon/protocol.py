"""Wire protocol for Beacon search over Reticulum. Compact umsgpack over an RNS
Link request handler, same shape as the sibling MeshAPI services. umsgpack is
pip-installed explicitly in the image (not reliable from an RNS install)."""
import umsgpack

APP_NAME = "rnsbeacon"
ASPECTS = ("query",)
PATH = "q"
VERSION = 1

OP_SEARCH = "search"     # {q, limit?, type?} -> {res:[{url,node_hash,path,ref?,title,type,
                          #   node_name,description,snippet,md,score,date?,also_on?,more_from_node?,
                          #   price?,currency?,availability?,sku?,vendor?,shop?}]}
                          # snippet may contain \x01/\x02 sentinel pairs marking the matched-term
                          # span (protocol-level, never raw micron control codes -- the client
                          # renders them, e.g. as a bold toggle). date = effective content date
                          # (ISO YYYY-MM-DD) when known. also_on = N other nodes collapsed into
                          # this result (canonical/content-hash dedup). more_from_node = N more
                          # results from this same node held back by the per-page crowding cap.
                          # price/currency/availability/sku/vendor/shop are only non-null on
                          # type=="product" results (MeshData commerce fields); `shop` is the
                          # seller's MeshAPI service destination hash (buy/cart ops), NOT the
                          # same hash as node_hash (which serves the browsable page).
OP_CLICK = "click"       # {url} -> {ok}   (promotes the result in ranking)
OPS = frozenset((OP_SEARCH, OP_CLICK))

MANIFEST_OP = "__manifest__"


def pack(obj):
    return umsgpack.packb(obj)


def unpack(data):
    return umsgpack.unpackb(data)


def ok(payload, req=None):
    d = {"v": VERSION, "ok": True}
    d.update(payload)
    if req is not None and "id" in req:
        d["id"] = req["id"]
    return d


def err(code, req=None, msg=None):
    d = {"v": VERSION, "ok": False, "err": code}
    if msg:
        d["msg"] = msg
    if req is not None and isinstance(req, dict) and "id" in req:
        d["id"] = req["id"]
    return d
