"""Wire protocol for Beacon search over Reticulum. Compact umsgpack over an RNS
Link request handler, same shape as the sibling MeshAPI services. umsgpack is
pip-installed explicitly in the image (not reliable from an RNS install)."""
import umsgpack

APP_NAME = "rnsbeacon"
ASPECTS = ("query",)
PATH = "q"
VERSION = 1

OP_SEARCH = "search"     # {q, limit?, type?} -> {res:[{url,node_hash,path,title,type,node_name,description,snippet,md,score}]}
OPS = frozenset((OP_SEARCH,))

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
