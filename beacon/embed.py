"""Query/document embeddings for hybrid vector search (lever 4), via the
shared .88 GPU Ollama deployment (nomic-embed-text, 768 dims; see memory
ollama-shared-model-store-88 -- host :11435 is the "background" instance,
kept separate from the interactive-chat instance on :11436 so a big backfill
batch doesn't contend with OWUI).

This module is the ONE place that talks to the embedder, and every call in it
is best-effort: a timeout, a non-200, a malformed response, an all-zero
vector, or (at the retrieval layer, see db.looks_uniform below) the "uniform
distance" pathology documented in script-library-embedder.md for the sibling
nomic-embed-text deployment -- ALL of these must degrade to "no vector
available", never raise into a crawl or a search request. Hybrid search is a
lever on top of lexical search, not a replacement for it; lexical must keep
working with the embedder fully offline.

nomic-embed-text is an ASYMMETRIC retrieval model: corpus text and query text
use different task prefixes ("search_document: " / "search_query: ") to get
proper retrieval quality -- this is a documented Nomic convention, not an
optional nicety, so it's baked into embed_document/embed_query rather than
left to callers.
"""
import json
import os
import urllib.error
import urllib.request

EMBED_URL = os.environ.get("BEACON_EMBED_URL", "http://192.168.1.88:11435").rstrip("/")
EMBED_MODEL = os.environ.get("BEACON_EMBED_MODEL", "nomic-embed-text")
EMBED_TIMEOUT = float(os.environ.get("BEACON_EMBED_TIMEOUT", "4"))
EMBED_DIM = int(os.environ.get("BEACON_EMBED_DIM", "768"))

# Char caps (not token-exact, but a safe, cheap guard). Found live
# (2026-08-16): this deployment's nomic-embed-text is loaded with a 2048-
# TOKEN context (`ollama ps` -> CONTEXT 2048, not the model's max 8192 --
# whatever loaded it capped it there), and Ollama 500s outright on overflow
# ("the input length exceeds the context length") rather than truncating --
# which _call() correctly treats as a soft failure (HTTPError is a
# URLError subclass, caught below) and returns None for, but an 8000-char
# cap assumed ~4 chars/token and blew straight through 2048 tokens on
# anything non-trivial: a 37KB Cyrillic page, a 7KB tech-jargon page, and a
# box-drawing-heavy page ALL 500'd, stalling the backfill on most of the
# corpus (non-Latin scripts and code/punctuation-dense text run well under
# 4 chars/token in BPE tokenizers). 3000 chars is a conservative ~1.5
# chars/token budget that leaves headroom for the "search_document: "/
# "search_query: " prefix -- safe across scripts, still plenty of text for
# a decent embedding. Documents get more room than queries.
_DOC_CHARS = 2500
_QUERY_CHARS = 1000


def _call(prompt, timeout):
    body = json.dumps({"model": EMBED_MODEL, "prompt": prompt}).encode("utf-8")
    req = urllib.request.Request(
        f"{EMBED_URL}/api/embeddings", data=body,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", "replace"))
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        return None
    except Exception:  # noqa: BLE001 -- embedder failures must never propagate
        return None
    vec = data.get("embedding") if isinstance(data, dict) else None
    if not isinstance(vec, list) or len(vec) != EMBED_DIM:
        return None
    try:
        vec = [float(v) for v in vec]
    except (TypeError, ValueError):
        return None
    if all(v == 0.0 for v in vec):     # dead-embedder tell: an all-zero vector
        return None
    return vec


def embed_document(text, timeout=None):
    if not text or not text.strip():
        return None
    return _call("search_document: " + text.strip()[:_DOC_CHARS], timeout or EMBED_TIMEOUT)


def embed_query(text, timeout=None):
    if not text or not text.strip():
        return None
    return _call("search_query: " + text.strip()[:_QUERY_CHARS], timeout or EMBED_TIMEOUT)


def to_vector_literal(vec):
    """Format a Python float list as a pgvector text literal for an
    `%s::vector` cast -- avoids adding the `pgvector` pip package just to get
    a Python<->vector adapter for this one direction."""
    return "[" + ",".join(f"{v:.6f}" for v in vec) + "]"


def looks_uniform(distances, eps=1e-4):
    """The 'dead embedder' pathology (script-library-embedder.md): every
    candidate comes back at ~identical cosine distance, the tell that the
    embedder silently returned degenerate/constant vectors rather than a real
    embedding (e.g. serving errors as zero vectors upstream of our own
    all-zero check, or a model swap that broke without erroring). Guards
    against confidently ranking on noise. Needs >=3 points to be meaningful."""
    if not distances or len(distances) < 3:
        return False
    lo, hi = min(distances), max(distances)
    return (hi - lo) < eps
