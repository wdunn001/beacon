"""Beacon Postgres layer: registry (nodes), page store, link graph, crawl queue.

Connection comes from env (BEACON_DB_*). FTS is built in: pages carry a generated
tsvector so search + ts_rank work from day one, ahead of the richer ranking stage.
"""
import math
import os
import re
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

SCHEMA = r"""
CREATE TABLE IF NOT EXISTS nodes (
    dest_hash       TEXT PRIMARY KEY,
    name            TEXT,
    first_seen      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen       TIMESTAMPTZ NOT NULL DEFAULT now(),
    announce_count  INTEGER NOT NULL DEFAULT 0,
    last_crawled    TIMESTAMPTZ,
    reachable       BOOLEAN
);

CREATE TABLE IF NOT EXISTS pages (
    id            BIGSERIAL PRIMARY KEY,
    url           TEXT UNIQUE NOT NULL,          -- "<hash>:/page/<path>"
    node_hash     TEXT NOT NULL,
    path          TEXT NOT NULL,
    title         TEXT,
    content       TEXT,
    content_hash  TEXT,
    bytes         INTEGER,
    ok            BOOLEAN NOT NULL DEFAULT TRUE,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    content_tsv   TSVECTOR GENERATED ALWAYS AS (
                     to_tsvector('english', coalesce(title,'') || ' ' || coalesce(content,''))
                  ) STORED
);
CREATE INDEX IF NOT EXISTS pages_tsv_idx  ON pages USING GIN (content_tsv);
CREATE INDEX IF NOT EXISTS pages_node_idx ON pages (node_hash);

CREATE TABLE IF NOT EXISTS links (
    from_url      TEXT NOT NULL,
    to_url        TEXT NOT NULL,
    to_node_hash  TEXT,
    to_path       TEXT,
    PRIMARY KEY (from_url, to_url)
);
CREATE INDEX IF NOT EXISTS links_to_idx ON links (to_url);

CREATE TABLE IF NOT EXISTS crawl_queue (
    url           TEXT PRIMARY KEY,
    node_hash     TEXT NOT NULL,
    path          TEXT NOT NULL,
    enqueued_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts      INTEGER NOT NULL DEFAULT 0,
    next_attempt  TIMESTAMPTZ NOT NULL DEFAULT now(),
    priority      INTEGER NOT NULL DEFAULT 5
);
CREATE INDEX IF NOT EXISTS queue_due_idx ON crawl_queue (next_attempt, priority);
"""


def connect():
    return psycopg2.connect(
        host=os.environ.get("BEACON_DB_HOST", "127.0.0.1"),
        port=int(os.environ.get("BEACON_DB_PORT", "5432")),
        dbname=os.environ.get("BEACON_DB_NAME", "beacon"),
        user=os.environ.get("BEACON_DB_USER", "beacon"),
        password=os.environ.get("BEACON_DB_PASSWORD", ""),
    )


MIGRATIONS = r"""
ALTER TABLE pages ADD COLUMN IF NOT EXISTS type        TEXT;
ALTER TABLE pages ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE pages ADD COLUMN IF NOT EXISTS lang        TEXT;
ALTER TABLE pages ADD COLUMN IF NOT EXISTS tags        TEXT[];
ALTER TABLE pages ADD COLUMN IF NOT EXISTS md_declared BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE pages ADD COLUMN IF NOT EXISTS fetch_ms    INTEGER;
CREATE INDEX IF NOT EXISTS pages_type_idx ON pages (type);

-- Click promotion: any result URL (a crawled page OR a federated wiki article)
-- accrues weight as users open it, so popular results rank up over time.
CREATE TABLE IF NOT EXISTS clicks (
    url          TEXT PRIMARY KEY,
    clicks       BIGINT NOT NULL DEFAULT 0,
    last_clicked TIMESTAMPTZ
);

-- Beacon-Analytics: RUM-style page views for our NomadNet nodes. `vid` is an
-- OPAQUE HASH of the visitor's identity (never the raw identity) for unique
-- counts only; the dashboard shows counts, never identities.
CREATE TABLE IF NOT EXISTS page_events (
    id   BIGSERIAL PRIMARY KEY,
    ts   TIMESTAMPTZ NOT NULL DEFAULT now(),
    node TEXT NOT NULL,
    path TEXT NOT NULL,
    vid  TEXT
);
CREATE INDEX IF NOT EXISTS page_events_ts_idx   ON page_events (ts);
CREATE INDEX IF NOT EXISTS page_events_node_idx ON page_events (node);

-- Search analytics: one row per query the search service handles, so the
-- dashboard can show most-searched terms and zero-result queries (to debug
-- ranking/coverage gaps). `q` is the normalised query text -- NOT tied to any
-- visitor identity (aggregate popularity only).
CREATE TABLE IF NOT EXISTS searches (
    id        BIGSERIAL PRIMARY KEY,
    ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
    q         TEXT NOT NULL,
    results   INTEGER NOT NULL DEFAULT 0,
    suggested BOOLEAN NOT NULL DEFAULT FALSE   -- did we offer a "did you mean?"
);
CREATE INDEX IF NOT EXISTS searches_ts_idx ON searches (ts);
CREATE INDEX IF NOT EXISTS searches_q_idx  ON searches (q);

-- "Did you mean?": a corpus lexicon (lexemes from page content) with a trigram
-- index, so a misspelt query ("colarado") can be matched to the nearest real
-- term ("colorado") via pg_trgm similarity. Refreshed periodically from ts_stat.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE TABLE IF NOT EXISTS lexicon (
    word TEXT PRIMARY KEY,
    df   INTEGER NOT NULL DEFAULT 1      -- document frequency (rarer = weaker suggestion)
);
CREATE INDEX IF NOT EXISTS lexicon_trgm_idx ON lexicon USING GIN (word gin_trgm_ops);

-- Recency-aware ranking columns -----------------------------------------
-- first_seen:    when THIS url was first crawled. Immutable after insert.
-- last_changed:  bumped only when a recrawl's content_hash differs from the
--                stored one -- i.e. an OBSERVED content change, not "we
--                recrawled it again and nothing moved".
-- md_date:       the declared MeshData date (date/published/updated,
--                tolerant-parsed in beacon.dates; junk/future/pre-2015 ->
--                NULL). Wins over the crawl-observed columns when present.
-- canonical:     declared MeshData canonical URL, for cross-node dedupe.
-- headings:      micron heading-line text, kept apart from body so the
--                field-weighted tsvector can tell "in a heading" from
--                "in the text" (weight B vs D below).
-- inferred_type: the STRUCTURAL type (meshdata.infer, ignoring any declared
--                block) kept alongside the displayed `type` so ranking can
--                fall back to it when a declared type contradicts page shape.
ALTER TABLE pages ADD COLUMN IF NOT EXISTS first_seen     TIMESTAMPTZ;
ALTER TABLE pages ADD COLUMN IF NOT EXISTS last_changed   TIMESTAMPTZ;
ALTER TABLE pages ADD COLUMN IF NOT EXISTS md_date        TIMESTAMPTZ;
ALTER TABLE pages ADD COLUMN IF NOT EXISTS canonical      TEXT;
ALTER TABLE pages ADD COLUMN IF NOT EXISTS headings       TEXT;
ALTER TABLE pages ADD COLUMN IF NOT EXISTS inferred_type  TEXT;
-- Honest backfill: rows crawled before this migration have no real history,
-- so both collapse to their last known crawl timestamp (imperfect but honest
-- -- never fabricate an earlier first_seen/last_changed than we can prove).
UPDATE pages SET first_seen   = fetched_at WHERE first_seen   IS NULL;
UPDATE pages SET last_changed = fetched_at WHERE last_changed IS NULL;
CREATE INDEX IF NOT EXISTS pages_content_hash_idx ON pages (content_hash);
CREATE INDEX IF NOT EXISTS pages_canonical_idx    ON pages (canonical) WHERE canonical IS NOT NULL;

-- Field-weighted tsvector: title (A) > headings (B) > description (C) >
-- body (D). A GENERATED STORED column's expression can't be ALTERed in
-- place, so this drops + re-adds it -- guarded so it only runs once (checked
-- via the stored generation expression itself, not a version table) and so a
-- fresh DB (SCHEMA above) doesn't pay for a pointless drop/recreate. The
-- DROP+ADD recomputes content_tsv for every existing row immediately (no
-- recrawl needed for the weighting change itself; only `headings`/md_date
-- need a recrawl to stop being NULL on old rows).
DO $mig$
DECLARE
  gen_expr text;
BEGIN
  SELECT pg_get_expr(d.adbin, d.adrelid) INTO gen_expr
  FROM pg_attrdef d JOIN pg_attribute a
    ON a.attrelid = d.adrelid AND a.attnum = d.adnum
  WHERE d.adrelid = 'pages'::regclass AND a.attname = 'content_tsv';

  IF gen_expr IS NULL OR position('setweight' in gen_expr) = 0 THEN
    EXECUTE 'ALTER TABLE pages DROP COLUMN IF EXISTS content_tsv';
    EXECUTE $ddl$ALTER TABLE pages ADD COLUMN content_tsv TSVECTOR GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(title,'')), 'A') ||
        setweight(to_tsvector('english', coalesce(headings,'')), 'B') ||
        setweight(to_tsvector('english', coalesce(description,'')), 'C') ||
        setweight(to_tsvector('english', coalesce(content,'')), 'D')
      ) STORED$ddl$;
    EXECUTE 'CREATE INDEX IF NOT EXISTS pages_tsv_idx ON pages USING GIN (content_tsv)';
  END IF;
END $mig$;
"""


def init_schema(conn):
    with conn, conn.cursor() as c:
        c.execute(SCHEMA)
        c.execute(MIGRATIONS)


def upsert_node(conn, dest_hash, name):
    with conn, conn.cursor() as c:
        c.execute(
            """INSERT INTO nodes (dest_hash, name, last_seen, announce_count)
               VALUES (%s, %s, now(), 1)
               ON CONFLICT (dest_hash) DO UPDATE
                 SET name = COALESCE(EXCLUDED.name, nodes.name),
                     last_seen = now(),
                     announce_count = nodes.announce_count + 1""",
            (dest_hash, name),
        )


def enqueue(conn, node_hash, path, priority=5, fresh_hours=None):
    """Queue a page for crawling. With fresh_hours set, a page we already crawled
    OK within that window is NOT re-queued -- this stops a densely interlinked site
    (e.g. The Mild Take) from re-enqueuing its own pages the instant they're crawled
    and monopolising the workers; enqueue_recrawl reclaims stale pages after the
    recrawl interval instead."""
    url = f"{node_hash}:{path}"
    with conn, conn.cursor() as c:
        if fresh_hours:
            c.execute(
                """INSERT INTO crawl_queue (url, node_hash, path, priority)
                   SELECT %s, %s, %s, %s
                   WHERE NOT EXISTS (
                       SELECT 1 FROM pages
                       WHERE url = %s AND ok
                         AND fetched_at > now() - (%s || ' hours')::interval)
                   ON CONFLICT (url) DO NOTHING""",
                (url, node_hash, path, priority, url, int(fresh_hours)),
            )
        else:
            c.execute(
                """INSERT INTO crawl_queue (url, node_hash, path, priority)
                   VALUES (%s, %s, %s, %s) ON CONFLICT (url) DO NOTHING""",
                (url, node_hash, path, priority),
            )
    return url


def due_items(conn, limit=1):
    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
        c.execute(
            """SELECT url, node_hash, path, attempts FROM crawl_queue
               WHERE next_attempt <= now() ORDER BY priority ASC, next_attempt ASC LIMIT %s""",
            (limit,),
        )
        return c.fetchall()


def claim_item(conn, lease_s=180):
    """Atomically lease one due item for a worker (FOR UPDATE SKIP LOCKED), bumping
    its next_attempt so no other worker grabs it. Enables concurrent crawlers.
    Returns the item dict or None. _process later drops it on success or reschedules."""
    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
        c.execute(
            """UPDATE crawl_queue SET next_attempt = now() + (%s || ' seconds')::interval
               WHERE url = (
                   SELECT url FROM crawl_queue WHERE next_attempt <= now()
                   ORDER BY priority ASC, next_attempt ASC
                   FOR UPDATE SKIP LOCKED LIMIT 1)
               RETURNING url, node_hash, path, attempts""",
            (int(lease_s),),
        )
        return c.fetchone()


def reschedule(conn, url, delay_s, ok):
    with conn, conn.cursor() as c:
        c.execute(
            """UPDATE crawl_queue
               SET attempts = attempts + 1, next_attempt = now() + (%s || ' seconds')::interval
               WHERE url = %s""",
            (int(delay_s), url),
        )


def drop_queue(conn, url):
    with conn, conn.cursor() as c:
        c.execute("DELETE FROM crawl_queue WHERE url = %s", (url,))


def record_page(conn, url, node_hash, path, title, content, content_hash, nbytes,
                ok=True, ptype=None, description=None, lang=None, tags=None,
                md_declared=False, fetch_ms=None, md_date=None, canonical=None,
                headings=None, inferred_type=None):
    with conn, conn.cursor() as c:
        c.execute(
            """INSERT INTO pages (url, node_hash, path, title, content, content_hash,
                                  bytes, ok, type, description, lang, tags, md_declared,
                                  fetch_ms, md_date, canonical, headings, inferred_type,
                                  fetched_at, first_seen, last_changed)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now(), now(), now())
               ON CONFLICT (url) DO UPDATE
                 SET title=EXCLUDED.title, content=EXCLUDED.content,
                     content_hash=EXCLUDED.content_hash, bytes=EXCLUDED.bytes,
                     ok=EXCLUDED.ok, type=EXCLUDED.type, description=EXCLUDED.description,
                     lang=EXCLUDED.lang, tags=EXCLUDED.tags, md_declared=EXCLUDED.md_declared,
                     fetch_ms=EXCLUDED.fetch_ms, md_date=EXCLUDED.md_date,
                     canonical=EXCLUDED.canonical, headings=EXCLUDED.headings,
                     inferred_type=EXCLUDED.inferred_type, fetched_at=now(),
                     -- first_seen is immutable once set. last_changed only
                     -- moves when the content actually changed (content-hash
                     -- comparison) -- an unchanged page recrawled daily must
                     -- NOT look freshly-published forever.
                     last_changed = CASE
                       WHEN pages.content_hash IS DISTINCT FROM EXCLUDED.content_hash
                       THEN now() ELSE pages.last_changed END""",
            (url, node_hash, path, title, content, content_hash, nbytes, ok,
             ptype, description, lang, tags, md_declared, fetch_ms, md_date,
             canonical, headings, inferred_type),
        )


def record_links(conn, from_url, edges):
    if not edges:
        return
    with conn, conn.cursor() as c:
        psycopg2.extras.execute_values(
            c,
            "INSERT INTO links (from_url, to_url, to_node_hash, to_path) VALUES %s "
            "ON CONFLICT (from_url, to_url) DO NOTHING",
            [(from_url, to_url, nh, p) for (to_url, nh, p) in edges],
        )


def mark_node_crawled(conn, node_hash, reachable):
    with conn, conn.cursor() as c:
        c.execute("UPDATE nodes SET last_crawled = now(), reachable = %s WHERE dest_hash = %s",
                  (reachable, node_hash))


def enqueue_recrawl(conn, max_age_hours=24):
    """Re-queue index pages of nodes not crawled in max_age_hours."""
    with conn, conn.cursor() as c:
        c.execute(
            """INSERT INTO crawl_queue (url, node_hash, path, priority)
               SELECT dest_hash || ':/page/index.mu', dest_hash, '/page/index.mu', 7
               FROM nodes
               WHERE last_crawled IS NULL
                  OR last_crawled < now() - (%s || ' hours')::interval
               ON CONFLICT (url) DO NOTHING""",
            (int(max_age_hours),),
        )


def stats(conn):
    with conn, conn.cursor() as c:
        out = {}
        c.execute("SELECT count(*) FROM nodes"); out["nodes"] = c.fetchone()[0]
        c.execute("SELECT count(*) FROM pages WHERE ok"); out["pages"] = c.fetchone()[0]
        c.execute("SELECT count(*) FROM links"); out["links"] = c.fetchone()[0]
        c.execute("SELECT count(*) FROM crawl_queue WHERE next_attempt <= now()"); out["queue_due"] = c.fetchone()[0]
        c.execute("SELECT count(*) FROM crawl_queue"); out["queue_total"] = c.fetchone()[0]
        c.execute("SELECT count(*) FROM pages WHERE md_declared"); out["md_declared"] = c.fetchone()[0]
        # Durable, DB-backed activity metrics (survive restarts, unlike the
        # in-memory crawler session counters that reset to 0 on every redeploy).
        c.execute("SELECT count(*) FROM pages WHERE ok AND fetched_at > now() - interval '24 hours'")
        out["fetched_24h"] = c.fetchone()[0]
        c.execute("SELECT count(*) FROM nodes WHERE reachable"); out["reachable"] = c.fetchone()[0]
        c.execute("SELECT count(*) FROM nodes WHERE reachable IS FALSE"); out["unreachable"] = c.fetchone()[0]
        return out


def categories(conn):
    with conn, conn.cursor() as c:
        c.execute("SELECT coalesce(type,'(none)') t, count(*) n FROM pages WHERE ok "
                  "GROUP BY 1 ORDER BY 2 DESC")
        return [{"type": r[0], "count": r[1]} for r in c.fetchall()]


def recent_pages(conn, limit=25):
    with conn, conn.cursor() as c:
        c.execute("SELECT url, coalesce(title,'(untitled)'), coalesce(type,'?'), "
                  "md_declared, fetched_at FROM pages WHERE ok "
                  "ORDER BY fetched_at DESC LIMIT %s", (limit,))
        return [{"url": r[0], "title": r[1], "type": r[2], "declared": r[3],
                 "fetched_at": r[4].isoformat() if r[4] else None} for r in c.fetchall()]


def top_nodes(conn, limit=15):
    with conn, conn.cursor() as c:
        c.execute("SELECT coalesce(name,'(unnamed)'), dest_hash, announce_count, "
                  "last_seen FROM nodes ORDER BY announce_count DESC, last_seen DESC LIMIT %s",
                  (limit,))
        return [{"name": r[0], "hash": r[1], "announces": r[2],
                 "last_seen": r[3].isoformat() if r[3] else None} for r in c.fetchall()]


# ---- Beacon-Analytics (mesh RUM) -------------------------------------------
def record_event(conn, node, path, vid):
    with conn, conn.cursor() as c:
        c.execute("INSERT INTO page_events (node, path, vid) VALUES (%s,%s,%s)",
                  (node[:64], path[:200], (vid or None)))


def rum_stats(conn):
    with conn, conn.cursor() as c:
        out = {}
        c.execute("SELECT count(*) FROM page_events"); out["events"] = c.fetchone()[0]
        c.execute("SELECT count(DISTINCT vid) FROM page_events WHERE vid IS NOT NULL")
        out["visitors"] = c.fetchone()[0]
        c.execute("SELECT count(*) FROM page_events WHERE ts > now() - interval '24 hours'")
        out["events_24h"] = c.fetchone()[0]
        return out


def rum_by_node(conn, limit=20):
    with conn, conn.cursor() as c:
        c.execute("SELECT node, count(*) v, count(DISTINCT vid) u FROM page_events "
                  "GROUP BY node ORDER BY v DESC LIMIT %s", (limit,))
        return [{"node": r[0], "views": r[1], "visitors": r[2]} for r in c.fetchall()]


def rum_top_pages(conn, limit=20):
    with conn, conn.cursor() as c:
        c.execute("SELECT node, path, count(*) v, count(DISTINCT vid) u FROM page_events "
                  "GROUP BY node, path ORDER BY v DESC LIMIT %s", (limit,))
        return [{"node": r[0], "path": r[1], "views": r[2], "visitors": r[3]}
                for r in c.fetchall()]


def rum_by_day(conn, days=14):
    with conn, conn.cursor() as c:
        c.execute("SELECT date_trunc('day', ts)::date d, count(*) v, count(DISTINCT vid) u "
                  "FROM page_events WHERE ts > now() - (%s || ' days')::interval "
                  "GROUP BY d ORDER BY d DESC", (days,))
        return [{"day": r[0].isoformat(), "views": r[1], "visitors": r[2]}
                for r in c.fetchall()]


# ---- Search analytics (what people search, and what returns nothing) --------
def record_search(conn, q, results, suggested=False):
    """Log a handled query for analytics. `q` is normalised (lowercased, trimmed,
    collapsed whitespace) so popularity groups cleanly; it is NOT linked to any
    visitor. Overly long queries are truncated."""
    qn = " ".join((q or "").lower().split())[:200]
    if not qn:
        return
    with conn, conn.cursor() as c:
        c.execute("INSERT INTO searches (q, results, suggested) VALUES (%s,%s,%s)",
                  (qn, int(results), bool(suggested)))


def search_stats(conn):
    with conn, conn.cursor() as c:
        out = {}
        c.execute("SELECT count(*) FROM searches"); out["searches"] = c.fetchone()[0]
        c.execute("SELECT count(*) FROM searches WHERE ts > now() - interval '24 hours'")
        out["searches_24h"] = c.fetchone()[0]
        c.execute("SELECT count(*) FROM searches WHERE results = 0"); out["zero_result"] = c.fetchone()[0]
        return out


def top_searches(conn, limit=15):
    with conn, conn.cursor() as c:
        c.execute("SELECT q, count(*) n, max(results) hits FROM searches "
                  "GROUP BY q ORDER BY n DESC, max(ts) DESC LIMIT %s", (limit,))
        return [{"q": r[0], "count": r[1], "hits": r[2]} for r in c.fetchall()]


def zero_result_searches(conn, limit=15):
    """Queries that returned nothing -- the coverage/ranking debug list."""
    with conn, conn.cursor() as c:
        c.execute("SELECT q, count(*) n, max(ts) last FROM searches WHERE results = 0 "
                  "GROUP BY q ORDER BY n DESC, last DESC LIMIT %s", (limit,))
        return [{"q": r[0], "count": r[1]} for r in c.fetchall()]


def record_click(conn, url):
    """Log an opened result; promotes it in ranking over time."""
    with conn, conn.cursor() as c:
        c.execute("INSERT INTO clicks (url, clicks, last_clicked) VALUES (%s, 1, now()) "
                  "ON CONFLICT (url) DO UPDATE SET clicks = clicks.clicks + 1, "
                  "last_clicked = now()", (url,))


def rank_bonus(conn, url):
    """Link-graph + click promotion for a URL that may NOT be a crawled page
    (e.g. a federated wiki article that gets linked/clicked). ln-scaled, additive."""
    import math
    with conn, conn.cursor() as c:
        c.execute("SELECT count(*) FROM links WHERE to_url = %s", (url,))
        inbound = c.fetchone()[0]
        c.execute("SELECT clicks FROM clicks WHERE url = %s", (url,))
        row = c.fetchone()
    clk = row[0] if row else 0
    return round(math.log1p(inbound) * 0.2 + math.log1p(clk) * 0.3, 5)


# ---- Recency + intent ranking levers -----------------------------------
# Per-type freshness: weight (max multiplier bonus at age=0) + half-life days.
# freshness = 1 + w * 0.5**(age_days/halflife) -- decays smoothly to 1.0 (NOT
# below), so old content never gets PUNISHED relative to other equal-relevance
# results, it just stops getting the fresh-content lift. w=0 types are
# perfectly neutral: an old wiki page and an ancient one score identically on
# this axis (no recency effect either way), which is the point -- a 2004
# encyclopedia article isn't "stale", it's just not news.
FRESHNESS = {
    "news":       (1.00, 7),
    "status":     (0.90, 3),
    "article":    (0.55, 45),
    "blog":       (0.55, 45),
    "product":    (0.65, 30),
    "wiki":       (0.0, None),
    "dataset":    (0.0, None),
    "file-index": (0.0, None),
    "index":      (0.0, None),
}
_FRESHNESS_DEFAULT = (0.20, 60)   # profile/service/forum/media/event/unlisted: mild, undocumented types

# Query-intent tokens -> per-type multiplier, for THAT query only (never
# stored on a page). A search for "recent news" should not need the word
# "news" to appear in a page's body to prefer news/article/status content
# over an encyclopedia -- and should actively discount wiki hits, which is
# what was swamping "recent news" results (any ZIM page containing the word
# "news" anywhere in its body text).
_TEMPORAL_TOKENS = {"recent", "latest", "new", "newest", "today", "news", "breaking", "now", "current"}
_TEMPORAL_BOOST = {"news": 1.35, "article": 1.15, "blog": 1.15, "status": 1.20}
_TEMPORAL_DEMOTE = {"wiki": 0.4}

_COMMERCE_TOKENS = {"buy", "shop", "price", "prices", "store", "sale", "purchase", "cheap", "order"}
_COMMERCE_BOOST = {"product": 1.5}


def _intent_multipliers(query):
    toks = set(re.findall(r"[a-z0-9]+", (query or "").lower()))
    boost, demote = {}, {}
    if toks & _TEMPORAL_TOKENS:
        boost.update(_TEMPORAL_BOOST)
        demote.update(_TEMPORAL_DEMOTE)
    if toks & _COMMERCE_TOKENS:
        boost.update(_COMMERCE_BOOST)
    return boost, demote


# MeshData anti-abuse: a declared type is an adversarial surface now that it
# feeds ranking (freshness weight + intent boosts both key off it). If the
# declared type is a "content-ish" claim (news/article/blog/status/product/
# event) but the page's INDEPENDENT structural inference says file-index
# (3+ file-download links -- a strong, hard-to-fake structural signal), the
# declared type is almost certainly wrong or gaming the ranker: fall back to
# the structural type for ranking multipliers only. The DISPLAYED type (what
# the UI shows) is untouched -- MeshData stays inert w.r.t. rendering, only
# the ranking weight is corrected.
_CONTENTY_TYPES = {"news", "article", "blog", "status", "product", "event"}


def _rank_type(declared_type, inferred_type):
    if inferred_type == "file-index" and declared_type in _CONTENTY_TYPES:
        return inferred_type
    return declared_type or inferred_type or "index"


def _freshness_multiplier(rank_type, effective_date, now):
    w, halflife = FRESHNESS.get(rank_type, _FRESHNESS_DEFAULT)
    if w <= 0 or not effective_date or not halflife:
        return 1.0
    age_days = max((now - effective_date).total_seconds() / 86400.0, 0.0)
    return 1.0 + w * (0.5 ** (age_days / halflife))


# ---- Snippets: best passage, highlighted, junk-guarded -------------------
# Box-drawing / block-element glyphs (U+2500-25FF): ASCII-art banners and
# file-index dividers are full of these. A snippet that's mostly this is
# useless as a result preview, so candidate fragments above the ratio are
# skipped in favour of the next one.
_JUNK_RE = re.compile("[─-◿]")
_JUNK_RATIO_MAX = 0.20
# Plain-ASCII markers written directly into the ts_headline() SQL text (see
# search() below) -- no quotes/backslashes/`%`, so no escaping hazards. The
# wire-protocol result converts these to \x01/\x02 sentinels (never raw
# micron control codes -- markup stays a client-side rendering concern).
_HL_START, _HL_STOP, _HL_FRAG = "@@H@@", "@@/H@@", "@@F@@"


def _junk_ratio(s):
    if not s:
        return 0.0
    return len(_JUNK_RE.findall(s)) / max(len(s), 1)


def _pick_snippet(raw_headline, description, title):
    """Split ts_headline's multi-fragment output on the fragment delimiter,
    keep the first fragment that isn't mostly box-drawing/ASCII-art, and
    re-mark its highlighted spans with \\x01-delimited sentinels (protocol-
    level, client-agnostic -- the NomadNet page turns these into micron bold
    toggles; this layer never emits raw micron control codes itself). Falls
    back to the description/title (cleanly, no markers) if every fragment is
    junk-heavy, and flags that so the caller can apply a small score penalty
    -- a page that's mostly ASCII art is genuinely a worse text result."""
    frags = [f for f in (raw_headline or "").split(_HL_FRAG) if f.strip()]
    saw_junk = False
    for frag in frags:
        plain = frag.replace(_HL_START, "").replace(_HL_STOP, "")
        if _junk_ratio(plain) <= _JUNK_RATIO_MAX:
            return frag.strip(), False
        saw_junk = True
    fallback = (description or title or "").strip()
    return fallback, saw_junk


_JUNK_PAGE_PENALTY = 0.85   # every candidate fragment was junk-heavy

# Built as ONE Python string (not relying on Postgres's adjacent-string-
# literal concatenation) so it's unambiguous as a single SQL string literal.
_HEADLINE_OPTS = (f"MaxFragments=3, MaxWords=28, MinWords=10, ShortWord=3, "
                  f"StartSel={_HL_START}, StopSel={_HL_STOP}, FragmentDelimiter={_HL_FRAG}")


# Ranking = field-weighted full-text relevance (ts_rank_cd over a title(A) /
# headings(B) / description(C) / body(D) tsvector) times:
#   * MeshData schema bonus (hard-capped flat +40% for a self-describing page
#     -- a single boolean multiplier, so it can't be inflated by declaring
#     more fields);
#   * node-trust (log-scaled by announce count);
#   * responsiveness (fast page loads lift slightly, slow ones drop);
#   * freshness (per-type weight/half-life over the effective date --
#     md_date else last_changed else first_seen -- neutral 1.0x for types
#     with no recency signal, e.g. wiki);
#   * query intent (temporal/commercial tokens re-weight types for THIS
#     query only);
# plus additive click promotion (ln clicks) + link-graph promotion (ln
# inbound links) -- so a page that gets opened or linked elsewhere ranks up
# over time regardless of source.
def search(conn, query, limit=15, ptype=None):
    if not query or not query.strip():
        return []
    params = [query]
    type_clause = ""
    if ptype:
        type_clause = " AND p.type = %s"
        params.append(ptype)
    params.append(limit)
    # Fragment/highlight markers are plain ASCII tokens (no quotes/backslashes/
    # `%`) so they're safe to write directly into the SQL text -- no need to
    # smuggle them through as bind params or Python repr() (which would hit
    # Postgres's standard_conforming_strings semantics and NOT round-trip
    # control bytes the way Python string literals do).
    sql = f"""
        WITH q AS (SELECT websearch_to_tsquery('english', %s) AS tsq)
        SELECT p.url, p.node_hash, p.path, coalesce(p.title,'(untitled)') AS title,
               coalesce(p.type,'?') AS type, coalesce(p.inferred_type, p.type, '?') AS inferred_type,
               p.description, p.md_declared, p.content_hash, p.canonical,
               coalesce(n.name,'') AS node_name,
               coalesce(n.announce_count,1) AS announces,
               p.fetch_ms,
               coalesce(p.md_date, p.last_changed, p.first_seen, p.fetched_at) AS effective_date,
               ts_rank_cd(p.content_tsv, q.tsq) AS rel,
               ts_headline('english', coalesce(p.content,''), q.tsq,
                   '{_HEADLINE_OPTS}') AS raw_headline,
               coalesce(cl.clicks, 0) AS clicks,
               (SELECT count(*) FROM links l WHERE l.to_url = p.url) AS inbound
        FROM pages p
        CROSS JOIN q
        LEFT JOIN nodes n ON n.dest_hash = p.node_hash
        LEFT JOIN clicks cl ON cl.url = p.url
        WHERE p.ok AND p.content_tsv @@ q.tsq{type_clause}
        ORDER BY rel DESC
        LIMIT %s
    """
    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
        c.execute(sql, params)
        rows = c.fetchall()

    boost, demote = _intent_multipliers(query)
    now = datetime.now(timezone.utc)
    out = []
    for r in rows:
        rtype = _rank_type(r["type"], r["inferred_type"])
        schema_bonus = 1.0 + 0.4 * int(bool(r["md_declared"]))              # hard-capped
        trust = 1.0 + math.log1p(r["announces"] or 1) * 0.1
        fetch_ms = min(r["fetch_ms"] if r["fetch_ms"] is not None else 3000, 15000)
        responsiveness = 1.15 - (fetch_ms / 15000.0) * 0.30
        fresh = _freshness_multiplier(rtype, r["effective_date"], now)
        intent = boost.get(rtype) or demote.get(rtype) or 1.0
        inbound_bonus = math.log1p(r["clicks"] or 0) * 0.3 + math.log1p(r["inbound"] or 0) * 0.2
        base = float(r["rel"]) * schema_bonus * trust * responsiveness * fresh * intent
        score = base + inbound_bonus

        snippet, all_junk = _pick_snippet(r["raw_headline"], r["description"], r["title"])
        if all_junk:
            score *= _JUNK_PAGE_PENALTY
        snippet = snippet.replace(_HL_START, "\x01").replace(_HL_STOP, "\x02")

        ed = r["effective_date"]
        out.append({
            "url": r["url"], "node_hash": r["node_hash"], "path": r["path"],
            "title": r["title"], "type": r["type"],
            "node_name": r["node_name"], "md": bool(r["md_declared"]),
            "description": (r["description"] or "").strip(),
            "snippet": snippet,
            "date": ed.date().isoformat() if ed else None,
            "content_hash": r["content_hash"], "canonical": r["canonical"],
            "score": round(score, 5),
        })
    out.sort(key=lambda x: x["score"], reverse=True)
    return out


# ---- "Did you mean?" spelling suggestions (pg_trgm) -------------------------
_WORD_RE = re.compile(r"[a-z0-9]+")
_SUGGEST_MIN_SIM = 0.42        # trigram similarity floor for a correction
_LEXICON_MIN_DF = 2            # ignore words seen in <2 pages (noise)


def refresh_lexicon(conn):
    """(Re)build the corpus lexicon from indexed page content. Uses ts_stat over
    the FTS vectors so we get the same lexemes search matches on. Cheap enough to
    run periodically (hundreds of pages)."""
    with conn, conn.cursor() as c:
        c.execute(
            """INSERT INTO lexicon (word, df)
               SELECT word, ndoc FROM ts_stat('SELECT content_tsv FROM pages WHERE ok')
               WHERE word ~ '^[a-z][a-z]{2,29}$' AND ndoc >= %s
               ON CONFLICT (word) DO UPDATE SET df = EXCLUDED.df""",
            (_LEXICON_MIN_DF,),
        )


def suggest(conn, query):
    """Return a corrected query string if some token is a likely misspelling of a
    real corpus word ("colarado" -> "colorado"), else None. Each unknown token is
    matched to its nearest trigram neighbour above a similarity floor; known tokens
    (present in the lexicon) are left untouched."""
    if not query or not query.strip():
        return None
    tokens = _WORD_RE.findall(query.lower())
    if not tokens:
        return None
    changed = False
    fixed = list(tokens)
    with conn, conn.cursor() as c:
        for i, tok in enumerate(tokens):
            if len(tok) < 4:                      # too short to correct reliably
                continue
            c.execute("SELECT 1 FROM lexicon WHERE word = %s", (tok,))
            if c.fetchone():                      # already a real corpus word
                continue
            # nearest neighbour by trigram similarity (index-assisted via `%`)
            c.execute(
                """SELECT word, similarity(word, %s) AS s
                   FROM lexicon
                   WHERE word %% %s
                   ORDER BY s DESC, df DESC
                   LIMIT 1""",
                (tok, tok),
            )
            row = c.fetchone()
            if row and row[1] is not None and float(row[1]) >= _SUGGEST_MIN_SIM and row[0] != tok:
                cand = row[0]
                # The lexicon holds STEMMED lexemes, so a valid inflection ("dogs"
                # vs the lexeme "dog") looks unknown. Don't "correct" a word to its
                # own stem/prefix -- only offer genuinely different spellings.
                if tok.startswith(cand) or cand.startswith(tok):
                    continue
                fixed[i] = cand
                changed = True
    if not changed:
        return None
    # rebuild the phrase, preserving original spacing loosely (single spaces)
    return " ".join(fixed)
