"""Beacon Postgres layer: registry (nodes), page store, link graph, crawl queue.

Connection comes from env (BEACON_DB_*). FTS is built in: pages carry a generated
tsvector so search + ts_rank work from day one, ahead of the richer ranking stage.
"""
import os
import re
import time

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

-- "Did you mean?": a corpus lexicon (lexemes from page content) with a trigram
-- index, so a misspelt query ("colarado") can be matched to the nearest real
-- term ("colorado") via pg_trgm similarity. Refreshed periodically from ts_stat.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE TABLE IF NOT EXISTS lexicon (
    word TEXT PRIMARY KEY,
    df   INTEGER NOT NULL DEFAULT 1      -- document frequency (rarer = weaker suggestion)
);
CREATE INDEX IF NOT EXISTS lexicon_trgm_idx ON lexicon USING GIN (word gin_trgm_ops);
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
                md_declared=False, fetch_ms=None):
    with conn, conn.cursor() as c:
        c.execute(
            """INSERT INTO pages (url, node_hash, path, title, content, content_hash,
                                  bytes, ok, type, description, lang, tags, md_declared,
                                  fetch_ms, fetched_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
               ON CONFLICT (url) DO UPDATE
                 SET title=EXCLUDED.title, content=EXCLUDED.content,
                     content_hash=EXCLUDED.content_hash, bytes=EXCLUDED.bytes,
                     ok=EXCLUDED.ok, type=EXCLUDED.type, description=EXCLUDED.description,
                     lang=EXCLUDED.lang, tags=EXCLUDED.tags, md_declared=EXCLUDED.md_declared,
                     fetch_ms=EXCLUDED.fetch_ms, fetched_at=now()""",
            (url, node_hash, path, title, content, content_hash, nbytes, ok,
             ptype, description, lang, tags, md_declared, fetch_ms),
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


# Ranking = full-text relevance (ts_rank_cd) with the boosts the user asked for:
#   * MeshData schema bonus (+40% for a self-describing page);
#   * node-trust (log-scaled by announce count);
#   * click promotion (ln clicks) + link-graph promotion (ln inbound links) --
#     so a page that gets opened or linked elsewhere ranks up over time. This is
#     what folds federated wiki hits into the same ranked data as it's referenced.
def search(conn, query, limit=15, ptype=None):
    if not query or not query.strip():
        return []
    params = [query]
    type_clause = ""
    if ptype:
        type_clause = " AND p.type = %s"
        params.append(ptype)
    params.append(limit)
    sql = f"""
        WITH q AS (SELECT websearch_to_tsquery('english', %s) AS tsq)
        SELECT p.url, p.node_hash, p.path, coalesce(p.title,'(untitled)') AS title,
               coalesce(p.type,'?') AS type, p.description, p.md_declared,
               coalesce(n.name,'') AS node_name,
               coalesce(n.announce_count,1) AS announces,
               ts_rank_cd(p.content_tsv, q.tsq) AS rel,
               ts_headline('english', coalesce(p.content,''), q.tsq,
                   'MaxWords=30, MinWords=12, ShortWord=3, MaxFragments=1, StartSel=[, StopSel=]') AS snippet,
               (ts_rank_cd(p.content_tsv, q.tsq)
                 * (1.0 + 0.4*(p.md_declared)::int)
                 * (1.0 + ln(1+coalesce(n.announce_count,1))*0.1)
                 -- responsiveness: fast load lifts, a 15s load drops (0.85..1.15x)
                 * (1.15 - least(coalesce(p.fetch_ms, 3000), 15000) / 15000.0 * 0.30)
                 + ln(1 + coalesce(cl.clicks, 0)) * 0.3
                 + ln(1 + (SELECT count(*) FROM links l WHERE l.to_url = p.url)) * 0.2
               ) AS score
        FROM pages p
        CROSS JOIN q
        LEFT JOIN nodes n ON n.dest_hash = p.node_hash
        LEFT JOIN clicks cl ON cl.url = p.url
        WHERE p.ok AND p.content_tsv @@ q.tsq{type_clause}
        ORDER BY score DESC
        LIMIT %s
    """
    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
        c.execute(sql, params)
        rows = c.fetchall()
    out = []
    for r in rows:
        out.append({
            "url": r["url"], "node_hash": r["node_hash"], "path": r["path"],
            "title": r["title"], "type": r["type"],
            "node_name": r["node_name"], "md": bool(r["md_declared"]),
            "description": (r["description"] or "").strip(),
            "snippet": (r["snippet"] or "").strip(),
            "score": round(float(r["score"]), 5),
        })
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
