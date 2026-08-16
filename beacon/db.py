"""Beacon Postgres layer: registry (nodes), page store, link graph, crawl queue.

Connection comes from env (BEACON_DB_*). FTS is built in: pages carry a generated
tsvector so search + ts_rank work from day one, ahead of the richer ranking stage.
"""
import os
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
CREATE INDEX IF NOT EXISTS pages_type_idx ON pages (type);
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


def enqueue(conn, node_hash, path, priority=5):
    url = f"{node_hash}:{path}"
    with conn, conn.cursor() as c:
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
                md_declared=False):
    with conn, conn.cursor() as c:
        c.execute(
            """INSERT INTO pages (url, node_hash, path, title, content, content_hash,
                                  bytes, ok, type, description, lang, tags, md_declared, fetched_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
               ON CONFLICT (url) DO UPDATE
                 SET title=EXCLUDED.title, content=EXCLUDED.content,
                     content_hash=EXCLUDED.content_hash, bytes=EXCLUDED.bytes,
                     ok=EXCLUDED.ok, type=EXCLUDED.type, description=EXCLUDED.description,
                     lang=EXCLUDED.lang, tags=EXCLUDED.tags, md_declared=EXCLUDED.md_declared,
                     fetched_at=now()""",
            (url, node_hash, path, title, content, content_hash, nbytes, ok,
             ptype, description, lang, tags, md_declared),
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


# Ranking = full-text relevance (ts_rank_cd) with two boosts the user asked for:
#   * MeshData schema bonus -- a page that declares its type/description via
#     MeshData is better-described and rewarded (+40%);
#   * node-trust -- pages on nodes that announce more (more established) get a
#     gentle log-scaled lift.
# Uniqueness/value scoring is a later stage; this is the honest first cut.
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
                 * (1.0 + ln(1+coalesce(n.announce_count,1))*0.1)) AS score
        FROM pages p
        CROSS JOIN q
        LEFT JOIN nodes n ON n.dest_hash = p.node_hash
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
