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


def init_schema(conn):
    with conn, conn.cursor() as c:
        c.execute(SCHEMA)


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


def record_page(conn, url, node_hash, path, title, content, content_hash, nbytes, ok=True):
    with conn, conn.cursor() as c:
        c.execute(
            """INSERT INTO pages (url, node_hash, path, title, content, content_hash, bytes, ok, fetched_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s, now())
               ON CONFLICT (url) DO UPDATE
                 SET title=EXCLUDED.title, content=EXCLUDED.content,
                     content_hash=EXCLUDED.content_hash, bytes=EXCLUDED.bytes,
                     ok=EXCLUDED.ok, fetched_at=now()""",
            (url, node_hash, path, title, content, content_hash, nbytes, ok),
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
        return out
