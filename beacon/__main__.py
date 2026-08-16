"""Beacon entrypoint: start Reticulum, the announce listener, the crawl loop, and
the /healthz+/stats HTTP surface."""
import argparse
import os
import time

import RNS

from . import crawler, db, web


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=os.environ.get("BEACON_RNS_CONFIG"),
                    help="RNS config dir")
    ap.add_argument("--http-port", type=int,
                    default=int(os.environ.get("BEACON_HTTP_PORT", "8214")))
    args = ap.parse_args()

    RNS.Reticulum(args.config)

    # Each thread gets its own connection (psycopg2 connections are not shareable).
    def conn_factory():
        c = db.connect()
        c.autocommit = False
        return c

    init_conn = conn_factory()
    db.init_schema(init_conn)
    init_conn.close()
    RNS.log("[beacon] schema ready")

    handler = crawler._NodeAnnounceHandler(conn_factory)
    RNS.Transport.register_announce_handler(handler)
    RNS.log("[beacon] listening for nomadnetwork.node announces")

    web.start(conn_factory, args.http_port)
    RNS.log(f"[beacon] http on :{args.http_port}")

    print("beacon: crawler + registry running", flush=True)
    crawler.crawl_loop(conn_factory)   # blocks


if __name__ == "__main__":
    main()
