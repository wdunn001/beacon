# Beacon

**A FOSS search engine for the Reticulum mesh.** Beacon discovers NomadNet nodes
from RNS announces, crawls their micron (`.mu`) pages over encrypted Reticulum
Links, and builds a searchable index. Open source, privacy-conscious, and
MeshAPI-native.

Prior art: [Roogle](https://roogle.us/) proved a mesh search engine is viable but
is closed-source with thin link-only ranking. Beacon is open, and ranks on text
relevance + node trust, not just a sparse link graph.

## Status
**Stage 1: crawler + registry** (this repo). Discovers nodes, crawls pages into a
Postgres registry + page store with full-text search ready (a generated
`tsvector`). Search API/UI, richer ranking, categorization, and analytics build on
top.

## Architecture
- **Announce listener** — an RNS `AnnounceHandler` on `nomadnetwork.node`; every
  node's hash + name + first/last-seen lands in the `nodes` registry, and its
  `index.mu` is queued.
- **Crawler** — opens a Link to each node, requests page paths, extracts readable
  text + a title + outgoing micron links, stores them, and follows links (same
  node + cross-mesh). Polite: one fetch at a time, a delay between fetches (LoRa
  is slow/intermittent), indexes only text pages (skips binaries), recrawls daily.
- **Store** — Postgres: `nodes`, `pages` (+ FTS `tsvector`), `links`, `crawl_queue`.
- **/healthz + /stats** — JSON, for monitoring and a future dashboard.

## Run
```bash
pip install rns umsgpack psycopg2-binary
# set BEACON_DB_* + point config/rns-config at a reachable transport node, then:
python3 -m beacon --config ./config
```
Docker: see `docker-compose.example.yml` + `config/rns-config.example`. Needs a
Postgres (BEACON_DB_* in `.env`) and an RNS instance that hears the announce stream
(a public backbone works).

## Ranking (planned)
BM25/TF-IDF text relevance (primary) + an announce-trust multiplier
(recency/frequency/uptime) + a light link signal (SALSA over the sparse micron
link graph). Details as the index/rank stage lands.

## License
MIT.
