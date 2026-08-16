"""MeshAPI 0.1 manifest for Beacon search. `dest` filled at runtime (searchd.start)."""
MANIFEST = {
    "meshapi": "0.1",
    "service": {
        "name": "beacon",
        "summary": "Search the Reticulum mesh",
        "description": ("Full-text search across NomadNet pages Beacon has crawled from "
                        "the mesh's announce stream. Ranking blends field-weighted text "
                        "relevance (title/headings/description/body) with a MeshData schema "
                        "bonus, node trust, per-type recency (a news query favours fresh "
                        "news/status pages and discounts wiki; an old encyclopedia entry is "
                        "never penalised, it's just recency-neutral), and query intent "
                        "(temporal/commercial tokens re-weight result types). Results are "
                        "capped at 3 per source node per page and link straight to the page "
                        "on its node."),
        "app": "rnsbeacon",
        "aspect": "query",
        "path": "q",
        "dest": "",
        "encoding": "umsgpack",
        "source": "https://github.com/wdunn001/beacon",
    },
    "ops": [
        {"op": "search", "summary": "Full-text search across crawled mesh pages",
         "auth": "none",
         "request": {
             "q": {"type": "str!", "desc": "free-text query (supports quoted phrases, OR, -term)"},
             "limit": {"type": "int<=20", "desc": "page size (results per page, default 10)"},
             "offset": {"type": "int?", "desc": "results to skip for pagination (default 0)"},
             "type": {"type": "str?", "desc": "filter by MeshData page type, e.g. article, news, index"}},
         "response": "{res:[{url,node_hash,path,title,type,node_name,description,snippet,md,"
                     "score,date:str?,also_on:int?,more_from_node:int?}], "
                     "total:int, offset:int, limit:int, suggestion:str?}"},
    ],
}
