"""MeshAPI 0.1 manifest for Beacon search. `dest` filled at runtime (searchd.start)."""
MANIFEST = {
    "meshapi": "0.1",
    "service": {
        "name": "beacon",
        "summary": "Search the Reticulum mesh",
        "description": ("Full-text search across NomadNet pages Beacon has crawled from "
                        "the mesh's announce stream. Ranking blends text relevance with a "
                        "MeshData schema bonus (well-described pages rank higher) and "
                        "node trust. Results link straight to the page on its node."),
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
             "limit": {"type": "int<=30", "desc": "max results (default 15)"},
             "type": {"type": "str?", "desc": "filter by MeshData page type, e.g. article, news, index"}},
         "response": "[{url,node_hash,path,title,type,node_name,description,snippet,md,score}]"},
    ],
}
