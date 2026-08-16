"""Minimal micron parser for Beacon: extract indexable plain text, a title, and
outgoing links from a NomadNet micron (.mu) page.

Micron's control character is the backtick. We strip color codes (`Fxxx/`Bxxx),
format toggles (`!, `_, `c, `a, `f, `b, `=, ...), section markers (>, -, #), and
turn links `[label`target] into their label text, leaving human-readable content.
"""
import re

# `[label`target]  -> label + target (target after the 2nd backtick, up to ])
_LINK_RE = re.compile(r"`\[([^`\]]*)`([^\]]*)\]")
_COLOR_RE = re.compile(r"`[FBfb][0-9a-fA-F]{3}")
_CTRL_RE = re.compile(r"`[a-zA-Z!_=\*<>]")
# a mesh target: optional "<hexhash>" then ":/page/..." ; same-node targets start ":"
_HEX = re.compile(r"^[0-9a-f]{16,64}$", re.I)


def extract_links(text, node_hash):
    """Return [(to_url, to_node_hash, to_path)] for in-mesh page links only."""
    out = []
    seen = set()
    for m in _LINK_RE.finditer(text or ""):
        # group(2) is "dest`field1=v1|field2=v2" -- split dest from link fields.
        raw = m.group(2)
        parts = raw.split("`")
        target = parts[0].strip()
        fields = parts[1] if len(parts) > 1 else ""
        if not target or target.startswith(("http://", "https://", "lxmf@", "mailto:")):
            continue
        nh, path = node_hash, None
        if target.startswith(":"):
            path = target[1:]                       # same node, ":/page/x.mu"
        elif target.startswith("/page/"):
            path = target                           # same node, bare "/page/x.mu"
        elif ":" in target:
            h, _, p = target.partition(":")
            if _HEX.match(h):
                nh, path = h.lower(), p
        if not path or not path.startswith("/page/"):
            continue
        # a dynamic reader link (read.mu?ref=…) is one distinct target per ref;
        # fold ref into the url so link-graph counting matches federated results.
        ref = None
        for kv in fields.split("|"):
            if kv.startswith("ref="):
                ref = kv[4:].strip()
        to_url = f"{nh}:{path}" + (f"#{ref}" if ref else "")
        if to_url in seen:
            continue
        seen.add(to_url)
        out.append((to_url, nh, path))
    return out


def to_text(raw):
    """micron -> readable plain text (for indexing)."""
    t = raw or ""
    t = _LINK_RE.sub(lambda m: (m.group(1) + " "), t)   # keep link label
    t = _COLOR_RE.sub("", t)
    t = _CTRL_RE.sub("", t)
    lines = []
    for line in t.splitlines():
        s = line
        if s[:1] in (">", "-", "#"):                    # section markers / dividers / comments
            s = s.lstrip(">-#").strip()
        s = s.replace("\\`", "`").replace("`", "")      # unescape + drop stray backticks
        lines.append(s)
    t = "\n".join(lines)
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


_HEAD_LINE = re.compile(r"^\s*>+\s*(.+)$")


def headings_of(raw):
    """All micron heading-line text ('>' lines), newline-joined -- the tsvector
    weight-B field (between title and body): a heading match should outrank a
    body match but not a title match."""
    out = []
    for line in (raw or "").splitlines():
        m = _HEAD_LINE.match(line)
        if not m:
            continue
        s = _COLOR_RE.sub("", m.group(1))
        s = _LINK_RE.sub(lambda mm: mm.group(1), s)
        s = _CTRL_RE.sub("", s).replace("`", "").strip()
        if s:
            out.append(s)
    return "\n".join(out)


def title_of(raw):
    """First heading line (starts with '>'), else first non-empty text line."""
    for line in (raw or "").splitlines():
        if line.lstrip().startswith(">"):
            cand = _COLOR_RE.sub("", line).lstrip(">").strip()
            cand = _LINK_RE.sub(lambda m: m.group(1), cand)
            cand = _CTRL_RE.sub("", cand).replace("`", "").strip()
            if cand:
                return cand[:200]
    for line in to_text(raw).splitlines():
        if line.strip():
            return line.strip()[:200]
    return None
