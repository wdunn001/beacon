"""Tolerant date parsing for MeshData declared dates (date/published/updated).

MeshData fields are free-text (the upstream parser is a tolerant line scan --
"one typo drops one line", never the whole block) so a declared date can be
almost anything an author typed. We only trust it enough to use as a ranking
signal, never as ground truth: a bad/junk value -> None, silently, never an
exception.

Anti-abuse (MeshData now feeds ranking, which makes it an adversarial
surface): a declared date is rejected outright if it's before MeshData/Beacon
could plausibly exist (< 2015) or in the future (beyond same-day clock skew) --
a page can't game the freshness boost by claiming 2099, or "win" an old-content
argument by claiming 1970.
"""
import re
from datetime import datetime, timedelta, timezone

_MIN_YEAR = 2015
_FUTURE_SLACK = timedelta(days=1)   # tolerate same-day / timezone clock skew

_ISO_DATETIME = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?")
_ISO_DATE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})\b")


def parse_declared_date(s):
    """'2026-08-16' / '2026-08-16T10:30:00Z' -> aware UTC datetime. Junk,
    empty, malformed, pre-2015, or future -> None (never raises)."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    m = _ISO_DATETIME.match(s)
    try:
        if m:
            y, mo, d, hh, mi = (int(m.group(i)) for i in range(1, 6))
            ss = int(m.group(6) or 0)
            dt = datetime(y, mo, d, hh, mi, ss, tzinfo=timezone.utc)
        else:
            m = _ISO_DATE.match(s)
            if not m:
                return None
            y, mo, d = (int(m.group(i)) for i in range(1, 4))
            dt = datetime(y, mo, d, tzinfo=timezone.utc)
    except ValueError:          # month=13, day=32, etc. -- a real date rejects it
        return None
    if dt.year < _MIN_YEAR:
        return None
    if dt > datetime.now(timezone.utc) + _FUTURE_SLACK:
        return None
    return dt


def effective_declared_date(md):
    """Pick the freshest-oriented declared field from a MeshData dict:
    `updated` (last edit) beats `published` (first publish) beats the generic
    `date`. Returns an aware UTC datetime or None."""
    if not md:
        return None
    for key in ("updated", "published", "date"):
        dt = parse_declared_date(md.get(key))
        if dt:
            return dt
    return None
