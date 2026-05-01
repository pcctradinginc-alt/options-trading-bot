"""
news_utils.py — Dedupe, URL-Kanonisierung und Quellengewichtung.
"""

from __future__ import annotations

import hashlib
import re
from urllib.parse import parse_qs, quote, unquote, urlparse, urlunparse

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid", "igshid", "ref", "cid",
}


def canonicalize_url(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    # Google-News RSS Links enthalten teils echte URL als Parameter.
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    for key in ("url", "u"):
        if key in qs and qs[key]:
            candidate = unquote(qs[key][0])
            if candidate.startswith("http"):
                url = candidate
                parsed = urlparse(url)
                qs = parse_qs(parsed.query)
                break
    clean_qs = []
    for k, vals in qs.items():
        if k.lower() in TRACKING_PARAMS:
            continue
        for v in vals:
            clean_qs.append((k, v))
    query = "&".join(f"{quote(k)}={quote(v)}" for k, v in clean_qs)
    path = parsed.path.rstrip("/")
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), path, "", query, ""))


def normalize_title(title: str) -> str:
    t = (title or "").lower()
    t = re.sub(r"[^a-z0-9 ]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def article_fingerprint(title: str, link: str = "", summary: str = "") -> str:
    canonical = canonicalize_url(link)
    if canonical:
        base = canonical
    else:
        base = normalize_title(title)[:120] + "|" + normalize_title(summary)[:80]
    return hashlib.sha256(base.encode("utf-8", errors="ignore")).hexdigest()[:16]


def near_duplicate_key(title: str) -> str:
    words = normalize_title(title).split()
    # Entferne häufige Füllwörter, damit gleiche Meldungen über mehrere RSS-Feeds matchen.
    stop = {"the", "a", "an", "to", "of", "and", "or", "for", "on", "in", "as", "with", "after", "before"}
    core = [w for w in words if w not in stop]
    return " ".join(core[:12])
