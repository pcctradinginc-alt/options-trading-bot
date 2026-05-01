"""
universe.py — kostenloses dynamisches US-Ticker-Universum.

Quelle: Nasdaq Trader Symbol Directory.
- nasdaqlisted.txt
- otherlisted.txt

Fail-safe: Wenn Download/Parse scheitert, nutzt news_analyzer.py die übergebene Fallback-Liste.
"""

from __future__ import annotations

import csv
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
CACHE_FILE = DATA_DIR / "universe_cache.json"
CACHE_TTL_HOURS = 24

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

STATIC_ETFS = {
    "SPY", "QQQ", "IWM", "DIA", "GLD", "SLV", "USO", "TLT", "GDX",
    "XLE", "XLF", "XLK", "XLV", "XLI", "XLU", "XLP", "XLY", "XLB", "XLRE",
}


def _is_cache_fresh(path: Path) -> bool:
    if not path.exists():
        return False
    age = datetime.now(timezone.utc) - datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return age < timedelta(hours=CACHE_TTL_HOURS)


def _download_text(url: str) -> str:
    r = requests.get(url, timeout=10, headers={"User-Agent": "daily-options-report/1.0"})
    r.raise_for_status()
    return r.text


def _parse_pipe_table(text: str, symbol_field: str) -> set[str]:
    result: set[str] = set()
    rows = [line for line in text.splitlines() if line and not line.startswith("File Creation Time")]
    reader = csv.DictReader(rows, delimiter="|")
    for row in reader:
        sym = (row.get(symbol_field) or "").strip().upper()
        if not sym or sym == "File Creation Time":
            continue
        # Ausschluss von Test-Issues und Sonder-Symbolen, die RSS oft falsch triggert.
        if row.get("Test Issue", "N").strip().upper() == "Y":
            continue
        if row.get("ETF", "N").strip().upper() == "Y":
            # Makro-ETFs separat kontrolliert behalten.
            if sym not in STATIC_ETFS:
                continue
        if "$" in sym or "." in sym or "^" in sym or "/" in sym:
            continue
        if 1 <= len(sym) <= 5 and sym.isalpha():
            result.add(sym)
    return result


def refresh_universe() -> set[str]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tickers: set[str] = set()
    nasdaq_text = _download_text(NASDAQ_LISTED_URL)
    other_text = _download_text(OTHER_LISTED_URL)
    tickers |= _parse_pipe_table(nasdaq_text, "Symbol")
    tickers |= _parse_pipe_table(other_text, "ACT Symbol")
    tickers |= STATIC_ETFS
    payload = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "count": len(tickers),
        "tickers": sorted(tickers),
    }
    CACHE_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info("Ticker-Universum aktualisiert: %d Symbole", len(tickers))
    return tickers


def get_known_tickers(fallback: set[str] | None = None) -> set[str]:
    fallback = fallback or set()
    try:
        if _is_cache_fresh(CACHE_FILE):
            payload = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
            cached = set(payload.get("tickers", []))
            if cached:
                return cached | STATIC_ETFS | fallback
        return refresh_universe() | fallback
    except Exception as e:
        logger.warning("Ticker-Universum Fallback aktiv: %s", e)
        return fallback | STATIC_ETFS
