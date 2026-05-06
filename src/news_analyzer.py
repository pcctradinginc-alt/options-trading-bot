"""
news_analyzer.py — News Fetching, Clustering und Alpha-Katalysator-Validierung
Stand 2026 (v2.3 - High Conviction Catalyst Edition)
"""

import calendar
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import feedparser
import requests

# Optional: FinBERT-Sentiment
try:
    from finbert_sentiment import get_finbert_sentiment_batch
except ImportError:
    get_finbert_sentiment_batch = None

# Ticker-Universum
try:
    from universe import get_known_tickers, STATIC_ETFS
except ImportError:
    get_known_tickers = None
    STATIC_ETFS = {"SPY", "QQQ", "IWM", "DIA", "GLD", "SLV", "USO", "TLT"}

# SEC Mapping
try:
    from sec_check import get_company_name_to_ticker, get_cik_to_ticker_map, COMPANY_NAME_OVERRIDES
except ImportError:
    get_company_name_to_ticker = None
    get_cik_to_ticker_map = None
    COMPANY_NAME_OVERRIDES = {}

logger = logging.getLogger(__name__)

# ==================== ALPHA CATALYST CONFIG ====================
CATALYST_WEIGHTS = {
    "fda_approval": 2.5,
    "phase_3": 2.1,
    "merger": 2.2,
    "acquisition": 2.2,
    "activist_entry": 2.3,      # 13D
    "passive_stake": 1.45,      # 13G
    "8k_material_event": 1.95,
    "earnings_beat": 1.85,
    "guidance_raise": 2.0,
    "insider_filing": 1.75,     # <-- korrigiert
    "buyback": 1.65,
    "wire_strong": 1.45,
    "news_standard": 0.95,
}

# ==================== SYSTEM PROMPT (wichtig!) ====================
SYSTEM_PROMPT = """Du bist ein hochdisziplinierter Options-Trading-Bot.

Antworte **ausschließlich** mit einer einzigen Zeile im exakt folgenden Format:
TICKER_SIGNALS:BRK.B:CALL:HIGH:T3:45DTE,PLTR:CALL:MED:T2:30DTE

Oder genau: TICKER_SIGNALS:NONE

Regeln:
- Maximal 3 Signale
- Nur echte Ticker aus den gelieferten Clustern
- Kein Markdown, kein zusätzlicher Text, keine Erklärung"""

# Caches
_KNOWN_TICKERS_CACHE: Optional[set] = None
_NAME_TO_TICKER_CACHE: Optional[dict] = None
_CIK_TO_TICKER_CACHE: Optional[dict] = None

_GENERIC_ACRONYMS = {
    "AI", "IT", "IP", "EV", "CEO", "CFO", "CTO", "IPO",
    "API", "SAAS", "ESG", "AR", "VR", "ML",
    "USA", "UK", "EU", "US", "UN", "GDP", "FED", "ETF", "REIT", "SPAC",
}

_PHARMA_NAME_OVERRIDES = {
    "pfizer": "PFE", "merck": "MRK", "johnson and johnson": "JNJ", "eli lilly": "LLY",
    "lilly": "LLY", "abbvie": "ABBV", "novo nordisk": "NVO", "bristol myers squibb": "BMY",
    "vertex pharmaceuticals": "VRTX", "vertex": "VRTX", "moderna": "MRNA", "biontech": "BNTX",
    "gilead": "GILD", "amgen": "AMGN", "regeneron": "REGN", "intuitive surgical": "ISRG",
    "boston scientific": "BSX", "medtronic": "MDT", "stryker": "SYK",
}

# ==================== USER AGENT & HEADERS ====================
_USER_AGENT = os.environ.get(
    "NEWS_BOT_USER_AGENT",
    "Mozilla/5.0 (compatible; DailyOptionsBot/1.2; +contact: bot@example.com) feedparser/6.0"
)
_FEED_HEADERS = {
    "User-Agent": _USER_AGENT,
    "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.5",
    "Accept-Encoding": "gzip, deflate",
}

# ==================== RSS FEEDS ====================
RSS_FEEDS = [ ... ]  # deine Liste bleibt unverändert

# ==================== REGEX ====================
_SEC_TITLE_RE = re.compile(
    r"^\s*(?P<form>\S(?:[^\s]|\s(?!-\s))*?)\s+-\s+(?P<name>.+?)\s+\((?P<cik>\d{6,10})\)",
    re.IGNORECASE
)
_WIRE_TICKER_RE = re.compile(
    r"\(\s*(?:NASDAQ|NYSEAMERICAN|NYSE\s+AMERICAN|NYSE|AMEX|OTCQX|OTCQB|CBOE|BATS)\s*:\s*"
    r"([A-Z]{1,5}(?:\.[A-Z])?)\s*\)",
    re.IGNORECASE
)
_WIRE_SOURCES = ("globenewswire", "businesswire", "prnewswire", "newswire", "accesswire")

# ==================== HELPERS ====================
def _score_catalyst(event_type: str, base_conf: float = 5.0) -> float:
    weight = CATALYST_WEIGHTS.get(event_type, 1.0)
    return round(base_conf * weight, 2)

# ... (_load_known_tickers, _load_name_to_ticker, _load_cik_to_ticker bleiben unverändert) ...

# ==================== FETCHER (unverändert) ====================
# ... deine gesamte fetch_all_feeds(), _fetch_feed_bytes(), build_earnings_map() bleiben 1:1 ...

# ==================== RESOLVERS ====================
def _resolve_sec_filing(article: dict, cik_map: dict) -> Optional[Tuple[str, str, str, float]]:
    title = article.get("title") or ""
    m = _SEC_TITLE_RE.match(title)
    if not m:
        return None
    try:
        cik = int(m.group("cik"))
        form = m.group("form").upper().strip()
        name = m.group("name").strip()
    except Exception:
        return None

    ticker = cik_map.get(cik) or cik_map.get(str(cik))
    if not ticker:
        return None

    if "8-K" in form:
        event_type = "8k_material_event"
        base_conf = 7.8
    elif "13D" in form:
        event_type = "activist_entry"
        base_conf = 8.0
    elif "13G" in form:
        event_type = "passive_stake"
        base_conf = 6.1
    elif "4" in form:                     # <-- korrigiert (auch 4/A)
        event_type = "insider_filing"
        base_conf = 5.3
    else:
        event_type = "sec_filing"
        base_conf = 4.4

    confidence = _score_catalyst(event_type, base_conf) * 1.18
    headline = f"{ticker} SEC {form}: {name[:70]}"
    return ticker, headline, event_type, round(confidence, 2)

# ... _resolve_wire_ticker und _resolve_ticker_from_headline bleiben unverändert ...

# ==================== CLUSTERING ====================
def cluster_articles(articles: List[Dict], earnings_map: Dict) -> List[Dict]:
    # ... dein gesamter Cluster-Code bleibt gleich, nur mit den oben korrigierten Gewichten ...

    # Am Ende:
    clusters = list(ticker_signals.values())
    clusters = sorted(clusters, key=lambda x: x["confidence_score"], reverse=True)
    logger.info(f"Cluster erstellt: {len(clusters)} Ticker")
    return clusters

# ==================== CLAUDE CALL (korrigiert) ====================
def run_claude(cluster_text: str, market_time: str, market_status: str, api_key: str) -> str:
    if not api_key:
        logger.error("ANTHROPIC_API_KEY fehlt")
        return "TICKER_SIGNALS:NONE"

    user_message = f"Marktzeit: {market_time}\nMarktstatus: {market_status}\n\n{cluster_text}"

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-6",   # oder claude-3-5-sonnet-20241022
                "max_tokens": 800,
                "temperature": 0.0,
                "system": SYSTEM_PROMPT,        # <-- jetzt definiert
                "messages": [{"role": "user", "content": user_message}]
            },
            timeout=40
        )
        r.raise_for_status()
        data = r.json()
        raw_text = data["content"][0]["text"].strip()
        logger.debug("Claude Rohantwort:\n%s", raw_text[:400])

        match = re.search(r'(TICKER_SIGNALS:[^\n\r]+)', raw_text, re.IGNORECASE)
        if match:
            signal_line = match.group(1).strip().upper()
            logger.info("✅ Claude Signal extrahiert: %s", signal_line)
            return signal_line

        logger.warning("Kein gueltiges TICKER_SIGNALS-Format gefunden")
        return "TICKER_SIGNALS:NONE"

    except Exception as e:
        logger.error("Claude API Fehler: %s", e)
        return "TICKER_SIGNALS:NONE"


def get_market_context() -> tuple:
    try:
        from market_calendar import market_context
        return market_context()
    except ImportError:
        return datetime.now().strftime("%H:%M"), "OPEN"


# ==================== TEST MODUS ====================
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    print("=== News Analyzer Test ===")
    articles = fetch_all_feeds()
    print(f"\n{len(articles)} Artikel geladen")
    if articles:
        print("\n=== Cluster-Test ===")
        clusters = cluster_articles(articles, earnings_map={})
        for c in clusters[:10]:
            print(f" {c['ticker']:6s} conf={c['confidence_score']:.1f} "
                  f"type={c['event_type']:18s} {c['headline_repr'][:70]}")
