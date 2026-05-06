"""
news_analyzer.py — News Fetching, Clustering und Alpha-Katalysator-Validierung
Stand 2026 (v2.3 - High Conviction Catalyst Edition)
"""

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional

import feedparser
import requests

# Optional: FinBERT
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
    "insider_filing": 1.75,
    "buyback": 1.65,
    "wire_strong": 1.45,
    "news_standard": 0.95,
}

# ==================== SYSTEM PROMPT ====================
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
RSS_FEEDS = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.reuters.com/reuters/technologyNews",
    "https://www.benzinga.com/feed",
    "https://rss.cnbc.com/id/100003114",
    "https://rss.cnbc.com/id/100727362",
    "https://feeds.bloomberg.com/markets/news.rss",
    "https://feeds.bloomberg.com/technology/news.rss",
    "https://finance.yahoo.com/rss/headline",
    "https://www.marketwatch.com/rss/topstories",
    "https://www.wsj.com/xml/rss/3_7085.xml",
    "https://www.ft.com/rss/companies",
    "https://www.sec.gov/news/pressreleases.rss",
]

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


def _load_known_tickers() -> set:
    global _KNOWN_TICKERS_CACHE
    if _KNOWN_TICKERS_CACHE is None:
        _KNOWN_TICKERS_CACHE = get_known_tickers() if get_known_tickers else set()
    return _KNOWN_TICKERS_CACHE


def _load_name_to_ticker() -> dict:
    global _NAME_TO_TICKER_CACHE
    if _NAME_TO_TICKER_CACHE is None:
        _NAME_TO_TICKER_CACHE = get_company_name_to_ticker() if get_company_name_to_ticker else {}
    return _NAME_TO_TICKER_CACHE


def _load_cik_to_ticker() -> dict:
    global _CIK_TO_TICKER_CACHE
    if _CIK_TO_TICKER_CACHE is None:
        _CIK_TO_TICKER_CACHE = get_cik_to_ticker_map() if get_cik_to_ticker_map else {}
    return _CIK_TO_TICKER_CACHE


# ==================== FETCHER ====================
def _fetch_feed_bytes(url: str, timeout: int = 12) -> bytes | None:
    try:
        r = requests.get(url, headers=_FEED_HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.content
    except Exception as e:
        logger.debug("Feed %s failed: %s", url, e)
        return None


def fetch_all_feeds() -> list[dict]:
    """Parallel fetch aller RSS-Feeds."""
    articles: list[dict] = []
    seen = set()

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_feed_bytes, url): url for url in RSS_FEEDS}
        for fut in as_completed(futures):
            url = futures[fut]
            raw = fut.result()
            if not raw:
                continue
            try:
                feed = feedparser.parse(raw)
                for entry in feed.entries[:30]:
                    title = (entry.get("title") or "").strip()
                    if not title or len(title) < 10:
                        continue
                    link = entry.get("link") or ""
                    summary = (entry.get("summary") or entry.get("description") or "")[:500]

                    key = title.lower()[:100]
                    if key in seen:
                        continue
                    seen.add(key)

                    articles.append({
                        "title": title,
                        "link": link,
                        "summary": summary,
                        "source": url.split("//")[-1].split("/")[0],
                    })
            except Exception as e:
                logger.debug("Parse error %s: %s", url, e)

    logger.info("Fetched %d articles from %d feeds", len(articles), len(RSS_FEEDS))
    return articles


def build_earnings_map(finnhub_key: str) -> dict:
    """Stub – später erweiterbar"""
    return {}


# ==================== RESOLVERS ====================
def _resolve_sec_filing(article: dict, cik_map: dict) -> Optional[tuple]:
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
    elif "4" in form:
        event_type = "insider_filing"
        base_conf = 5.3
    else:
        event_type = "sec_filing"
        base_conf = 4.4

    confidence = _score_catalyst(event_type, base_conf) * 1.18
    headline = f"{ticker} SEC {form}: {name[:70]}"
    return ticker, headline, event_type, round(confidence, 2)


def _resolve_wire_ticker(article: dict) -> Optional[str]:
    source = article.get("source", "").lower()
    if not any(ws in source for ws in _WIRE_SOURCES):
        return None
    match = _WIRE_TICKER_RE.search(article.get("title", "") + " " + article.get("summary", ""))
    return match.group(1) if match else None


def _resolve_ticker_from_headline(title: str, summary: str = "") -> Optional[str]:
    text = (title + " " + summary).upper()
    known = _load_known_tickers()

    for t in known:
        if t in text and len(t) >= 2:
            return t

    name_map = _load_name_to_ticker()
    for name, ticker in name_map.items():
        if name.upper() in text:
            return ticker

    return None


# ==================== CLUSTERING ====================
def cluster_articles(articles: List[Dict], earnings_map: Dict) -> List[Dict]:
    ticker_signals: Dict[str, Dict] = {}
    cik_map = _load_cik_to_ticker()

    for art in articles:
        ticker = None
        conf = 5.0
        event_type = "news_standard"

        # SEC Filing
        sec_res = _resolve_sec_filing(art, cik_map)
        if sec_res:
            ticker, headline, event_type, conf = sec_res
        else:
            # Wire Ticker
            ticker = _resolve_wire_ticker(art)
            if ticker:
                event_type = "wire_strong"
                conf = 7.5

        if not ticker:
            ticker = _resolve_ticker_from_headline(art["title"], art.get("summary", ""))

        if not ticker:
            continue

        # Catalyst Boost
        lower_title = art["title"].lower()
        if any(x in lower_title for x in ["fda", "approval", "phase 3"]):
            event_type = "fda_approval"
            conf = 8.5
        elif any(x in lower_title for x in ["merger", "acquisition", "buyout"]):
            event_type = "merger"
            conf = 8.2

        if ticker not in ticker_signals or conf > ticker_signals[ticker]["confidence_score"]:
            ticker_signals[ticker] = {
                "ticker": ticker,
                "confidence_score": round(conf, 2),
                "event_type": event_type,
                "headline_repr": art["title"][:120],
                "sentiment_score": 0.0,
                "sentiment_source": "keyword",
            }

    clusters = sorted(ticker_signals.values(), key=lambda x: x["confidence_score"], reverse=True)
    logger.info("Cluster erstellt: %d Ticker", len(clusters))
    return clusters


# ==================== CLAUDE ====================
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
                "model": "claude-3-5-sonnet-20241022",
                "max_tokens": 800,
                "temperature": 0.0,
                "system": SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": user_message}]
            },
            timeout=40
        )
        r.raise_for_status()
        data = r.json()
        raw_text = data["content"][0]["text"].strip()

        match = re.search(r'(TICKER_SIGNALS:[^\n\r]+)', raw_text, re.IGNORECASE)
        if match:
            signal_line = match.group(1).strip().upper()
            logger.info("✅ Claude Signal: %s", signal_line)
            return signal_line

        logger.warning("Kein gültiges Signal-Format gefunden")
        return "TICKER_SIGNALS:NONE"

    except Exception as e:
        logger.error("Claude API Fehler: %s", e)
        return "TICKER_SIGNALS:NONE"


def get_market_context() -> tuple[str, str]:
    try:
        from market_calendar import market_context
        return market_context()
    except ImportError:
        return datetime.now().strftime("%H:%M ET"), "OPEN"


# ==================== TEST ====================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    print("=== News Analyzer Test ===")
    articles = fetch_all_feeds()
    print(f"{len(articles)} Artikel geladen")
    clusters = cluster_articles(articles, {})
    for c in clusters[:10]:
        print(f" → {c['ticker']:6} | {c['confidence_score']:.1f} | {c['event_type']:18} | {c['headline_repr'][:70]}")
