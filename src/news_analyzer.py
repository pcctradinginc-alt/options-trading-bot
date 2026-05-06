"""
news_analyzer.py — News Fetching, Clustering und Alpha-Katalysator-Validierung
Stand 2026 (v2.2 - High Conviction Catalyst Edition)
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
    "insider_buy": 1.75,
    "buyback": 1.65,
    "wire_strong": 1.45,
    "news_standard": 0.95,
}

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
RSS_FEEDS = [
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "https://www.cnbc.com/id/10001147/device/rss/rss.html",
    "https://www.cnbc.com/id/15839135/device/rss/rss.html",
    "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain",
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&output=atom",
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=SC+13D&output=atom",
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=SC+13G&output=atom",
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&output=atom",
    "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/press-releases/rss.xml",
    "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/drugs/rss.xml",
    "https://www.eia.gov/rss/todayinenergy.xml",
    "https://www.bls.gov/feed/empsit.rss",
    "https://www.bls.gov/feed/cpi.rss",
    "https://www.federalreserve.gov/feeds/press_all.xml",
    "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/Public%20Companies",
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
    if _KNOWN_TICKERS_CACHE is not None:
        return _KNOWN_TICKERS_CACHE
    if get_known_tickers:
        try:
            _KNOWN_TICKERS_CACHE = get_known_tickers(fallback=STATIC_ETFS)
            logger.info("Ticker-Universum geladen: %d Symbole", len(_KNOWN_TICKERS_CACHE))
            return _KNOWN_TICKERS_CACHE
        except Exception as e:
            logger.warning("Ticker-Universum Fallback: %s", e)
    _KNOWN_TICKERS_CACHE = set(STATIC_ETFS)
    return _KNOWN_TICKERS_CACHE


def _load_name_to_ticker() -> dict:
    global _NAME_TO_TICKER_CACHE
    if _NAME_TO_TICKER_CACHE is not None:
        return _NAME_TO_TICKER_CACHE
    base = {}
    if get_company_name_to_ticker:
        try:
            base = dict(get_company_name_to_ticker())
        except Exception as e:
            logger.warning("Name->Ticker Mapping nicht verfügbar: %s", e)
    base.update(_PHARMA_NAME_OVERRIDES)
    _NAME_TO_TICKER_CACHE = base
    return _NAME_TO_TICKER_CACHE


def _load_cik_to_ticker() -> dict:
    global _CIK_TO_TICKER_CACHE
    if _CIK_TO_TICKER_CACHE is not None:
        return _CIK_TO_TICKER_CACHE
    if get_cik_to_ticker_map:
        try:
            _CIK_TO_TICKER_CACHE = get_cik_to_ticker_map()
            return _CIK_TO_TICKER_CACHE
        except Exception as e:
            logger.warning("CIK->Ticker Mapping nicht verfügbar: %s", e)
    _CIK_TO_TICKER_CACHE = {}
    return _CIK_TO_TICKER_CACHE


# ==================== FETCHER (Original) ====================
def _fetch_feed_bytes(url: str, timeout: int = 15, retries: int = 2) -> Tuple[Optional[bytes], str]:
    last_err = "unknown"
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=_FEED_HEADERS, timeout=timeout, allow_redirects=True)
            if r.status_code != 200:
                last_err = f"HTTP {r.status_code}"
                if r.status_code in (403, 404, 410):
                    return None, last_err
                time.sleep(0.5 * (attempt + 1))
                continue
            ctype = r.headers.get("Content-Type", "").lower()
            if "html" in ctype and "xml" not in ctype:
                return None, f"HTML statt RSS (CT={ctype.split(';')[0]})"
            if not r.content:
                last_err = "leere Antwort"
                time.sleep(0.5 * (attempt + 1))
                continue
            return r.content, "ok"
        except requests.exceptions.SSLError:
            last_err = "SSL-Error"
            time.sleep(1.0 * (attempt + 1))
        except requests.exceptions.Timeout:
            last_err = "Timeout"
            time.sleep(0.5 * (attempt + 1))
        except requests.exceptions.RequestException as e:
            last_err = type(e).__name__
            time.sleep(0.5 * (attempt + 1))
    return None, last_err


def fetch_all_feeds(max_age_minutes: int = 720) -> List[Dict]:
    cutoff_ts = datetime.now(timezone.utc).timestamp() - max_age_minutes * 60
    articles: List[Dict] = []
    feed_stats: List[Tuple[str, int, str, str]] = []

    for url in RSS_FEEDS:
        url_short = url.split("/")[2].replace("www.", "").replace("feeds.", "")
        before = len(articles)
        raw, fetch_status = _fetch_feed_bytes(url)
        if raw is None:
            feed_stats.append((url_short, 0, fetch_status, "warning"))
            continue
        try:
            feed = feedparser.parse(raw)
        except Exception as e:
            feed_stats.append((url_short, 0, f"PARSE_EXCEPTION: {type(e).__name__}", "warning"))
            continue

        if getattr(feed, "bozo", 0) and not feed.entries:
            exc = getattr(feed, "bozo_exception", "unknown")
            feed_stats.append((url_short, 0, f"PARSE_ERROR: {str(exc)[:60]}", "warning"))
            continue

        entries = feed.entries[:12] if feed.entries else []
        kept = 0
        stale = 0
        for entry in entries:
            pub_struct = entry.get("published_parsed") or entry.get("updated_parsed")
            if pub_struct:
                try:
                    pub_ts = calendar.timegm(pub_struct)
                    if pub_ts < cutoff_ts:
                        stale += 1
                        continue
                except (TypeError, ValueError, OverflowError):
                    pub_ts = None
            else:
                pub_ts = None
            title = entry.get("title")
            if not title:
                continue
            articles.append({
                "title": title,
                "link": entry.get("link", ""),
                "published": entry.get("published", entry.get("updated", "")),
                "published_ts": pub_ts,
                "source": url_short,
                "summary": entry.get("summary", "")[:300]
            })
            kept += 1

        delivered = len(articles) - before
        if delivered == 0 and stale == 0:
            feed_stats.append((url_short, 0, "LEER", "warning"))
        elif delivered == 0 and stale > 0:
            feed_stats.append((url_short, 0, f"alle {stale} zu alt (>{max_age_minutes}min)", "info"))
        else:
            note = f"ok ({stale} verworfen wegen Alter)" if stale else "ok"
            feed_stats.append((url_short, delivered, note, "info"))

    alive = sum(1 for _, n, _, _ in feed_stats if n > 0)
    logger.info("Feed-Report: %d von %d Feeds lieferten Artikel", alive, len(RSS_FEEDS))
    for url_short, n, status, level in feed_stats:
        if n > 0:
            logger.info(" ok %-32s %2d Artikel (%s)", url_short, n, status)
        elif level == "info":
            logger.info(" -- %-32s 0 Artikel (%s)", url_short, status)
        else:
            logger.warning(" -- %-32s 0 Artikel (%s)", url_short, status)

    logger.info("%d Artikel gesamt aus %d aktiven Feeds (Frische-Filter %dmin)",
                len(articles), alive, max_age_minutes)
    return articles


def build_earnings_map(finnhub_key: str) -> Dict[str, bool]:
    if not finnhub_key:
        return {}
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        end = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        r = requests.get(
            "https://finnhub.io/api/v1/calendar/earnings",
            params={"from": today, "to": end, "token": finnhub_key},
            timeout=10
        )
        if r.status_code == 200:
            symbols = [e.get("symbol") for e in r.json().get("earningsCalendar", []) if e.get("symbol")]
            return {sym.upper(): True for sym in symbols}
    except Exception as e:
        logger.warning("Earnings-Map Fehler: %s", e)
    return {}


# ==================== RESOLVERS (neu + verbessert) ====================
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
    elif form == "4":
        event_type = "insider_filing"
        base_conf = 5.3
    else:
        event_type = "sec_filing"
        base_conf = 4.4

    confidence = _score_catalyst(event_type, base_conf) * 1.18
    headline = f"{ticker} SEC {form}: {name[:70]}"
    return ticker, headline, event_type, round(confidence, 2)


def _resolve_wire_ticker(article: dict, known_tickers: set) -> Optional[Tuple[str, str, str, float]]:
    source = (article.get("source") or "").lower()
    if not any(ws in source for ws in _WIRE_SOURCES):
        return None

    text = (article.get("title") or "") + " " + (article.get("summary") or "")
    m = _WIRE_TICKER_RE.search(text)
    if not m:
        return None

    ticker = m.group(1).upper()
    if ticker not in known_tickers:
        return None

    title_upper = text.upper()
    if any(x in title_upper for x in ["FDA", "APPROVAL", "PHASE 3", "TOPLINE"]):
        event_type = "fda_approval"
        base = 8.6
    elif any(x in title_upper for x in ["MERGER", "ACQUISITION", "DEFINITIVE AGREEMENT"]):
        event_type = "merger"
        base = 8.3
    elif any(x in title_upper for x in ["EARNINGS BEAT", "BEAT AND RAISE", "RAISES GUIDANCE"]):
        event_type = "earnings_beat"
        base = 7.9
    elif "BUYBACK" in title_upper or "REPURCHASE" in title_upper:
        event_type = "buyback"
        base = 6.7
    else:
        event_type = "wire_strong"
        base = 5.6

    confidence = _score_catalyst(event_type, base)
    return ticker, article.get("title", "")[:100], event_type, round(confidence, 2)


def _resolve_ticker_from_headline(
    title: str, known_tickers: set, name_map: dict, override_tickers: set, seen: set
) -> Optional[str]:
    """Originale Funktion aus deinem Code"""
    for word in title.split():
        clean = word.strip(".,:;()[]{}'\"")
        if (clean.isupper() and 2 <= len(clean) <= 5 and clean.isalpha() and
            clean in known_tickers and clean not in _GENERIC_ACRONYMS and clean not in seen):
            return clean

    if not name_map:
        return None

    title_lower = title.lower().replace("&", " and ")
    title_lower = re.sub(r"[^a-z0-9\s\-]", " ", title_lower)
    title_lower = re.sub(r"\s+", " ", title_lower)
    title_lower = " " + title_lower.strip() + " "

    best_ticker = None
    best_len = 0
    for name, ticker in name_map.items():
        if ticker in seen:
            continue
        if f" {name} " not in title_lower:
            continue
        if len(name) <= best_len:
            continue
        is_override = ticker in override_tickers
        if " " not in name and len(name) < 5 and not is_override:
            continue
        if not is_override and ticker not in known_tickers:
            continue
        best_ticker = ticker
        best_len = len(name)
    return best_ticker


# ==================== CLUSTERING (neu & verbessert) ====================
def cluster_articles(articles: List[Dict], earnings_map: Dict) -> List[Dict]:
    known_tickers = _load_known_tickers()
    name_map = _load_name_to_ticker()
    cik_map = _load_cik_to_ticker()
    override_tickers = set(COMPANY_NAME_OVERRIDES.values()) | set(_PHARMA_NAME_OVERRIDES.values())

    ticker_signals: Dict[str, Dict] = {}

    for art in articles:
        res = None
        # 1. SEC
        res = _resolve_sec_filing(art, cik_map)
        # 2. Wire
        if not res:
            res = _resolve_wire_ticker(art, known_tickers)
        # 3. Classic Headline
        if not res:
            ticker = _resolve_ticker_from_headline(
                art.get("title", ""), known_tickers, name_map, override_tickers, set(ticker_signals.keys())
            )
            if ticker:
                title_upper = art.get("title", "").upper()
                is_earnings = any(kw in title_upper for kw in ["EARNINGS", "BEAT", "RESULTS", "REPORT"])
                res = (ticker, art.get("title", "")[:100], "earnings" if is_earnings else "news", 6.5 if is_earnings else 4.2)

        if not res:
            continue

        ticker, headline, event_type, confidence = res

        if ticker in ticker_signals:
            ticker_signals[ticker]["confidence_score"] = min(9.9, ticker_signals[ticker]["confidence_score"] + 1.3)
            ticker_signals[ticker]["article_count"] += 1
            continue

        if ticker in earnings_map:
            confidence = min(9.9, confidence + 2.0)
            event_type = "pre_earnings_high_conviction"

        ticker_signals[ticker] = {
            "ticker": ticker,
            "headline_repr": headline,
            "confidence_score": round(confidence, 2),
            "event_type": event_type,
            "sentiment_source": "catalyst_analyzer_v2",
            "article_count": 1
        }

    clusters = list(ticker_signals.values())
    clusters = sorted(clusters, key=lambda x: x["confidence_score"], reverse=True)
    logger.info(f"Cluster erstellt: {len(clusters)} Ticker")
    return clusters


def format_clusters_for_claude(clusters: List[Dict]) -> str:
    if not clusters:
        return "Keine relevanten Cluster heute."
    lines = ["Aktuelle relevante Cluster:"]
    for c in clusters[:12]:
        lines.append(
            f"Ticker: {c.get('ticker')} | "
            f"Confidence: {c.get('confidence_score', 0):.2f} | "
            f"Type: {c.get('event_type', 'news')} | "
            f"Headline: {c.get('headline_repr', '')}"
        )
    return "\n".join(lines)


# ==================== CLAUDE CALL (Original) ====================
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
                "model": "claude-sonnet-4-6",
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
