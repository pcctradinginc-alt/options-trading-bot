"""
news_analyzer.py — News Fetching, Clustering und Claude-Signal-Generierung
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List

import feedparser
import requests

# Falls finbert_sentiment vorhanden ist, ansonsten Dummy-Logik nutzen
try:
    from finbert_sentiment import get_finbert_sentiment_batch
except ImportError:
    get_finbert_sentiment_batch = None

# Handelbares Ticker-Universum (Nasdaq + ETFs) für Validierung der News-Ticker
try:
    from universe import get_known_tickers, STATIC_ETFS
except ImportError:
    get_known_tickers = None
    STATIC_ETFS = {"SPY", "QQQ", "IWM", "DIA", "GLD", "SLV", "USO", "TLT"}

# Firmenname -> Ticker, für Headlines wie "Apple reports earnings"
try:
    from sec_check import get_company_name_to_ticker, COMPANY_NAME_OVERRIDES
except ImportError:
    get_company_name_to_ticker = None
    COMPANY_NAME_OVERRIDES = {}

# Werden beim ersten Aufruf von cluster_articles() gefüllt (Lazy-Load)
_KNOWN_TICKERS_CACHE: set[str] | None = None
_NAME_TO_TICKER_CACHE: dict[str, str] | None = None

logger = logging.getLogger(__name__)

# ==================== RSS FEEDS ====================
RSS_FEEDS = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.reuters.com/reuters/marketsNews",
    "https://www.cnbc.com/id/100003114/device/rss/rss.xml",
    "https://www.cnbc.com/id/100727362/device/rss/rss.xml",
    "https://feeds.benzinga.com/benzinga",
    "https://www.benzinga.com/feed",
    "http://feeds.feedburner.com/zerohedge/feed",
    "https://finance.yahoo.com/rss/headline",
    "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "https://www.wsj.com/xml/rss/3_7041.xml",
]

# ==================== SYSTEM PROMPT ====================
# Der Prompt ist jetzt extrem strikt, um "Chatty Claude" zu verhindern.
SYSTEM_PROMPT = """Du bist ein hochdisziplinierter Options-Trading-Bot.

Antworte **ausschließlich** mit einer einzigen Zeile im exakt folgenden Format:

TICKER_SIGNALS:BRK.B:CALL:HIGH:T3:45DTE,PLTR:CALL:MED:T2:30DTE,USO:CALL:HIGH:T1:21DTE

Oder genau: TICKER_SIGNALS:NONE

Wichtige Regeln:
- Verwende nur echte, handelbare Ticker (BRK.B, PLTR, NVDA, TSLA, SPY, QQQ usw.)
- Bei UNKNOWN Ticker aus dem Kontext ableiten (Berkshire → BRK.B, Alphabet → GOOGL)
- Maximal 3 Signale
- Kein zusätzlicher Text, keine Erklärungen, kein Markdown"""

# ==================== CORE FUNCTIONS ====================

def fetch_all_feeds() -> List[Dict]:
    """Holt Artikel aus allen RSS-Feeds."""
    articles = []
    for url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:12]:
                articles.append({
                    "title": entry.title,
                    "link": entry.link,
                    "published": entry.get("published", entry.get("updated", "")),
                    "source": url.split("/")[2].replace("www.", "").replace("feeds.", ""),
                    "summary": entry.get("summary", "")[:300]
                })
        except Exception as e:
            logger.debug("Feed-Fehler %s: %s", url[:50], e)
    logger.info("%d Artikel aus %d Feeds geladen", len(articles), len(RSS_FEEDS))
    return articles


def build_earnings_map(finnhub_key: str) -> Dict[str, bool]:
    """Prüft anstehende Earnings über Finnhub."""
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


def format_clusters_for_claude(clusters: List[Dict]) -> str:
    """Wandelt Cluster-Daten in Text für das LLM um."""
    if not clusters:
        return "Keine relevanten Cluster heute."

    lines = ["Aktuelle relevante Cluster:"]
    for c in clusters[:12]:
        lines.append(
            f"Ticker: {c.get('ticker')} | "
            f"Confidence: {c.get('confidence_score', 0):.2f} | "
            f"Sentiment: {c.get('sentiment_score', 0):.2f} | "
            f"Type: {c.get('event_type', 'news')} | "
            f"Headline: {c.get('headline_repr', '')}"
        )
    return "\n".join(lines)


def _load_known_tickers() -> set[str]:
    """Lädt das Ticker-Universum genau einmal pro Run (Lazy + Cache)."""
    global _KNOWN_TICKERS_CACHE
    if _KNOWN_TICKERS_CACHE is not None:
        return _KNOWN_TICKERS_CACHE

    if get_known_tickers is not None:
        try:
            _KNOWN_TICKERS_CACHE = get_known_tickers(fallback=STATIC_ETFS)
            logger.info("Ticker-Universum geladen: %d Symbole", len(_KNOWN_TICKERS_CACHE))
            return _KNOWN_TICKERS_CACHE
        except Exception as e:
            logger.warning("Ticker-Universum konnte nicht geladen werden, nutze Fallback: %s", e)

    _KNOWN_TICKERS_CACHE = set(STATIC_ETFS)
    return _KNOWN_TICKERS_CACHE


def _load_name_to_ticker() -> dict[str, str]:
    """Lädt das Firmenname->Ticker-Mapping einmal pro Run (Lazy + Cache)."""
    global _NAME_TO_TICKER_CACHE
    if _NAME_TO_TICKER_CACHE is not None:
        return _NAME_TO_TICKER_CACHE

    if get_company_name_to_ticker is not None:
        try:
            _NAME_TO_TICKER_CACHE = get_company_name_to_ticker()
            return _NAME_TO_TICKER_CACHE
        except Exception as e:
            logger.warning("Name->Ticker Mapping nicht verfügbar: %s", e)

    _NAME_TO_TICKER_CACHE = {}
    return _NAME_TO_TICKER_CACHE


def _resolve_ticker_from_headline(
    title: str,
    known_tickers: set[str],
    name_map: dict[str, str],
    override_tickers: set[str],
    seen: set[str],
) -> str | None:
    """Versucht, einen Ticker aus der Headline zu extrahieren.
    Reihenfolge: 1) direkter Ticker im Originaltext, 2) Firmenname.

    Anti-False-Positive-Regeln für Firmennamen-Match:
      - Match muss im handelbaren Ticker-Universum sein,
        außer er stammt aus der hand-kuratierten Override-Liste.
      - Einwort-Firmennamen müssen mindestens 5 Buchstaben haben,
        sonst zu generisch (z.B. 'vik' für Viking Holdings).
    """
    # 1) Direkter Ticker — im Originaltext groß geschrieben
    for word in title.split():
        clean = word.strip(".,:;()[]{}'\"")
        if (clean.isupper()
                and 2 <= len(clean) <= 5
                and clean.isalpha()
                and clean in known_tickers
                and clean not in seen):
            return clean

    # 2) Firmenname — längste Übereinstimmung gewinnt.
    # Normalisierung muss zu sec_check._normalize_company_name passen
    # (lowercase, & -> "and", nur Buchstaben/Ziffern/Bindestrich)
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
        # Word-Boundary-Check, damit "apple" nicht in "pineapple" matcht
        if f" {name} " not in title_lower:
            continue
        if len(name) <= best_len:
            continue

        is_override = ticker in override_tickers

        # Einwort-Namen unter 5 Buchstaben sind zu generisch
        if " " not in name and len(name) < 5 and not is_override:
            continue

        # Cross-Validation: SEC-Treffer müssen im handelbaren Universe sein.
        # Override-Ticker (BRK.B usw.) sind kuratiert und überspringen den Check.
        if not is_override and ticker not in known_tickers:
            continue

        best_ticker = ticker
        best_len = len(name)

    return best_ticker


def cluster_articles(articles: List[Dict], earnings_map: Dict) -> List[Dict]:
    """Gruppiert News und erkennt Ticker sowie Earnings-Events.

    Ticker-Erkennung in zwei Stufen:
      1. Direkter Ticker in Headline ("AAPL beats Q4...")
      2. Firmenname in Headline ("Apple reports record earnings")
    Cluster ohne erkennbaren Ticker werden verworfen.
    """
    known_tickers = _load_known_tickers()
    name_map = _load_name_to_ticker()
    override_tickers = set(COMPANY_NAME_OVERRIDES.values())
    clusters = []
    seen = set()

    for art in articles:
        original_title = art["title"]
        title_upper = original_title.upper()  # für Earnings-Keyword-Suche

        ticker = _resolve_ticker_from_headline(
            original_title, known_tickers, name_map, override_tickers, seen
        )
        if ticker is None:
            continue

        is_earnings = any(kw in title_upper for kw in
                          ["EARNINGS", "BEAT", "MISS", "REPORT", "RESULTS",
                           "Q1", "Q2", "Q3", "Q4"])

        clusters.append({
            "ticker": ticker,
            "headline_repr": original_title[:100],
            "confidence_score": 8.0 if is_earnings else 5.0,
            "sentiment_score": 0.65 if is_earnings else 0.2,
            "sentiment_source": "keyword",
            "event_type": "earnings" if is_earnings else "news",
        })
        seen.add(ticker)

    return clusters


def run_claude(cluster_text: str, market_time: str, market_status: str, api_key: str) -> str:
    """Ruft Claude auf und extrahiert das Signal mittels Regex."""
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
                "temperature": 0.0,  # Maximale Deterministik
                "system": SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": user_message}]
            },
            timeout=40
        )
        r.raise_for_status()
        data = r.json()

        raw_text = data["content"][0]["text"].strip()
        logger.debug("Claude Rohantwort:\n%s", raw_text[:400])

        # ROBUSTE EXTRAKTION: Sucht nach TICKER_SIGNALS überall im Text
        match = re.search(r'(TICKER_SIGNALS:[^\n\r]+)', raw_text, re.IGNORECASE)
        if match:
            signal_line = match.group(1).strip().upper()
            logger.info("✅ Claude Signal extrahiert: %s", signal_line)
            return signal_line

        logger.warning("Kein gültiges TICKER_SIGNALS-Format gefunden")
        return "TICKER_SIGNALS:NONE"

    except Exception as e:
        logger.error("Claude API Fehler: %s", e)
        return "TICKER_SIGNALS:NONE"


def get_market_context() -> tuple:
    """Schnittstelle zum Markt-Kalender."""
    try:
        from market_calendar import market_context
        return market_context()
    except ImportError:
        return datetime.now().strftime("%H:%M"), "OPEN"


# ==================== TEST MODUS ====================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("=== News Analyzer Test ===")
    articles = fetch_all_feeds()
    print(f"{len(articles)} Artikel geladen")
