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
# CIK -> Ticker, fuer SEC-EDGAR-Atom-Feeds
try:
    from sec_check import get_company_name_to_ticker, get_cik_to_ticker_map, COMPANY_NAME_OVERRIDES
except ImportError:
    get_company_name_to_ticker = None
    get_cik_to_ticker_map = None
    COMPANY_NAME_OVERRIDES = {}

# Werden beim ersten Aufruf von cluster_articles() gefüllt (Lazy-Load)
_KNOWN_TICKERS_CACHE: set[str] | None = None
_NAME_TO_TICKER_CACHE: dict[str, str] | None = None
_CIK_TO_TICKER_CACHE: dict[int, str] | None = None

# Generische Akronyme, die in Headlines fast immer das Konzept meinen,
# nicht den gleichnamigen Ticker. AI matcht z.B. C3.ai (Ticker: AI),
# obwohl die Headline über künstliche Intelligenz im Allgemeinen geht.
_GENERIC_ACRONYMS = {
    "AI", "IT", "IP", "EV", "CEO", "CFO", "CTO", "IPO",
    "API", "SAAS", "ESG", "AR", "VR", "ML",
    "USA", "UK", "EU", "US", "UN", "GDP", "FED", "ETF", "REIT", "SPAC",
    # NEU 2026: Headlines wie "UK Gilt Yields Near 30-Year Highs" matchten
    # frueher faelschlich auf den Ticker UK. EU/US/UN aus gleichem Grund ergaenzt.
}

logger = logging.getLogger(__name__)

# ==================== RSS FEEDS ====================
# Kuratierte Liste: nur Feeds, die in 2026 noch zuverlaessig liefern.
# Reuters wurde 2020 abgeschaltet, FeedBurner halb-tot, Benzinga-Duplikat entfernt.
# Ergaenzt: SEC EDGAR (8-K Material Events) und MarketWatch (Dow-Jones-Marke).
RSS_FEEDS = [
    # Aggregatoren / Breaking News
    "https://www.cnbc.com/id/100003114/device/rss/rss.xml",
    "https://www.cnbc.com/id/100727362/device/rss/rss.xml",
    "https://feeds.benzinga.com/benzinga",
    "https://finance.yahoo.com/rss/headline",
    "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "https://www.wsj.com/xml/rss/3_7041.xml",
    "https://feeds.content.dowjones.io/public/rss/mw_topstories",  # MarketWatch
    # Direktquelle Material Events (CIK -> Ticker mapping noetig)
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&output=atom",
    # Macro-Trigger fuer VIX/TLT/SPY-Plays
    "https://www.federalreserve.gov/feeds/press_all.xml",
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

def fetch_all_feeds(max_age_minutes: int = 720) -> List[Dict]:
    """Holt Artikel aus allen RSS-Feeds.

    Verbesserungen 2026:
    - Pro-Feed-Diagnose auf INFO/WARNING-Level (zeigt tote Feeds sofort).
    - Frische-Filter: Artikel aelter als max_age_minutes werden verworfen.
      Default 720min = 12h. Bei Cron-Lauf um 09:35 ET deckt das Pre-Market und
      die letzten Abend-News des Vortags ab.
    - User-Agent setzen (SEC EDGAR und einige andere Quellen blocken Default-UA).
    """
    import time
    from datetime import datetime, timezone

    cutoff_ts = datetime.now(timezone.utc).timestamp() - max_age_minutes * 60
    articles: List[Dict] = []
    feed_stats: List[tuple] = []  # (url_short, anzahl, status)

    # Hoeflicher User-Agent. SEC EDGAR verlangt ihn explizit.
    ua = "DailyOptionsBot/1.0 (contact: bot@example.com) feedparser"

    for url in RSS_FEEDS:
        url_short = url.split("/")[2].replace("www.", "").replace("feeds.", "")
        before = len(articles)
        try:
            feed = feedparser.parse(url, agent=ua)

            # bozo=1 + keine Eintraege => Parse-/Netzwerk-Fehler
            if getattr(feed, "bozo", 0) and not feed.entries:
                exc = getattr(feed, "bozo_exception", "unknown")
                feed_stats.append((url_short, 0, f"PARSE_ERROR: {str(exc)[:60]}"))
                continue

            entries = feed.entries[:12] if feed.entries else []
            kept = 0
            stale = 0
            for entry in entries:
                # Frische-Filter
                pub_struct = entry.get("published_parsed") or entry.get("updated_parsed")
                if pub_struct:
                    try:
                        pub_ts = time.mktime(pub_struct)
                        if pub_ts < cutoff_ts:
                            stale += 1
                            continue
                    except (TypeError, ValueError, OverflowError):
                        pub_ts = None
                else:
                    pub_ts = None

                # title kann fehlen (selten, aber bei SEC EDGAR moeglich)
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
                feed_stats.append((url_short, 0, "LEER"))
            elif delivered == 0 and stale > 0:
                feed_stats.append((url_short, 0, f"alle {stale} zu alt (>{max_age_minutes}min)"))
            else:
                note = f"ok ({stale} verworfen wegen Alter)" if stale else "ok"
                feed_stats.append((url_short, delivered, note))

        except Exception as e:
            feed_stats.append((url_short, 0, f"EXCEPTION: {type(e).__name__}: {str(e)[:60]}"))

    # Pro-Feed-Report
    alive = sum(1 for _, n, _ in feed_stats if n > 0)
    logger.info("Feed-Report: %d von %d Feeds lieferten Artikel", alive, len(RSS_FEEDS))
    for url_short, n, status in feed_stats:
        if n > 0:
            logger.info("  ok %-32s %2d Artikel  (%s)", url_short, n, status)
        else:
            logger.warning("  -- %-32s  0 Artikel  (%s)", url_short, status)
    logger.info("%d Artikel gesamt aus %d aktiven Feeds (Frische-Filter %dmin)",
                len(articles), alive, max_age_minutes)
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


def _load_cik_to_ticker() -> dict[int, str]:
    """Laedt CIK->Ticker-Mapping fuer SEC-EDGAR-Atom-Feeds (Lazy + Cache)."""
    global _CIK_TO_TICKER_CACHE
    if _CIK_TO_TICKER_CACHE is not None:
        return _CIK_TO_TICKER_CACHE

    if get_cik_to_ticker_map is not None:
        try:
            _CIK_TO_TICKER_CACHE = get_cik_to_ticker_map()
            return _CIK_TO_TICKER_CACHE
        except Exception as e:
            logger.warning("CIK->Ticker Mapping nicht verfuegbar: %s", e)

    _CIK_TO_TICKER_CACHE = {}
    return _CIK_TO_TICKER_CACHE


# SEC-EDGAR-Atom-Titles haben das Format:
#   "8-K - APPLE INC (0000320193) (Filer)"
#   "10-K/A - MICROSOFT CORP (0000789019) (Filer)"
#   "4 - SMITH JOHN  (0001234567) (Reporting)"
#   "SC 13G - SOMECORP (0000037996) (Subject Company)"
# Der Trenner ist " - " mit Whitespace BEIDSEITIG, sodass interner Bindestrich
# in der Form ("8-K", "10-K/A") nicht als Trenner missverstanden wird.
_SEC_TITLE_RE = re.compile(
    r"^\s*(?P<form>\S(?:[^\s]|\s(?!-\s))*?)"  # Form-Typ (kein " - " erlaubt)
    r"\s+-\s+"                                  # erzwungener Trenner " - "
    r"(?P<name>.+?)\s+"                          # Firmenname
    r"\((?P<cik>\d{6,10})\)\s*"                 # CIK in Klammern
    r"\((?P<role>[^)]+)\)\s*$",                  # Filer/Issuer/Reporting/Subject
    re.IGNORECASE
)


def _resolve_sec_filing(article: dict, cik_map: dict[int, str]) -> tuple[str, str, str, float] | None:
    """Behandelt SEC-EDGAR-Atom-Eintraege gesondert.

    Statt im Title nach einem Ticker-Wort zu suchen, extrahiert diese Funktion
    die CIK aus dem Klammerausdruck und schlaegt sie in der CIK->Ticker-Map nach.

    Returns:
        Tuple (ticker, headline_repr, event_type, confidence) oder None,
        wenn der Artikel nicht von der SEC kommt oder nicht aufloesbar ist.
    """
    source = (article.get("source") or "").lower()
    link = (article.get("link") or "").lower()
    if "sec.gov" not in source and "sec.gov" not in link:
        return None

    title = article.get("title") or ""
    cik = None
    form = "filing"
    name = ""

    m = _SEC_TITLE_RE.match(title)
    if m:
        try:
            cik = int(m.group("cik"))
        except (TypeError, ValueError):
            cik = None
        form = m.group("form").upper().strip()
        name = m.group("name").strip()
    else:
        # Fallback: irgendwo in Title oder Link eine 6-10-stellige Zahl in Klammern
        cik_match = re.search(r"\((\d{6,10})\)", title)
        if not cik_match:
            cik_match = re.search(r"cik=0*(\d{6,10})", link)
        if cik_match:
            try:
                cik = int(cik_match.group(1))
            except (TypeError, ValueError):
                cik = None
        # Form aus Title herausziehen (vor dem ersten Bindestrich)
        if "-" in title:
            head = title.split("-", 1)[0].strip()
            if head and len(head) <= 8:
                form = head.upper()
        name = title[:80]

    if cik is None:
        return None

    ticker = cik_map.get(cik)
    if not ticker:
        # CIK nicht in unserer Map (z.B. Privatfonds, ausl. Filer ohne Ticker)
        return None

    # Kompakte Headline fuer den LLM. SEC-Titles sind redundant ("8-K - APPLE INC (...)"),
    # wir komprimieren auf eine Form, die fuer Claude direkt einordbar ist.
    short_name = name[:50].strip(" .,-") or ticker
    headline = f"{ticker} SEC {form}: {short_name}"

    if form in ("8-K", "8K"):
        event_type = "8k_filing"
        # 8-K = Material Event by SEC-Definition. Hoehere Vorab-Confidence
        # als ein normaler News-Artikel.
        confidence = 7.0
    elif form == "4":
        event_type = "form4_insider"
        # Form 4 ohne Inhaltsanalyse ist neutral-bis-leicht-relevant.
        confidence = 4.0
    elif form in ("10-Q", "10-K"):
        event_type = "earnings_filing"
        confidence = 6.0
    elif form in ("13D", "13G", "SC 13D", "SC 13G"):
        event_type = "ownership_filing"
        confidence = 5.0
    else:
        event_type = "sec_filing"
        confidence = 3.5

    return ticker, headline, event_type, confidence


def _resolve_ticker_from_headline(
    title: str,
    known_tickers: set[str],
    name_map: dict[str, str],
    override_tickers: set[str],
    seen: set[str],
) -> str | None:
    """Versucht, einen Ticker aus der Headline zu extrahieren.
    Reihenfolge: 1) direkter Ticker im Originaltext, 2) Firmenname.

    Anti-False-Positive-Regeln:
      - Generische Akronyme (AI, IT, IPO, ...) werden ignoriert,
        auch wenn ein gleichnamiger Ticker existiert.
      - Firmennamen-Match muss im handelbaren Universe sein,
        außer er stammt aus der hand-kuratierten Override-Liste.
      - Einwort-Firmennamen müssen mindestens 5 Buchstaben haben,
        sonst zu generisch (z.B. 'vik' für Viking Holdings).
    """
    # 1) Direkter Ticker — im Originaltext groß geschrieben.
    # Generische Akronyme ausschließen, auch wenn der Ticker existiert.
    for word in title.split():
        clean = word.strip(".,:;()[]{}'\"")
        if (clean.isupper()
                and 2 <= len(clean) <= 5
                and clean.isalpha()
                and clean in known_tickers
                and clean not in _GENERIC_ACRONYMS
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

    Ticker-Erkennung in drei Stufen:
      0. SEC-EDGAR-Atom-Eintraege: CIK aus Title -> Ticker via SEC-Map.
         (Wird zuerst versucht, weil SEC-Titles kein Ticker-Wort enthalten.)
      1. Direkter Ticker in Headline ("AAPL beats Q4...")
      2. Firmenname in Headline ("Apple reports record earnings")
    Cluster ohne erkennbaren Ticker werden verworfen.
    """
    known_tickers = _load_known_tickers()
    name_map = _load_name_to_ticker()
    cik_map = _load_cik_to_ticker()
    override_tickers = set(COMPANY_NAME_OVERRIDES.values())
    clusters = []
    seen = set()

    for art in articles:
        # Stufe 0: SEC-EDGAR-Atom hat CIK statt Ticker im Titel.
        sec_resolved = _resolve_sec_filing(art, cik_map)
        if sec_resolved is not None:
            ticker, headline, event_type, confidence = sec_resolved
            if ticker in seen:
                continue
            # Sicherheits-Check: Ticker muss handelbar sein. Manche SEC-Tickers
            # sind nur Pink-Sheet, OTC oder zu illiquide.
            if ticker not in known_tickers and ticker not in override_tickers:
                logger.debug("SEC-Ticker %s nicht im handelbaren Universum, verworfen", ticker)
                continue
            clusters.append({
                "ticker": ticker,
                "headline_repr": headline[:100],
                "confidence_score": confidence,
                "sentiment_score": 0.0,  # Aus Filing-Title nicht ableitbar
                "sentiment_source": "sec_filing",
                "event_type": event_type,
            })
            seen.add(ticker)
            continue

        # Stufe 1+2: Klassischer Headline-Resolver
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
