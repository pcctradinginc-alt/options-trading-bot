"""
news_analyzer.py — News Fetching, Clustering und Claude-Signal-Generierung

Stand 2026 (v2):
- Robuster Fetcher: requests+retry, Content-Type-Check, Browser-UA gegen CDN-Blocks.
- Erweitertes Quellen-Set: FDA, EIA, BLS, Treasury, GlobeNewswire, BusinessWire.
- Erweiterte SEC-Coverage: 8-K + 13D/13G + Form 4.
- Press-Release-Wire-Resolver: extrahiert Ticker aus "(NASDAQ: AAPL)"-Pattern.
- Pharma-Name-Overrides: deckt FDA-Releases ab, die nur Firmennamen nennen.
- Sauberere Logs: stale-only-Feeds auf INFO statt WARNING herabgestuft.
"""

import calendar
import logging
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

# Handelbares Ticker-Universum (Nasdaq + ETFs) fuer Validierung
try:
    from universe import get_known_tickers, STATIC_ETFS
except ImportError:
    get_known_tickers = None
    STATIC_ETFS = {"SPY", "QQQ", "IWM", "DIA", "GLD", "SLV", "USO", "TLT"}

# Firmenname/CIK -> Ticker
try:
    from sec_check import get_company_name_to_ticker, get_cik_to_ticker_map, COMPANY_NAME_OVERRIDES
except ImportError:
    get_company_name_to_ticker = None
    get_cik_to_ticker_map = None
    COMPANY_NAME_OVERRIDES = {}

# Caches (Lazy-Load pro Run)
_KNOWN_TICKERS_CACHE: Optional[set] = None
_NAME_TO_TICKER_CACHE: Optional[dict] = None
_CIK_TO_TICKER_CACHE: Optional[dict] = None

# Generische Akronyme: niemals als Ticker matchen, auch wenn gleichnamiger
# Ticker existiert. AI -> C3.ai, EU -> "iShares MSCI Europe", etc.
_GENERIC_ACRONYMS = {
    "AI", "IT", "IP", "EV", "CEO", "CFO", "CTO", "IPO",
    "API", "SAAS", "ESG", "AR", "VR", "ML",
    "USA", "UK", "EU", "US", "UN", "GDP", "FED", "ETF", "REIT", "SPAC",
}

# Pharma/Biotech-Name-Overrides fuer FDA-Releases. FDA-Headlines erwaehnen
# nur Firmennamen, kein Ticker. Erweitere bei Bedarf.
# Wichtig: lowercase + "&" -> "and", damit Normalisierung zum Resolver passt.
_PHARMA_NAME_OVERRIDES = {
    # Big Pharma
    "pfizer": "PFE",
    "merck": "MRK",
    "johnson and johnson": "JNJ",
    "eli lilly": "LLY",
    "lilly": "LLY",
    "abbvie": "ABBV",
    "novo nordisk": "NVO",
    "bristol myers squibb": "BMY",
    "bristol-myers squibb": "BMY",
    "gilead": "GILD",
    "amgen": "AMGN",
    "regeneron": "REGN",
    # Big Biotech
    "vertex pharmaceuticals": "VRTX",
    "vertex": "VRTX",
    "moderna": "MRNA",
    "biontech": "BNTX",
    "biogen": "BIIB",
    "alnylam": "ALNY",
    "incyte": "INCY",
    # Mid-Cap Biotech
    "sarepta": "SRPT",
    "exelixis": "EXEL",
    "ionis": "IONS",
    "blueprint medicines": "BPMC",
    # International (ADRs)
    "novartis": "NVS",
    "astrazeneca": "AZN",
    "sanofi": "SNY",
    "glaxosmithkline": "GSK",
    "roche": "RHHBY",
    "bayer": "BAYRY",
    # Med-Devices
    "intuitive surgical": "ISRG",
    "boston scientific": "BSX",
    "medtronic": "MDT",
    "edwards lifesciences": "EW",
    "stryker": "SYK",
}

logger = logging.getLogger(__name__)

# ==================== USER AGENT & HEADERS ====================
# Browser-aehnlicher UA gegen Yahoo/WSJ/CDN-Blocks. SEC EDGAR verlangt
# zwingend Kontakt-Info im UA, sonst Rate-Limiting. Override via env var
# NEWS_BOT_USER_AGENT (z.B. "MyBot/1.0 my-email@domain.com") ist empfohlen.
import os
_USER_AGENT = os.environ.get(
    "NEWS_BOT_USER_AGENT",
    "Mozilla/5.0 (compatible; DailyOptionsBot/1.2; +contact: bot@example.com) "
    "feedparser/6.0"
)

_FEED_HEADERS = {
    "User-Agent": _USER_AGENT,
    "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.5",
    "Accept-Encoding": "gzip, deflate",
}

# ==================== RSS FEEDS ====================
# Kuratierte Liste fuer 2026.
# Entfernt: finance.yahoo.com (HTML-Wall), wsj.com/xml/rss/* (deprecated),
#           Reuters (2020 abgeschaltet).
RSS_FEEDS = [
    # --- Breaking News / Aggregatoren ---
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",            # CNBC Top News
    "https://www.cnbc.com/id/10001147/device/rss/rss.html",             # CNBC Business
    "https://www.cnbc.com/id/15839135/device/rss/rss.html",             # CNBC Earnings
    # NOTE: Benzinga (feeds.benzinga.com/benzinga) entfernt — seit Monaten
    # SSL-Error trotz Retry. Aggregator-Coverage durch CNBC + GlobeNewswire
    # ausreichend.

    # --- Dow Jones / WSJ / MarketWatch (alle ueber content.dowjones.io) ---
    "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "https://feeds.content.dowjones.io/public/rss/mw_topstories",       # MarketWatch Top
    "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain",      # WSJ Markets

    # --- SEC EDGAR (alle Form-Types ueber CIK->Ticker-Resolver) ---
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&output=atom",
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=SC+13D&output=atom",
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=SC+13G&output=atom",
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&output=atom",

    # --- FDA (Biotech-Catalysts) ---
    "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/press-releases/rss.xml",
    "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/drugs/rss.xml",

    # --- EIA / BLS / Fed: Event-getriggerte Quellen ---
    # An den meisten Tagen LEER oder STALE — das ist erwartetes Verhalten.
    # CPI: 1x/Monat. NFP: 1x/Monat (1. Freitag). Fed: unregelmaessig.
    # EIA Weekly Petroleum (Mi 10:30 ET) ist NICHT in press_rss.xml - dafuer
    # gibt es keinen offiziellen RSS-Feed, muss ggf. via Scraping geholt werden.
    "https://www.eia.gov/rss/todayinenergy.xml",
    "https://www.bls.gov/feed/empsit.rss",
    "https://www.bls.gov/feed/cpi.rss",
    "https://www.federalreserve.gov/feeds/press_all.xml",
    # NOTE: home.treasury.gov/news/press-releases/feed entfernt — gibt 404.
    # Treasury hat keinen offiziellen Press-Release-RSS mehr, nur XML-Feeds
    # fuer Zinsraten unter /resource-center/data-chart-center/...

    # --- Press-Release-Wires (Resolver via (NASDAQ: XYZ)-Pattern) ---
    "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/Public%20Companies",
    # NOTE: BusinessWire feeds.businesswire.com/BW/IND_* URLs entfernt — diese
    # Struktur existiert nicht oeffentlich. GlobeNewswire deckt die meisten
    # gleichen Pressemitteilungen ab.
]

# ==================== SYSTEM PROMPT ====================
SYSTEM_PROMPT = """Du bist ein hochdisziplinierter Options-Trading-Bot.

Antworte **ausschließlich** mit einer einzigen Zeile im exakt folgenden Format:

TICKER_SIGNALS:BRK.B:CALL:HIGH:T3:45DTE,PLTR:CALL:MED:T2:30DTE,USO:CALL:HIGH:T1:21DTE

Oder genau: TICKER_SIGNALS:NONE

Wichtige Regeln:
- Verwende nur echte, handelbare Ticker (BRK.B, PLTR, NVDA, TSLA, SPY, QQQ usw.)
- Bei UNKNOWN Ticker aus dem Kontext ableiten (Berkshire → BRK.B, Alphabet → GOOGL)
- Maximal 3 Signale
- Kein zusätzlicher Text, keine Erklärungen, kein Markdown"""


# ==================== FETCHER ====================

def _fetch_feed_bytes(url: str, timeout: int = 15, retries: int = 2) -> Tuple[Optional[bytes], str]:
    """Holt Rohbytes eines Feeds mit Retry und Content-Type-Validierung.

    - Retry mit Backoff bei SSL/Timeout/Connection-Errors (Benzinga-Symptom).
    - Sofortabbruch bei 403/404/410 (kein sinnvoller Retry).
    - Lehnt HTML-Antworten ab, bevor feedparser sich daran verschluckt
      (Yahoo/WSJ liefern auf Legacy-URLs HTML-Loginwall statt RSS).

    Returns:
        (raw_bytes_or_None, status_string)
    """
    last_err = "unknown"
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=_FEED_HEADERS, timeout=timeout, allow_redirects=True)
            if r.status_code != 200:
                last_err = f"HTTP {r.status_code}"
                if r.status_code in (403, 404, 410):
                    return None, last_err  # permanent
                time.sleep(0.5 * (attempt + 1))
                continue
            ctype = r.headers.get("Content-Type", "").lower()
            # "text/html" ohne XML-Anteil = Feed deprecated oder hinter Login
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
    """Holt Artikel aus allen RSS-Feeds mit Diagnose und Frische-Filter.

    Verbesserungen 2026:
    - Robuster Fetcher (requests+retry) statt direktem feedparser.parse(url).
    - Pro-Feed-Diagnose mit semantischer Unterscheidung:
      Fehler -> WARNING, leer-aber-okay (alle stale) -> INFO.
    - Frische-Filter ueber published_parsed/updated_parsed.
    """
    cutoff_ts = datetime.now(timezone.utc).timestamp() - max_age_minutes * 60
    articles: List[Dict] = []
    feed_stats: List[Tuple[str, int, str, str]] = []  # (url, anzahl, status, level)

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

        # bozo + leer = wirklicher Parse-Fehler
        if getattr(feed, "bozo", 0) and not feed.entries:
            exc = getattr(feed, "bozo_exception", "unknown")
            feed_stats.append((url_short, 0, f"PARSE_ERROR: {str(exc)[:60]}", "warning"))
            continue

        entries = feed.entries[:12] if feed.entries else []
        kept = 0
        stale = 0
        for entry in entries:
            # Frische-Filter
            pub_struct = entry.get("published_parsed") or entry.get("updated_parsed")
            if pub_struct:
                try:
                    # WICHTIG: calendar.timegm() interpretiert das Struct als UTC.
                    # time.mktime() wuerde es als lokale Zeit interpretieren und je
                    # nach Server-TZ einen 1-9h-Offset im Frische-Filter erzeugen.
                    # feedparser liefert published_parsed garantiert in UTC.
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
            # Kein Fehler, nur keine frischen Artikel — auf INFO-Level
            feed_stats.append((url_short, 0, f"alle {stale} zu alt (>{max_age_minutes}min)", "info"))
        else:
            note = f"ok ({stale} verworfen wegen Alter)" if stale else "ok"
            feed_stats.append((url_short, delivered, note, "info"))

    # Pro-Feed-Report
    alive = sum(1 for _, n, _, _ in feed_stats if n > 0)
    logger.info("Feed-Report: %d von %d Feeds lieferten Artikel", alive, len(RSS_FEEDS))
    for url_short, n, status, level in feed_stats:
        if n > 0:
            logger.info("  ok %-32s %2d Artikel  (%s)", url_short, n, status)
        elif level == "info":
            logger.info("  -- %-32s  0 Artikel  (%s)", url_short, status)
        else:
            logger.warning("  -- %-32s  0 Artikel  (%s)", url_short, status)
    logger.info("%d Artikel gesamt aus %d aktiven Feeds (Frische-Filter %dmin)",
                len(articles), alive, max_age_minutes)
    return articles


# ==================== EARNINGS MAP ====================

def build_earnings_map(finnhub_key: str) -> Dict[str, bool]:
    """Prueft anstehende Earnings ueber Finnhub."""
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
    """Wandelt Cluster-Daten in Text fuer das LLM um."""
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


# ==================== LAZY LOADERS ====================

def _load_known_tickers() -> set:
    """Laedt das Ticker-Universum genau einmal pro Run (Lazy + Cache)."""
    global _KNOWN_TICKERS_CACHE
    if _KNOWN_TICKERS_CACHE is not None:
        return _KNOWN_TICKERS_CACHE
    if get_known_tickers is not None:
        try:
            _KNOWN_TICKERS_CACHE = get_known_tickers(fallback=STATIC_ETFS)
            logger.info("Ticker-Universum geladen: %d Symbole", len(_KNOWN_TICKERS_CACHE))
            return _KNOWN_TICKERS_CACHE
        except Exception as e:
            logger.warning("Ticker-Universum Fallback: %s", e)
    _KNOWN_TICKERS_CACHE = set(STATIC_ETFS)
    return _KNOWN_TICKERS_CACHE


def _load_name_to_ticker() -> dict:
    """Laedt das Firmenname->Ticker-Mapping einmal pro Run (Lazy + Cache).

    Pharma-Overrides werden am Ende drueber-gelegt, damit FDA-Headlines wie
    'FDA approves Vertex CASGEVY' direkt resolven, auch wenn 'vertex' nicht
    im SEC-Mapping steht (zu kurz / generisch).
    """
    global _NAME_TO_TICKER_CACHE
    if _NAME_TO_TICKER_CACHE is not None:
        return _NAME_TO_TICKER_CACHE
    base = {}
    if get_company_name_to_ticker is not None:
        try:
            base = dict(get_company_name_to_ticker())
        except Exception as e:
            logger.warning("Name->Ticker Mapping nicht verfuegbar: %s", e)
    base.update(_PHARMA_NAME_OVERRIDES)
    _NAME_TO_TICKER_CACHE = base
    return _NAME_TO_TICKER_CACHE


def _load_cik_to_ticker() -> dict:
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


# ==================== TICKER RESOLVERS ====================

# SEC-EDGAR-Atom-Title-Format:
#   "8-K - APPLE INC (0000320193) (Filer)"
#   "10-K/A - MICROSOFT CORP (0000789019) (Filer)"
#   "4 - SMITH JOHN (0001234567) (Reporting)"
_SEC_TITLE_RE = re.compile(
    r"^\s*(?P<form>\S(?:[^\s]|\s(?!-\s))*?)"
    r"\s+-\s+"
    r"(?P<name>.+?)\s+"
    r"\((?P<cik>\d{6,10})\)\s*"
    r"\((?P<role>[^)]+)\)\s*$",
    re.IGNORECASE
)

# Press-Release-Wire-Pattern: "(NASDAQ: AAPL)", "(NYSE:MSFT)", "(NASDAQ: BRK.B)"
# Reihenfolge wichtig: NYSEAMERICAN/NYSE AMERICAN VOR NYSE.
_WIRE_TICKER_RE = re.compile(
    r"\(\s*(?:NASDAQ|NYSEAMERICAN|NYSE\s+AMERICAN|NYSE|AMEX|OTCQX|OTCQB|CBOE|BATS|TSX|TSXV)\s*:\s*"
    r"([A-Z]{1,5}(?:\.[A-Z])?)\s*\)",
    re.IGNORECASE
)

# Quellen die zuverlaessig (EXCHANGE: TICKER) im Title fuehren
_WIRE_SOURCES = ("globenewswire", "businesswire", "prnewswire", "newswire", "accesswire")


def _resolve_sec_filing(article: dict, cik_map: dict) -> Optional[Tuple[str, str, str, float]]:
    """SEC-EDGAR-Atom: CIK aus Title -> Ticker via SEC-Map.

    Confidence-Tabelle:
      8-K           7.0  (Material Event by SEC-Definition)
      10-Q / 10-K   6.0  (Earnings/Annual)
      13D / 13G     6.0  (Activist / 5%+ Stakes)
      Form 4        4.0  (Insider, einzelner low signal)
      sonstige      3.5
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
        # Fallback: CIK irgendwo in Title oder Link finden
        cik_match = re.search(r"\((\d{6,10})\)", title) or re.search(r"cik=0*(\d{6,10})", link)
        if cik_match:
            try:
                cik = int(cik_match.group(1))
            except (TypeError, ValueError):
                cik = None
        if "-" in title:
            head = title.split("-", 1)[0].strip()
            if head and len(head) <= 8:
                form = head.upper()
        name = title[:80]

    if cik is None:
        return None

    # Defensive: cik_map kann je nach sec_check.py-Implementierung Int- oder
    # String-Keys haben (mit/ohne fuehrenden Nullen). Beide Varianten probieren.
    ticker = cik_map.get(cik) or cik_map.get(str(cik)) or cik_map.get(f"{cik:010d}")
    if not ticker:
        return None

    short_name = name[:50].strip(" .,-") or ticker
    headline = f"{ticker} SEC {form}: {short_name}"

    form_norm = form.replace(" ", "").upper()
    if form_norm in ("8-K", "8K"):
        event_type, confidence = "8k_filing", 7.0
    elif form_norm == "4":
        event_type, confidence = "form4_insider", 4.0
    elif form_norm in ("10-Q", "10Q", "10-K", "10K"):
        event_type, confidence = "earnings_filing", 6.0
    elif form_norm in ("13D", "13G", "SC13D", "SC13G"):
        event_type, confidence = "ownership_filing", 6.0
    else:
        event_type, confidence = "sec_filing", 3.5

    return ticker, headline, event_type, confidence


def _resolve_wire_ticker(article: dict, known_tickers: set) -> Optional[Tuple[str, str, str, float]]:
    """Press-Release-Wire-Resolver: extrahiert Ticker aus '(NASDAQ: XYZ)'-Pattern.

    GlobeNewswire, BusinessWire & PR Newswire fuehren in 90%+ der Faelle den
    Ticker direkt im Title. Zuverlaessiger als Firmennamen-Match, weil das
    Pattern eindeutig ist.

    Confidence-Tabelle (Keyword-basiert):
      FDA-Event     7.5
      M&A           7.0
      Earnings      6.5
      Guidance      6.0
      Capital-Ret.  5.5
      Sonstiges     4.5
    """
    source = (article.get("source") or "").lower()
    if not any(wire in source for wire in _WIRE_SOURCES):
        return None

    title = article.get("title") or ""
    summary = article.get("summary") or ""
    text = f"{title} {summary}"

    m = _WIRE_TICKER_RE.search(text)
    if not m:
        return None

    ticker = m.group(1).upper()
    if ticker not in known_tickers:
        return None

    title_upper = title.upper()
    if any(kw in title_upper for kw in ("FDA", "APPROV", "PDUFA", "CRL ", "PHASE 3", "PHASE III", "TOPLINE")):
        event_type, confidence = "fda_event", 7.5
    elif any(kw in title_upper for kw in ("MERGER", "ACQUIRE", "ACQUIRES", "ACQUISITION",
                                          "TAKEOVER", "TENDER OFFER")):
        event_type, confidence = "m_and_a", 7.0
    elif any(kw in title_upper for kw in ("EARNINGS", "BEAT", "MISS", "REPORTS Q",
                                          "Q1 ", "Q2 ", "Q3 ", "Q4 ", "QUARTERLY RESULTS")):
        event_type, confidence = "earnings_wire", 6.5
    elif any(kw in title_upper for kw in ("GUIDANCE", "OUTLOOK", "RAISES", "LOWERS", "REAFFIRM")):
        event_type, confidence = "guidance", 6.0
    elif any(kw in title_upper for kw in ("DIVIDEND", "BUYBACK", "REPURCHASE", "STOCK SPLIT")):
        event_type, confidence = "capital_return", 5.5
    else:
        event_type, confidence = "wire_news", 4.5

    return ticker, title[:100], event_type, confidence


def _resolve_ticker_from_headline(
    title: str,
    known_tickers: set,
    name_map: dict,
    override_tickers: set,
    seen: set,
) -> Optional[str]:
    """Klassischer Headline-Resolver mit Anti-False-Positive-Regeln.

    Reihenfolge:
      1) direkter Ticker im Originaltext (gross geschrieben)
      2) Firmenname (laengste Uebereinstimmung gewinnt)

    Anti-False-Positive:
      - Generische Akronyme (AI, IT, IPO ...) ignorieren
      - Firmenname muss im handelbaren Universum sein, ausser Override
      - Einwort-Namen <5 Buchstaben sind zu generisch (ausser Override)
    """
    # 1) Direkter Ticker
    for word in title.split():
        clean = word.strip(".,:;()[]{}'\"")
        if (clean.isupper()
                and 2 <= len(clean) <= 5
                and clean.isalpha()
                and clean in known_tickers
                and clean not in _GENERIC_ACRONYMS
                and clean not in seen):
            return clean

    # 2) Firmenname
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


# ==================== CLUSTERING ====================

def cluster_articles(articles: List[Dict], earnings_map: Dict) -> List[Dict]:
    """Gruppiert News und erkennt Ticker.

    Resolver-Reihenfolge:
      0.  SEC-EDGAR-Atom-Eintraege: CIK -> Ticker
      0.5 Press-Release-Wires: (NASDAQ: XYZ)-Pattern (99% zuverlaessig)
      1.  Direkter Ticker in Headline ("AAPL beats Q4...")
      2.  Firmenname in Headline ("Apple reports record earnings")
    Cluster ohne erkennbaren Ticker werden verworfen.
    """
    known_tickers = _load_known_tickers()
    name_map = _load_name_to_ticker()
    cik_map = _load_cik_to_ticker()
    override_tickers = (set(COMPANY_NAME_OVERRIDES.values())
                       | set(_PHARMA_NAME_OVERRIDES.values()))
    clusters = []
    seen = set()

    for art in articles:
        # Stufe 0: SEC EDGAR
        sec_resolved = _resolve_sec_filing(art, cik_map)
        if sec_resolved is not None:
            ticker, headline, event_type, confidence = sec_resolved
            if ticker in seen:
                continue
            if ticker not in known_tickers and ticker not in override_tickers:
                logger.debug("SEC-Ticker %s nicht im handelbaren Universum, verworfen", ticker)
                continue
            clusters.append({
                "ticker": ticker,
                "headline_repr": headline[:100],
                "confidence_score": confidence,
                "sentiment_score": 0.0,
                "sentiment_source": "sec_filing",
                "event_type": event_type,
            })
            seen.add(ticker)
            continue

        # Stufe 0.5: Press-Release-Wires (vor klassischem Resolver, weil zuverlaessiger)
        wire_resolved = _resolve_wire_ticker(art, known_tickers)
        if wire_resolved is not None:
            ticker, headline, event_type, confidence = wire_resolved
            if ticker in seen:
                continue
            # Sentiment-Heuristik nach Event-Type
            if event_type in ("fda_event", "earnings_wire", "guidance", "m_and_a"):
                sentiment = 0.5
            else:
                sentiment = 0.2
            clusters.append({
                "ticker": ticker,
                "headline_repr": headline,
                "confidence_score": confidence,
                "sentiment_score": sentiment,
                "sentiment_source": "wire_keyword",
                "event_type": event_type,
            })
            seen.add(ticker)
            continue

        # Stufe 1+2: Klassischer Headline-Resolver
        original_title = art["title"]
        title_upper = original_title.upper()

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


# ==================== CLAUDE CALL ====================

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

        # Robuste Extraktion: TICKER_SIGNALS irgendwo im Text finden
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
    """Schnittstelle zum Markt-Kalender."""
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
            print(f"  {c['ticker']:6s}  conf={c['confidence_score']:.1f}  "
                  f"type={c['event_type']:18s}  {c['headline_repr'][:70]}")
