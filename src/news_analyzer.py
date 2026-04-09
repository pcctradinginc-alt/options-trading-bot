"""
news_analyzer.py — News-Analyse (Step 1)

Änderungen v3:
- WSJ Feed (Tier 1, Credibility 0.89) hinzugefügt
- Nasdaq Feed (Tier 2, Credibility 0.75) hinzugefügt
- Benzinga_Breaking Credibility 0.51 → 0.45
- DECAY_LAMBDA 0.15 → 0.18 (schnelleres Altern)
- VELOCITY_WINDOW_MIN 60 → 45 min
- KEYWORDS erweitert: ai/tariff/fomc/rate cut/rate hike/rally/surge/default
- KNOWN_TICKERS erweitert: PLTR/ARM/CRWD/MRVL/SMCI
- TICKER_ALIASES erweitert
- MACRO_TICKER_MAP erweitert: middle east/china/trade war
- SENTIMENT erweitert
- Confidence-Formel: Sentiment-Gewichtung (±30%)
- Min. 3 Cluster vor Claude-Call
"""

import hashlib
import logging
import math
import re
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests
from requests.exceptions import RequestException, Timeout

logger = logging.getLogger(__name__)

DECAY_LAMBDA            = 0.18
VELOCITY_WINDOW_MIN     = 45
EARNINGS_K              = 0.5
EARNINGS_CUTOFF_DAYS    = 7
MIN_CLUSTERS_FOR_CLAUDE = 3

CREDIBILITY = {
    "Reuters_Markets":  0.92, "Bloomberg_Markets": 0.90, "Bloomberg_Finance": 0.88,
    "WSJ_Markets":      0.89,
    "CNBC_Finance":     0.85, "CNBC_Investing": 0.84, "CNBC_Earnings": 0.82,
    "CNBC_Economy":     0.78, "CNBC_WorldNews": 0.72,
    "Benzinga_Ratings": 0.74, "Benzinga_Insider": 0.73, "Benzinga_Options": 0.68,
    "Nasdaq_Markets":   0.75,
    "MarketWatch":      0.70,
    "Benzinga_Markets": 0.60, "Benzinga_Breaking": 0.45,
}
CREDIBILITY_DEFAULT = 0.65

FEEDS = [
    {"name": "Reuters_Markets",   "tier": 1, "url": "https://news.google.com/rss/search?q=site:reuters.com/business+OR+site:reuters.com/markets+when:1d&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Bloomberg_Markets", "tier": 1, "url": "https://news.google.com/rss/search?q=site:bloomberg.com/markets+OR+site:bloomberg.com/finance&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Bloomberg_Finance", "tier": 1, "url": "https://news.google.com/rss/search?q=site:bloomberg.com+finance"},
    {"name": "WSJ_Markets",       "tier": 1, "url": "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain"},
    {"name": "CNBC_Finance",      "tier": 1, "url": "https://www.cnbc.com/id/10000664/device/rss/rss.html"},
    {"name": "CNBC_Investing",    "tier": 1, "url": "https://www.cnbc.com/id/15839069/device/rss/rss.html"},
    {"name": "CNBC_Earnings",     "tier": 1, "url": "https://www.cnbc.com/id/15839135/device/rss/rss.html"},
    {"name": "Benzinga_Ratings",  "tier": 1, "url": "https://news.google.com/rss/search?q=site:benzinga.com/analyst-ratings&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Benzinga_Insider",  "tier": 1, "url": "https://news.google.com/rss/search?q=site:benzinga.com/news/insider-trades&hl=en-US&gl=US&ceid=US:en"},
    {"name": "CNBC_Economy",      "tier": 2, "url": "https://www.cnbc.com/id/20910258/device/rss/rss.html"},
    {"name": "CNBC_WorldNews",    "tier": 2, "url": "https://www.cnbc.com/id/100727362/device/rss/rss.html"},
    {"name": "MarketWatch",       "tier": 2, "url": "https://feeds.marketwatch.com/marketwatch/topstories/"},
    {"name": "Nasdaq_Markets",    "tier": 2, "url": "https://www.nasdaq.com/feed/rssoutbound?category=Markets"},
    {"name": "Benzinga_Options",  "tier": 2, "url": "https://news.google.com/rss/search?q=site:benzinga.com/markets/options&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Benzinga_Markets",  "tier": 3, "url": "https://news.google.com/rss/search?q=site:benzinga.com/markets&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Benzinga_Breaking", "tier": 3, "url": "https://news.google.com/rss/search?q=site:benzinga.com/news&hl=en-US&gl=US&ceid=US:en"},
]

KEYWORDS = {
    "earnings": 3, "upgrade": 3, "downgrade": 3, "merger": 2, "acquisition": 2,
    "guidance": 2, "fda": 3, "bankruptcy": 3, "layoffs": 2, "restructuring": 2,
    "beat": 2, "miss": 2, "raised": 2, "cut": 2, "recall": 2, "investigation": 2,
    "settlement": 2, "insider": 2, "options": 1, "dividend": 1, "buyback": 1,
    "ai": 3, "artificial intelligence": 3,
    "tariff": 3, "tariffs": 3, "trade war": 3,
    "fomc": 3, "rate cut": 3, "rate hike": 3,
    "rally": 2, "surge": 2,
    "default": 3,
}

KNOWN_TICKERS = {
    "AAPL","MSFT","GOOGL","GOOG","AMZN","NVDA","META","TSLA","BRK","JPM","V",
    "XOM","JNJ","WMT","PG","MA","HD","CVX","MRK","ABBV","PEP","KO",
    "AVGO","COST","MCD","TMO","ACN","ABT","DHR","LIN","TXN","PM",
    "NEE","UNH","LLY","CRM","ORCL","ADBE","NFLX","INTC","AMD","QCOM",
    "BA","GE","CAT","MMM","HON","UPS","FDX","LMT","RTX","NOC",
    "GS","MS","BAC","WFC","C","AXP","BLK","SCHW","USB","PNC",
    "DAL","UAL","AAL","LUV","CCL","RCL","MAR","HLT","MGM","WYNN",
    "CVS","WBA","CI","HUM","ELV","MCK","ABC","CAH",
    "DIS","CMCSA","T","VZ","PARA","WBD","FOX",
    "SPY","QQQ","IWM","DIA","GLD","SLV","USO","TLT",
    "UBER","LYFT","ABNB","SNAP","PINS","RBLX","HOOD","COIN",
    "PFE","BMY","GILD","BIIB","REGN","VRTX","MRNA","BNTX",
    "NKE","LULU","TGT","SBUX","CMG","YUM","DPZ",
    "F","GM","RIVN","LCID","TM","HMC","STLA",
    "PLTR","ARM","CRWD","MRVL","SMCI",
}

TICKER_ALIASES = {
    "J&J": "JNJ", "Johnson & Johnson": "JNJ",
    "Apple": "AAPL", "Microsoft": "MSFT",
    "Google": "GOOGL", "Alphabet": "GOOGL",
    "Amazon": "AMZN", "Tesla": "TSLA",
    "Meta": "META", "Nvidia": "NVDA",
    "Netflix": "NFLX",
    "JPMorgan": "JPM", "JP Morgan": "JPM",
    "Goldman": "GS", "Goldman Sachs": "GS",
    "Morgan Stanley": "MS",
    "Bank of America": "BAC", "Wells Fargo": "WFC",
    "Pfizer": "PFE", "Merck": "MRK", "AbbVie": "ABBV",
    "ExxonMobil": "XOM", "Exxon": "XOM", "Chevron": "CVX",
    "Boeing": "BA", "Disney": "DIS", "Walmart": "WMT",
    "Uber": "UBER", "Airbnb": "ABNB",
    "Coinbase": "COIN", "FedEx": "FDX", "Eli Lilly": "LLY",
    "Palantir": "PLTR",
    "CrowdStrike": "CRWD",
    "Marvell": "MRVL",
    "Intel": "INTC",
    "Super Micro": "SMCI", "Supermicro": "SMCI",
    "Broadcom": "AVGO",
    "ARM Holdings": "ARM",
}

MACRO_TICKER_MAP = {
    "fed": "TLT", "rate": "TLT", "rates": "TLT", "interest rate": "TLT",
    "fomc": "TLT", "treasury": "TLT",
    "oil": "USO", "crude": "USO", "opec": "USO",
    "gold": "GLD", "inflation": "GLD",
    "tariff": "SPY", "tariffs": "SPY",
    "recession": "SPY",
    "iran": "USO", "war": "GLD", "sanctions": "GLD",
    "hormuz": "USO", "strait": "USO", "shipper": "USO", "shipping": "USO",
    "middle east": "USO",
    "china": "SPY",
    "trade war": "SPY",
}

TICKER_PATTERN = re.compile(r'\b([A-Z]{2,5})\b')

SENTIMENT_POSITIVE = {
    "beat": 0.8, "beats": 0.8, "exceeded": 0.7, "record": 0.6,
    "upgrade": 0.9, "upgraded": 0.9, "outperform": 0.7, "buy": 0.6,
    "raised": 0.7, "strong buy": 0.9, "merger": 0.5, "acquisition": 0.5,
    "buyback": 0.6, "dividend": 0.5, "approved": 0.7, "growth": 0.5, "recovery": 0.6,
    "rally": 0.8, "surge": 0.8, "momentum": 0.7,
    "beat expectations": 0.9, "guidance raised": 0.85,
}
SENTIMENT_NEGATIVE = {
    "miss": 0.8, "misses": 0.8, "loss": 0.7, "decline": 0.6, "fell": 0.6,
    "downgrade": 0.9, "downgraded": 0.9, "underperform": 0.7, "sell": 0.7,
    "cut": 0.6, "layoffs": 0.7, "bankruptcy": 1.0, "recall": 0.7,
    "investigation": 0.7, "warning": 0.7, "guidance cut": 0.9, "recession": 0.8,
    "plunge": 0.85, "collapse": 0.85,
    "default": 1.0, "insolvency": 1.0,
    "crisis": 0.85,
    "tariff": 0.7, "trade war": 0.75,
}

PROMPT = """Du bist ein quantitativer Options-Analyst. Analysiere News-Cluster und gib direkt handelbare Signale aus.

AKTUELLE ZEIT ET: {market_time}
MARKT-STATUS: {market_status}

SCORE-FELDER:
- CONFIDENCE: gewichtet durch Decay x Velocity x Credibility x Earnings-Penalty x Sentiment
- DECAY: Frische (1.0=frisch | 0.5=3.9h alt | <0.1=veraltet)
- VELOCITY_MULT: >1.0 = Breaking-Signal (Fenster: 45 Minuten)
- EARNINGS_PENALTY: <0.5 = Earnings innerhalb 7 Tage
- SENTIMENT: positiv erhoeht Confidence bis +30%, negativ senkt bis -30%

FILTER (verwirf sofort):
- DECAY < 0.05 oder CONFIDENCE < 1.0
- EARNINGS_PENALTY < 0.15
- Ticker = UNKNOWN ohne klares Makro-Keyword (iran/oil/fed/gold/war/hormuz/china)
- ADRs: TM, TSM, NVO, BABA, ASML, SAP, BP, AZN, GSK
- Nur 1 Artikel UND FEED_TIER_MAX >= 3 UND CONFIDENCE < 1.5

BEHALTE IMMER:
- Einzelaktien-Events (Earnings, Upgrade, FDA, M&A, Insider, AI-News) mit CONFIDENCE >= 1.0
- ETF-Makro-Events (Fed->TLT, Oel->USO, Gold->GLD, Tarife->SPY) mit CONFIDENCE >= 1.5
- USO/TLT/GLD/SPY bei klarem geopolitischen Event IMMER wenn CONFIDENCE >= 2.0

RICHTUNG:
earnings_beat/upgrade/approval/insider_buy/ai_deal -> CALL
earnings_miss/downgrade/recall/bankruptcy/default -> PUT
fed_cut/macro_positiv -> CALL auf TLT oder SPY
fed_hold/oil_spike/iran/hormuz/krieg/trade_war -> PUT auf TLT, CALL auf USO/GLD
china_risk/tariff_escalation -> PUT auf SPY
Unklare Richtung -> ueberspringen

DTE nach Horizont:
T1 (Einzelaktie kurzfristig): 21DTE
T2 (mittelfristig): 45DTE
T3 (Makro/geopolitisch): 45DTE

OUTPUT — NUR DIESE EINE ZEILE:
Format: TICKER_SIGNALS:TICKER:RICHTUNG:SCORE:HORIZONT:DTE,...
Beispiel: TICKER_SIGNALS:USO:CALL:HIGH:T3:45DTE,PLTR:CALL:MED:T1:21DTE

Regeln:
- Max 8 Ticker | Sortiert HIGH->MED->LOW
- SCORE: HIGH (CONFIDENCE>=4) | MED (1.5-3.9) | LOW (1.0-1.4)
- Bei 0 validen Signalen: TICKER_SIGNALS:NONE
- ETFs bei Makro-Events bevorzugen"""


def decay_weight(age_minutes: float) -> float:
    return round(math.exp(-DECAY_LAMBDA * (age_minutes / 60.0)), 4)

def velocity_multiplier(articles: list) -> float:
    if not articles:
        return 1.0
    recent = sum(1 for a in articles if a.get("age_min", 9999) <= VELOCITY_WINDOW_MIN)
    ratio  = recent / len(articles)
    if ratio >= 0.50:   return 1.5
    elif ratio >= 0.25: return 1.25
    return 1.0

def credibility_multiplier(sources: list) -> float:
    if not sources:
        return CREDIBILITY_DEFAULT
    scores = [CREDIBILITY.get(s["name"], CREDIBILITY_DEFAULT) for s in sources]
    avg    = sum(scores) / len(scores)
    return round(max(0.3, min(1.0, (avg - 0.50) / (0.92 - 0.50))), 4)

def earnings_proximity_penalty(ticker: str, earnings_map: dict) -> float:
    if ticker not in earnings_map:
        return 1.0
    days    = earnings_map[ticker]
    sigmoid = 1.0 / (1.0 + math.exp(-EARNINGS_K * (days - EARNINGS_CUTOFF_DAYS)))
    return round(max(0.05, sigmoid), 4)

def calculate_sentiment(title: str, summary: str = "") -> float:
    text  = (title + " " + summary).lower()
    pos   = sum(w for p, w in SENTIMENT_POSITIVE.items() if p in text)
    neg   = sum(w for p, w in SENTIMENT_NEGATIVE.items() if p in text)
    total = pos + neg
    if total == 0:
        return 0.0
    return max(-1.0, min(1.0, round((pos - neg) / (total + 0.001), 2)))

def sentiment_multiplier(avg_sentiment: float) -> float:
    """±30% Einfluss auf Confidence. Neutral=1.0, positiv>1.0, negativ<1.0."""
    return round(max(0.5, min(1.5, 1.0 + 0.3 * avg_sentiment)), 4)

def get_market_context() -> tuple:
    now_utc = datetime.now(timezone.utc)
    offset  = -4 if 3 <= now_utc.month <= 11 else -5
    now_et  = now_utc + timedelta(hours=offset)
    hour    = now_et.hour + now_et.minute / 60
    weekday = now_et.weekday()
    if weekday >= 5:           status = "CLOSED-WEEKEND"
    elif 9.5 <= hour < 16.0:  status = "OPEN"
    elif 4.0 <= hour < 9.5:   status = "PRE-MARKET"
    elif 16.0 <= hour < 20.0: status = "AFTER-HOURS"
    else:                      status = "CLOSED"
    days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    return days[weekday] + " " + now_et.strftime("%H:%M") + " ET", status

def parse_pub_date(date_str: str) -> datetime:
    if not date_str:
        return datetime.now(timezone.utc)
    for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S GMT", "%Y-%m-%dT%H:%M:%SZ"]:
        try:
            return datetime.strptime(date_str.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return datetime.now(timezone.utc)


def fetch_one_feed(feed: dict) -> list:
    try:
        r = requests.get(feed["url"], timeout=4, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        root   = ET.fromstring(r.content)
        result = []
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        for item in root.findall(".//item")[:10]:
            title   = item.findtext("title", "").strip()
            summary = re.sub(r'<[^>]+>', '', item.findtext("description", ""))[:300].strip()
            pub_dt  = parse_pub_date(item.findtext("pubDate", ""))
            if pub_dt < cutoff or not title:
                continue
            age_min    = int((datetime.now(timezone.utc) - pub_dt).total_seconds() / 60)
            text_lower = (title + " " + summary).lower()
            kw_score, detected = 0, []
            for kw, weight in KEYWORDS.items():
                if kw in text_lower:
                    kw_score += weight
                    detected.append(kw)
            tickers = [t for t in TICKER_PATTERN.findall(title + " " + summary)
                       if t in KNOWN_TICKERS]
            for alias, sym in TICKER_ALIASES.items():
                if alias.lower() in (title + " " + summary).lower() and sym not in tickers:
                    tickers.append(sym)
            result.append({
                "hash":         hashlib.md5(title[:60].encode()).hexdigest()[:8],
                "title":        title,
                "summary":      summary[:200],
                "source":       feed["name"],
                "tier":         feed["tier"],
                "age_min":      age_min,
                "decay_weight": decay_weight(age_min),
                "kw_score":     kw_score,
                "keywords":     detected,
                "tickers":      list(set(tickers)),
                "sentiment":    calculate_sentiment(title, summary),
            })
        return result
    except (RequestException, Timeout) as e:
        logger.debug("Feed %s nicht erreichbar: %s", feed["name"], e)
        return []
    except ET.ParseError as e:
        logger.debug("Feed %s XML-Fehler: %s", feed["name"], e)
        return []
    except (KeyError, ValueError) as e:
        logger.debug("Feed %s Daten-Fehler: %s", feed["name"], e)
        return []


def fetch_all_feeds() -> list:
    all_articles, seen = [], set()
    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = [ex.submit(fetch_one_feed, feed) for feed in FEEDS]
        for f in as_completed(futures, timeout=10):
            try:
                for art in f.result():
                    if art["hash"] not in seen:
                        seen.add(art["hash"])
                        all_articles.append(art)
            except Exception as e:
                logger.debug("Feed-Future Fehler: %s", e)
    logger.info("%d Artikel aus %d Feeds geladen", len(all_articles), len(FEEDS))
    return all_articles


def build_earnings_map(finnhub_key: str) -> dict:
    if not finnhub_key:
        return {}
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        end   = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        r     = requests.get(
            "https://finnhub.io/api/v1/calendar/earnings",
            params={"from": today, "to": end, "token": finnhub_key},
            timeout=5,
        )
        r.raise_for_status()
        result = {}
        for e in r.json().get("earningsCalendar", []):
            sym, date = e.get("symbol",""), e.get("date","")
            if sym and date:
                try:
                    days = (datetime.strptime(date, "%Y-%m-%d") - datetime.now()).days
                    if sym not in result or days < result[sym]:
                        result[sym] = max(0, days)
                except ValueError:
                    pass
        logger.info("Earnings-Map: %d Ticker", len(result))
        return result
    except (RequestException, ValueError, KeyError) as e:
        logger.warning("Earnings-Map Fehler: %s", e)
        return {}


def cluster_articles(articles: list, earnings_map: dict) -> list:
    clusters = {}
    for art in articles:
        if art["tickers"] and art["keywords"]:
            base_key = art["tickers"][0] + "_" + art["keywords"][0]
        elif art["tickers"]:
            base_key = art["tickers"][0]
        elif art["keywords"]:
            base_key = art["keywords"][0]
        else:
            base_key = art["hash"]
        key = base_key
        if key not in clusters:
            fallback = "UNKNOWN"
            if not art["tickers"]:
                text_low = (art["title"] + " " + art["summary"]).lower()
                for kw, etf in MACRO_TICKER_MAP.items():
                    if kw in text_low:
                        fallback = etf
                        break
            clusters[key] = {
                "ticker":       art["tickers"][0] if art["tickers"] else fallback,
                "event_type":   art["keywords"][0] if art["keywords"] else "general",
                "articles":     [], "sources":      [],
                "min_age":      art["age_min"],
                "kw_score_sum": 0,
                "top_headline": art["title"],
            }
        c = clusters[key]
        c["articles"].append(art)
        c["sources"].append({"name": art["source"], "tier": art["tier"]})
        c["min_age"]       = min(c["min_age"], art["age_min"])
        c["kw_score_sum"] += art["kw_score"]

    result = []
    for key, c in clusters.items():
        n    = len(c["articles"])
        tier = min(s["tier"] for s in c["sources"])
        base = 0
        if n >= 4:                   base += 2
        elif n >= 2:                 base += 1
        if tier == 1:                base += 2
        elif tier == 2:              base += 1
        if c["min_age"] < 120:       base += 2
        elif c["min_age"] < 480:     base += 1
        if c["kw_score_sum"] >= 6:   base += 2
        elif c["kw_score_sum"] >= 3: base += 1

        decay_vals    = [a.get("decay_weight", 1.0) for a in c["articles"]]
        avg_decay     = sum(decay_vals) / len(decay_vals)
        vel_mult      = velocity_multiplier(c["articles"])
        cred_mult     = credibility_multiplier(c["sources"])
        ep_pen        = earnings_proximity_penalty(c["ticker"], earnings_map)
        sentiments    = [a.get("sentiment", 0.0) for a in c["articles"]]
        avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0.0
        sent_mult     = sentiment_multiplier(avg_sentiment)
        final_conf    = base * avg_decay * vel_mult * cred_mult * ep_pen * sent_mult

        result.append({
            "cluster_id":       key[:20],
            "ticker":           c["ticker"],
            "event_type":       c["event_type"],
            "artikel_anzahl":   n,
            "quellen":          list({s["name"] for s in c["sources"]}),
            "feed_tier_max":    tier,
            "alter_minuten":    c["min_age"],
            "sentiment_score":  round(avg_sentiment, 2),
            "sentiment_mult":   sent_mult,
            "headline_repr":    c["top_headline"][:120],
            "confidence_score": round(final_conf, 2),
            "decay_avg":        round(avg_decay, 3),
            "velocity_mult":    vel_mult,
            "credibility_mult": round(cred_mult, 3),
            "earnings_penalty": round(ep_pen, 3),
        })

    result.sort(key=lambda x: x["confidence_score"], reverse=True)
    return result[:12]


def format_clusters_for_claude(clusters: list) -> str:
    lines = []
    for c in clusters:
        lines.append(
            "CLUSTER_ID:" + c["cluster_id"] +
            " | TICKER:" + c["ticker"] +
            " | EVENT_TYPE:" + c["event_type"] +
            " | ARTIKEL_ANZAHL:" + str(c["artikel_anzahl"]) +
            " | FEED_TIER_MAX:" + str(c["feed_tier_max"]) +
            " | ALTER_MINUTEN:" + str(c["alter_minuten"]) +
            " | CONFIDENCE:" + str(c["confidence_score"]) +
            " | DECAY:" + str(c["decay_avg"]) +
            " | VELOCITY_MULT:" + str(c["velocity_mult"]) +
            " | SENTIMENT:" + str(c["sentiment_score"]) +
            " | EARNINGS_PENALTY:" + str(c["earnings_penalty"]) +
            ' | HEADLINE:"' + c["headline_repr"] + '"'
        )
    return "\n---\n".join(lines)


def run_claude(cluster_text: str, market_time: str, market_status: str,
               api_key: str, max_retries: int = 2) -> str:
    prompt = PROMPT.replace("{market_time}", market_time).replace("{market_status}", market_status)
    for attempt in range(max_retries):
        try:
            r = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key":         api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json",
                },
                json={
                    "model":      "claude-sonnet-4-6",
                    "max_tokens": 200,
                    "system":     prompt,
                    "messages":   [{"role": "user", "content": "Cluster:\n" + cluster_text}],
                },
                timeout=22,
            )
            r.raise_for_status()
            resp = r.json()
            if "content" not in resp or not resp["content"]:
                continue
            raw = resp["content"][0]["text"].strip()
            for line in raw.splitlines():
                line = line.strip()
                if line.startswith("TICKER_SIGNALS:"):
                    signals = re.findall(
                        r'([A-Z]{1,5}):(CALL|PUT):(HIGH|MED|LOW):(T1|T2|T3):(\d+DTE)', line
                    )
                    if signals or line == "TICKER_SIGNALS:NONE":
                        logger.info("Claude Signal: %s", line)
                        return line
        except (RequestException, Timeout) as e:
            logger.warning("Claude-Call Versuch %d fehlgeschlagen: %s", attempt + 1, e)
        except (KeyError, ValueError) as e:
            logger.warning("Claude-Response Parse-Fehler: %s", e)
    logger.warning("Keine validen Signale nach %d Versuchen", max_retries)
    return "TICKER_SIGNALS:NONE"


if __name__ == "__main__":
    import argparse
    from config_loader import load_config, validate_config

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="News Analyzer v3")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--output",  help="Signale in Datei speichern")
    args = parser.parse_args()

    cfg = load_config()
    if not validate_config(cfg):
        raise SystemExit("Konfiguration unvollständig")

    articles     = fetch_all_feeds()
    earnings_map = build_earnings_map(cfg.get("finnhub_key",""))
    clusters     = cluster_articles(articles, earnings_map)

    if len(clusters) < MIN_CLUSTERS_FOR_CLAUDE:
        logger.warning("Nur %d Cluster — min. %d erforderlich", len(clusters), MIN_CLUSTERS_FOR_CLAUDE)
        print("TICKER_SIGNALS:NONE")
    else:
        if args.verbose:
            for c in clusters[:5]:
                print(f"  [{c['confidence_score']:.2f}] {c['ticker']:8} "
                      f"sent={c['sentiment_score']:+.2f} {c['headline_repr'][:50]}")
        market_time, market_status = get_market_context()
        cluster_text   = format_clusters_for_claude(clusters)
        ticker_signals = run_claude(cluster_text, market_time, market_status,
                                    cfg.get("anthropic_api_key",""))
        print(ticker_signals)
        if args.output:
            with open(args.output, "w") as f:
                f.write(ticker_signals)
