"""
sec_check.py — strukturierter SEC EDGAR Catalyst-Check ohne API-Key.

Datenquellen:
- https://www.sec.gov/files/company_tickers.json
- https://data.sec.gov/submissions/CIK##########.json
- Filing-Dokumente aus sec.gov/Archives

Ziel:
- Form 4 differenzierter: Kauf != Award != Optionsausübung != Steuerverkauf.
- 8-K nach Items/Keywords klassifizieren.
- Fail-safe: bei Fehler neutral.
- Bonus: get_company_name_to_ticker() liefert Name->Ticker-Mapping
  für News-Headline-Auflösung (z.B. "Apple reports..." -> "AAPL").
"""

from __future__ import annotations

import json
import logging
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
CIK_CACHE = DATA_DIR / "sec_company_tickers.json"
SEC_UA = (os.environ.get("SEC_USER_AGENT") or "DailyOptionsReport/1.0 contact@example.com").strip()

ETF_TICKERS = {
    "TLT", "USO", "GLD", "SLV", "GDX", "SPY", "QQQ", "IWM", "DIA",
    "XLE", "XLF", "XLK", "XLV", "XLI", "XLU", "XLP", "XLY", "XLB", "XLRE",
}

BEARISH_8K_KEYWORDS = {
    "restatement": 0.75,
    "material weakness": 0.85,
    "going concern": 0.90,
    "bankruptcy": 0.95,
    "default": 0.80,
    "delisting": 0.80,
    "investigation": 0.65,
    "sec subpoena": 0.80,
    "class action": 0.55,
    "securities fraud": 0.80,
    "ceo resignation": 0.60,
    "cfo resignation": 0.60,
    "impairment": 0.65,
    "restructuring charge": 0.60,
}

BULLISH_8K_KEYWORDS = {
    "acquisition": 0.55,
    "merger agreement": 0.65,
    "definitive agreement": 0.55,
    "share repurchase": 0.55,
    "buyback": 0.55,
    "dividend increase": 0.50,
    "fda approval": 0.75,
    "fda clearance": 0.65,
    "accelerated approval": 0.80,
    "strategic partnership": 0.50,
    "licensing agreement": 0.55,
    "record revenue": 0.45,
    "record earnings": 0.50,
}

EMPTY_RESULT = {
    "bullish": False,
    "bearish": False,
    "insider_buy": False,
    "insider_sell": False,
    "reason": "Keine SEC-Daten",
    "confidence": 0.0,
    "filings_checked": 0,
    "events": [],
}

# ==================== NAME → TICKER MAPPING (Konstanten) ====================

# Suffixe, die beim Normalisieren von Firmennamen entfernt werden
_CORP_SUFFIXES = {
    "inc", "corp", "corporation", "incorporated", "co", "company",
    "ltd", "limited", "llc", "plc", "lp", "lllp",
    "holdings", "holding", "group", "trust",
    "sa", "ag", "nv", "bv", "spa", "kgaa",
    "common", "stock", "ordinary", "shares",
    "class", "a", "b", "c", "adr", "ads",
    "the",
}

# Hand-kuratierte Aliase haben Vorrang vor der SEC-Map.
# Hier landen Marketing-Namen ("Google" statt "Alphabet"), Klassenwahl
# (BRK.B liquider als BRK.A), alte Firmennamen ("Facebook" -> META) und
# "Rettungsanker" für kurze Ticker oder Ein-Buchstaben-Symbole, die sonst
# durch die Sicherheitsregeln im Resolver fallen würden.
COMPANY_NAME_OVERRIDES = {
    # --- TECH & GROWTH ---
    "alphabet": "GOOGL",
    "google": "GOOGL",
    "meta platforms": "META",
    "meta": "META",
    "facebook": "META",
    "apple": "AAPL",
    "microsoft": "MSFT",
    "amazon": "AMZN",
    "nvidia": "NVDA",
    "tesla": "TSLA",
    "netflix": "NFLX",
    "salesforce": "CRM",
    "oracle": "ORCL",
    "adobe": "ADBE",
    "palantir": "PLTR",
    "shopify": "SHOP",
    "spotify": "SPOT",
    "uber": "UBER",
    "airbnb": "ABNB",
    "lyft": "LYFT",
    "doordash": "DASH",
    "door dash": "DASH",
    "super micro": "SMCI",
    "supermicro": "SMCI",
    "snowflake": "SNOW",
    "crowdstrike": "CRWD",
    "palo alto networks": "PANW",

    # --- CHIPS & HARDWARE ---
    "advanced micro devices": "AMD",
    "amd": "AMD",
    "intel": "INTC",
    "broadcom": "AVGO",
    "qualcomm": "QCOM",
    "taiwan semiconductor": "TSM",
    "tsmc": "TSM",
    "asml": "ASML",
    "arm holdings": "ARM",
    "applied materials": "AMAT",
    "ibm": "IBM",
    "dell": "DELL",

    # --- FINANCE ---
    "jpmorgan": "JPM",
    "jp morgan": "JPM",
    "jpmorgan chase": "JPM",
    "goldman sachs": "GS",
    "morgan stanley": "MS",
    "bank of america": "BAC",
    "wells fargo": "WFC",
    "citigroup": "C",            # 1-Buchstabe-Ticker, Override-Privileg
    "visa": "V",                 # 1-Buchstabe-Ticker, Override-Privileg
    "mastercard": "MA",
    "paypal": "PYPL",
    "robinhood": "HOOD",
    "coinbase": "COIN",
    "blackrock": "BLK",
    "charles schwab": "SCHW",

    # --- RETAIL & CONSUMER ---
    "walmart": "WMT",
    "costco": "COST",
    "home depot": "HD",
    "lowes": "LOW",
    "nike": "NKE",
    "starbucks": "SBUX",
    "mcdonalds": "MCD",
    "coca cola": "KO",
    "coca-cola": "KO",
    "pepsi": "PEP",
    "pepsico": "PEP",
    "procter and gamble": "PG",  # nach &-Normalisierung
    "p and g": "PG",
    "estee lauder": "EL",
    "lululemon": "LULU",
    "ford": "F",                 # 1-Buchstabe-Ticker, Override-Privileg
    "general motors": "GM",
    "ebay": "EBAY",

    # --- ENERGY & INDUSTRIAL ---
    "exxon": "XOM",
    "exxon mobil": "XOM",
    "exxonmobil": "XOM",
    "chevron": "CVX",
    "shell": "SHEL",
    "boeing": "BA",
    "lockheed martin": "LMT",
    "raytheon": "RTX",
    "general electric": "GE",
    "ge aerospace": "GE",
    "ge healthcare": "GEHC",
    "ge vernova": "GEV",
    "caterpillar": "CAT",

    # --- HEALTHCARE & PHARMA ---
    "pfizer": "PFE",
    "eli lilly": "LLY",
    "johnson and johnson": "JNJ",  # nach &-Normalisierung
    "j and j": "JNJ",
    "merck": "MRK",
    "unitedhealth": "UNH",
    "cigna": "CI",
    "moderna": "MRNA",
    "abbvie": "ABBV",
    "amgen": "AMGN",
    "gilead": "GILD",
    "astrazeneca": "AZN",
    "novo nordisk": "NVO",

    # --- TELECOM, MEDIA, SPECIALS ---
    "disney": "DIS",
    "walt disney": "DIS",
    "at and t": "T",             # nach &-Normalisierung
    "verizon": "VZ",
    "t-mobile": "TMUS",
    "berkshire hathaway": "BRK.B",
    "berkshire": "BRK.B",
}

# Generische Wörter, die als Firmenname zu False-Positives führen.
# Liste wird laufend erweitert basierend auf beobachteten Match-Fehlern.
_NAME_BLOCKLIST = {
    # Generische Geo/Größe-Adjektive
    "global", "international", "american", "national", "general",
    "first", "new", "us", "usa", "united", "world",
    "the", "and", "of", "for",
    # Penny-Stock-Falle: Ticker existiert, Wort ist aber zu häufig
    "block", "match", "snap", "square", "trade", "city", "state",
    "here", "there", "this", "that", "them",
    "viking", "emerging", "target",
    # Sektor-/Branchenwörter, die News oft enthalten
    "strategy",  # MSTR seit Umbenennung von MicroStrategy
    "energy", "tech", "financial", "industrial", "consumer",
    "media", "data", "research", "services", "solutions",
    "systems", "products", "technologies", "innovations",
    # Rohstoff-Begriffe — Headlines über Preise, nicht über die Firmen
    "coffee", "cocoa", "wheat", "corn", "oil", "gold", "silver",
    "copper", "platinum", "uranium", "lithium", "nickel",
    "natural gas", "crude",
    # Investment-Vokabular
    "capital", "equity", "fund", "income", "growth", "value",
    "dividend", "premium", "core", "alpha", "beta",
    # Generische Akronyme — meinen in Headlines fast immer das Konzept,
    # nicht den gleichnamigen Ticker (z.B. "AI" statt C3.ai)
    "ai", "it", "ip", "ev", "ceo", "cfo", "cto", "ipo",
    "api", "saas", "esg", "ar", "vr", "ml",
}

# Modul-weiter Cache, vermeidet wiederholtes Parsen der SEC-Datei
_cached_name_map: dict[str, str] | None = None
_cached_cik_map: dict[int, str] | None = None


def _headers() -> dict:
    # Kein manueller Host-Header: derselbe Helper wird fuer www.sec.gov
    # und data.sec.gov genutzt. Ein falscher Host verursacht 403.
    return {
        "User-Agent": SEC_UA,
        "Accept-Encoding": "gzip, deflate",
        "Accept": "application/json,text/plain,*/*",
    }


def _archive_headers() -> dict:
    return {
        "User-Agent": SEC_UA,
        "Accept-Encoding": "gzip, deflate",
        "Accept": "application/xml,text/html,text/plain,*/*",
    }


def _get_json(url: str) -> Any:
    r = requests.get(url, headers=_headers(), timeout=12)
    r.raise_for_status()
    return r.json()


def _get_text(url: str) -> str:
    r = requests.get(url, headers=_archive_headers(), timeout=12)
    r.raise_for_status()
    return r.text


def _load_sec_raw_tickers() -> dict:
    """Lädt company_tickers.json (mit 7-Tage-Cache). Zentrale Helper-Funktion,
    damit _load_ticker_map und get_company_name_to_ticker dieselbe Logik nutzen.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if CIK_CACHE.exists():
        age = datetime.now(timezone.utc) - datetime.fromtimestamp(
            CIK_CACHE.stat().st_mtime, tz=timezone.utc)
        if age.days < 7:
            return json.loads(CIK_CACHE.read_text(encoding="utf-8"))

    raw = _get_json("https://www.sec.gov/files/company_tickers.json")
    CIK_CACHE.write_text(json.dumps(raw), encoding="utf-8")
    return raw


def _load_ticker_map() -> dict[str, int]:
    try:
        raw = _load_sec_raw_tickers()
        return {v["ticker"].upper(): int(v["cik_str"]) for v in raw.values()}
    except Exception as e:
        logger.warning("Ticker-Map konnte nicht geladen werden: %s", e)
        return {}


def _filing_url(cik: int, accession: str, primary_doc: str) -> str:
    cik_plain = str(cik)
    acc_plain = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_plain}/{acc_plain}/{primary_doc}"


def _recent_filings(cik: int) -> list[dict]:
    cik10 = str(cik).zfill(10)
    data = _get_json(f"https://data.sec.gov/submissions/CIK{cik10}.json")
    recent = data.get("filings", {}).get("recent", {})
    keys = ["form", "filingDate", "accessionNumber", "primaryDocument", "items", "primaryDocDescription"]
    n = len(recent.get("form", []))
    rows = []
    for i in range(n):
        rows.append({k: (recent.get(k, [None] * n)[i] if i < len(recent.get(k, [])) else None) for k in keys})
    return rows


def _within_days(date_str: str, days_back: int) -> bool:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return d >= datetime.now(timezone.utc) - timedelta(days=days_back)
    except Exception:
        return False


def _xml_text(root: ET.Element, tag: str) -> str:
    # SEC XML nutzt Namespaces teils inkonsistent; suffix match ist robuster.
    for el in root.iter():
        if el.tag.lower().endswith(tag.lower()):
            return (el.text or "").strip()
    return ""


def _iter_form4_transactions(xml_text: str) -> list[dict]:
    try:
        root = ET.fromstring(xml_text.encode("utf-8", errors="ignore"))
    except ET.ParseError:
        return []

    txns = []
    for txn in root.iter():
        if not txn.tag.lower().endswith("nonderivativetransaction"):
            continue
        code = ""
        shares = 0.0
        price = 0.0
        footnote_text = ""
        for el in txn.iter():
            name = el.tag.lower()
            text = (el.text or "").strip()
            if name.endswith("transactioncode"):
                code = text.upper()
            elif name.endswith("transactionshares"):
                try: shares = float(text)
                except Exception: pass
            elif name.endswith("transactionpricepershare"):
                try: price = float(text)
                except Exception: pass
            elif "footnote" in name:
                footnote_text += " " + text.lower()
        txns.append({"code": code, "shares": shares, "price": price, "value": shares * price, "footnotes": footnote_text})
    return txns


def _classify_form4(text: str) -> list[dict]:
    events = []
    for txn in _iter_form4_transactions(text):
        code = txn["code"]
        value = txn["value"]
        shares = txn["shares"]
        foot = txn.get("footnotes", "")
        tenb51 = "10b5" in foot

        # P = Open-market purchase: klar bullisher als Awards.
        if code == "P" and value >= 50_000:
            events.append({
                "type": "insider_purchase",
                "bullish": True,
                "bearish": False,
                "confidence": min(0.9, 0.55 + value / 2_000_000),
                "reason": f"Form 4 Insider-Kauf ${value:,.0f}",
            })
        # S = Sale. Nur große Verkäufe bearish; 10b5-1 wird gedämpft.
        elif code == "S" and value >= 1_000_000:
            conf = min(0.65, 0.30 + value / 10_000_000)
            if tenb51:
                conf *= 0.5
            events.append({
                "type": "insider_sale_10b5" if tenb51 else "insider_sale",
                "bullish": False,
                "bearish": conf >= 0.35,
                "confidence": round(conf, 2),
                "reason": f"Form 4 Insider-Verkauf ${value:,.0f}" + (" 10b5-1" if tenb51 else ""),
            })
        # A/M/F sind meist Award, Option Exercise, Tax Withholding → nicht als Alpha-Signal werten.
        elif code in {"A", "M", "F", "G"} and shares > 0:
            events.append({
                "type": f"neutral_form4_{code}",
                "bullish": False,
                "bearish": False,
                "confidence": 0.05,
                "reason": f"Form 4 neutral Code {code}",
            })
    return events


def _classify_8k(text: str, filing: dict) -> list[dict]:
    low = (text[:250_000] + " " + str(filing.get("items", "")) + " " + str(filing.get("primaryDocDescription", ""))).lower()
    events = []
    for kw, conf in BEARISH_8K_KEYWORDS.items():
        if kw in low:
            events.append({"type": "8k_bearish", "bullish": False, "bearish": True, "confidence": conf, "reason": f"8-K Warnsignal: {kw}"})
            break
    for kw, conf in BULLISH_8K_KEYWORDS.items():
        if kw in low:
            events.append({"type": "8k_bullish", "bullish": True, "bearish": False, "confidence": conf, "reason": f"8-K Katalysator: {kw}"})
            break
    return events


def get_sec_signal(ticker: str, days_back: int = 14) -> dict:
    if ticker in ETF_TICKERS:
        return {**EMPTY_RESULT, "reason": "ETF — kein SEC-Check"}

    try:
        ticker_map = _load_ticker_map()
        cik = ticker_map.get(ticker.upper())
        if not cik:
            return {**EMPTY_RESULT, "reason": "Ticker nicht in SEC Map"}

        filings = [f for f in _recent_filings(cik) if _within_days(str(f.get("filingDate", "")), days_back)]
        events = []
        checked = 0

        for f in filings[:30]:
            form = str(f.get("form", ""))
            if form not in {"4", "8-K"}:
                continue
            primary = f.get("primaryDocument")
            accession = f.get("accessionNumber")
            if not primary or not accession:
                continue
            checked += 1
            try:
                text = _get_text(_filing_url(cik, accession, primary))
                if form == "4":
                    events.extend(_classify_form4(text))
                elif form == "8-K":
                    events.extend(_classify_8k(text, f))
            except Exception as e:
                logger.debug("SEC Dokument %s %s Fehler: %s", ticker, form, e)
                continue

        bullish_events = [e for e in events if e.get("bullish")]
        bearish_events = [e for e in events if e.get("bearish")]
        bullish_conf = max([e.get("confidence", 0.0) for e in bullish_events] or [0.0])
        bearish_conf = max([e.get("confidence", 0.0) for e in bearish_events] or [0.0])

        bullish = bullish_conf > bearish_conf and bullish_conf >= 0.45
        bearish = bearish_conf > bullish_conf and bearish_conf >= 0.45
        confidence = max(bullish_conf, bearish_conf)
        if bullish_conf and bearish_conf:
            confidence *= 0.65

        top_events = sorted(events, key=lambda e: e.get("confidence", 0), reverse=True)[:4]
        reason = " | ".join(e.get("reason", "") for e in top_events) or "Keine relevanten Filings"

        result = {
            "bullish": bullish,
            "bearish": bearish,
            "insider_buy": any(e.get("type") == "insider_purchase" for e in events),
            "insider_sell": any(str(e.get("type", "")).startswith("insider_sale") for e in events),
            "reason": reason,
            "confidence": round(confidence, 2),
            "filings_checked": checked,
            "events": top_events,
        }
        if checked:
            logger.info("SEC %s: %d Filings | bull=%s bear=%s | %s", ticker, checked, bullish, bearish, reason[:80])
        return result

    except Exception as e:
        logger.warning("SEC-Check %s fehlgeschlagen: %s", ticker, e)
        return {**EMPTY_RESULT, "reason": f"SEC-Fehler: {str(e)[:60]}"}


# ==================== NAME → TICKER MAPPING (Funktionen) ====================

def _normalize_company_name(name: str) -> str:
    """'Apple Inc.' -> 'apple'
       'BERKSHIRE HATHAWAY INC /DE/' -> 'berkshire hathaway'
       'AT&T INC' -> 'at and t'
       'Johnson & Johnson' -> 'johnson and johnson'
    """
    s = name.lower()
    s = s.replace("&", " and ")                 # AT&T -> at and t
    s = re.sub(r"/[a-z]{2,3}/", " ", s)         # /DE/, /MD/, /NY/ Suffixe
    s = re.sub(r"[^a-z0-9\s\-]", " ", s)        # Punkte, Kommas raus
    s = re.sub(r"\s+", " ", s).strip()
    tokens = s.split()
    # Suffix-Tokens am Ende abschneiden, solange welche da sind
    while tokens and tokens[-1] in _CORP_SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


def get_company_name_to_ticker() -> dict[str, str]:
    """Liefert Mapping 'apple' -> 'AAPL', 'microsoft' -> 'MSFT', etc.

    Quelle: bereits gecachte SEC-Datei sec_company_tickers.json + Overrides.
    Bei Konflikt gewinnt der Override. Bei doppelten SEC-Einträgen mit
    gleichem normalisierten Namen gewinnt der erste (= meist die Haupt-Aktienklasse).

    Modul-weiter Cache verhindert mehrfaches Parsen der ~1 MB SEC-Datei.
    """
    global _cached_name_map
    if _cached_name_map is not None:
        return _cached_name_map

    name_map: dict[str, str] = {}

    try:
        raw = _load_sec_raw_tickers()
        for v in raw.values():
            ticker = (v.get("ticker") or "").upper().strip()
            title = (v.get("title") or "").strip()
            if not ticker or not title:
                continue
            normalized = _normalize_company_name(title)
            if not normalized or len(normalized) < 4:
                continue
            if normalized in _NAME_BLOCKLIST:
                continue
            # Erster gewinnt (vermeidet, dass z.B. BRK.A später BRK.B überschreibt)
            if normalized not in name_map:
                name_map[normalized] = ticker
    except Exception as e:
        logger.warning("SEC Name-Map konnte nicht geladen werden: %s", e)

    # Overrides drüberlegen — die haben immer Vorrang
    name_map.update(COMPANY_NAME_OVERRIDES)

    _cached_name_map = name_map
    logger.info("Name->Ticker Mapping: %d Einträge geladen", len(name_map))
    return name_map


def get_cik_to_ticker_map() -> dict[int, str]:
    """Liefert Mapping CIK -> Ticker fuer SEC EDGAR Filings-Aufloesung.

    Beispiel: 320193 -> "AAPL", 1318605 -> "TSLA"

    Hintergrund: Der SEC-EDGAR-Atom-Feed identifiziert Firmen ueber CIK
    (Central Index Key), nicht ueber Ticker. Wenn der News-Bot 8-K-Filings
    aus dem SEC-Feed verarbeiten will, braucht er die Inverse der
    Ticker->CIK-Map, die _load_ticker_map() bereits liefert.

    Caveat: Mehrere Tickers koennen denselben CIK haben (z.B. BRK.A und BRK.B
    teilen den Berkshire-CIK). Hier gewinnt der erste Eintrag in der SEC-Datei,
    was praktisch oft die Klasse-A-Aktie ist. Fuer Trading-Zwecke ist das
    suboptimal (BRK.B ist liquider), deshalb sollten kritische Faelle ueber
    COMPANY_NAME_OVERRIDES nachgesteuert werden.
    """
    global _cached_cik_map
    if _cached_cik_map is not None:
        return _cached_cik_map

    cik_map: dict[int, str] = {}
    try:
        raw = _load_sec_raw_tickers()
        for v in raw.values():
            ticker = (v.get("ticker") or "").upper().strip()
            cik = v.get("cik_str")
            if not ticker or cik is None:
                continue
            try:
                cik_int = int(cik)
            except (TypeError, ValueError):
                continue
            # Erster gewinnt; Override-Tickers haetten hier keine Wirkung,
            # weil die SEC-Map nur primaere Tickers liefert.
            if cik_int not in cik_map:
                cik_map[cik_int] = ticker
    except Exception as e:
        logger.warning("CIK->Ticker Map konnte nicht geladen werden: %s", e)
        return {}

    _cached_cik_map = cik_map
    logger.info("CIK->Ticker Mapping: %d Eintraege geladen", len(cik_map))
    return cik_map
