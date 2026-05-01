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
SEC_UA = os.environ.get("SEC_USER_AGENT", "DailyOptionsReport/1.0 research@example.com")

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


def _headers() -> dict:
    return {"User-Agent": SEC_UA, "Accept-Encoding": "gzip, deflate", "Host": "data.sec.gov"}


def _archive_headers() -> dict:
    return {"User-Agent": SEC_UA, "Accept-Encoding": "gzip, deflate"}


def _get_json(url: str) -> Any:
    r = requests.get(url, headers=_headers(), timeout=12)
    r.raise_for_status()
    return r.json()


def _get_text(url: str) -> str:
    r = requests.get(url, headers=_archive_headers(), timeout=12)
    r.raise_for_status()
    return r.text


def _load_ticker_map() -> dict[str, int]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    try:
        if CIK_CACHE.exists():
            age = datetime.now(timezone.utc) - datetime.fromtimestamp(CIK_CACHE.stat().st_mtime, tz=timezone.utc)
            if age.days < 7:
                raw = json.loads(CIK_CACHE.read_text(encoding="utf-8"))
                return {v["ticker"].upper(): int(v["cik_str"]) for v in raw.values()}
    except Exception:
        pass

    raw = _get_json("https://www.sec.gov/files/company_tickers.json")
    CIK_CACHE.write_text(json.dumps(raw), encoding="utf-8")
    return {v["ticker"].upper(): int(v["cik_str"]) for v in raw.values()}


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
