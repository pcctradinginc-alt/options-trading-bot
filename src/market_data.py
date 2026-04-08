"""
market_data.py — Marktdaten + Score-Berechnung (Step 2)

Fixes gegenüber v1:
- Intraday-Volumen-Hochrechnung erst ab 10:30 ET (nicht 09:30)
- ThreadPoolExecutor mit explizitem cancel() bei Timeout
- Bare excepts durch spezifische Exceptions ersetzt
- Score=0 disambiguiert: '_score_reason' Feld hinzugefügt
- ZeroDivision Guards explizit
- Logging statt print()
"""

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from datetime import datetime, timedelta

import requests
from requests.exceptions import RequestException, Timeout

logger = logging.getLogger(__name__)

ETF_TICKERS = {
    'TLT','USO','GLD','SLV','GDX','SPY','QQQ','IWM','DIA',
    'XLE','XLF','XLK','XLV','XLI','XLU','XLP','XLY','XLB','XLRE',
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
    "python-requests/2.31.0",
]

# Marktöffnung in UTC: 09:30 ET = 13:30 UTC (Sommerzeit) / 14:30 UTC (Winterzeit)
# Volumen-Hochrechnung erst ab 30min nach Öffnung (Opening Auction abgeklungen)
VOLUME_EXTRAPOLATION_DELAY_H = 0.5   # 30 Minuten nach Öffnung
MARKET_OPEN_UTC_H             = 13.5  # 09:30 ET in UTC (Sommerzeit)
MARKET_CLOSE_UTC_H            = 20.0  # 16:00 ET in UTC


# ══════════════════════════════════════════════════════════
# HTTP HELPER
# ══════════════════════════════════════════════════════════

def robust_get(url: str, params=None, headers=None, timeouts=(6, 8, 10)):
    for i, timeout in enumerate(timeouts):
        try:
            h = {"User-Agent": USER_AGENTS[i % len(USER_AGENTS)]}
            if headers:
                h.update(headers)
            r = requests.get(url, params=params, headers=h, timeout=timeout)
            if r.status_code == 200:
                return r
        except (RequestException, Timeout):
            pass
    return None


# ══════════════════════════════════════════════════════════
# KURS-QUELLEN (Fallback-Kette)
# ══════════════════════════════════════════════════════════

def get_quote_alphavantage(symbol: str, api_key: str):
    try:
        if not api_key:
            return None
        r = robust_get(
            "https://www.alphavantage.co/query",
            params={"function": "GLOBAL_QUOTE", "symbol": symbol, "apikey": api_key},
        )
        if not r:
            return None
        q = r.json().get("Global Quote", {})
        if not q:
            return None
        price_str = q.get("05. price", "0")
        price     = float(price_str) if price_str else 0.0
        if price <= 0:
            return None
        chg_str = q.get("10. change percent", "0%").replace("%", "")
        return (
            round(price, 2),
            round(float(chg_str) if chg_str else 0.0, 2),
            round(float(q.get("03. high") or price), 2),
            round(float(q.get("04. low")  or price), 2),
            "alphavantage",
        )
    except (ValueError, KeyError, RequestException) as e:
        logger.debug("AlphaVantage %s: %s", symbol, e)
        return None


def get_history_alphavantage(symbol: str, api_key: str):
    try:
        if not api_key:
            return [], []
        r = robust_get(
            "https://www.alphavantage.co/query",
            params={"function": "TIME_SERIES_DAILY", "symbol": symbol,
                    "outputsize": "compact", "apikey": api_key},
        )
        if not r:
            return [], []
        ts = r.json().get("Time Series (Daily)", {})
        if not ts:
            return [], []
        sorted_days = sorted(ts.items())
        closes  = [float(v["4. close"])       for _, v in sorted_days if v.get("4. close")]
        volumes = [int(float(v["5. volume"])) for _, v in sorted_days if v.get("5. volume")]
        return closes, volumes
    except (ValueError, KeyError, RequestException) as e:
        logger.debug("AlphaVantage history %s: %s", symbol, e)
        return [], []


def get_quote_yahoo_v8(symbol: str):
    try:
        r = None
        for host in ["query1", "query2"]:
            r = robust_get(
                "https://" + host + ".finance.yahoo.com/v8/finance/chart/" + symbol,
                params={"interval": "1d", "range": "5d"},
            )
            if r:
                break
        if not r:
            return None
        meta  = r.json()["chart"]["result"][0].get("meta", {})
        price = meta.get("regularMarketPrice") or meta.get("previousClose", 0)
        prev  = meta.get("chartPreviousClose") or meta.get("previousClose", price)
        if not price or price <= 0:
            return None
        chg_pct = round((price - prev) / prev * 100, 2) if prev and prev != 0 else 0.0
        return (
            round(price, 2), chg_pct,
            round(meta.get("regularMarketDayHigh", price), 2),
            round(meta.get("regularMarketDayLow",  price), 2),
            "yahoo_v8",
        )
    except (ValueError, KeyError, IndexError, RequestException) as e:
        logger.debug("Yahoo v8 %s: %s", symbol, e)
        return None


def get_quote_finnhub(symbol: str, api_key: str):
    if not api_key:
        return None
    try:
        r = robust_get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": symbol, "token": api_key},
        )
        if not r:
            return None
        j     = r.json()
        price = j.get("c", 0) or 0
        if price <= 0:
            return None
        return (
            round(price, 2),
            round(j.get("dp", 0) or 0, 2),
            round(j.get("h",  0) or 0, 2),
            round(j.get("l",  0) or 0, 2),
            "finnhub",
        )
    except (ValueError, KeyError, RequestException) as e:
        logger.debug("Finnhub %s: %s", symbol, e)
        return None


def get_quote(symbol: str, cfg: dict):
    """Fallback-Kette: AlphaVantage → Yahoo v8 → Finnhub."""
    for fn, args in [
        (get_quote_alphavantage, (symbol, cfg.get("alpha_vantage_key",""))),
        (get_quote_yahoo_v8,     (symbol,)),
        (get_quote_finnhub,      (symbol, cfg.get("finnhub_key",""))),
    ]:
        result = fn(*args)
        if result:
            return result
    logger.warning("Alle Kurs-Quellen für %s fehlgeschlagen", symbol)
    return (0.0, 0.0, 0.0, 0.0, "failed")


def get_history(symbol: str, cfg: dict):
    """
    Historische Daten: AlphaVantage → Yahoo.
    Fix: Intraday-Hochrechnung erst ab 30min nach Marktöffnung.
    """
    closes, volumes = get_history_alphavantage(symbol, cfg.get("alpha_vantage_key",""))

    if len(closes) < 20:
        # Yahoo Fallback
        for host in ["query1", "query2"]:
            try:
                r = robust_get(
                    "https://" + host + ".finance.yahoo.com/v8/finance/chart/" + symbol,
                    params={"interval": "1d", "range": "90d"},
                )
                if not r:
                    continue
                quotes  = r.json()["chart"]["result"][0]["indicators"]["quote"][0]
                closes  = [c for c in quotes.get("close",  []) if c is not None]
                volumes = [v for v in quotes.get("volume", []) if v is not None]
                if closes:
                    break
            except (ValueError, KeyError, IndexError, RequestException) as e:
                logger.debug("Yahoo history %s %s: %s", host, symbol, e)
                continue

    if not closes:
        return [], [], "failed"

    # Fix R1: Volumen-Hochrechnung erst ab 10:00 ET (30min nach Öffnung)
    # Verhindert 20x-Multiplikator in den ersten Minuten
    try:
        now_utc_h = datetime.utcnow().hour + datetime.utcnow().minute / 60.0
        market_open_with_delay = MARKET_OPEN_UTC_H + VOLUME_EXTRAPOLATION_DELAY_H  # 14.0 UTC = 10:00 ET

        if market_open_with_delay <= now_utc_h < MARKET_CLOSE_UTC_H and volumes:
            elapsed  = now_utc_h - MARKET_OPEN_UTC_H
            fraction = max(0.1, elapsed / 6.5)   # min 10% statt 5% — verhindert >10x Multiplikator
            volumes  = volumes.copy()             # kein In-Place-Mutieren der Original-Liste
            volumes[-1] = int(volumes[-1] / fraction)
    except (ValueError, ZeroDivisionError) as e:
        logger.debug("Volumen-Hochrechnung %s: %s", symbol, e)

    source = "alphavantage" if cfg.get("alpha_vantage_key") else "yahoo"
    return closes, volumes, source


# ══════════════════════════════════════════════════════════
# SENTIMENT
# Fix: Unsichere change_pct-Fallback-Propagation markiert
# ══════════════════════════════════════════════════════════

def get_sentiment(symbol: str, change_pct: float, finnhub_key: str):
    """
    Gibt (bullish, bearish, buzz, fallback_used) zurück.
    fallback_used=True wenn Sentiment aus change_pct abgeleitet wurde
    (weniger zuverlässig als Finnhub-Daten).
    """
    if finnhub_key:
        try:
            r = robust_get(
                "https://finnhub.io/api/v1/news-sentiment",
                params={"symbol": symbol, "token": finnhub_key},
            )
            if r:
                j       = r.json()
                sent    = j.get("sentiment", {}) or {}
                bullish = float(sent.get("bullishPercent", 0) or 0)
                bearish = float(sent.get("bearishPercent", 0) or 0)
                buzz    = float((j.get("buzz", {}) or {}).get("buzz", 0) or 0)
                if bullish > 0 or bearish > 0:
                    return bullish, bearish, buzz, False
        except (ValueError, KeyError, RequestException) as e:
            logger.debug("Finnhub Sentiment %s: %s", symbol, e)

    # Fallback aus change_pct — markiert als unzuverlässig
    bullish = round(max(0.0, min(100.0, 55 + change_pct * 3 if change_pct > 0 else 45 + change_pct * 3)), 1)
    return bullish, round(100.0 - bullish, 1), round(abs(change_pct), 2), True


# ══════════════════════════════════════════════════════════
# VIX + EARNINGS
# ══════════════════════════════════════════════════════════

def get_vix():
    for host in ["query1", "query2"]:
        try:
            r = robust_get(
                "https://" + host + ".finance.yahoo.com/v8/finance/chart/%5EVIX",
                params={"interval": "1d", "range": "5d"},
            )
            if not r:
                continue
            closes = [
                c for c in
                r.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
                if c is not None
            ]
            if closes:
                return round(closes[-1], 2)
        except (ValueError, KeyError, IndexError, RequestException) as e:
            logger.debug("VIX %s: %s", host, e)
    logger.warning("VIX nicht verfügbar")
    return "n/v"


def get_earnings(start: str, end: str, finnhub_key: str) -> list:
    if not finnhub_key:
        return []
    try:
        r = robust_get(
            "https://finnhub.io/api/v1/calendar/earnings",
            params={"from": start, "to": end, "token": finnhub_key},
        )
        if not r:
            return []
        return [e.get("symbol","") for e in r.json().get("earningsCalendar",[]) if e.get("symbol")]
    except (ValueError, KeyError, RequestException) as e:
        logger.warning("Earnings-Kalender Fehler: %s", e)
        return []


# ══════════════════════════════════════════════════════════
# TRADIER OPTIONS
# ══════════════════════════════════════════════════════════

def get_tradier_options(symbol: str, direction: str, tradier_token: str, sandbox: bool = True) -> dict:
    try:
        if not tradier_token:
            return {}
        base = "https://sandbox.tradier.com" if sandbox else "https://api.tradier.com"
        hdrs = {"Authorization": "Bearer " + tradier_token, "Accept": "application/json"}

        r_exp = robust_get(
            base + "/v1/markets/options/expirations",
            params={"symbol": symbol, "includeAllRoots": "true"},
            headers=hdrs,
        )
        if not r_exp:
            return {}
        exps = r_exp.json().get("expirations", {}).get("date", [])
        if not exps:
            return {}

        today_dt   = datetime.now()
        target_exp = None
        for exp in exps:
            days = (datetime.strptime(exp, "%Y-%m-%d") - today_dt).days
            if 21 <= days <= 35:
                target_exp = exp
                break
        if not target_exp:
            for exp in exps:
                if (datetime.strptime(exp, "%Y-%m-%d") - today_dt).days >= 21:
                    target_exp = exp
                    break
        if not target_exp:
            return {}

        r_chain = robust_get(
            base + "/v1/markets/options/chains",
            params={"symbol": symbol, "expiration": target_exp, "greeks": "true"},
            headers=hdrs,
        )
        if not r_chain:
            return {}
        opts = r_chain.json().get("options", {}).get("option", [])
        if not opts:
            return {}

        opt_type  = "call" if direction == "CALL" else "put"
        best      = None
        best_diff = 999.0
        for opt in opts:
            if opt.get("option_type") != opt_type:
                continue
            delta = (opt.get("greeks") or {}).get("delta")
            if delta is None:
                continue
            diff = abs(abs(float(delta)) - 0.45)
            if diff < best_diff:
                best_diff = diff
                best      = opt
        if not best:
            return {}

        g   = best.get("greeks") or {}
        bid = float(best.get("bid", 0) or 0)
        ask = float(best.get("ask", 0) or 0)
        mid = round((bid + ask) / 2, 2) if bid > 0 and ask > 0 else None

        return {
            "direction":     direction,
            "expiration":    target_exp,
            "strike":        best.get("strike"),
            "bid":           bid,
            "ask":           ask,
            "midpoint":      mid,
            "spread_pct":    round((ask - bid) / ask * 100, 2) if ask > 0 else None,
            "delta":         g.get("delta"),
            "gamma":         g.get("gamma"),
            "theta":         g.get("theta"),
            "vega":          g.get("vega"),
            "iv":            round(g.get("mid_iv", 0) * 100, 1) if g.get("mid_iv") else None,
            "open_interest": best.get("open_interest"),
            "volume":        best.get("volume"),
            "contracts":     None,  # wird dynamisch in report_generator berechnet
        }
    except (ValueError, KeyError, RequestException) as e:
        logger.debug("Tradier Options %s: %s", symbol, e)
        return {}


# ══════════════════════════════════════════════════════════
# INDIKATOREN
# ══════════════════════════════════════════════════════════

def calc_ma(values: list, period: int):
    if len(values) < period:
        return None
    window = values[-period:]
    return round(sum(window) / period, 2)

def calc_rel_volume(volumes: list):
    """
    Fix R11: Explizite Null-Behandlung.
    volumes[-1] = 0 ist valide (Handelsschluss) — wird nicht gefiltert.
    Nur None-Werte werden ausgeschlossen.
    """
    valid = [v for v in volumes if v is not None and v >= 0]
    if len(valid) < 21:
        return None
    avg_20 = sum(valid[-21:-1]) / 20
    if avg_20 <= 0:
        return None
    return round(valid[-1] / avg_20, 2)


# ══════════════════════════════════════════════════════════
# SCORE (normalisiert 0-100)
# ══════════════════════════════════════════════════════════

def calculate_score(price: float, change_pct: float, above_ma50, ma20,
                    direction: str, bullish: float, unusual: bool,
                    earnings_soon: bool, is_etf: bool) -> tuple:
    """
    Gibt (score, reason) zurück.
    Fix R3: reason erklärt warum score=0 — disambiguiert 'kein Kurs' vs 'schlechte Qualität'.
    """
    if price <= 0:
        return 0.0, "no_price"

    base     = 50.0
    momentum = min(30.0, abs(change_pct) * 8)

    trend_bonus = 0.0
    if direction == "CALL" and above_ma50 is True:
        trend_bonus = 20.0
    elif direction == "PUT" and above_ma50 is False:
        trend_bonus = 20.0

    direction_malus = 0.0
    if direction == "CALL" and change_pct < -0.5:
        direction_malus = -35.0
    elif direction == "PUT" and change_pct > 0.5:
        direction_malus = -35.0

    extra = 0.0
    if not is_etf:
        extra += bullish * 0.3
        extra += 20.0 if unusual else 0.0
        extra -= 50.0 if earnings_soon else 0.0

    etf_roc_bonus = 0.0
    if is_etf and ma20 and price > 0:
        roc = (price - ma20) / ma20 * 100
        if direction == "CALL" and roc > 0:
            etf_roc_bonus = min(15.0, roc * 2)
        elif direction == "PUT" and roc < 0:
            etf_roc_bonus = min(15.0, abs(roc) * 2)

    raw   = base + momentum + trend_bonus + direction_malus + extra + etf_roc_bonus
    score = round(max(0.0, min(100.0, raw)), 2)
    return score, "calculated"


# ══════════════════════════════════════════════════════════
# TICKER VERARBEITUNG
# Fix R4: ThreadPoolExecutor mit cancel() bei Timeout
# ══════════════════════════════════════════════════════════

def process_ticker(ticker: str, direction: str, earnings_list: list, cfg: dict) -> dict:
    is_etf      = ticker in ETF_TICKERS
    finnhub_key = cfg.get("finnhub_key", "")

    q_fut: Future = None
    h_fut: Future = None

    try:
        executor = ThreadPoolExecutor(max_workers=2)
        q_fut = executor.submit(get_quote,   ticker, cfg)
        h_fut = executor.submit(get_history, ticker, cfg)

        try:
            price, change_pct, high, low, quote_src = q_fut.result(timeout=12)
        except TimeoutError:
            logger.warning("%s: Kurs-Timeout", ticker)
            q_fut.cancel()
            price, change_pct, high, low, quote_src = 0.0, 0.0, 0.0, 0.0, "timeout"

        try:
            closes, volumes, hist_src = h_fut.result(timeout=12)
        except TimeoutError:
            logger.warning("%s: History-Timeout", ticker)
            h_fut.cancel()
            closes, volumes, hist_src = [], [], "timeout"

        executor.shutdown(wait=False)

        # ETF ohne Kurs → Sofort-Return
        if is_etf and price <= 0:
            return {
                "ticker": ticker, "price": 0.0, "change_pct": 0.0,
                "score": 0.0, "_score_reason": "etf_no_price",
                "options": {}, "news_direction": direction,
                "is_etf": True, "etf_no_data": True,
                "_src_quote": quote_src, "_closes_count": 0,
                "rel_vol": "n/v", "unusual": False, "ma50": None, "ma20": None,
                "above_ma50": None, "new_20d_high": None, "trend_status": "n/v",
                "bullish": 50.0, "sent_fallback": True, "earnings_soon": False,
            }

        bullish, bearish, buzz, sent_fallback = get_sentiment(ticker, change_pct, finnhub_key)

        rel_vol    = calc_rel_volume(volumes)
        unusual    = bool(rel_vol and rel_vol >= 1.5)
        ma50       = calc_ma(closes, 50)
        ma20       = calc_ma(closes, 20)
        above_ma50 = (price > ma50) if (ma50 is not None and price > 0) else None

        # 20-Tage-Hoch: nur wenn ausreichend Daten
        new_20d = None
        if len(closes) >= 20 and price > 0:
            recent_high = max(closes[-20:])
            new_20d     = price >= recent_high * 0.98 if recent_high > 0 else None

        earnings_soon = ticker in earnings_list

        score, score_reason = calculate_score(
            price, change_pct, above_ma50, ma20, direction,
            bullish, unusual, earnings_soon, is_etf,
        )

        options_data = get_tradier_options(
            ticker, direction,
            cfg.get("tradier_token", ""),
            cfg.get("tradier_sandbox", True),
        )

        # Liquiditäts-Malus — nach Options-Call
        if options_data and price > 0:
            spread_pct = options_data.get("spread_pct") or 999
            open_int   = options_data.get("open_interest") or 0
            if spread_pct > 12 or open_int < 150:
                score        = round(max(0.0, score - 40.0), 2)
                score_reason = "liquidity_malus"

        logger.info("%s: price=%.2f score=%.1f src=%s", ticker, price, score, quote_src)

        return {
            "ticker":         ticker,
            "price":          price,
            "change_pct":     change_pct,
            "rel_vol":        str(rel_vol) if rel_vol is not None else "n/v",
            "unusual":        unusual,
            "ma50":           ma50,
            "ma20":           ma20,
            "above_ma50":     above_ma50,
            "new_20d_high":   new_20d,
            "trend_status":   ("über MA50" if above_ma50 is True
                               else ("unter MA50" if above_ma50 is False else "n/v")),
            "bullish":        round(bullish, 1),
            "sent_fallback":  sent_fallback,
            "earnings_soon":  earnings_soon,
            "score":          score,
            "_score_reason":  score_reason,
            "options":        options_data,
            "news_direction": direction,
            "is_etf":         is_etf,
            "_src_quote":     quote_src,
            "_src_hist":      hist_src,
            "_closes_count":  len(closes),
        }

    except Exception as e:
        logger.error("%s: Unerwarteter Fehler: %s", ticker, e)
        if q_fut: q_fut.cancel()
        if h_fut: h_fut.cancel()
        return {
            "ticker": ticker, "price": 0.0, "change_pct": 0.0,
            "score": 0.0, "_score_reason": "exception",
            "options": {}, "news_direction": direction,
            "_src_quote": "error", "_closes_count": 0,
            "rel_vol": "n/v", "unusual": False, "ma50": None, "ma20": None,
            "above_ma50": None, "new_20d_high": None, "trend_status": "n/v",
            "bullish": 40.0, "sent_fallback": True, "earnings_soon": False,
            "_error": str(e)[:120],
        }


# ══════════════════════════════════════════════════════════
# SUMMARY BUILDER
# ══════════════════════════════════════════════════════════

def build_summary(ranked: list, vix_value, ticker_directions: dict,
                  earnings_list: list, unusual_list: list, failed: list) -> str:
    today    = datetime.now().strftime("%Y-%m-%d")
    srcs_str = ", ".join(d["ticker"] + "=" + d.get("_src_quote","?") for d in ranked)

    s  = "DATUM: " + today + "\n"
    s += "VIX: " + str(vix_value) + "\n"
    s += "NEWS-SIGNALE: " + (", ".join(t + ":" + d for t, d in ticker_directions.items()) or "keine") + "\n"
    s += "EARNINGS NAECHSTE 10 TAGE: " + (", ".join(earnings_list) if earnings_list else "Keine") + "\n"
    s += "UNUSUAL ACTIVITY (RelVol >= 1.5x): " + (", ".join(unusual_list) or "Keiner") + "\n"
    s += "TOP 3: " + ", ".join(d["ticker"] for d in ranked[:3]) + "\n"
    s += "QUOTE-QUELLEN: " + srcs_str + "\n"
    if failed:
        s += "API-FEHLER (Kurs=0): " + ", ".join(failed) + "\n"

    s += "\nMARKTDATEN (sortiert nach Score):\n"
    s += (f"{'Ticker':<6} | {'Kurs':>7} | {'Δ%':>6} | {'MA50':>7} | "
          f"{'Trend':<14} | {'20dH':<5} | {'RelVol':>7} | {'News':>5} | "
          f"{'Bull%':>6} | {'Score':>6}\n" + "-" * 110 + "\n")

    for d in ranked:
        if d.get("etf_no_data"):
            s += (d["ticker"].ljust(6) + " | ETF-SIGNAL | Richtung: " +
                  d["news_direction"] + " | Score: 0\n")
            continue
        news_flag = ("📈" if d["news_direction"] == "CALL" else "📉") + d["news_direction"]
        kurs_str  = f"{d['price']:>7.2f}" if d["price"] > 0 else "   n/v!"
        high_str  = ("JA" if d.get("new_20d_high") is True
                     else ("nein" if d.get("new_20d_high") is False else "n/v"))
        s += (f"{d['ticker']:<6} | {kurs_str} | {d['change_pct']:>6.2f}% | "
              f"{str(d.get('ma50','n/v')):>7} | {d.get('trend_status','n/v'):<14} | "
              f"{high_str:<5} | {str(d['rel_vol']):>6}{'🔥' if d.get('unusual') else ''} | "
              f"{news_flag:>5} | {d['bullish']:>6.1f}% | {d['score']:>6.2f}\n")
        if d.get("options"):
            opt = d["options"]
            s += ("  └─ OPTIONS: Strike=" + str(opt.get("strike","n/v")) +
                  " | Exp=" + str(opt.get("expiration","n/v")) +
                  " | Bid=" + str(opt.get("bid","n/v")) +
                  "/Ask=" + str(opt.get("ask","n/v")) +
                  " | Delta=" + str(opt.get("delta","n/v")) +
                  " | IV=" + str(opt.get("iv","n/v")) + "%" +
                  " | OI=" + str(opt.get("open_interest","n/v")) + "\n")

    s += "\nSENTIMENT-FALLBACK: " + (
        ", ".join(d["ticker"] for d in ranked if d.get("sent_fallback")) or "keiner"
    )
    return s


# ══════════════════════════════════════════════════════════
# DIREKTE AUSFÜHRUNG
# ══════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    from config_loader import load_config, validate_config

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Market Data Fetcher")
    parser.add_argument("--signals",      help="Ticker-Signale z.B. 'UBER:CALL:MED:T1:21DTE'")
    parser.add_argument("--signals-file", help="Datei mit Signalen")
    parser.add_argument("--output",       help="Market Summary speichern")
    args = parser.parse_args()

    cfg = load_config()
    if not validate_config(cfg):
        raise SystemExit("Konfiguration unvollständig")

    raw = ""
    if args.signals:
        raw = args.signals
    elif args.signals_file:
        with open(args.signals_file) as f:
            raw = f.read().strip()
    else:
        raise SystemExit("--signals oder --signals-file erforderlich")

    if "TICKER_SIGNALS:" in raw:
        raw = raw[raw.index("TICKER_SIGNALS:") + len("TICKER_SIGNALS:"):]

    ticker_directions = {}
    tickers = []
    for entry in raw.split(","):
        parts = re.split(r'[:\[]', entry.strip())
        if len(parts) >= 2:
            sym, d = parts[0].strip().upper(), parts[1].strip().upper()
            if sym and d in ("CALL", "PUT"):
                tickers.append(sym)
                ticker_directions[sym] = d
    tickers = list(dict.fromkeys(tickers))[:12]

    finnhub_key = cfg.get("finnhub_key", "")
    today       = datetime.now().strftime("%Y-%m-%d")
    end         = (datetime.now() + timedelta(days=10)).strftime("%Y-%m-%d")

    with ThreadPoolExecutor(max_workers=2) as ex:
        vix_fut       = ex.submit(get_vix)
        earnings_fut  = ex.submit(get_earnings, today, end, finnhub_key)
        vix_value     = vix_fut.result(timeout=12)
        earnings_list = earnings_fut.result(timeout=12)

    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = {ex.submit(process_ticker, t, ticker_directions[t], earnings_list, cfg): t
                   for t in tickers}
        results = []
        for f in as_completed(futures, timeout=30):
            try:
                results.append(f.result())
            except Exception as e:
                logger.error("Ticker-Future Fehler: %s", e)

    market_data  = [r for r in results if r]
    ranked       = sorted(market_data, key=lambda x: x["score"], reverse=True)
    unusual_list = [d["ticker"] for d in market_data if d.get("unusual")]
    failed       = [d["ticker"] for d in market_data if d.get("_src_quote") == "failed"]

    summary = build_summary(ranked, vix_value, ticker_directions, earnings_list, unusual_list, failed)
    print(summary)
    if args.output:
        with open(args.output, "w") as f:
            f.write(summary)
