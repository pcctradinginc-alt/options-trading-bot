"""
market_data.py — Marktdaten + Score-Berechnung (Step 2)

Fixes v3:
- Liquidität als harter Filter via check_liquidity() (nicht Malus)
- Fail-Closed: fehlende Bid/Ask/Midpoint → score=0, _liquidity_fail=True
- get_tradier_options() nutzt target_dte
- Logging statt print()
"""

import logging
import math
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from datetime import datetime, timedelta

import requests
from requests.exceptions import RequestException, Timeout

from rules import (
    RULES, check_liquidity, conservative_entry_price, estimate_fill_probability,
)
from market_calendar import market_elapsed_fraction

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

MARKET_OPEN_UTC_H            = 13.5
MARKET_CLOSE_UTC_H           = 20.0
VOLUME_EXTRAPOLATION_DELAY_H = 0.5


# ══════════════════════════════════════════════════════════
# HTTP HELPER
# ══════════════════════════════════════════════════════════

def robust_get(url, params=None, headers=None, timeouts=(6, 8, 10)):
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
# KURS-QUELLEN
# ══════════════════════════════════════════════════════════

def get_quote_alphavantage(symbol, api_key):
    try:
        if not api_key:
            return None
        r = robust_get("https://www.alphavantage.co/query",
                       params={"function": "GLOBAL_QUOTE", "symbol": symbol,
                               "apikey": api_key})
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
        return (round(price, 2),
                round(float(chg_str) if chg_str else 0.0, 2),
                round(float(q.get("03. high") or price), 2),
                round(float(q.get("04. low")  or price), 2),
                "alphavantage")
    except (ValueError, KeyError, RequestException) as e:
        logger.debug("AlphaVantage %s: %s", symbol, e)
        return None


def get_history_alphavantage(symbol, api_key):
    try:
        if not api_key:
            return [], []
        r = robust_get("https://www.alphavantage.co/query",
                       params={"function": "TIME_SERIES_DAILY", "symbol": symbol,
                               "outputsize": "compact", "apikey": api_key})
        if not r:
            return [], []
        ts = r.json().get("Time Series (Daily)", {})
        if not ts:
            return [], []
        sorted_days = sorted(ts.items())
        return ([float(v["4. close"])       for _, v in sorted_days if v.get("4. close")],
                [int(float(v["5. volume"])) for _, v in sorted_days if v.get("5. volume")])
    except (ValueError, KeyError, RequestException) as e:
        logger.debug("AlphaVantage history %s: %s", symbol, e)
        return [], []


def get_quote_yahoo_v8(symbol):
    try:
        r = None
        for host in ["query1", "query2"]:
            r = robust_get(
                "https://" + host + ".finance.yahoo.com/v8/finance/chart/" + symbol,
                params={"interval": "1d", "range": "5d"})
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
        return (round(price, 2), chg_pct,
                round(meta.get("regularMarketDayHigh", price), 2),
                round(meta.get("regularMarketDayLow",  price), 2),
                "yahoo_v8")
    except (ValueError, KeyError, IndexError, RequestException) as e:
        logger.debug("Yahoo v8 %s: %s", symbol, e)
        return None


def get_quote_finnhub(symbol, api_key):
    if not api_key:
        return None
    try:
        r = robust_get("https://finnhub.io/api/v1/quote",
                       params={"symbol": symbol, "token": api_key})
        if not r:
            return None
        j     = r.json()
        price = j.get("c", 0) or 0
        if price <= 0:
            return None
        return (round(price, 2), round(j.get("dp", 0) or 0, 2),
                round(j.get("h",  0) or 0, 2), round(j.get("l",  0) or 0, 2),
                "finnhub")
    except (ValueError, KeyError, RequestException) as e:
        logger.debug("Finnhub %s: %s", symbol, e)
        return None


def get_quote(symbol, cfg):
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


def get_history(symbol, cfg):
    closes, volumes = get_history_alphavantage(symbol, cfg.get("alpha_vantage_key",""))

    if len(closes) < 20:
        for host in ["query1", "query2"]:
            try:
                r = robust_get(
                    "https://" + host + ".finance.yahoo.com/v8/finance/chart/" + symbol,
                    params={"interval": "1d", "range": "90d"})
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

    try:
        fraction = market_elapsed_fraction()
        if fraction is not None and volumes:
            volumes = volumes.copy()
            volumes[-1] = int(volumes[-1] / max(0.1, fraction))
    except (ValueError, ZeroDivisionError) as e:
        logger.debug("Volumen-Hochrechnung %s: %s", symbol, e)

    source = "alphavantage" if cfg.get("alpha_vantage_key") else "yahoo"
    return closes, volumes, source


# ══════════════════════════════════════════════════════════
# SENTIMENT
# ══════════════════════════════════════════════════════════

def get_sentiment(symbol, change_pct, finnhub_key):
    if finnhub_key:
        try:
            r = robust_get("https://finnhub.io/api/v1/news-sentiment",
                           params={"symbol": symbol, "token": finnhub_key})
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

    bullish = round(max(0.0, min(100.0,
        55 + change_pct * 3 if change_pct > 0 else 45 + change_pct * 3)), 1)
    return bullish, round(100.0 - bullish, 1), round(abs(change_pct), 2), True


# ══════════════════════════════════════════════════════════
# VIX + EARNINGS
# ══════════════════════════════════════════════════════════

def get_vix():
    for host in ["query1", "query2"]:
        try:
            r = robust_get(
                "https://" + host + ".finance.yahoo.com/v8/finance/chart/%5EVIX",
                params={"interval": "1d", "range": "5d"})
            if not r:
                continue
            closes = [c for c in
                      r.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
                      if c is not None]
            if closes:
                return round(closes[-1], 2)
        except (ValueError, KeyError, IndexError, RequestException) as e:
            logger.debug("VIX %s: %s", host, e)
    logger.warning("VIX nicht verfügbar")
    return "n/v"


def get_earnings(start, end, finnhub_key):
    if not finnhub_key:
        return []
    try:
        r = robust_get("https://finnhub.io/api/v1/calendar/earnings",
                       params={"from": start, "to": end, "token": finnhub_key})
        if not r:
            return []
        return [e.get("symbol","") for e in r.json().get("earningsCalendar",[])
                if e.get("symbol")]
    except (ValueError, KeyError, RequestException) as e:
        logger.warning("Earnings-Kalender Fehler: %s", e)
        return []



# ══════════════════════════════════════════════════════════
# OPTIONS-EV / KOSTENMODELL
# ══════════════════════════════════════════════════════════

def calc_realized_volatility(closes: list, lookback: int = 20) -> float | None:
    """Annualisierte Realized Vol aus Schlusskursen, als Dezimalzahl."""
    if not closes or len(closes) < lookback + 1:
        return None
    rets = []
    recent = closes[-(lookback + 1):]
    for prev, cur in zip(recent[:-1], recent[1:]):
        if prev and prev > 0 and cur and cur > 0:
            rets.append(math.log(cur / prev))
    if len(rets) < 10:
        return None
    daily = statistics.stdev(rets)
    return max(0.05, min(2.0, daily * math.sqrt(252)))


def estimate_expected_move_pct(price: float, change_pct: float, rel_vol,
                               score: float, closes: list, target_dte: int) -> float:
    """
    Erwarteter Underlying-Move in Prozent für das Signal.
    Konservativ: nur ein Teil der historischen DTE-Vol wird als Edge akzeptiert.
    """
    if price <= 0:
        return 0.0
    rv = calc_realized_volatility(closes) or 0.35
    days = max(1, min(target_dte, RULES.ev_hold_days))
    vol_move_pct = rv * math.sqrt(days / 252.0) * 100.0
    intraday_impulse = abs(change_pct) * 1.15
    rel = 1.0
    try:
        rel = float(rel_vol) if rel_vol not in (None, "n/v") else 1.0
    except (ValueError, TypeError):
        rel = 1.0
    rel_mult = max(0.85, min(1.35, 0.85 + 0.20 * rel))
    score_mult = max(0.65, min(1.25, score / 70.0))
    expected = max(intraday_impulse, vol_move_pct * 0.65) * rel_mult * score_mult
    return round(max(0.3, min(12.0, expected)), 2)


def _safe_float(value, default=0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def evaluate_option_ev(option: dict, direction: str, underlying_price: float,
                       expected_move_pct: float) -> dict | None:
    """
    Bewertet eine einzelne Long-Option auf erwarteten Vorteil nach Kosten.
    Kein echtes Optionspreismodell, sondern robuster, konservativer Filter.
    """
    g = option.get("greeks") or {}
    bid = _safe_float(option.get("bid"))
    ask = _safe_float(option.get("ask"))
    if bid <= 0 or ask <= 0 or ask < bid:
        return None
    mid = round((bid + ask) / 2, 2)
    spread = ask - bid
    spread_pct = round(spread / ask * 100.0, 2) if ask > 0 else None
    strike = _safe_float(option.get("strike"))
    delta = _safe_float(g.get("delta"))
    gamma = _safe_float(g.get("gamma"))
    theta = _safe_float(g.get("theta"))
    iv = _safe_float(g.get("mid_iv"), None)
    oi = int(_safe_float(option.get("open_interest"), 0))
    volume = int(_safe_float(option.get("volume"), 0))

    opt_data = {
        "bid": bid, "ask": ask, "midpoint": mid, "spread_pct": spread_pct,
        "open_interest": oi, "volume": volume,
    }
    entry = conservative_entry_price(opt_data)
    if not entry:
        return None

    # Directional expected move im Underlying.
    move_abs = underlying_price * expected_move_pct / 100.0
    delta_gain = abs(delta) * move_abs
    gamma_gain = 0.5 * abs(gamma) * (move_abs ** 2)
    theta_cost = abs(theta) * RULES.ev_hold_days if theta else 0.0

    # Eintritts- und Exit-Slippage in Optionspreis-Punkten.
    entry_slippage = max(0.0, entry - mid)
    exit_slippage = spread * 0.35
    expected_option_gain = max(0.0, delta_gain + gamma_gain - theta_cost)
    ev_points = expected_option_gain - entry_slippage - exit_slippage
    ev_dollars = round(ev_points * 100.0, 2)
    ev_pct = round(ev_points / entry * 100.0, 2) if entry > 0 else -999.0

    if direction == "CALL":
        breakeven_move_pct = ((strike + entry - underlying_price) / underlying_price * 100.0
                              if underlying_price > 0 else 999.0)
    else:
        breakeven_move_pct = ((underlying_price - (strike - entry)) / underlying_price * 100.0
                              if underlying_price > 0 else 999.0)
    breakeven_move_pct = round(max(0.0, breakeven_move_pct), 2)

    fill_p = estimate_fill_probability(opt_data)
    ev_ok = (
        ev_pct >= RULES.min_option_ev_pct and
        ev_dollars >= RULES.min_option_ev_dollars and
        breakeven_move_pct <= expected_move_pct * 1.25 and
        fill_p >= RULES.min_fill_probability
    )

    delta_penalty = abs(abs(delta) - RULES.target_delta_abs) * 12.0
    liquidity_bonus = min(8.0, oi / 1000.0) + min(4.0, volume / 100.0)
    ev_score = round(ev_pct + liquidity_bonus - delta_penalty, 2)

    return {
        "direction": direction,
        "strike": option.get("strike"),
        "bid": bid,
        "ask": ask,
        "midpoint": mid,
        "conservative_entry": entry,
        "entry_price": entry,
        "spread_pct": spread_pct,
        "delta": g.get("delta"),
        "gamma": g.get("gamma"),
        "theta": g.get("theta"),
        "vega": g.get("vega"),
        "iv": round(iv * 100, 1) if iv else None,
        "open_interest": oi,
        "volume": volume,
        "fill_probability": fill_p,
        "expected_move_pct": expected_move_pct,
        "breakeven_move_pct": breakeven_move_pct,
        "ev_points": round(ev_points, 3),
        "ev_dollars": ev_dollars,
        "ev_pct": ev_pct,
        "ev_score": ev_score,
        "ev_ok": ev_ok,
        "contracts": None,
    }

# ══════════════════════════════════════════════════════════
# TRADIER OPTIONS
# ══════════════════════════════════════════════════════════

def get_tradier_options(symbol, direction, tradier_token,
                        sandbox=True, target_dte=21, underlying_price=0.0,
                        change_pct=0.0, closes=None, rel_vol=None,
                        signal_score=50.0) -> dict:
    try:
        if not tradier_token:
            return {}
        base = "https://sandbox.tradier.com" if sandbox else "https://api.tradier.com"
        hdrs = {"Authorization": "Bearer " + tradier_token, "Accept": "application/json"}

        r_exp = robust_get(base + "/v1/markets/options/expirations",
                           params={"symbol": symbol, "includeAllRoots": "true"},
                           headers=hdrs)
        if not r_exp:
            return {}
        exps = r_exp.json().get("expirations", {}).get("date", [])
        if not exps:
            return {}

        today_dt = datetime.now()
        target_exp = None
        best_diff = 999
        for exp in exps:
            days = (datetime.strptime(exp, "%Y-%m-%d") - today_dt).days
            if days < 7:
                continue
            diff = abs(days - target_dte)
            if diff < best_diff:
                best_diff = diff
                target_exp = exp

        if not target_exp:
            return {}

        r_chain = robust_get(base + "/v1/markets/options/chains",
                             params={"symbol": symbol, "expiration": target_exp,
                                     "greeks": "true"},
                             headers=hdrs)
        if not r_chain:
            return {}
        opts = r_chain.json().get("options", {}).get("option", [])
        if not opts:
            return {}

        opt_type = "call" if direction == "CALL" else "put"
        expected_move_pct = estimate_expected_move_pct(
            underlying_price, change_pct, rel_vol, signal_score, closes or [], target_dte
        )

        candidates = []
        for opt in opts:
            if opt.get("option_type") != opt_type:
                continue
            ev = evaluate_option_ev(opt, direction, underlying_price, expected_move_pct)
            if ev is None:
                continue
            ev["expiration"] = target_exp
            candidates.append(ev)

        if not candidates:
            return {}

        # Erst EV-positive, liquide Kandidaten. Fallback: höchster EV-Score für Diagnose.
        good = [c for c in candidates if c.get("ev_ok")]
        chosen_pool = good if good else candidates
        best = sorted(chosen_pool, key=lambda c: c.get("ev_score", -999), reverse=True)[0]
        best["candidate_count"] = len(candidates)
        best["ev_candidates_ok"] = len(good)
        return best

    except (ValueError, KeyError, RequestException) as e:
        logger.debug("Tradier Options %s: %s", symbol, e)
        return {}


# ══════════════════════════════════════════════════════════
# INDIKATOREN
# ══════════════════════════════════════════════════════════

def calc_ma(values, period):
    if len(values) < period:
        return None
    return round(sum(values[-period:]) / period, 2)


def calc_rel_volume(volumes):
    valid = [v for v in volumes if v is not None and v >= 0]
    if len(valid) < 21:
        return None
    avg_20 = sum(valid[-21:-1]) / 20
    if avg_20 <= 0:
        return None
    return round(valid[-1] / avg_20, 2)


# ══════════════════════════════════════════════════════════
# SCORE (0-100)
# ══════════════════════════════════════════════════════════

def calculate_score(price, change_pct, above_ma50, ma20,
                    direction, bullish, unusual, earnings_soon, is_etf):
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
# Fix: check_liquidity() als harter Filter
# ══════════════════════════════════════════════════════════

def process_ticker(ticker, direction, earnings_list, cfg,
                   target_dte: int = 21) -> dict:
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
                "_liquidity_fail": False, "_liquidity_reason": "",
            }

        bullish, bearish, buzz, sent_fallback = get_sentiment(
            ticker, change_pct, finnhub_key)
        rel_vol       = calc_rel_volume(volumes)
        unusual       = bool(rel_vol and rel_vol >= 1.5)
        ma50          = calc_ma(closes, 50)
        ma20          = calc_ma(closes, 20)
        above_ma50    = (price > ma50) if (ma50 is not None and price > 0) else None
        new_20d       = None
        if len(closes) >= 20 and price > 0:
            recent_high = max(closes[-20:])
            new_20d     = price >= recent_high * 0.98 if recent_high > 0 else None
        earnings_soon = ticker in earnings_list

        score, score_reason = calculate_score(
            price, change_pct, above_ma50, ma20, direction,
            bullish, unusual, earnings_soon, is_etf)

        options_data = get_tradier_options(
            ticker, direction,
            cfg.get("tradier_token", ""),
            cfg.get("tradier_sandbox", True),
            target_dte=target_dte,
            underlying_price=price,
            change_pct=change_pct,
            closes=closes,
            rel_vol=rel_vol,
            signal_score=score,
        )

        # Harter Liquiditäts- und EV-Filter
        is_liquid, liquidity_reason = check_liquidity(options_data)
        if not is_liquid:
            logger.info("%s: Liquiditäts-Filter: %s", ticker, liquidity_reason)
            score        = 0.0
            score_reason = "liquidity_fail"
        elif not options_data.get("ev_ok", False):
            logger.info("%s: EV-Filter: EV%%=%s EV$=%s BE=%s%% expMove=%s%%",
                        ticker, options_data.get("ev_pct"), options_data.get("ev_dollars"),
                        options_data.get("breakeven_move_pct"), options_data.get("expected_move_pct"))
            score        = 0.0
            score_reason = "option_ev_fail"
            is_liquid    = False
            liquidity_reason = "Options-EV nach Kosten nicht ausreichend"

        logger.info("%s: price=%.2f score=%.1f liquid=%s ev=%s src=%s dte=%d",
                    ticker, price, score, is_liquid, options_data.get("ev_pct"), quote_src, target_dte)

        return {
            "ticker":           ticker,
            "price":            price,
            "change_pct":       change_pct,
            "rel_vol":          str(rel_vol) if rel_vol is not None else "n/v",
            "unusual":          unusual,
            "ma50":             ma50,
            "ma20":             ma20,
            "above_ma50":       above_ma50,
            "new_20d_high":     new_20d,
            "trend_status":     ("über MA50" if above_ma50 is True
                                 else ("unter MA50" if above_ma50 is False else "n/v")),
            "bullish":          round(bullish, 1),
            "sent_fallback":    sent_fallback,
            "earnings_soon":    earnings_soon,
            "score":            score,
            "_score_reason":    score_reason,
            "_liquidity_fail":  not is_liquid,
            "_liquidity_reason": liquidity_reason,
            "options":          options_data,
            "news_direction":   direction,
            "is_etf":           is_etf,
            "_src_quote":       quote_src,
            "_src_hist":        hist_src,
            "_closes_count":    len(closes),
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
            "_liquidity_fail": True, "_liquidity_reason": "exception",
            "_error": str(e)[:120],
        }


# ══════════════════════════════════════════════════════════
# SUMMARY BUILDER
# ══════════════════════════════════════════════════════════

def build_summary(ranked, vix_value, ticker_directions,
                  earnings_list, unusual_list, failed):
    today    = datetime.now().strftime("%Y-%m-%d")
    srcs_str = ", ".join(d["ticker"] + "=" + d.get("_src_quote","?") for d in ranked)

    s  = "DATUM: " + today + "\n"
    s += "VIX: " + str(vix_value) + "\n"
    s += "NEWS-SIGNALE: " + (
        ", ".join(t + ":" + d for t, d in ticker_directions.items()) or "keine") + "\n"
    s += "EARNINGS NAECHSTE 10 TAGE: " + (
        ", ".join(earnings_list) if earnings_list else "Keine") + "\n"
    s += "UNUSUAL ACTIVITY (RelVol >= 1.5x): " + (
        ", ".join(unusual_list) or "Keiner") + "\n"
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
        liq_flag  = " ⛔" if d.get("_liquidity_fail") else ""

        s += (f"{d['ticker']:<6} | {kurs_str} | {d['change_pct']:>6.2f}% | "
              f"{str(d.get('ma50','n/v')):>7} | {d.get('trend_status','n/v'):<14} | "
              f"{high_str:<5} | {str(d['rel_vol']):>6}{'🔥' if d.get('unusual') else ''} | "
              f"{news_flag:>5} | {d['bullish']:>6.1f}% | "
              f"{d['score']:>6.2f}{liq_flag}\n")

        if d.get("_liquidity_fail") and d.get("_liquidity_reason"):
            s += "  ⛔ LIQUIDITÄT: " + d["_liquidity_reason"] + "\n"
        elif d.get("options"):
            opt = d["options"]
            s += ("  └─ OPTIONS: Strike=" + str(opt.get("strike","n/v")) +
                  " | Exp=" + str(opt.get("expiration","n/v")) +
                  " | Bid=" + str(opt.get("bid","n/v")) +
                  "/Ask=" + str(opt.get("ask","n/v")) +
                  " | Mid=" + str(opt.get("midpoint","n/v")) +
                  " | Entry=" + str(opt.get("conservative_entry","n/v")) +
                  " | Delta=" + str(opt.get("delta","n/v")) +
                  " | IV=" + str(opt.get("iv","n/v")) + "%" +
                  " | OI=" + str(opt.get("open_interest","n/v")) +
                  " | FillP=" + str(opt.get("fill_probability","n/v")) +
                  " | EV%=" + str(opt.get("ev_pct","n/v")) +
                  " | EV$=" + str(opt.get("ev_dollars","n/v")) +
                  " | EV_OK=" + str(opt.get("ev_ok", False)) + "\n")

    s += "\nSENTIMENT-FALLBACK: " + (
        ", ".join(d["ticker"] for d in ranked if d.get("sent_fallback")) or "keiner"
    )
    return s


# ══════════════════════════════════════════════════════════
# DIREKTE AUSFÜHRUNG
# ══════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    import re
    from config_loader import load_config, validate_config
    from rules import parse_ticker_signals

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Market Data Fetcher")
    parser.add_argument("--signals",      help="Ticker-Signale")
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

    parsed            = parse_ticker_signals(raw)
    ticker_directions = {s["ticker"]: s["direction"] for s in parsed}
    dte_map           = {s["ticker"]: s["dte_days"]  for s in parsed}
    tickers           = list(ticker_directions.keys())

    finnhub_key = cfg.get("finnhub_key", "")
    today       = datetime.now().strftime("%Y-%m-%d")
    end         = (datetime.now() + timedelta(days=10)).strftime("%Y-%m-%d")

    with ThreadPoolExecutor(max_workers=2) as ex:
        vix_fut       = ex.submit(get_vix)
        earnings_fut  = ex.submit(get_earnings, today, end, finnhub_key)
        vix_value     = vix_fut.result(timeout=12)
        earnings_list = earnings_fut.result(timeout=12)

    with ThreadPoolExecutor(max_workers=RULES.max_tickers) as ex:
        futures = {
            ex.submit(process_ticker, t, ticker_directions[t],
                      earnings_list, cfg, dte_map.get(t, 21)): t
            for t in tickers
        }
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

    summary = build_summary(ranked, vix_value, ticker_directions,
                            earnings_list, unusual_list, failed)
    print(summary)
    if args.output:
        with open(args.output, "w") as f:
            f.write(summary)
