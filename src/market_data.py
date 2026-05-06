"""
market_data.py — Marktdaten + Score-Berechnung (Step 2)

v12 Final Production Version
- Robuste Gap + RVOL Validierung mit korrekter Trend-Direction-Confirmation
- Einheitliche RVOL-Berechnung
- Bonus nur bei echter High-Conviction (smoother Penalty)
- Kein Double-Counting mit old unusual logic
- Separate raw_score / final_score für Backtesting
"""

import logging
import math
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from datetime import datetime, timedelta, timezone

import requests
from requests.exceptions import RequestException, Timeout

from rules import (
    RULES, check_liquidity, conservative_entry_price, estimate_fill_probability,
    exit_slippage_points, check_data_quality, check_earnings_iv_gate, merge_reasons,
    build_time_stop_plan,
)
from market_calendar import market_elapsed_fraction
from data_validator import (
    validate_ohlcv_history, detect_unexplained_price_spike,
    data_flags_to_text, realized_volatility,
)
from sector_map import evaluate_sector_filter

logger = logging.getLogger(__name__)

ETF_TICKERS = {
    'TLT','USO','GLD','SLV','GDX','SPY','QQQ','IWM','DIA',
    'XLE','XLF','XLK','XLV','XLI','XLU','XLP','XLY','XLB','XLRE','XLC','SMH','SOXX',
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
    "python-requests/2.31.0",
]

MARKET_OPEN_UTC_H            = 13.5
MARKET_CLOSE_UTC_H           = 20.0
VOLUME_EXTRAPOLATION_DELAY_H = 0.5


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
# KURS-QUELLEN (unverändert)
# ══════════════════════════════════════════════════════════

def get_quote_tradier(symbol, tradier_token, sandbox=False):
    if not tradier_token:
        return None
    try:
        base = "https://sandbox.tradier.com" if sandbox else "https://api.tradier.com"
        hdrs = {"Authorization": "Bearer " + tradier_token, "Accept": "application/json"}
        r = robust_get(base + "/v1/markets/quotes",
                       params={"symbols": symbol, "greeks": "false"},
                       headers=hdrs)
        if not r:
            return None
        q = (r.json().get("quotes") or {}).get("quote")
        if isinstance(q, list):
            q = q[0] if q else None
        if not q:
            return None
        price = q.get("last") or q.get("close") or q.get("bid") or q.get("ask")
        if not price or float(price) <= 0:
            return None
        prev = q.get("prevclose") or q.get("open") or price
        chg_pct = ((float(price) - float(prev)) / float(prev) * 100.0) if prev else 0.0
        high = q.get("high") or price
        low = q.get("low") or price
        return (round(float(price), 2), round(float(chg_pct), 2),
                round(float(high), 2), round(float(low), 2), "tradier_sandbox" if sandbox else "tradier_production")
    except (ValueError, KeyError, RequestException) as e:
        logger.debug("Tradier quote %s: %s", symbol, e)
        return None


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
    sources = [
        (get_quote_tradier, (symbol, cfg.get("tradier_token", ""), cfg.get("tradier_sandbox", False))),
        (get_quote_alphavantage, (symbol, cfg.get("alpha_vantage_key",""))),
        (get_quote_yahoo_v8,     (symbol,)),
        (get_quote_finnhub,      (symbol, cfg.get("finnhub_key",""))),
    ]
    for fn, args in sources:
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

    source = "alphavantage" if cfg.get("alpha_vantage_key") else "yahoo"
    return closes, volumes, source


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


def classify_sentiment_price_reaction(direction: str, bullish: float, bearish: float,
                                      change_pct: float, sent_fallback: bool) -> dict:
    direction = (direction or "").upper()
    b = float(bullish or 0.0)
    br = float(bearish or 0.0)
    gap = b - br
    label = "neutral"
    score_adjustment = 0.0
    confidence = "low" if sent_fallback else "medium"

    if br - b >= 15 and change_pct >= -0.20:
        label = "bearish_news_absorbed"
        confidence = "medium" if not sent_fallback else "low"
        score_adjustment = 5.0 if direction == "CALL" else -5.0
    elif b - br >= 15 and change_pct <= 0.10:
        label = "bullish_news_not_confirmed"
        confidence = "medium" if not sent_fallback else "low"
        score_adjustment = 5.0 if direction == "PUT" else -6.0
    elif gap >= 20 and change_pct > 0.40:
        label = "bullish_confirmed"
        score_adjustment = 3.0 if direction == "CALL" else -3.0
    elif gap <= -20 and change_pct < -0.40:
        label = "bearish_confirmed"
        score_adjustment = 3.0 if direction == "PUT" else -3.0

    if sent_fallback:
        score_adjustment *= 0.5

    return {
        "sentiment_price_label": label,
        "sentiment_price_score_adjustment": round(score_adjustment, 2),
        "sentiment_price_confidence": confidence,
        "sentiment_gap": round(gap, 2),
    }


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


# OPTIONS-EV / KOSTENMODELL (unverändert)
def calc_realized_volatility(closes: list, lookback: int = 20) -> float | None:
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
                       expected_move_pct: float, realized_vol_20d: float | None = None,
                       earnings_soon: bool = False, news_driven: bool = False,
                       iv_percentile: float | None = None) -> dict | None:
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
    vega = _safe_float(g.get("vega"))
    iv_raw = g.get("mid_iv") or g.get("ask_iv") or g.get("bid_iv")
    iv = _safe_float(iv_raw, None)
    oi = int(_safe_float(option.get("open_interest"), 0))
    volume = int(_safe_float(option.get("volume"), 0))

    opt_data = {
        "bid": bid, "ask": ask, "midpoint": mid, "spread_pct": spread_pct,
        "open_interest": oi, "volume": volume,
    }
    entry = conservative_entry_price(opt_data)
    if not entry:
        return None

    move_abs = underlying_price * expected_move_pct / 100.0
    delta_gain = abs(delta) * move_abs
    gamma_gain = 0.5 * abs(gamma) * (move_abs ** 2) * 0.6   # gedämpft
    theta_cost = abs(theta) * RULES.ev_hold_days if theta else 0.0

    iv_drop_decimal = 0.0
    iv_crush_factor_used = 0.0
    if iv and iv > 0:
        if earnings_soon:
            crush_pct = RULES.iv_crush_after_earnings_pct
        elif news_driven:
            crush_pct = min(0.35, RULES.iv_crush_after_news_pct)
        else:
            crush_pct = RULES.iv_crush_baseline_pct

        high_iv_flag = False
        if realized_vol_20d and realized_vol_20d > 0 and iv / realized_vol_20d >= RULES.mature_iv_to_rv_hard_block:
            high_iv_flag = True
        if iv_percentile is not None and iv_percentile >= 90.0:
            high_iv_flag = True
        if high_iv_flag:
            crush_pct += RULES.iv_crush_high_iv_bonus_pct

        crush_pct = max(0.0, min(0.60, crush_pct))
        iv_drop_decimal = iv * crush_pct
        iv_crush_factor_used = crush_pct

    vega_cost = abs(vega) * iv_drop_decimal

    entry_slippage = max(0.0, entry - mid)
    exit_slip = exit_slippage_points(opt_data)

    iv_to_rv = None
    iv_rv_penalty = 0.0
    if iv and realized_vol_20d and realized_vol_20d > 0:
        iv_to_rv = round(iv / realized_vol_20d, 3)
        if iv_to_rv > RULES.max_iv_to_rv_general:
            iv_rv_penalty = entry * min(0.35, (iv_to_rv - RULES.max_iv_to_rv_general) * RULES.iv_rv_penalty_factor)

    expected_option_gain = max(0.0, delta_gain + gamma_gain - theta_cost - vega_cost)
    ev_points = expected_option_gain - entry_slippage - exit_slip - iv_rv_penalty
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
    ev_reasons = []
    if ev_pct < RULES.min_option_ev_pct:
        ev_reasons.append(f"EV% {ev_pct} < {RULES.min_option_ev_pct}")
    if ev_dollars < RULES.min_option_ev_dollars:
        ev_reasons.append(f"EV$ {ev_dollars} < {RULES.min_option_ev_dollars}")
    if breakeven_move_pct > expected_move_pct * 1.25:
        ev_reasons.append("Break-even-Move zu hoch")
    if fill_p < RULES.min_fill_probability:
        ev_reasons.append(f"FillP {fill_p} < {RULES.min_fill_probability}")

    ev_ok = not ev_reasons
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
        "iv_decimal": round(iv, 5) if iv else None,
        "realized_vol_20d": round(realized_vol_20d, 5) if realized_vol_20d else None,
        "iv_to_rv": iv_to_rv,
        "iv_rv_penalty": round(iv_rv_penalty, 4),
        "vega_cost_points": round(vega_cost, 4),
        "vega_cost_dollars": round(vega_cost * 100.0, 2),
        "iv_drop_assumed_decimal": round(iv_drop_decimal, 5),
        "iv_crush_factor_used": round(iv_crush_factor_used, 3),
        "iv_crush_mode": ("earnings" if earnings_soon else "news" if news_driven else "baseline"),
        "open_interest": oi,
        "volume": volume,
        "fill_probability": fill_p,
        "expected_move_pct": expected_move_pct,
        "breakeven_move_pct": breakeven_move_pct,
        "entry_slippage_points": round(entry_slippage, 4),
        "exit_slippage_points": exit_slip,
        "ev_points": round(ev_points, 3),
        "ev_dollars": ev_dollars,
        "ev_pct": ev_pct,
        "ev_score": ev_score,
        "ev_ok": ev_ok,
        "ev_fail_reason": merge_reasons(ev_reasons),
        "option_source": "tradier",
        "contracts": None,
    }


def enrich_with_journal_iv_rank(symbol: str, option_ev: dict) -> dict:
    try:
        from trading_journal import get_iv_stats
        stats = get_iv_stats(symbol, option_ev.get("iv_decimal"), min_samples=2)
    except Exception as exc:
        stats = {
            "iv_rank": None,
            "iv_percentile": None,
            "iv_history_count": 0,
            "iv_rank_reason": "IV-Rank nicht berechenbar: " + str(exc)[:80],
        }

    option_ev.update(stats)
    n = int(stats.get("iv_history_count") or 0)
    iv_rank = stats.get("iv_rank")
    iv_percentile = stats.get("iv_percentile")

    iv_to_rv = _safe_float(option_ev.get("iv_to_rv"), None)

    if n < RULES.min_iv_history_samples_for_rank:
        option_ev["iv_cold_start"] = True
        if iv_to_rv is not None and iv_to_rv >= RULES.cold_start_iv_to_rv_hard_block:
            option_ev["ev_ok"] = False
            option_ev["ev_fail_reason"] = merge_reasons(
                option_ev.get("ev_fail_reason"),
                f"Cold-Start IV/RV {iv_to_rv:.2f} >= {RULES.cold_start_iv_to_rv_hard_block:.2f} Long-Option zu teuer",
            )
    else:
        option_ev["iv_cold_start"] = False
        if iv_to_rv is not None and iv_to_rv >= RULES.mature_iv_to_rv_hard_block:
            option_ev["ev_ok"] = False
            option_ev["ev_fail_reason"] = merge_reasons(
                option_ev.get("ev_fail_reason"),
                f"IV/RV {iv_to_rv:.2f} >= {RULES.mature_iv_to_rv_hard_block:.2f} Long-Option zu teuer",
            )

    if n >= RULES.min_iv_history_samples_for_rank:
        if iv_rank is not None and iv_rank >= RULES.iv_rank_hard_block_long:
            option_ev["ev_ok"] = False
            option_ev["ev_fail_reason"] = merge_reasons(
                option_ev.get("ev_fail_reason"),
                f"IV-Rank {iv_rank:.1f} >= {RULES.iv_rank_hard_block_long:.1f} Long-Option zu teuer",
            )
        if iv_percentile is not None and iv_percentile >= RULES.iv_percentile_hard_block_long:
            option_ev["ev_ok"] = False
            option_ev["ev_fail_reason"] = merge_reasons(
                option_ev.get("ev_fail_reason"),
                f"IV-Percentile {iv_percentile:.1f} >= {RULES.iv_percentile_hard_block_long:.1f}",
            )
    return option_ev


def get_tradier_options(symbol, direction, tradier_token,
                        sandbox=False, target_dte=21, underlying_price=0.0,
                        change_pct=0.0, closes=None, rel_vol=None,
                        signal_score=50.0, earnings_soon=False) -> dict:
    try:
        if not tradier_token:
            return {"option_source": "none", "ev_ok": False, "ev_fail_reason": "Tradier Token fehlt"}
        base = "https://sandbox.tradier.com" if sandbox else "https://api.tradier.com"
        hdrs = {"Authorization": "Bearer " + tradier_token, "Accept": "application/json"}

        r_exp = robust_get(base + "/v1/markets/options/expirations",
                           params={"symbol": symbol, "includeAllRoots": "true"},
                           headers=hdrs)
        if not r_exp:
            return {"option_source": "tradier", "ev_ok": False, "ev_fail_reason": "Options-Expirations nicht verfügbar"}
        exps = r_exp.json().get("expirations", {}).get("date", [])
        if not exps:
            return {"option_source": "tradier", "ev_ok": False, "ev_fail_reason": "Keine Expirations"}

        today_dt = datetime.now()
        target_exp = None
        best_diff = 999
        target_days = None
        for exp in exps:
            days = (datetime.strptime(exp, "%Y-%m-%d") - today_dt).days
            if days < RULES.min_dte_days:
                continue
            diff = abs(days - target_dte)
            if diff < best_diff:
                best_diff = diff
                target_exp = exp
                target_days = days

        if not target_exp:
            return {"option_source": "tradier", "ev_ok": False, "ev_fail_reason": "Keine passende Laufzeit"}

        r_chain = robust_get(base + "/v1/markets/options/chains",
                             params={"symbol": symbol, "expiration": target_exp,
                                     "greeks": "true"},
                             headers=hdrs)
        if not r_chain:
            return {"option_source": "tradier", "ev_ok": False, "expiration": target_exp,
                    "ev_fail_reason": "Options-Chain nicht verfügbar"}
        opts = r_chain.json().get("options", {}).get("option", [])
        if not opts:
            return {"option_source": "tradier", "ev_ok": False, "expiration": target_exp,
                    "ev_fail_reason": "Options-Chain leer"}

        opt_type = "call" if direction == "CALL" else "put"
        rv20 = calc_realized_volatility(closes or [])
        expected_move_pct = estimate_expected_move_pct(
            underlying_price, change_pct, rel_vol, signal_score, closes or [], target_dte
        )

        candidates = []
        for opt in opts:
            if opt.get("option_type") != opt_type:
                continue
            ev = evaluate_option_ev(opt, direction, underlying_price, expected_move_pct,
                                    realized_vol_20d=rv20,
                                    earnings_soon=earnings_soon,
                                    news_driven=True)
            if ev is None:
                continue
            ev["expiration"] = target_exp
            ev["dte_actual"] = target_days
            ok_earnings, earnings_reason = check_earnings_iv_gate(ev, earnings_soon)
            ev["earnings_iv_ok"] = ok_earnings
            ev["earnings_iv_reason"] = earnings_reason
            if not ok_earnings:
                ev["ev_ok"] = False
                ev["ev_fail_reason"] = merge_reasons(ev.get("ev_fail_reason"), earnings_reason)
            candidates.append(ev)

        if not candidates:
            return {"option_source": "tradier", "ev_ok": False, "expiration": target_exp,
                    "ev_fail_reason": "Keine bewertbaren Optionen"}

        good = [c for c in candidates if c.get("ev_ok")]
        chosen_pool = good if good else candidates
        best = sorted(chosen_pool, key=lambda c: c.get("ev_score", -999), reverse=True)[0]
        best["candidate_count"] = len(candidates)
        best["ev_candidates_ok"] = len(good)
        best.update(build_time_stop_plan(direction, best.get("dte_actual")))
        best = enrich_with_journal_iv_rank(symbol, best)
        if not best.get("ev_ok") and not best.get("ev_fail_reason"):
            best["ev_fail_reason"] = "Kein Kandidat nach EV/Kosten/Earnings-Gates"
        return best

    except (ValueError, KeyError, RequestException) as e:
        logger.debug("Tradier Options %s: %s", symbol, e)
        return {"option_source": "tradier", "ev_ok": False, "ev_fail_reason": "Tradier Options Fehler"}


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


# ==================== GAP + VOLUME CONVICTION (FINAL) ====================

def validate_gap_and_go(price: float, change_pct: float, volumes: list, closes: list) -> dict:
    """Finale Version: Gap + RVOL mit korrekter Trend-Direction-Confirmation."""
    if price <= 0 or not volumes or len(volumes) < 21 or not closes or len(closes) < 6:
        return {
            "gap_pct": round(change_pct, 2),
            "rvol": None,
            "is_high_conviction": False,
            "score_bonus": 0.0
        }

    rvol = calc_rel_volume(volumes)
    if rvol is None:
        return {
            "gap_pct": round(change_pct, 2),
            "rvol": None,
            "is_high_conviction": False,
            "score_bonus": 0.0
        }

    gap_pct = change_pct

    recent_range = max(closes[-5:]) - min(closes[-5:])
    trend_direction = closes[-1] - closes[-5]
    trend_strength = abs(trend_direction)

    min_move = max(0.5, recent_range * 0.3)
    trend_confirmed = (
        trend_strength >= min_move and
        ((gap_pct > 0 and trend_direction > 0) or
         (gap_pct < 0 and trend_direction < 0))
    )

    is_high_conviction = (
        abs(gap_pct) >= 3.0 and
        rvol >= 1.5 and
        trend_confirmed
    )

    gap_bonus = min(abs(gap_pct) * 1.8, 18.0)
    rvol_bonus = min(max((rvol - 1.0) * 8.0, 0), 16.0)
    score_bonus = min(round(gap_bonus + rvol_bonus, 1), 20.0)

    if not is_high_conviction:
        score_bonus *= 0.3

    return {
        "gap_pct": round(gap_pct, 2),
        "rvol": round(rvol, 2),
        "is_high_conviction": is_high_conviction,
        "score_bonus": round(score_bonus, 1)
    }


# ==================== SCORE (angepasst) ====================

def calculate_score(price, change_pct, above_ma50, ma20,
                    direction, bullish, unusual, earnings_soon, is_etf,
                    gap_volume_bonus: float = 0.0):
    if price <= 0:
        return 0.0, "no_price"

    base = 50.0
    momentum = min(25.0, abs(change_pct) * 7)

    trend_bonus = 0.0
    if direction == "CALL" and above_ma50 is True:
        trend_bonus = 18.0
    elif direction == "PUT" and above_ma50 is False:
        trend_bonus = 18.0

    direction_malus = 0.0
    if direction == "CALL" and change_pct < -0.5:
        direction_malus = -35.0
    elif direction == "PUT" and change_pct > 0.5:
        direction_malus = -35.0

    volume_bonus = 0.0 if gap_volume_bonus > 0 else (12.0 if unusual and not is_etf else 0.0)

    etf_roc_bonus = 0.0
    if is_etf and ma20 and price > 0:
        roc = (price - ma20) / ma20 * 100
        if direction == "CALL" and roc > 0:
            etf_roc_bonus = min(12.0, roc * 2)
        elif direction == "PUT" and roc < 0:
            etf_roc_bonus = min(12.0, abs(roc) * 2)

    raw = base + momentum + trend_bonus + direction_malus + volume_bonus + etf_roc_bonus + gap_volume_bonus
    score = round(max(0.0, min(100.0, raw)), 2)
    return score, "calculated_structural_no_sentiment"


# ==================== TICKER VERARBEITUNG ====================

def process_ticker(ticker, direction, earnings_list, cfg, target_dte: int = 21) -> dict:
    is_etf = ticker in ETF_TICKERS
    finnhub_key = cfg.get("finnhub_key", "")
    q_fut: Future = None
    h_fut: Future = None

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            q_fut = executor.submit(get_quote, ticker, cfg)
            h_fut = executor.submit(get_history, ticker, cfg)

            try:
                price, change_pct, high, low, quote_src = q_fut.result(timeout=12)
            except TimeoutError:
                logger.warning("%s: Kurs-Timeout", ticker)
                price, change_pct, high, low, quote_src = 0.0, 0.0, 0.0, 0.0, "timeout"

            try:
                closes, volumes, hist_src = h_fut.result(timeout=12)
            except TimeoutError:
                logger.warning("%s: History-Timeout", ticker)
                closes, volumes, hist_src = [], [], "timeout"

        quote_age_seconds = 0

        if is_etf and price <= 0:
            return {
                "ticker": ticker, "price": 0.0, "change_pct": 0.0,
                "score": 0.0, "_score_reason": "etf_no_price",
                "options": {}, "news_direction": direction,
                "is_etf": True, "etf_no_data": True,
                "_src_quote": quote_src, "quote_age_seconds": quote_age_seconds,
                "_closes_count": 0, "rel_vol": "n/v", "unusual": False,
                "ma50": None, "ma20": None, "above_ma50": None,
                "new_20d_high": None, "trend_status": "n/v",
                "bullish": 50.0, "sent_fallback": True, "earnings_soon": False,
                "_data_quality_ok": False, "_data_quality_reason": "ETF ohne Preis",
                "_liquidity_fail": True, "_liquidity_reason": "ETF ohne Preis",
                "_no_trade_reason": "ETF ohne Preis",
            }

        bullish, bearish, buzz, sent_fallback = get_sentiment(ticker, change_pct, finnhub_key)
        sentiment_reaction = classify_sentiment_price_reaction(direction, bullish, bearish, change_pct, sent_fallback)

        history_validation = validate_ohlcv_history(closes, volumes)
        spike_validation = detect_unexplained_price_spike(
            price, closes, news_signal_present=True, threshold_pct=10.0
        ) if closes else None

        data_validation_reason = data_flags_to_text(history_validation, spike_validation)
        data_validation_ok = bool(history_validation.ok and (spike_validation.ok if spike_validation else True))

        rel_vol = calc_rel_volume(volumes)

        # === NEU: Gap + Volume Conviction ===
        gap_volume = validate_gap_and_go(price, change_pct, volumes, closes)

        unusual = bool(rel_vol and rel_vol >= RULES.daily_rvol_unusual_threshold)
        ma50 = calc_ma(closes, 50)
        ma20 = calc_ma(closes, 20)
        rv20 = calc_realized_volatility(closes)
        above_ma50 = (price > ma50) if (ma50 is not None and price > 0) else None
        new_20d = None
        if len(closes) >= 20 and price > 0:
            recent_high = max(closes[-20:])
            new_20d = price >= recent_high * 0.98 if recent_high > 0 else None
        earnings_soon = ticker in earnings_list

        gap_bonus = gap_volume["score_bonus"] if data_validation_ok else 0.0

        score, score_reason = calculate_score(
            price, change_pct, above_ma50, ma20, direction,
            bullish, unusual, earnings_soon, is_etf,
            gap_volume_bonus=gap_bonus
        )

        sector_result = evaluate_sector_filter(ticker, direction, change_pct, cfg, get_quote)
        score = round(max(0.0, min(100.0,
            score + sector_result.score_adjustment + sentiment_reaction.get("sentiment_price_score_adjustment", 0.0)
        )), 2)

        raw_signal_score = score
        score_reason = score_reason + "; sector=" + sector_result.severity + "; sent_price=" + sentiment_reaction.get("sentiment_price_label", "neutral")

        if RULES.require_tradier_quote_for_tradier_options and not str(quote_src).lower().startswith("tradier"):
            options_data = {
                "option_source": "tradier",
                "ev_ok": False,
                "ev_fail_reason": "Hard Block: Tradier-Optionen ohne Tradier-Underlying-Snapshot",
                "snapshot_consistency_ok": False,
            }
        else:
            options_data = get_tradier_options(
                ticker, direction,
                cfg.get("tradier_token", ""),
                cfg.get("tradier_sandbox", False),
                target_dte=target_dte,
                underlying_price=price,
                change_pct=change_pct,
                closes=closes,
                rel_vol=rel_vol,
                signal_score=score,
                earnings_soon=earnings_soon,
            )

        market_stub = {"price": price, "_src_quote": quote_src, "quote_source": quote_src, "quote_age_seconds": quote_age_seconds}
        snapshot_ok, snapshot_reason = check_data_quality(market_stub, options_data)
        data_ok = bool(snapshot_ok and data_validation_ok)
        data_reason = merge_reasons(snapshot_reason if not snapshot_ok else "", data_validation_reason if data_validation_reason != "ok" else "") or "ok"
        is_liquid, liquidity_reason = check_liquidity(options_data)
        ev_ok = bool(options_data.get("ev_ok"))
        sector_ok = bool(sector_result.ok)

        no_trade_reason = []
        if not data_ok:
            no_trade_reason.append(data_reason)
        if not sector_ok:
            no_trade_reason.append(sector_result.reason)
        if not is_liquid:
            no_trade_reason.append(liquidity_reason)
        if not ev_ok:
            no_trade_reason.append(options_data.get("ev_fail_reason") or "Options-EV nach Kosten nicht ausreichend")

        final_score = score
        if no_trade_reason:
            if not data_ok:
                score_reason = "data_quality_fail"
            elif not is_liquid:
                score_reason = "liquidity_fail"
            else:
                score_reason = "option_ev_fail"
            final_score = 0.0

        final_reason = merge_reasons(no_trade_reason)
        if final_reason:
            logger.info("%s: No-Trade-Gate: %s", ticker, final_reason)

        logger.info(
            "%s: price=%.2f score=%.1f raw=%.1f data_ok=%s liquid=%s ev_ok=%s ev=%s src=%s dte=%d",
            ticker, price, final_score, raw_signal_score, data_ok, is_liquid, ev_ok, options_data.get("ev_pct"), quote_src, target_dte
        )

        return {
            "ticker": ticker,
            "price": price,
            "change_pct": change_pct,
            "rel_vol": str(rel_vol) if rel_vol is not None else "n/v",
            "rel_vol_quality": "daily_only_no_intraday_curve",
            "data_validation_ok": data_validation_ok,
            "data_validation_reason": data_validation_reason,
            "data_quality_score": getattr(history_validation, "quality_score", None),
            "price_spike_pct": getattr(spike_validation, "spike_pct", None) if spike_validation else None,
            "sector": sector_result.sector,
            "sector_etf": sector_result.sector_etf,
            "sector_change_pct": sector_result.sector_change_pct,
            "market_change_pct": sector_result.market_change_pct,
            "qqq_change_pct": sector_result.qqq_change_pct,
            "relative_to_sector_pct": sector_result.relative_to_sector_pct,
            "sector_vs_market_pct": getattr(sector_result, "sector_vs_market_pct", None),
            "sector_momentum_confirmation": getattr(sector_result, "momentum_confirmation", "neutral"),
            "sector_filter_ok": sector_result.ok,
            "sector_filter_reason": sector_result.reason,
            "sector_score_adjustment": sector_result.score_adjustment,
            "sentiment_price_label": sentiment_reaction.get("sentiment_price_label"),
            "sentiment_price_score_adjustment": sentiment_reaction.get("sentiment_price_score_adjustment"),
            "sentiment_price_confidence": sentiment_reaction.get("sentiment_price_confidence"),
            "sentiment_gap": sentiment_reaction.get("sentiment_gap"),
            "unusual": unusual,
            "ma50": ma50,
            "ma20": ma20,
            "realized_vol_20d": round(rv20, 5) if rv20 else None,
            "above_ma50": above_ma50,
            "new_20d_high": new_20d,
            "trend_status": ("über MA50" if above_ma50 is True else ("unter MA50" if above_ma50 is False else "n/v")),
            "bullish": round(bullish, 1),
            "bearish": round(bearish, 1),
            "sentiment_rank_only": True,
            "sent_fallback": sent_fallback,
            "earnings_soon": earnings_soon,
            "raw_signal_score": raw_signal_score,
            "gate_adjusted_score": final_score,
            "score": final_score,
            "_score_reason": score_reason + f" | gap_bonus={gap_bonus:.1f}",
            "_data_quality_ok": data_ok,
            "_data_quality_reason": data_reason,
            "_liquidity_fail": not is_liquid,
            "_liquidity_reason": liquidity_reason,
            "_no_trade_reason": final_reason,
            "options": options_data,
            "news_direction": direction,
            "is_etf": is_etf,
            "_src_quote": quote_src,
            "quote_age_seconds": quote_age_seconds,
            "_src_hist": hist_src,
            "_closes_count": len(closes),
            # NEUE FELDER
            "gap_pct": gap_volume["gap_pct"],
            "rvol": gap_volume["rvol"],
            "gap_volume_confirmed": gap_volume["is_high_conviction"],
            "gap_volume_bonus": gap_bonus,
        }

    except Exception as e:
        logger.error("%s: Unerwarteter Fehler: %s", ticker, e)
        if q_fut: q_fut.cancel()
        if h_fut: h_fut.cancel()
        return {
            "ticker": ticker, "price": 0.0, "change_pct": 0.0,
            "score": 0.0, "_score_reason": "exception",
            "options": {}, "news_direction": direction,
            "_src_quote": "error", "quote_age_seconds": 0, "_closes_count": 0,
            "rel_vol": "n/v", "unusual": False, "ma50": None, "ma20": None,
            "above_ma50": None, "new_20d_high": None, "trend_status": "n/v",
            "bullish": 40.0, "bearish": 60.0, "sentiment_rank_only": True,
            "sent_fallback": True, "earnings_soon": False,
            "_data_quality_ok": False, "_data_quality_reason": "exception",
            "_liquidity_fail": True, "_liquidity_reason": "exception",
            "_no_trade_reason": "exception",
            "_error": str(e)[:120],
        }


# ══════════════════════════════════════════════════════════
# SUMMARY BUILDER + DIREKTE AUSFÜHRUNG (unverändert)
# ══════════════════════════════════════════════════════════

def build_summary(ranked, vix_value, ticker_directions,
                  earnings_list, unusual_list, failed):
    today = datetime.now().strftime("%Y-%m-%d")
    srcs_str = ", ".join(d["ticker"] + "=" + d.get("_src_quote","?") for d in ranked)

    s = "DATUM: " + today + "\n"
    s += "VIX: " + str(vix_value) + "\n"
    s += "NEWS-SIGNALE: " + (
        ", ".join(t + ":" + d for t, d in ticker_directions.items()) or "keine") + "\n"
    s += "EARNINGS NAECHSTE 10 TAGE: " + (
        ", ".join(earnings_list) if earnings_list else "Keine") + "\n"
    s += "UNUSUAL ACTIVITY: nur Diagnose; keine lineare Intraday-Extrapolation\n"
    s += "TOP 3: " + ", ".join(d["ticker"] for d in ranked[:3]) + "\n"
    s += "QUOTE-QUELLEN: " + srcs_str + "\n"
    if failed:
        s += "API-FEHLER (Kurs=0): " + ", ".join(failed) + "\n"

    s += "\nHARTE GATES: Tradier-Snapshot, DATA_QUALITY_OK, LIQUIDITY_OK, EV_OK, EARNINGS_IV_OK, SECTOR_MARKET_OK muessen alle True sein.\n"
    s += "SENTIMENT: nur Ranking-/Kontextinfo, kein EV-Retter. Final Decision sieht keine News-Texte.\n"
    s += "SPREAD-REGIME: <=5% bevorzugt, 5-8% vorsichtig, 8-10% nur bei starkem EV, >10% harter Block.\n"

    s += "\nMARKTDATEN (sortiert nach Score):\n"
    s += (f"{'Ticker':<6} | {'Kurs':>7} | {'Δ%':>6} | {'MA50':>7} | "
          f"{'Trend':<14} | {'RelVol':>7} | {'News':>5} | {'Raw':>6} | {'Score':>6} | {'Gate':<4}\n" + "-" * 128 + "\n")

    for d in ranked:
        if d.get("etf_no_data"):
            s += (d["ticker"].ljust(6) + " | ETF-SIGNAL | Richtung: " +
                  d["news_direction"] + " | Score: 0 | NO_TRADE: " + d.get("_no_trade_reason", "n/v") + "\n")
            continue

        news_flag = ("📈" if d["news_direction"] == "CALL" else "📉") + d["news_direction"]
        kurs_str = f"{d['price']:>7.2f}" if d["price"] > 0 else "   n/v!"
        gate_ok = (
            bool(d.get("_data_quality_ok"))
            and bool(d.get("sector_filter_ok", True))
            and not d.get("_liquidity_fail")
            and bool(d.get("options", {}).get("ev_ok"))
        )
        gate_flag = "OK" if gate_ok else "FAIL"

        raw_score = d.get("raw_signal_score", d.get("score", 0.0))
        s += (f"{d['ticker']:<6} | {kurs_str} | {d['change_pct']:>6.2f}% | "
              f"{str(d.get('ma50','n/v')):>7} | {d.get('trend_status','n/v'):<14} | "
              f"{str(d['rel_vol']):>6}{'🔥' if d.get('unusual') else ''} | "
              f"{news_flag:>5} | {raw_score:>6.2f} | {d['score']:>6.2f} | {gate_flag:<4}\n")

        if d.get("_no_trade_reason"):
            s += "  ⛔ NO_TRADE_REASON: " + d["_no_trade_reason"] + "\n"
        s += ("  └─ SECTOR: ETF=" + str(d.get("sector_etf","n/v")) +
              " | SectorΔ=" + str(d.get("sector_change_pct","n/v")) +
              " | MarketΔ=" + str(d.get("market_change_pct","n/v")) +
              " | RelSector=" + str(d.get("relative_to_sector_pct","n/v")) +
              " | SectorVsMarket=" + str(d.get("sector_vs_market_pct","n/v")) +
              " | Momentum=" + str(d.get("sector_momentum_confirmation","neutral")) + "\n")

        opt = d.get("options") or {}
        if opt:
            s += ("  └─ OPTIONS: Strike=" + str(opt.get("strike","n/v")) +
                  " | Exp=" + str(opt.get("expiration","n/v")) +
                  " | Bid=" + str(opt.get("bid","n/v")) +
                  "/Ask=" + str(opt.get("ask","n/v")) +
                  " | Mid=" + str(opt.get("midpoint","n/v")) +
                  " | Entry=" + str(opt.get("conservative_entry","n/v")) +
                  " | ExitSlip=" + str(opt.get("exit_slippage_points","n/v")) +
                  " | Delta=" + str(opt.get("delta","n/v")) +
                  " | IV=" + str(opt.get("iv","n/v")) + "%" +
                  " | IV/RV=" + str(opt.get("iv_to_rv","n/v")) +
                  " | IVRank=" + str(opt.get("iv_rank","n/v")) +
                  " | IVPct=" + str(opt.get("iv_percentile","n/v")) +
                  " | IVHist=" + str(opt.get("iv_history_count","n/v")) +
                  " | IVCOLD=" + str(opt.get("iv_cold_start","n/v")) +
                  " | TimeStop=" + str(opt.get("time_stop_hours","n/v")) + "h/" + str(opt.get("time_stop_required_move_pct","n/v")) + "%" +
                  " | OI=" + str(opt.get("open_interest","n/v")) +
                  " | FillP=" + str(opt.get("fill_probability","n/v")) +
                  " | EV%=" + str(opt.get("ev_pct","n/v")) +
                  " | EV$=" + str(opt.get("ev_dollars","n/v")) +
                  " | EV_OK=" + str(opt.get("ev_ok", False)) +
                  " | EARN_IV_OK=" + str(opt.get("earnings_iv_ok", True)) + "\n")

    sources = []
    keyword_fallback = []
    market_fallback = []
    for d in ranked:
        news_src = d.get("news_sentiment_source")
        if news_src:
            sources.append(d["ticker"] + "=" + str(news_src))
            if news_src == "keyword":
                keyword_fallback.append(d["ticker"])
        if d.get("sent_fallback"):
            market_fallback.append(d["ticker"])

    s += "\nSENTIMENT-QUELLEN: " + (", ".join(sources) or "n/v")
    s += "\nKEYWORD-FALLBACK NEWS: " + (", ".join(keyword_fallback) or "keiner")
    s += "\nMARKTDATEN-SENTIMENT-FALLBACK: " + (", ".join(market_fallback) or "keiner")
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
