"""
rules.py — Zentrale Trading-Regeln

v7 Rational-Gates:
- EV nur mit konsistentem Snapshot sinnvoll: Tradier-Optionen brauchen bevorzugt Tradier-Underlying.
- Realistisches Kostenmodell: Entry-Slippage + härtere Exit-Slippage.
- Earnings/IV-Crush-Schutz: Long-Optionen bei nahen Earnings und hoher/unklarer IV blockieren.
- Sentiment darf Ranking unterstützen, aber keine harten Gates überschreiben.
- No-Trade-Gründe werden maschinenlesbar journalisiert.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TradingRules:
    # VIX-Grenzen
    vix_hard_limit: float = 25.0
    vix_reduced_limit: float = 20.0

    # Einsatz in EUR/USD-Äquivalent für Positionsgröße
    einsatz_normal: int = 250
    einsatz_reduced: int = 150

    # Risiko
    stop_loss_pct: float = 0.30

    # Score-Schwellen
    min_score: int = 50

    # Datenqualität / Snapshot-Konsistenz
    # Wenn Optionsdaten von Tradier kommen, soll der Underlying-Preis ebenfalls von Tradier kommen.
    # Falls Tradier-Quote nicht verfügbar ist, wird die Option nicht als tradebar behandelt.
    require_tradier_quote_for_tradier_options: bool = True
    max_quote_age_seconds: int = 900

    # Liquidität / Ausführbarkeit
    max_spread_pct: float = 6.0
    warn_spread_pct: float = 4.0
    min_open_interest: int = 500
    min_option_volume: int = 1
    max_entry_spread_share: float = 0.50   # Entry = Mid + 50% Spread, gedeckelt durch Ask
    base_exit_spread_share: float = 0.60   # Exit konservativer als Entry
    high_spread_exit_share: float = 0.80
    stress_exit_spread_share: float = 1.00
    min_fill_probability: float = 0.35

    # Options-EV Filter
    target_delta_abs: float = 0.45
    min_option_ev_pct: float = 12.0        # erwarteter Vorteil relativ zum konservativen Entry
    min_option_ev_dollars: float = 12.0    # pro Kontrakt nach Kosten
    ev_hold_days: int = 2

    # IV-/Earnings-Schutz
    # Kostenlose IV-Rank-Historie fehlt; deshalb Proxy: aktuelle IV / 20d realisierte Volatilität.
    earnings_window_days: int = 10
    block_long_options_if_earnings_soon: bool = True
    block_earnings_if_iv_missing: bool = True
    max_iv_to_rv_for_earnings: float = 1.35
    max_iv_to_rv_general: float = 2.20
    iv_rv_penalty_factor: float = 0.18     # reduziert EV bei sehr teurer IV außerhalb Earnings

    # Signal-Parsing
    valid_directions: tuple = ("CALL", "PUT")
    valid_scores: tuple = ("HIGH", "MED", "LOW")
    valid_horizons: tuple = ("T1", "T2", "T3")
    max_tickers: int = 5


RULES = TradingRules()


def _to_float(value: Any, default=None):
    try:
        if value is None:
            return default
        return float(str(value).replace("€", "").replace("$", "").replace(",", ".").strip())
    except (ValueError, TypeError):
        return default


def merge_reasons(*parts: Any) -> str:
    """Kompakter, deduplizierter No-Trade-Grund."""
    seen = set()
    out = []
    for part in parts:
        if not part:
            continue
        if isinstance(part, (list, tuple, set)):
            values = part
        else:
            values = str(part).split("|")
        for raw in values:
            item = str(raw).strip()
            if not item or item in seen:
                continue
            seen.add(item)
            out.append(item)
    return " | ".join(out)


# ══════════════════════════════════════════════════════════
# KOSTENMODELL / AUSFÜHRBARKEIT
# ══════════════════════════════════════════════════════════

def conservative_entry_price(options_data: dict) -> float | None:
    """
    Realistischer Einstieg statt Midpoint.
    Für Long-Optionen ist der echte Fill oft zwischen Mid und Ask.
    """
    if not options_data:
        return None
    bid = _to_float(options_data.get("bid"))
    ask = _to_float(options_data.get("ask"))
    mid = _to_float(options_data.get("midpoint"))
    if bid is None or ask is None or mid is None or bid <= 0 or ask <= 0 or ask < bid:
        return None
    spread = ask - bid
    entry = min(ask, mid + spread * RULES.max_entry_spread_share)
    return round(entry, 2)


def exit_slippage_points(options_data: dict) -> float:
    """
    Exit ist konservativer als Entry. Bei breiteren Spreads steigt der Haircut.
    Rückgabe in Optionspreis-Punkten, nicht Prozent.
    """
    if not options_data:
        return 0.0
    bid = _to_float(options_data.get("bid"), 0.0)
    ask = _to_float(options_data.get("ask"), 0.0)
    spread_pct = _to_float(options_data.get("spread_pct"), 999.0)
    spread = max(0.0, ask - bid)
    if spread_pct >= 10.0:
        share = RULES.stress_exit_spread_share
    elif spread_pct >= RULES.warn_spread_pct:
        share = RULES.high_spread_exit_share
    else:
        share = RULES.base_exit_spread_share
    return round(spread * share, 4)


def estimate_fill_probability(options_data: dict) -> float:
    """
    Grobe Fill-Wahrscheinlichkeit aus Spread, OI und Volumen.
    Kein Broker-Orderbuch, aber besser als Midpoint-Fantasie.
    """
    if not options_data:
        return 0.0
    spread_pct = _to_float(options_data.get("spread_pct"), 999.0)
    oi = _to_float(options_data.get("open_interest"), 0.0)
    vol = _to_float(options_data.get("volume"), 0.0)

    spread_score = max(0.0, min(1.0, 1.0 - spread_pct / 20.0))
    oi_score = max(0.0, min(1.0, oi / 5000.0))
    vol_score = max(0.0, min(1.0, vol / 500.0))
    p = 0.55 * spread_score + 0.30 * oi_score + 0.15 * vol_score
    return round(max(0.0, min(1.0, p)), 3)


def check_data_quality(market_data: dict, options_data: dict) -> tuple[bool, str]:
    """
    Prüft, ob Underlying- und Optionssnapshot zusammenpassen.
    Fail-closed, wenn Tradier-Optionsdaten mit nicht-Tradier-Underlying kombiniert würden.
    """
    if not market_data:
        return False, "Marktdaten fehlen"
    price = _to_float(market_data.get("price"), 0.0)
    quote_src = str(market_data.get("_src_quote") or market_data.get("quote_source") or "").lower()
    option_src = str((options_data or {}).get("option_source") or "").lower()

    if price <= 0:
        return False, "Underlying-Preis fehlt"
    if option_src == "tradier" and RULES.require_tradier_quote_for_tradier_options:
        if not quote_src.startswith("tradier"):
            return False, "Inkonsistenter Snapshot: Option Tradier aber Underlying nicht Tradier"
    quote_age = _to_float(market_data.get("quote_age_seconds"), 0.0)
    if quote_age and quote_age > RULES.max_quote_age_seconds:
        return False, f"Quote zu alt: {int(quote_age)}s"
    return True, "ok"


def check_liquidity(options_data: dict) -> tuple[bool, str]:
    """
    Prüft Optionsliquidität als harten Filter.
    Gibt (is_liquid, reason) zurück. Fail-closed bei fehlenden Daten.
    """
    if not options_data:
        return False, "Keine Optionsdaten verfuegbar"

    bid = _to_float(options_data.get("bid"))
    ask = _to_float(options_data.get("ask"))
    mid = _to_float(options_data.get("midpoint"))
    spread_pct = _to_float(options_data.get("spread_pct"))
    open_int = _to_float(options_data.get("open_interest"))
    volume = _to_float(options_data.get("volume"), 0.0)

    if bid is None or bid <= 0:
        return False, "Bid fehlt oder 0"
    if ask is None or ask <= 0:
        return False, "Ask fehlt oder 0"
    if mid is None or mid <= 0:
        return False, "Midpoint fehlt"
    if ask < bid:
        return False, "Ask kleiner als Bid"
    if spread_pct is None:
        return False, "Spread nicht berechenbar"
    if open_int is None:
        return False, "Open Interest fehlt"

    if spread_pct > RULES.max_spread_pct:
        return False, f"Spread {spread_pct:.1f}% > {RULES.max_spread_pct}% Limit"
    if open_int < RULES.min_open_interest:
        return False, f"OI {int(open_int)} < {RULES.min_open_interest} Limit"
    if volume < RULES.min_option_volume:
        return False, f"Optionsvolumen {int(volume)} < {RULES.min_option_volume}"

    fill_p = estimate_fill_probability(options_data)
    if fill_p < RULES.min_fill_probability:
        return False, f"Fill-Wahrscheinlichkeit {fill_p:.2f} < {RULES.min_fill_probability:.2f}"

    return True, "ok"


def check_earnings_iv_gate(options_data: dict, earnings_soon: bool) -> tuple[bool, str]:
    """
    Harte Sperre für Long-Optionen bei nahen Earnings und teurer/unklarer IV.
    Kostenlose Daten liefern selten sauberen IV-Rank; deshalb wird IV/RV als Proxy genutzt.
    """
    if not earnings_soon or not RULES.block_long_options_if_earnings_soon:
        return True, "ok"
    iv = _to_float((options_data or {}).get("iv_decimal"))
    rv = _to_float((options_data or {}).get("realized_vol_20d"))
    iv_to_rv = _to_float((options_data or {}).get("iv_to_rv"))

    if iv is None or iv <= 0 or rv is None or rv <= 0 or iv_to_rv is None:
        if RULES.block_earnings_if_iv_missing:
            return False, "Earnings nahe und IV/RV unbekannt"
        return True, "ok"

    if iv_to_rv >= RULES.max_iv_to_rv_for_earnings:
        return False, f"Earnings nahe und IV/RV {iv_to_rv:.2f} zu hoch"
    return True, "ok"


# ══════════════════════════════════════════════════════════
# VIX-REGELPRÜFUNG
# ══════════════════════════════════════════════════════════

def apply_vix_rules(vix_direct, claude_output: dict) -> dict:
    """
    VIX ist autoritativ aus get_vix().
    Nutzt konservativen Entry, wenn vorhanden; fallback Midpoint.
    """
    result = dict(claude_output)

    try:
        vix = float(str(vix_direct).replace(",", "."))
        vix_unknown = vix <= 0
    except (ValueError, TypeError):
        vix_unknown = True
        vix = None

    if vix_unknown:
        result.update({
            "no_trade": True,
            "no_trade_grund": merge_reasons(result.get("no_trade_grund"), "VIX nicht verfuegbar kein Trade"),
            "vix_warnung": False,
            "einsatz": 0,
            "stop_loss_eur": 0,
            "kontrakte": "n/v",
        })
        return result

    if vix >= RULES.vix_hard_limit:
        result.update({
            "no_trade": True,
            "no_trade_grund": merge_reasons(result.get("no_trade_grund"), "VIX zu hoch Kapitalschutz aktiv"),
            "vix_warnung": False,
            "einsatz": 0,
            "stop_loss_eur": 0,
            "kontrakte": "n/v",
        })
        return result

    einsatz = RULES.einsatz_reduced if vix >= RULES.vix_reduced_limit else RULES.einsatz_normal
    result["einsatz"] = einsatz
    result["vix_warnung"] = vix >= RULES.vix_reduced_limit
    result["stop_loss_eur"] = round(einsatz * RULES.stop_loss_pct)

    if not result.get("no_trade"):
        entry = _to_float(result.get("conservative_entry"))
        if entry is None:
            entry = _to_float(result.get("entry_price"))
        if entry is None:
            entry = _to_float(result.get("midpoint"))

        if entry and entry > 0:
            kontrakte = int(einsatz // (entry * 100))
            if kontrakte < 1:
                result.update({
                    "no_trade": True,
                    "no_trade_grund": merge_reasons(result.get("no_trade_grund"), "Entry zu hoch Budget reicht nicht"),
                    "einsatz": 0,
                    "stop_loss_eur": 0,
                    "kontrakte": "n/v",
                })
                return result
            result["kontrakte"] = str(kontrakte)
            result["entry_price"] = round(entry, 2)
        else:
            result["kontrakte"] = "n/v"

    return result


# ══════════════════════════════════════════════════════════
# CLAUDE-OUTPUT VALIDIERUNG
# ══════════════════════════════════════════════════════════

def validate_claude_output(data: dict) -> tuple:
    errors = []
    for field in ["datum", "vix", "regime", "no_trade"]:
        if field not in data:
            errors.append(f"Pflichtfeld fehlt: {field}")

    no_trade = data.get("no_trade", False)
    if not no_trade:
        for field in ["ticker", "strike", "laufzeit", "delta", "midpoint"]:
            if not data.get(field):
                errors.append(f"Trade-Feld fehlt: {field}")

        if data.get("direction") not in RULES.valid_directions:
            errors.append(f"Ungültige direction: {data.get('direction')}")

        einsatz = data.get("einsatz")
        if einsatz is not None:
            try:
                e = int(str(einsatz).replace("€", "").strip())
                if e not in (RULES.einsatz_normal, RULES.einsatz_reduced):
                    errors.append(f"Einsatz {e} ungültig")
            except (ValueError, TypeError):
                errors.append(f"Einsatz nicht numerisch: {einsatz}")

    if data.get("regime") and data.get("regime") not in ("LOW-VOL", "TRENDING", "HIGH-VOL"):
        errors.append(f"Ungültiges Regime: {data.get('regime')}")

    if data.get("regime_farbe") and data.get("regime_farbe") not in ("gruen", "gelb", "rot"):
        errors.append(f"Ungültige regime_farbe: {data.get('regime_farbe')}")

    tabelle = data.get("ticker_tabelle", [])
    if not isinstance(tabelle, list) or len(tabelle) == 0:
        errors.append("ticker_tabelle fehlt oder leer")

    return len(errors) == 0, errors


# ══════════════════════════════════════════════════════════
# SIGNAL-PARSING
# ══════════════════════════════════════════════════════════

def parse_ticker_signals(raw: str) -> list:
    """
    Parser für TICKER_SIGNALS:TICKER:RICHTUNG:SCORE:HORIZONT:DTE,...
    """
    if not raw:
        return []

    clean = raw.strip()
    if clean.startswith("TICKER_SIGNALS:"):
        clean = clean[len("TICKER_SIGNALS:"):]

    if not clean or clean == "NONE":
        return []

    results = []
    for entry in clean.split(","):
        entry = entry.strip()
        if not entry:
            continue

        parts = entry.split(":")
        if len(parts) < 5:
            continue

        ticker = parts[0].strip().upper()
        direction = parts[1].strip().upper()
        score = parts[2].strip().upper()
        horizon = parts[3].strip().upper()
        dte_raw = parts[4].strip().upper()

        if not ticker or len(ticker) > 5:
            continue
        if direction not in RULES.valid_directions:
            continue
        if score not in RULES.valid_scores:
            continue
        if horizon not in RULES.valid_horizons:
            continue
        if not dte_raw.endswith("DTE"):
            continue

        try:
            dte_days = int(dte_raw.replace("DTE", ""))
        except ValueError:
            continue

        if dte_days < 7 or dte_days > 120:
            continue

        results.append({
            "ticker": ticker,
            "direction": direction,
            "score": score,
            "horizon": horizon,
            "dte": dte_raw,
            "dte_days": dte_days,
        })

    return results
