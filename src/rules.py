"""
rules.py — Zentrale Trading-Regeln

v5:
- Options-Einstieg nicht mehr blind Midpoint, sondern konservativer Entry.
- Kostenmodell: Spread-Slippage + Mindest-EV + Fill-Wahrscheinlichkeit.
- PUT/CALL werden getrennt validiert.
- VIX unbekannt bleibt fail-closed.
"""

from dataclasses import dataclass


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

    # Liquidität / Ausführbarkeit
    # Hinweis: 2% ist sehr streng bei Optionen. Der EV-Filter ist der härtere Schutz.
    max_spread_pct: float = 6.0
    min_open_interest: int = 500
    min_option_volume: int = 1
    max_entry_spread_share: float = 0.50   # Entry = Mid + 50% Spread, gedeckelt durch Ask
    min_fill_probability: float = 0.35

    # Options-EV Filter
    target_delta_abs: float = 0.45
    min_option_ev_pct: float = 12.0        # erwarteter Vorteil relativ zum konservativen Entry
    min_option_ev_dollars: float = 12.0    # pro Kontrakt nach Kosten
    ev_hold_days: int = 2

    # Signal-Parsing
    valid_directions: tuple = ("CALL", "PUT")
    valid_scores: tuple = ("HIGH", "MED", "LOW")
    valid_horizons: tuple = ("T1", "T2", "T3")
    max_tickers: int = 5

    # Earnings-Fenster
    earnings_window_days: int = 10


RULES = TradingRules()


def _to_float(value, default=None):
    try:
        if value is None:
            return default
        return float(str(value).replace("€", "").replace("$", "").replace(",", ".").strip())
    except (ValueError, TypeError):
        return default


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


def check_liquidity(options_data: dict) -> tuple:
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
            "no_trade_grund": "VIX nicht verfuegbar kein Trade",
            "vix_warnung": False,
            "einsatz": 0,
            "stop_loss_eur": 0,
            "kontrakte": "n/v",
        })
        return result

    if vix >= RULES.vix_hard_limit:
        result.update({
            "no_trade": True,
            "no_trade_grund": "VIX zu hoch Kapitalschutz aktiv",
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
                    "no_trade_grund": "Entry zu hoch Budget reicht nicht",
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
            if dte_days <= 0 or dte_days > 180:
                continue
        except ValueError:
            continue

        results.append({
            "ticker": ticker,
            "direction": direction,
            "score": score,
            "horizon": horizon,
            "dte": dte_raw,
            "dte_days": dte_days,
        })

    return results[:RULES.max_tickers]
