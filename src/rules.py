"""
rules.py — Zentrale Trading-Regeln

Fixes v2:
- apply_vix_rules() nimmt jetzt autoritativen vix_direct Parameter
- VIX unbekannt → no_trade statt Einsatz 0
- Kontrakt=0 → no_trade (Budget-Schutz)
- Spread/OI Grenzen vereinheitlicht (Prompt + Code konsistent)
"""

from dataclasses import dataclass
from typing import Optional

# ══════════════════════════════════════════════════════════
# ZENTRALE REGEL-KONSTANTEN
# ══════════════════════════════════════════════════════════

@dataclass(frozen=True)
class TradingRules:
    # VIX-Grenzen
    vix_hard_limit:     float = 25.0
    vix_reduced_limit:  float = 20.0

    # Einsatz in EUR
    einsatz_normal:     int   = 250
    einsatz_reduced:    int   = 150

    # Stop-Loss
    stop_loss_pct:      float = 0.30

    # Score-Schwellen
    min_score:          int   = 50

    # Fix Nr. 5: Spread/OI vereinheitlicht — README + Prompt + Code konsistent
    max_spread_pct:     float = 2.0    # war 12.0 — jetzt wie im Prompt
    min_open_interest:  int   = 5000   # war 150 — jetzt wie im Prompt

    # Signal-Parsing
    valid_directions:   tuple = ("CALL", "PUT")
    valid_scores:       tuple = ("HIGH", "MED", "LOW")
    valid_horizons:     tuple = ("T1", "T2", "T3")
    max_tickers:        int   = 12

    # Earnings-Fenster
    earnings_window_days: int = 10


RULES = TradingRules()


# ══════════════════════════════════════════════════════════
# VIX-REGELPRÜFUNG
# Fix Nr. 1+2: vix_direct = autoritativer VIX aus get_vix()
# Claude-JSON-Feld wird ignoriert für die Risikoregel
# Fix Nr. 3: Kontrakt=0 → no_trade (Budget-Schutz)
# ══════════════════════════════════════════════════════════

def apply_vix_rules(vix_direct, claude_output: dict) -> dict:
    """
    Wendet VIX-Regeln auf Claude-Output an.

    vix_direct: autoritativer VIX-Wert direkt aus get_vix() in main.py
                NICHT aus claude_output["vix"] — verhindert LLM-Halluzination.

    Fix Nr. 1: VIX kommt immer direkt von get_vix(), nie aus Claude-JSON.
    Fix Nr. 2: VIX unbekannt (n/v oder nicht parsebar) → no_trade=True,
               nicht vix=0.0 mit normalem Einsatz.
    Fix Nr. 3: Kontrakt-Berechnung ergibt 0 → no_trade=True (Budget überschritten).
    """
    result = dict(claude_output)

    # Fix Nr. 1+2: Autoritativen VIX parsen
    vix_unknown = False
    try:
        vix = float(str(vix_direct).replace(",", "."))
        if vix <= 0:
            vix_unknown = True
    except (ValueError, TypeError):
        vix_unknown = True

    # Fix Nr. 2: Unbekannter VIX → no_trade (sicherer Zustand)
    if vix_unknown:
        result["no_trade"]       = True
        result["no_trade_grund"] = "VIX nicht verfuegbar kein Trade"
        result["vix_warnung"]    = False
        result["einsatz"]        = 0
        result["stop_loss_eur"]  = 0
        result["kontrakte"]      = "n/v"
        return result

    # Hard Limit
    if vix >= RULES.vix_hard_limit:
        result["no_trade"]       = True
        result["no_trade_grund"] = "VIX zu hoch Kapitalschutz aktiv"
        result["vix_warnung"]    = False
        result["einsatz"]        = 0
        result["stop_loss_eur"]  = 0
        result["kontrakte"]      = "n/v"
        return result

    # Einsatz nach VIX
    if vix >= RULES.vix_reduced_limit:
        einsatz = RULES.einsatz_reduced
        result["vix_warnung"] = True
    else:
        einsatz = RULES.einsatz_normal
        result["vix_warnung"] = False

    result["einsatz"]       = einsatz
    result["stop_loss_eur"] = round(einsatz * RULES.stop_loss_pct)

    # Fix Nr. 3: Kontrakt-Berechnung — 0 Kontrakte = no_trade
    if not result.get("no_trade"):
        mid = result.get("midpoint", "n/v")
        try:
            mid_f = float(str(mid).replace(",", "."))
            if mid_f > 0:
                kontrakte = round(einsatz / (mid_f * 100))
                if kontrakte < 1:
                    # Budget reicht nicht für einen Kontrakt → kein Trade
                    result["no_trade"]       = True
                    result["no_trade_grund"] = "Midpoint zu hoch Budget reicht nicht"
                    result["einsatz"]        = 0
                    result["stop_loss_eur"]  = 0
                    result["kontrakte"]      = "n/v"
                    return result
                result["kontrakte"] = str(kontrakte)
            else:
                result["kontrakte"] = "n/v"
        except (ValueError, TypeError):
            result["kontrakte"] = "n/v"

    return result


# ══════════════════════════════════════════════════════════
# CLAUDE-OUTPUT VALIDIERUNG
# ══════════════════════════════════════════════════════════

def validate_claude_output(data: dict) -> tuple:
    """
    Prüft Claude-Output auf Pflichtfelder und logische Konsistenz.
    Gibt (is_valid, list_of_errors) zurück.
    """
    errors = []

    required = ["datum", "vix", "regime", "no_trade"]
    for field in required:
        if field not in data:
            errors.append(f"Pflichtfeld fehlt: {field}")

    no_trade = data.get("no_trade", False)

    if not no_trade:
        trade_fields = ["ticker", "strike", "laufzeit", "delta", "midpoint"]
        for field in trade_fields:
            if not data.get(field):
                errors.append(f"Trade-Feld fehlt oder leer: {field}")

        einsatz = data.get("einsatz")
        if einsatz is not None:
            try:
                e = int(str(einsatz).replace("€","").strip())
                if e not in (RULES.einsatz_normal, RULES.einsatz_reduced):
                    errors.append(
                        f"Einsatz {e} ungültig — erwartet "
                        f"{RULES.einsatz_reduced} oder {RULES.einsatz_normal}"
                    )
            except (ValueError, TypeError):
                errors.append(f"Einsatz nicht numerisch: {einsatz}")

        valid_regimes = ("LOW-VOL", "TRENDING", "HIGH-VOL")
        if data.get("regime") not in valid_regimes:
            errors.append(f"Ungültiges Regime: {data.get('regime')}")

        valid_farben = ("gruen", "gelb", "rot")
        if data.get("regime_farbe") not in valid_farben:
            errors.append(f"Ungültige regime_farbe: {data.get('regime_farbe')}")

    tabelle = data.get("ticker_tabelle", [])
    if not isinstance(tabelle, list):
        errors.append("ticker_tabelle ist keine Liste")
    elif len(tabelle) == 0:
        errors.append("ticker_tabelle ist leer")

    is_valid = len(errors) == 0
    return is_valid, errors


# ══════════════════════════════════════════════════════════
# SIGNAL-VALIDIERUNG
# ══════════════════════════════════════════════════════════

def parse_ticker_signals(raw: str) -> list:
    """
    Robuster Parser für TICKER_SIGNALS-String.
    Gibt vollständige Signal-Dicts zurück inkl. dte_days.

    Input:  "TICKER_SIGNALS:USO:CALL:HIGH:T3:45DTE,TLT:PUT:MED:T3:45DTE"
    Output: [{"ticker": "USO", "direction": "CALL", "dte_days": 45, ...}, ...]
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

        ticker    = parts[0].strip().upper()
        direction = parts[1].strip().upper()
        score     = parts[2].strip().upper()
        horizon   = parts[3].strip().upper()
        dte_raw   = parts[4].strip().upper()

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
            "ticker":    ticker,
            "direction": direction,
            "score":     score,
            "horizon":   horizon,
            "dte":       dte_raw,
            "dte_days":  dte_days,
        })

    return results[:RULES.max_tickers]
