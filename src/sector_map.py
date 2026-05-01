"""
sector_map.py — Markt-/Sektorfilter für Daily-Options-Signale.

Ziel:
- Keine Long-Calls gegen klaren Sektor-/Marktwind.
- Keine Long-Puts gegen klar starke Sektor-/Marktbreite.
- Relative Stärke/Schwäche als Feature journalisieren.

Die Zuordnung ist bewusst pragmatisch und kostenlos: Sektor-ETFs + einfache Ticker-Maps.
Unbekannte Ticker fallen auf QQQ/SPY zurück, damit das Gate nicht unbrauchbar wird.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Any

from rules import RULES


SECTOR_ETFS = {
    "technology": "XLK",
    "semiconductors": "SMH",
    "communication": "XLC",
    "consumer_discretionary": "XLY",
    "consumer_staples": "XLP",
    "energy": "XLE",
    "financials": "XLF",
    "healthcare": "XLV",
    "industrials": "XLI",
    "materials": "XLB",
    "real_estate": "XLRE",
    "utilities": "XLU",
    "small_caps": "IWM",
    "market": "SPY",
    "nasdaq": "QQQ",
}

# Nur die häufigsten/alpha-relevanten Namen. Unbekannte fallen auf QQQ/SPY zurück.
TICKER_TO_SECTOR = {
    # Mega-cap tech / AI
    "AAPL": "technology", "MSFT": "technology", "ORCL": "technology", "CRM": "technology",
    "ADBE": "technology", "NOW": "technology", "SNOW": "technology", "PLTR": "technology",
    "AI": "technology", "SHOP": "technology", "DDOG": "technology", "NET": "technology",
    "CRWD": "technology", "PANW": "technology", "ZS": "technology", "MDB": "technology",

    # Semis
    "NVDA": "semiconductors", "AMD": "semiconductors", "AVGO": "semiconductors",
    "INTC": "semiconductors", "MU": "semiconductors", "ARM": "semiconductors",
    "TSM": "semiconductors", "ASML": "semiconductors", "QCOM": "semiconductors",
    "TXN": "semiconductors", "AMAT": "semiconductors", "LRCX": "semiconductors",

    # Communication / internet
    "GOOGL": "communication", "GOOG": "communication", "META": "communication",
    "NFLX": "communication", "DIS": "communication", "ROKU": "communication",
    "SNAP": "communication", "PINS": "communication", "SPOT": "communication",

    # Consumer discretionary / autos / retail
    "TSLA": "consumer_discretionary", "AMZN": "consumer_discretionary", "NKE": "consumer_discretionary",
    "SBUX": "consumer_discretionary", "MCD": "consumer_discretionary", "HD": "consumer_discretionary",
    "LOW": "consumer_discretionary", "TGT": "consumer_discretionary", "WMT": "consumer_staples",
    "COST": "consumer_staples", "PG": "consumer_staples", "KO": "consumer_staples", "PEP": "consumer_staples",

    # Energy / commodities
    "XOM": "energy", "CVX": "energy", "OXY": "energy", "COP": "energy", "SLB": "energy",
    "HAL": "energy", "USO": "energy",

    # Financials
    "JPM": "financials", "BAC": "financials", "C": "financials", "WFC": "financials",
    "GS": "financials", "MS": "financials", "BLK": "financials", "SCHW": "financials",
    "AXP": "financials", "V": "financials", "MA": "financials", "PYPL": "financials",

    # Healthcare / biotech
    "LLY": "healthcare", "PFE": "healthcare", "MRNA": "healthcare", "BMY": "healthcare",
    "JNJ": "healthcare", "UNH": "healthcare", "HUM": "healthcare", "ABBV": "healthcare",
    "MRK": "healthcare", "GILD": "healthcare", "REGN": "healthcare", "VRTX": "healthcare",

    # Industrials/materials/utilities/real estate
    "BA": "industrials", "CAT": "industrials", "DE": "industrials", "GE": "industrials",
    "HON": "industrials", "UPS": "industrials", "FDX": "industrials", "LMT": "industrials",
    "NOC": "industrials", "RTX": "industrials",
    "FCX": "materials", "NEM": "materials", "AA": "materials", "LIN": "materials",
    "NEE": "utilities", "DUK": "utilities", "SO": "utilities",
    "PLD": "real_estate", "AMT": "real_estate", "O": "real_estate",

    # ETFs map to themselves/market context
    "SPY": "market", "QQQ": "nasdaq", "IWM": "small_caps",
    "XLK": "technology", "SMH": "semiconductors", "SOXX": "semiconductors",
    "XLE": "energy", "XLF": "financials", "XLV": "healthcare", "XLY": "consumer_discretionary",
    "XLP": "consumer_staples", "XLI": "industrials", "XLB": "materials", "XLU": "utilities",
    "XLRE": "real_estate", "XLC": "communication",
}


@dataclass(frozen=True)
class SectorFilterResult:
    ok: bool
    reason: str
    sector: str
    sector_etf: str
    sector_change_pct: float | None
    market_change_pct: float | None
    qqq_change_pct: float | None
    relative_to_sector_pct: float | None
    sector_vs_market_pct: float | None
    momentum_confirmation: str
    score_adjustment: float
    severity: str


def _quote_change(symbol: str, cfg: dict, quote_fn: Callable[[str, dict], Any]) -> float | None:
    try:
        result = quote_fn(symbol, cfg)
        if not result:
            return None
        # get_quote liefert: price, change_pct, high, low, source
        return float(result[1])
    except Exception:
        return None


def sector_for_ticker(ticker: str) -> tuple[str, str]:
    t = (ticker or "").upper().strip()
    sector = TICKER_TO_SECTOR.get(t)
    if not sector:
        # Grober Fallback: unbekannte Single Stocks gegen QQQ + SPY prüfen.
        sector = "nasdaq"
    return sector, SECTOR_ETFS.get(sector, "QQQ")


def evaluate_sector_filter(ticker: str, direction: str, stock_change_pct: float,
                           cfg: dict, quote_fn: Callable[[str, dict], Any]) -> SectorFilterResult:
    """
    Bewertet Markt-/Sektorbestätigung.
    Fail-closed nur bei klaren Konflikten; fehlende ETF-Daten führen zu Warnung, nicht zu Block.
    """
    direction = (direction or "").upper()
    sector, sector_etf = sector_for_ticker(ticker)

    sector_change = _quote_change(sector_etf, cfg, quote_fn)
    spy_change = _quote_change("SPY", cfg, quote_fn)
    qqq_change = _quote_change("QQQ", cfg, quote_fn)
    market_change = spy_change if spy_change is not None else qqq_change

    if sector_change is None and market_change is None:
        return SectorFilterResult(
            ok=True, reason="Sektor-/Marktdaten fehlen; kein harter Block", sector=sector,
            sector_etf=sector_etf, sector_change_pct=None, market_change_pct=market_change,
            qqq_change_pct=qqq_change, relative_to_sector_pct=None, sector_vs_market_pct=None,
            momentum_confirmation="unknown", score_adjustment=-3.0,
            severity="warning",
        )

    rel = None
    if sector_change is not None:
        rel = round(stock_change_pct - sector_change, 2)

    sector_vs_market = None
    if sector_change is not None and market_change is not None:
        sector_vs_market = round(sector_change - market_change, 2)

    reasons: list[str] = []
    score_adj = 0.0
    momentum_confirmation = "neutral"
    ok = True
    severity = "ok"

    # CALL: ideal ist Aktie > Sektor, Sektor/Markt nicht klar negativ.
    if direction == "CALL":
        if sector_change is not None and sector_change < -0.60 and (rel is None or rel < 0.20):
            ok = False
            severity = "block"
            reasons.append(f"CALL gegen schwachen Sektor {sector_etf} {sector_change:.2f}% ohne relative Staerke")
        if market_change is not None and market_change < -0.80 and stock_change_pct <= 0:
            ok = False
            severity = "block"
            reasons.append(f"CALL gegen schwachen Markt SPY/QQQ {market_change:.2f}%")
        if rel is not None:
            if rel >= RULES.sector_relative_strength_min:
                score_adj += RULES.sector_confirms_score_bonus
                momentum_confirmation = "stock_outperforms_sector"
            elif rel < -0.40:
                score_adj += RULES.sector_disagrees_score_malus
                momentum_confirmation = "stock_lags_sector"
        if sector_vs_market is not None:
            if sector_vs_market >= RULES.sector_vs_market_confirm_min and direction == "CALL":
                score_adj += 4.0
                if momentum_confirmation == "stock_outperforms_sector":
                    momentum_confirmation = "stock_and_sector_outperform_market"
            elif sector_vs_market < -0.30:
                score_adj -= 5.0
        if market_change is not None and market_change < -0.40:
            score_adj -= 4.0

    # PUT: ideal ist Aktie < Sektor, Sektor/Markt nicht klar stark.
    elif direction == "PUT":
        if sector_change is not None and sector_change > 0.60 and (rel is None or rel > -0.20):
            ok = False
            severity = "block"
            reasons.append(f"PUT gegen starken Sektor {sector_etf} {sector_change:.2f}% ohne relative Schwaeche")
        if market_change is not None and market_change > 0.80 and stock_change_pct >= 0:
            ok = False
            severity = "block"
            reasons.append(f"PUT gegen starken Markt SPY/QQQ {market_change:.2f}%")
        if rel is not None:
            if rel <= -RULES.sector_relative_strength_min:
                score_adj += RULES.sector_confirms_score_bonus
                momentum_confirmation = "stock_underperforms_sector"
            elif rel > 0.40:
                score_adj += RULES.sector_disagrees_score_malus
                momentum_confirmation = "stock_stronger_than_sector"
        if sector_vs_market is not None:
            if sector_vs_market <= -RULES.sector_vs_market_confirm_min and direction == "PUT":
                score_adj += 4.0
                if momentum_confirmation == "stock_underperforms_sector":
                    momentum_confirmation = "stock_and_sector_underperform_market"
            elif sector_vs_market > 0.30:
                score_adj -= 5.0
        if market_change is not None and market_change > 0.40:
            score_adj -= 4.0

    else:
        reasons.append("Unbekannte Richtung fuer Sektorfilter")
        score_adj -= 5.0
        severity = "warning"

    if not reasons:
        reasons.append("ok")

    return SectorFilterResult(
        ok=ok,
        reason=" | ".join(reasons),
        sector=sector,
        sector_etf=sector_etf,
        sector_change_pct=round(sector_change, 2) if sector_change is not None else None,
        market_change_pct=round(market_change, 2) if market_change is not None else None,
        qqq_change_pct=round(qqq_change, 2) if qqq_change is not None else None,
        relative_to_sector_pct=rel,
        sector_vs_market_pct=sector_vs_market,
        momentum_confirmation=momentum_confirmation,
        score_adjustment=round(score_adj, 2),
        severity=severity,
    )
