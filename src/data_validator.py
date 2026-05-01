"""
data_validator.py — Datenhärtung für kostenlose und Broker-Datenquellen.

Ziel:
- Keine Scheingenauigkeit durch kaputte OHLCV-Historien.
- Spikes/Gaps markieren statt blind handeln.
- Underlying-/Options-Snapshot fail-closed prüfen.

Die Funktionen sind bewusst konservativ, aber nicht blind: Ein 10% Gap wird nicht automatisch
als Fehler verworfen. Es wird als Risiko-Flag gespeichert und kann über Gates wirken.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DataValidationResult:
    ok: bool
    reason: str
    flags: tuple[str, ...] = ()
    quality_score: float = 1.0
    spike_pct: float | None = None


def _to_float(value: Any, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def validate_ohlcv_history(closes: list, volumes: list | None = None,
                           min_closes: int = 50) -> DataValidationResult:
    """
    Validiert Daily-Historie. Für MA50, Realized Vol und Sektorfilter werden mindestens
    50 Schlusskurse bevorzugt. Unter 21 ist die Historie nicht tradebar.
    """
    flags: list[str] = []
    quality = 1.0

    if not closes or len(closes) < 21:
        return DataValidationResult(False, "Historie <21 Handelstage", ("history_too_short",), 0.0)

    clean = [_to_float(c) for c in closes if _to_float(c) is not None and _to_float(c) > 0]
    if len(clean) < 21:
        return DataValidationResult(False, "Zu wenige valide Schlusskurse", ("invalid_closes",), 0.0)

    if len(clean) < min_closes:
        flags.append("history_below_preferred_50d")
        quality *= 0.85

    # Null-/Negativpreise sind bereits entfernt; nun extreme Lücken erkennen.
    rets = []
    for prev, cur in zip(clean[:-1], clean[1:]):
        if prev > 0 and cur > 0:
            rets.append((cur / prev - 1.0) * 100.0)
    if rets:
        max_abs_ret = max(abs(r) for r in rets[-20:])
        if max_abs_ret > 25:
            flags.append("extreme_recent_gap_gt25pct")
            quality *= 0.70
        elif max_abs_ret > 12:
            flags.append("recent_gap_gt12pct")
            quality *= 0.85

    if volumes:
        vclean = [v for v in volumes if isinstance(v, (int, float)) and v >= 0]
        if len(vclean) >= 21:
            if statistics.median(vclean[-20:]) == 0:
                flags.append("volume_median_zero")
                quality *= 0.80
        else:
            flags.append("volume_history_short")
            quality *= 0.95
    else:
        flags.append("volume_missing")
        quality *= 0.95

    return DataValidationResult(True, "ok", tuple(flags), round(max(0.0, min(1.0, quality)), 3))


def detect_unexplained_price_spike(price: float, closes: list, news_signal_present: bool = True,
                                   threshold_pct: float = 10.0) -> DataValidationResult:
    """
    Markiert große Kurslücken. Ein Spike ohne erkannte News ist kein automatischer Datenfehler,
    aber ein Risikosignal, weil der Bot möglicherweise den echten Katalysator nicht kennt.
    """
    p = _to_float(price, 0.0)
    if p <= 0 or not closes:
        return DataValidationResult(False, "Preis oder Historie fehlt", ("price_or_history_missing",), 0.0)
    prev = _to_float(closes[-1], None)
    if prev is None or prev <= 0:
        return DataValidationResult(False, "Voriger Schlusskurs fehlt", ("prev_close_missing",), 0.0)

    spike_pct = (p / prev - 1.0) * 100.0
    flags = []
    quality = 1.0
    if abs(spike_pct) >= threshold_pct:
        flags.append("price_spike_gt10pct")
        quality *= 0.75
        if not news_signal_present:
            flags.append("spike_without_detected_news")
            quality *= 0.60
            return DataValidationResult(False, "Preis-Spike >10% ohne erkannte News", tuple(flags), round(quality, 3), round(spike_pct, 2))
        return DataValidationResult(True, "Preis-Spike >10% mit News-Kontext", tuple(flags), round(quality, 3), round(spike_pct, 2))

    return DataValidationResult(True, "ok", tuple(flags), 1.0, round(spike_pct, 2))


def realized_volatility(closes: list, lookback: int = 20) -> float | None:
    """Annualisierte realisierte Volatilität aus Daily-Schlusskursen als Dezimalzahl."""
    clean = [_to_float(c) for c in closes if _to_float(c) is not None and _to_float(c) > 0]
    if len(clean) < lookback + 1:
        return None
    recent = clean[-(lookback + 1):]
    rets = [math.log(cur / prev) for prev, cur in zip(recent[:-1], recent[1:]) if prev > 0 and cur > 0]
    if len(rets) < 10:
        return None
    return max(0.05, min(2.50, statistics.stdev(rets) * math.sqrt(252)))


def data_flags_to_text(*results: DataValidationResult | None) -> str:
    flags: list[str] = []
    reasons: list[str] = []
    for res in results:
        if not res:
            continue
        if res.reason and res.reason != "ok":
            reasons.append(res.reason)
        flags.extend(list(res.flags or ()))
    dedup = []
    seen = set()
    for item in reasons + flags:
        if item and item not in seen:
            seen.add(item)
            dedup.append(item)
    return " | ".join(dedup) if dedup else "ok"
