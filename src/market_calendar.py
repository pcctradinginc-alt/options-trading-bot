"""
market_calendar.py — US-Market-Time ohne harte UTC-Annahmen.

Primär: exchange_calendars, wenn installiert.
Fallback: zoneinfo America/New_York mit regulären Handelszeiten.
"""

from __future__ import annotations

from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
UTC = timezone.utc


def now_et() -> datetime:
    return datetime.now(UTC).astimezone(NY)


def _status_from_et(dt: datetime) -> str:
    if dt.weekday() >= 5:
        return "CLOSED-WEEKEND"
    t = dt.time()
    if time(9, 30) <= t < time(16, 0):
        return "OPEN"
    if time(4, 0) <= t < time(9, 30):
        return "PRE-MARKET"
    if time(16, 0) <= t < time(20, 0):
        return "AFTER-HOURS"
    return "CLOSED"


def market_status(dt: datetime | None = None) -> str:
    """NYSE-Status mit optionalem exchange_calendars Holiday-Check."""
    dt = dt or now_et()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=NY)
    dt_et = dt.astimezone(NY)

    # Optional: Feiertage / verkürzte Sessions über exchange_calendars.
    try:
        import exchange_calendars as xcals
        cal = xcals.get_calendar("XNYS")
        minute_utc = dt_et.astimezone(UTC)
        if cal.is_trading_minute(minute_utc):
            return "OPEN"
        # außerhalb regulärer Minute trotzdem PRE/AFTER anhand ET-Zeit ausgeben
        base = _status_from_et(dt_et)
        if base == "OPEN":
            return "CLOSED-HOLIDAY"
        return base
    except Exception:
        return _status_from_et(dt_et)


def market_context(dt: datetime | None = None) -> tuple[str, str]:
    dt_et = (dt or now_et()).astimezone(NY)
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    return f"{days[dt_et.weekday()]} {dt_et:%H:%M} ET", market_status(dt_et)


def market_elapsed_fraction(dt: datetime | None = None) -> float | None:
    """
    Anteil der regulären 6.5h Session, nur wenn Markt offen.
    Für intraday Volumen-Hochrechnung.
    """
    dt_et = (dt or now_et()).astimezone(NY)
    if market_status(dt_et) != "OPEN":
        return None
    start = dt_et.replace(hour=9, minute=30, second=0, microsecond=0)
    end = dt_et.replace(hour=16, minute=0, second=0, microsecond=0)
    total = (end - start).total_seconds()
    elapsed = (dt_et - start).total_seconds()
    return max(0.05, min(1.0, elapsed / total))
