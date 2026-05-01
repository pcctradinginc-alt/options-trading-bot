"""
llm_schema.py — Pydantic-Schema-Guard für LLM-Ausgaben.

Ziel:
- Ungültiger LLM-Output darf niemals zu einem Trade führen.
- Signal-Output wird auf ein kleines, deterministisches Format reduziert.
- Report-JSON wird validiert; bei Fehler wird fail-closed ein No-Trade-Payload erzeugt.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
import re

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator


VALID_DIRECTIONS = {"CALL", "PUT"}
VALID_STRENGTHS = {"HIGH", "MED", "LOW"}
VALID_HORIZONS = {"T1", "T2", "T3"}


class TickerSignal(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    ticker: str = Field(pattern=r"^[A-Z]{1,5}$")
    direction: Literal["CALL", "PUT"]
    strength: Literal["HIGH", "MED", "LOW"]
    horizon: Literal["T1", "T2", "T3"]
    dte_days: int = Field(ge=7, le=120)

    @field_validator("ticker", mode="before")
    @classmethod
    def normalize_ticker(cls, value: Any) -> str:
        return str(value or "").strip().upper()

    def to_wire(self) -> str:
        return f"{self.ticker}:{self.direction}:{self.strength}:{self.horizon}:{self.dte_days}DTE"


class SignalEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")
    signals: list[TickerSignal] = Field(default_factory=list, max_length=5)

    def to_wire(self) -> str:
        if not self.signals:
            return "TICKER_SIGNALS:NONE"
        return "TICKER_SIGNALS:" + ",".join(s.to_wire() for s in self.signals)


def validate_ticker_signal_line(raw_line: str, max_tickers: int = 5) -> tuple[str | None, list[str]]:
    """
    Validiert TICKER_SIGNALS:TICKER:CALL:HIGH:T1:21DTE,...
    Rückgabe: (canonical_line|None, errors)
    """
    if not raw_line or not str(raw_line).strip():
        return None, ["Signalzeile leer"]

    line = str(raw_line).strip().replace("`", "")
    upper = line.upper().replace(" ", "")
    if upper in {"TICKER_SIGNALS:NONE", "NONE"}:
        return "TICKER_SIGNALS:NONE", []

    if upper.startswith("TICKER_SIGNALS:"):
        payload = line.split(":", 1)[1]
    else:
        payload = line

    if not payload.strip() or payload.strip().upper() == "NONE":
        return "TICKER_SIGNALS:NONE", []

    errors: list[str] = []
    signals: list[TickerSignal] = []
    seen: set[str] = set()

    for raw_entry in payload.split(","):
        entry = raw_entry.strip()
        if not entry:
            continue
        parts = [p.strip().upper() for p in entry.split(":")]
        if len(parts) != 5:
            errors.append(f"ungueltiges Signalformat: {entry[:80]}")
            continue
        ticker, direction, strength, horizon, dte_raw = parts
        if strength == "MEDIUM":
            strength = "MED"
        dte_match = re.fullmatch(r"(\d{1,3})DTE", dte_raw)
        if not dte_match:
            errors.append(f"ungueltige DTE: {entry[:80]}")
            continue
        try:
            sig = TickerSignal(
                ticker=ticker,
                direction=direction,
                strength=strength,
                horizon=horizon,
                dte_days=int(dte_match.group(1)),
            )
        except ValidationError as exc:
            errors.append(f"Schemafehler {ticker or '?'}: {exc.errors()[0].get('msg', str(exc))}")
            continue
        if sig.ticker in seen:
            continue
        seen.add(sig.ticker)
        signals.append(sig)
        if len(signals) >= max_tickers:
            break

    if errors:
        return None, errors
    envelope = SignalEnvelope(signals=signals)
    return envelope.to_wire(), []


class ReportReasonDetail(BaseModel):
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)
    ticker_wahl: str = ""
    option_wahl: str = ""
    timing: str = ""
    chance_risiko: str = ""
    risiko: str = ""


class TickerTableRow(BaseModel):
    model_config = ConfigDict(extra="allow", str_strip_whitespace=True)
    ticker: str
    direction: str | None = None
    kurs: str | None = None
    chg: str | None = None
    ma50: str | None = None
    trend: str | None = None
    sector: str | None = None
    rel_sector: str | None = None
    sentpx: str | None = None
    relvol: str | None = None
    bull: str | None = None
    score: str | None = None
    ev_ok: bool | None = None
    ev_pct: str | None = None
    gewinner: bool | None = None
    ausgeschlossen: bool | None = None
    no_trade_reason: str | None = None

    @field_validator("ticker", mode="before")
    @classmethod
    def normalize_ticker(cls, value: Any) -> str:
        return str(value or "").strip().upper()


class ReportPayload(BaseModel):
    """Bewusst tolerantes Report-Schema, aber fail-closed bei Trade-Feldern."""

    model_config = ConfigDict(extra="allow", str_strip_whitespace=True)

    datum: str = Field(default_factory=lambda: datetime.now().strftime("%d.%m.%Y"))
    vix: str | float = "n/v"
    regime: Literal["LOW-VOL", "TRENDING", "HIGH-VOL"] = "TRENDING"
    regime_farbe: Literal["gruen", "gelb", "rot"] = "gelb"
    no_trade: bool = False
    no_trade_grund: str = ""
    vix_warnung: bool = False

    direction: str | None = None
    ticker: str | None = None
    strike: str | float | None = None
    laufzeit: str | None = None
    delta: str | float | None = None
    iv: str | float | None = None
    iv_to_rv: str | float | None = None
    bid: str | float | None = None
    ask: str | float | None = None
    midpoint: str | float | None = None
    conservative_entry: str | float | None = None
    entry_price: str | float | None = None
    exit_slippage_points: str | float | None = None
    fill_probability: str | float | None = None
    ev_pct: str | float | None = None
    ev_dollars: str | float | None = None
    breakeven_move_pct: str | float | None = None
    time_stop: str | None = None
    time_stop_rule: str | None = None
    time_stop_hours: int | str | None = None
    time_stop_required_move_pct: str | float | None = None
    kontrakte: str | int | None = None
    einsatz: int | str | None = None
    stop_loss_eur: int | float | str | None = None
    unusual: bool | None = None

    begruendung_detail: ReportReasonDetail = Field(default_factory=ReportReasonDetail)
    markt: str = ""
    strategie: str = ""
    ausgeschlossen: str = ""
    ticker_tabelle: list[TickerTableRow] = Field(default_factory=list)

    @field_validator("ticker", mode="before")
    @classmethod
    def normalize_optional_ticker(cls, value: Any) -> str | None:
        if value in (None, ""):
            return None
        return str(value).strip().upper()

    @field_validator("direction", mode="before")
    @classmethod
    def normalize_direction(cls, value: Any) -> str | None:
        if value in (None, ""):
            return None
        return str(value).strip().upper()

    @model_validator(mode="after")
    def validate_trade_payload(self) -> "ReportPayload":
        if self.no_trade:
            if not self.no_trade_grund:
                self.no_trade_grund = "Kein valider Trade nach Schema Guard"
            return self

        required = {
            "ticker": self.ticker,
            "direction": self.direction,
            "strike": self.strike,
            "laufzeit": self.laufzeit,
            "delta": self.delta,
            "bid": self.bid,
            "ask": self.ask,
            "midpoint": self.midpoint,
            "conservative_entry": self.conservative_entry,
            "entry_price": self.entry_price,
            "ev_pct": self.ev_pct,
            "ev_dollars": self.ev_dollars,
            "ticker_tabelle": self.ticker_tabelle,
        }
        missing = [k for k, v in required.items() if v in (None, "", [])]
        if missing:
            raise ValueError("Trade-Payload unvollstaendig: " + ", ".join(missing))
        if self.direction not in VALID_DIRECTIONS:
            raise ValueError(f"ungueltige direction: {self.direction}")
        return self


def validate_report_payload(data: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    try:
        payload = ReportPayload.model_validate(data)
        return payload.model_dump(mode="python"), []
    except ValidationError as exc:
        return None, [f"{'.'.join(str(x) for x in err.get('loc', []))}: {err.get('msg')}" for err in exc.errors()]
    except ValueError as exc:
        return None, [str(exc)]


def build_cancelled_report(reason: str, raw: str | None = None) -> dict[str, Any]:
    detail = reason[:450]
    if raw:
        detail += " | Raw: " + raw[:250].replace("\n", " ")
    return {
        "datum": datetime.now().strftime("%d.%m.%Y"),
        "vix": "n/v",
        "regime": "TRENDING",
        "regime_farbe": "gelb",
        "no_trade": True,
        "no_trade_grund": "CANCELLED_SCHEMA_GUARD " + detail,
        "vix_warnung": False,
        "ticker_tabelle": [],
        "begruendung_detail": {
            "ticker_wahl": "LLM-Ausgabe war nicht schema-valide.",
            "option_wahl": "Kein Trade.",
            "timing": "Kein Trade.",
            "chance_risiko": "Kapitalschutz.",
            "risiko": detail,
        },
        "markt": "Kein Trade, weil der Report-Output nicht schema-valide war.",
        "strategie": "Fail-closed.",
        "ausgeschlossen": detail,
    }
