"""
trading_journal.py — Signal-/Trade-Journal und Outcome-Tracking.

Speichert jeden Lauf in SQLite:
- Rohsignale aus News/Claude
- Marktdaten, Optionsdaten, SEC-Daten
- finale Report-Entscheidung
- spätere Underlying-Outcomes für Event-Study

Wichtig: In GitHub Actions muss data/ persistent gemacht werden, sonst ist SQLite nach jedem Lauf weg.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DB_PATH = DATA_DIR / "trading_journal.sqlite"

OUTCOME_HORIZONS = {
    "1H": timedelta(hours=1),
    "EOD": None,  # wird auf 21:00 UTC des Signaltags gesetzt
    "1D": timedelta(days=1),
    "3D": timedelta(days=3),
    "5D": timedelta(days=5),
    "10D": timedelta(days=10),
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime | None = None) -> str:
    return (dt or utc_now()).astimezone(timezone.utc).isoformat(timespec="seconds")


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)


def connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path, timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    init_db(con)
    return con


def init_db(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            market_date TEXT NOT NULL,
            market_status TEXT,
            vix TEXT,
            raw_ticker_signals TEXT,
            article_count INTEGER,
            cluster_count INTEGER,
            no_trade INTEGER DEFAULT 0,
            no_trade_reason TEXT,
            final_ticker TEXT,
            final_direction TEXT,
            final_payload_json TEXT
        );

        CREATE TABLE IF NOT EXISTS signals (
            signal_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            ticker TEXT NOT NULL,
            direction TEXT NOT NULL,
            signal_strength TEXT,
            horizon TEXT,
            dte_days INTEGER,
            cluster_json TEXT,
            market_json TEXT,
            option_json TEXT,
            sec_json TEXT,
            price REAL,
            change_pct REAL,
            rel_vol TEXT,
            score REAL,
            score_reason TEXT,
            liquidity_fail INTEGER,
            liquidity_reason TEXT,
            ev_ok INTEGER,
            ev_pct REAL,
            ev_dollars REAL,
            conservative_entry REAL,
            data_quality_ok INTEGER,
            data_quality_reason TEXT,
            no_trade_reason TEXT,
            quote_source TEXT,
            option_source TEXT,
            realized_vol_20d REAL,
            option_iv REAL,
            iv_to_rv REAL,
            exit_slippage_points REAL,
            earnings_iv_ok INTEGER,
            earnings_iv_reason TEXT,
            sector TEXT,
            sector_etf TEXT,
            sector_change_pct REAL,
            market_change_pct REAL,
            relative_to_sector_pct REAL,
            sector_filter_ok INTEGER,
            sector_filter_reason TEXT,
            sentiment_price_label TEXT,
            sentiment_price_score_adjustment REAL,
            data_quality_score REAL,
            price_spike_pct REAL,
            selected_trade INTEGER DEFAULT 0,
            FOREIGN KEY(run_id) REFERENCES runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS outcomes (
            outcome_id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER NOT NULL,
            horizon TEXT NOT NULL,
            due_at TEXT NOT NULL,
            checked_at TEXT,
            start_price REAL,
            end_price REAL,
            underlying_return_pct REAL,
            direction_return_pct REAL,
            status TEXT DEFAULT 'open',
            FOREIGN KEY(signal_id) REFERENCES signals(signal_id),
            UNIQUE(signal_id, horizon)
        );

        CREATE INDEX IF NOT EXISTS idx_signals_ticker ON signals(ticker);
        CREATE INDEX IF NOT EXISTS idx_outcomes_due ON outcomes(status, due_at);
        """
    )
    _ensure_columns(con, "signals", {
        "data_quality_ok": "INTEGER",
        "data_quality_reason": "TEXT",
        "no_trade_reason": "TEXT",
        "quote_source": "TEXT",
        "option_source": "TEXT",
        "realized_vol_20d": "REAL",
        "option_iv": "REAL",
        "iv_to_rv": "REAL",
        "exit_slippage_points": "REAL",
        "earnings_iv_ok": "INTEGER",
        "earnings_iv_reason": "TEXT",
        "sector": "TEXT",
        "sector_etf": "TEXT",
        "sector_change_pct": "REAL",
        "market_change_pct": "REAL",
        "relative_to_sector_pct": "REAL",
        "sector_filter_ok": "INTEGER",
        "sector_filter_reason": "TEXT",
        "sentiment_price_label": "TEXT",
        "sentiment_price_score_adjustment": "REAL",
        "data_quality_score": "REAL",
        "price_spike_pct": "REAL",
    })
    con.commit()


def _ensure_columns(con: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {row[1] for row in con.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, ddl in columns.items():
        if name not in existing:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def create_run(market_status: str = "", vix: Any = None, raw_ticker_signals: str = "",
               article_count: int = 0, cluster_count: int = 0) -> int:
    con = connect()
    now = utc_now()
    cur = con.execute(
        """
        INSERT INTO runs(started_at, market_date, market_status, vix, raw_ticker_signals,
                         article_count, cluster_count)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (iso(now), now.date().isoformat(), market_status, str(vix), raw_ticker_signals,
         article_count, cluster_count),
    )
    con.commit()
    run_id = int(cur.lastrowid)
    con.close()
    return run_id


def update_run_context(run_id: int, market_status: str = "", vix: Any = None,
                       raw_ticker_signals: str = "", article_count: int | None = None,
                       cluster_count: int | None = None) -> None:
    con = connect()
    con.execute(
        """
        UPDATE runs
        SET market_status = COALESCE(NULLIF(?, ''), market_status),
            vix = COALESCE(NULLIF(?, ''), vix),
            raw_ticker_signals = COALESCE(NULLIF(?, ''), raw_ticker_signals),
            article_count = COALESCE(?, article_count),
            cluster_count = COALESCE(?, cluster_count)
        WHERE run_id = ?
        """,
        (market_status, str(vix) if vix is not None else "", raw_ticker_signals,
         article_count, cluster_count, run_id),
    )
    con.commit()
    con.close()


def _cluster_for_ticker(clusters: list[dict], ticker: str) -> dict:
    matches = [c for c in clusters if c.get("ticker") == ticker]
    if not matches:
        return {}
    return sorted(matches, key=lambda c: c.get("confidence_score", 0), reverse=True)[0]


def _parsed_signal_for_ticker(parsed_signals: list[dict], ticker: str) -> dict:
    for s in parsed_signals:
        if s.get("ticker") == ticker:
            return s
    return {}


def log_market_signals(run_id: int, parsed_signals: list[dict], market_data: list[dict],
                       clusters: list[dict] | None = None) -> None:
    """Schreibt alle geprüften Ticker inkl. Options-/SEC-/Kostenfeldern."""
    clusters = clusters or []
    con = connect()
    created = utc_now()
    signal_ids = []

    for d in market_data:
        ticker = d.get("ticker", "")
        ps = _parsed_signal_for_ticker(parsed_signals, ticker)
        opt = d.get("options") or {}
        sec = {
            "sec_bullish": d.get("sec_bullish"),
            "sec_bearish": d.get("sec_bearish"),
            "sec_insider": d.get("sec_insider"),
            "sec_reason": d.get("sec_reason"),
            "sec_confidence": d.get("sec_confidence"),
        }
        cur = con.execute(
            """
            INSERT INTO signals(
                run_id, created_at, ticker, direction, signal_strength, horizon, dte_days,
                cluster_json, market_json, option_json, sec_json, price, change_pct, rel_vol,
                score, score_reason, liquidity_fail, liquidity_reason, ev_ok, ev_pct,
                ev_dollars, conservative_entry, data_quality_ok, data_quality_reason,
                no_trade_reason, quote_source, option_source, realized_vol_20d, option_iv,
                iv_to_rv, exit_slippage_points, earnings_iv_ok, earnings_iv_reason,
                sector, sector_etf, sector_change_pct, market_change_pct, relative_to_sector_pct,
                sector_filter_ok, sector_filter_reason, sentiment_price_label,
                sentiment_price_score_adjustment, data_quality_score, price_spike_pct
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id, iso(created), ticker, d.get("news_direction") or ps.get("direction"),
                ps.get("score"), ps.get("horizon"), ps.get("dte_days"),
                _json(_cluster_for_ticker(clusters, ticker)), _json(d), _json(opt), _json(sec),
                d.get("price"), d.get("change_pct"), str(d.get("rel_vol")), d.get("score"),
                d.get("_score_reason"), 1 if d.get("_liquidity_fail") else 0,
                d.get("_liquidity_reason", ""), 1 if opt.get("ev_ok") else 0,
                opt.get("ev_pct"), opt.get("ev_dollars"), opt.get("conservative_entry"),
                1 if d.get("_data_quality_ok") else 0, d.get("_data_quality_reason", ""),
                d.get("_no_trade_reason", ""), d.get("_src_quote", ""), opt.get("option_source", ""),
                d.get("realized_vol_20d"), opt.get("iv_decimal"), opt.get("iv_to_rv"),
                opt.get("exit_slippage_points"), 1 if opt.get("earnings_iv_ok", True) else 0,
                opt.get("earnings_iv_reason", ""),
                d.get("sector"), d.get("sector_etf"), d.get("sector_change_pct"),
                d.get("market_change_pct"), d.get("relative_to_sector_pct"),
                1 if d.get("sector_filter_ok", True) else 0, d.get("sector_filter_reason", ""),
                d.get("sentiment_price_label", ""), d.get("sentiment_price_score_adjustment"),
                d.get("data_quality_score"), d.get("price_spike_pct"),
            ),
        )
        signal_id = int(cur.lastrowid)
        signal_ids.append((signal_id, d.get("price")))

    # Outcome-Zeitpunkte anlegen.
    for signal_id, start_price in signal_ids:
        if not start_price or start_price <= 0:
            continue
        for horizon, delta in OUTCOME_HORIZONS.items():
            if delta is None:
                now = created
                due = now.replace(hour=21, minute=0, second=0, microsecond=0)
                if due <= now:
                    due = now + timedelta(hours=1)
            else:
                due = created + delta
            con.execute(
                """
                INSERT OR IGNORE INTO outcomes(signal_id, horizon, due_at, start_price)
                VALUES (?, ?, ?, ?)
                """,
                (signal_id, horizon, iso(due), start_price),
            )

    con.commit()
    con.close()
    logger.info("Journal: %d Signale gespeichert", len(signal_ids))


def log_final_decision(run_id: int, result: dict) -> None:
    con = connect()
    no_trade = 1 if result.get("no_trade") else 0
    ticker = result.get("ticker", "")
    direction = result.get("direction", "")
    con.execute(
        """
        UPDATE runs
        SET no_trade=?, no_trade_reason=?, final_ticker=?, final_direction=?, final_payload_json=?
        WHERE run_id=?
        """,
        (no_trade, result.get("no_trade_grund", ""), ticker, direction, _json(result), run_id),
    )
    if ticker:
        con.execute(
            """
            UPDATE signals SET selected_trade = 1
            WHERE run_id = ? AND ticker = ? AND direction = ?
            """,
            (run_id, ticker, direction),
        )
    con.commit()
    con.close()


def update_due_outcomes(cfg: dict, max_updates: int = 50) -> int:
    """
    Aktualisiert fällige Outcomes mit aktuellem Underlying-Preis.
    Wird bei jedem Lauf aufgerufen.
    """
    con = connect()
    due_rows = con.execute(
        """
        SELECT o.outcome_id, o.signal_id, o.horizon, o.start_price,
               s.ticker, s.direction
        FROM outcomes o
        JOIN signals s ON s.signal_id = o.signal_id
        WHERE o.status = 'open' AND o.due_at <= ?
        ORDER BY o.due_at ASC
        LIMIT ?
        """,
        (iso(), max_updates),
    ).fetchall()

    if not due_rows:
        con.close()
        return 0

    try:
        from market_data import get_quote
    except Exception as e:
        logger.warning("Outcome-Update ohne market_data nicht möglich: %s", e)
        con.close()
        return 0

    updated = 0
    quote_cache: dict[str, float] = {}
    for row in due_rows:
        ticker = row["ticker"]
        if ticker not in quote_cache:
            price, *_ = get_quote(ticker, cfg)
            quote_cache[ticker] = price
        end_price = quote_cache[ticker]
        start_price = row["start_price"]
        if not end_price or not start_price or start_price <= 0:
            continue
        ret = round((end_price - start_price) / start_price * 100.0, 3)
        direction_ret = ret if row["direction"] == "CALL" else -ret
        con.execute(
            """
            UPDATE outcomes
            SET checked_at=?, end_price=?, underlying_return_pct=?,
                direction_return_pct=?, status='done'
            WHERE outcome_id=?
            """,
            (iso(), end_price, ret, direction_ret, row["outcome_id"]),
        )
        updated += 1

    con.commit()
    con.close()
    if updated:
        logger.info("Journal: %d Outcomes aktualisiert", updated)
    return updated
