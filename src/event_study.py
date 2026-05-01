"""
event_study.py — Auswertung des SQLite-Journals.

Beispiele:
    python src/event_study.py
    python src/event_study.py --selected-only
    python src/event_study.py --csv data/event_study.csv
    python src/event_study.py --group sector
    python src/event_study.py --group sentpx
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path

from trading_journal import DB_PATH, connect


VALID_GROUPS = {"base", "sector", "sector_momentum", "sentpx", "ev_bucket", "ivrv_bucket", "iv_rank_bucket", "data_quality"}


def fetch_rows(selected_only: bool = False):
    con = connect()
    where = "AND s.selected_trade = 1" if selected_only else ""
    rows = con.execute(
        f"""
        SELECT s.ticker, s.direction, s.signal_strength, s.score, s.score_reason,
               s.ev_ok, s.ev_pct, s.ev_dollars, s.selected_trade,
               s.sector, s.sector_etf, s.sector_filter_ok, s.sector_filter_reason,
               s.sector_vs_market_pct, s.sector_momentum_confirmation,
               s.sentiment_price_label, s.sentiment_price_score_adjustment,
               s.data_quality_ok, s.data_quality_reason, s.data_quality_score,
               s.iv_to_rv, s.option_iv, s.iv_rank, s.iv_percentile, s.iv_history_count, s.no_trade_reason,
               o.horizon, o.start_price, o.end_price,
               o.underlying_return_pct, o.direction_return_pct
        FROM outcomes o
        JOIN signals s ON s.signal_id = o.signal_id
        WHERE o.status = 'done' {where}
        ORDER BY o.horizon, s.direction, s.ticker
        """
    ).fetchall()
    con.close()
    return rows


def _bucket_ev(ev_pct):
    if ev_pct is None:
        return "ev_unknown"
    try:
        ev = float(ev_pct)
    except (TypeError, ValueError):
        return "ev_unknown"
    if ev < 0:
        return "ev_neg"
    if ev < 12:
        return "ev_0_12"
    if ev < 25:
        return "ev_12_25"
    return "ev_25_plus"


def _bucket_ivrv(iv_to_rv):
    if iv_to_rv is None:
        return "ivrv_unknown"
    try:
        x = float(iv_to_rv)
    except (TypeError, ValueError):
        return "ivrv_unknown"
    if x < 1.0:
        return "ivrv_lt1"
    if x < 1.35:
        return "ivrv_1_1.35"
    if x < 2.0:
        return "ivrv_1.35_2"
    return "ivrv_gt2"



def _bucket_iv_rank(iv_rank, iv_history_count):
    try:
        n = int(iv_history_count or 0)
    except (TypeError, ValueError):
        n = 0
    if n < 30 or iv_rank is None:
        return "ivrank_insufficient"
    try:
        x = float(iv_rank)
    except (TypeError, ValueError):
        return "ivrank_unknown"
    if x < 25:
        return "ivrank_lt25"
    if x < 50:
        return "ivrank_25_50"
    if x < 80:
        return "ivrank_50_80"
    return "ivrank_80_plus"

def _group_key(row, group: str):
    selected = "selected" if row["selected_trade"] else "all"
    if group == "sector":
        bucket = row["sector_etf"] or row["sector"] or "unknown"
    elif group == "sector_momentum":
        bucket = row["sector_momentum_confirmation"] or "unknown"
    elif group == "sentpx":
        bucket = row["sentiment_price_label"] or "unknown"
    elif group == "ev_bucket":
        bucket = _bucket_ev(row["ev_pct"])
    elif group == "ivrv_bucket":
        bucket = _bucket_ivrv(row["iv_to_rv"])
    elif group == "iv_rank_bucket":
        bucket = _bucket_iv_rank(row["iv_rank"], row["iv_history_count"])
    elif group == "data_quality":
        bucket = "dq_ok" if row["data_quality_ok"] else "dq_fail"
    else:
        bucket = selected
    return (row["horizon"], row["direction"], bucket)


def summarize(rows, group: str = "base"):
    groups = {}
    for r in rows:
        key = _group_key(r, group)
        groups.setdefault(key, []).append(r["direction_return_pct"])

    lines = []
    title = "GROUP" if group != "base" else "SET"
    lines.append(f"HORIZON | DIR  | {title:<18} | N   | HIT%  | AVG%   | MEDIAN%")
    lines.append("-" * 82)
    for key in sorted(groups.keys()):
        vals = [v for v in groups[key] if v is not None]
        if not vals:
            continue
        vals_sorted = sorted(vals)
        n = len(vals)
        hit = sum(1 for v in vals if v > 0) / n * 100.0
        avg = sum(vals) / n
        med = vals_sorted[n // 2] if n % 2 else (vals_sorted[n // 2 - 1] + vals_sorted[n // 2]) / 2
        lines.append(f"{key[0]:<7} | {key[1]:<4} | {str(key[2])[:18]:<18} | {n:<3} | {hit:>5.1f} | {avg:>6.2f} | {med:>7.2f}")
    return "\n".join(lines)


def write_csv(rows, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for r in rows:
            writer.writerow(dict(r))


def main():
    parser = argparse.ArgumentParser(description="Event-Study aus trading_journal.sqlite")
    parser.add_argument("--selected-only", action="store_true", help="nur finale Trade-Auswahl")
    parser.add_argument("--csv", help="CSV Export-Pfad")
    parser.add_argument("--group", default="base", choices=sorted(VALID_GROUPS),
                        help="Gruppierung: base, sector, sector_momentum, sentpx, ev_bucket, ivrv_bucket, iv_rank_bucket, data_quality")
    args = parser.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"Kein Journal gefunden: {DB_PATH}")

    rows = fetch_rows(args.selected_only)
    if not rows:
        print("Noch keine abgeschlossenen Outcomes. Nach einigen Läufen erneut ausführen.")
        return
    print(summarize(rows, args.group))
    if args.csv:
        write_csv(rows, Path(args.csv))
        print(f"CSV geschrieben: {args.csv}")


if __name__ == "__main__":
    main()
