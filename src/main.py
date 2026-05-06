"""
main.py — Daily Options Report Pipeline (mit simple_journal + neuen Hard Gates)
v13: Integrierte TradingRules (evaluate_trade + calculate_position_size)
"""

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from config_loader import load_config, validate_config
from news_analyzer import (
    fetch_all_feeds, build_earnings_map, cluster_articles,
    format_clusters_for_claude, run_claude, get_market_context,
)
from market_data import (
    process_ticker, get_vix, get_earnings, build_summary,
)
from report_generator import call_claude, build_html, send_email
from rules import parse_ticker_signals, RULES
from simple_journal import journal

def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
    datefmt = "%H:%M:%S"
    logging.basicConfig(level=level, format=fmt, datefmt=datefmt)
    for noisy in ("urllib3", "requests", "httpcore", "httpx", "huggingface_hub",
                  "transformers", "torch", "filelock"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


# ====================== HTML HELPER ======================
def _no_trade_html(today: str, vix=None, market_status: str = "",
                   clusters: list = None, reason: str = "Kein valides Signal") -> str:
    vix_str = str(vix) if vix and vix != "n/v" else "n/v"
    status_str = market_status or "unbekannt"
    clusters = clusters or []
    cluster_rows = ""
    for c in clusters[:5]:
        conf = c.get("confidence_score", 0)
        tick = c.get("ticker", "?")
        head = c.get("headline_repr", "")[:60]
        sent = c.get("sentiment_score", 0)
        src = c.get("sentiment_source", "keyword")
        sent_icon = "📈" if sent > 0.1 else ("📉" if sent < -0.1 else "➖")
        src_badge = "🤖" if src == "finbert" else "🔤"
        cluster_rows += f'<tr><td style="padding:6px 8px;font-weight:600;">{tick}</td>' \
                        f'<td style="padding:6px 8px;text-align:center;">{conf:.2f}</td>' \
                        f'<td style="padding:6px 8px;text-align:center;">{sent_icon}{src_badge}</td>' \
                        f'<td style="padding:6px 8px;color:#86868b;">{head}</td></tr>'
    cluster_section = f'<div style="margin-top:20px;">... {cluster_rows} ...</div>' if cluster_rows else ""
    return f'''<html><head><meta charset="UTF-8"></head><body style="background:#f5f5f7;">
    <div style="max-width:520px;margin:0 auto;padding:32px 16px;background:white;border-radius:18px;">
        <h2>Daily Options Report — {today}</h2>
        <h3 style="color:#ff3b30;">Heute kein Trade</h3>
        <p>VIX: {vix_str} | Grund: {reason}</p>
        {cluster_section}
    </div></body></html>'''


def _error_html(error: str, today: str) -> str:
    return f'<html><body><h2>Fehler am {today}</h2><p>{error}</p></body></html>'


def _send_or_save(html: str, subject: str, cfg: dict, dry_run: bool) -> None:
    if dry_run:
        with open("report_preview.html", "w", encoding="utf-8") as f:
            f.write(html)
        logger.info("Dry-run: report_preview.html gespeichert")
    else:
        send_email(subject, html, cfg)


def _enrich_market_data_with_cluster_context(market_data: list, clusters: list) -> None:
    for d in market_data:
        ticker = d.get("ticker", "")
        matches = [c for c in (clusters or []) if c.get("ticker") == ticker]
        if matches:
            best = max(matches, key=lambda c: c.get("confidence_score", 0))
            d["news_confidence_score"] = best.get("confidence_score")
            d["news_sentiment_score"] = best.get("sentiment_score")
            d["news_sentiment_source"] = best.get("sentiment_source", "keyword")


# ====================== MAIN ======================
def main() -> int:
    parser = argparse.ArgumentParser(description="Daily Options Report")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)

    cfg = load_config()
    if not validate_config(cfg):
        logger.error("Konfiguration unvollständig")
        return 1

    today = datetime.now().strftime("%d.%m.%Y")
    t_start = time.monotonic()

    journal.start_run()
    logger.info("=" * 70)
    logger.info("Daily Options Report — %s (Run ID: %s)", today, journal.get_run_id())
    logger.info("=" * 70)

    try:
        journal.update_outcomes(cfg)
    except Exception as e:
        logger.warning("Outcome-Update übersprungen: %s", e)

    # STEP 1: News
    logger.info("[1/3] News-Analyse...")
    t1 = time.monotonic()
    articles = fetch_all_feeds()
    earnings_map = build_earnings_map(cfg.get("finnhub_key", ""))
    clusters = cluster_articles(articles, earnings_map)

    logger.info("Nach Ticker-Filterung: %d Cluster übrig (von %d Artikeln)", len(clusters), len(articles))
    if clusters:
        top = sorted(clusters, key=lambda c: c.get("confidence_score", 0), reverse=True)[:5]
        for c in top:
            logger.info(" → %s (conf=%.1f, %s): %s",
                        c["ticker"], c["confidence_score"],
                        c["event_type"], c["headline_repr"][:80])

    cluster_text = format_clusters_for_claude(clusters)
    market_time, market_status = get_market_context()

    ticker_signals = run_claude(
        cluster_text, market_time, market_status, cfg.get("anthropic_api_key", "")
    )
    vix_value = get_vix()
    logger.info("Claude Signal: %s | VIX: %s", ticker_signals[:100], vix_value)

    if ticker_signals in ("TICKER_SIGNALS:NONE", "", None):
        data = {"no_trade": True, "no_trade_grund": "Kein valides Signal", "vix": vix_value}
        journal.log_decision(data)
        html = _no_trade_html(today, vix_value, market_status, clusters[:3], "Kein valides Signal")
        _send_or_save(html, f"⏸️ Daily Options Report – Kein Trade – {today}", cfg, args.dry_run)
        return 0

    # STEP 2: Marktdaten
    logger.info("[2/3] Marktdaten...")
    t2 = time.monotonic()
    parsed_signals = parse_ticker_signals(ticker_signals)
    if not parsed_signals:
        logger.error("Keine gültigen Ticker geparst")
        return 1

    ticker_directions = {s["ticker"]: s["direction"] for s in parsed_signals}
    tickers = list(ticker_directions.keys())
    dte_map = {s["ticker"]: s["dte_days"] for s in parsed_signals}

    # Earnings
    with ThreadPoolExecutor(max_workers=2) as ex:
        earnings_fut = ex.submit(get_earnings,
                                 datetime.now().strftime("%Y-%m-%d"),
                                 (datetime.now() + timedelta(days=10)).strftime("%Y-%m-%d"),
                                 cfg.get("finnhub_key", ""))
        earnings_list = earnings_fut.result(timeout=15)

    # Ticker verarbeiten
    with ThreadPoolExecutor(max_workers=RULES.max_tickers) as ex:
        futures = {
            ex.submit(process_ticker, t, ticker_directions[t], earnings_list, cfg, dte_map.get(t, 21)): t
            for t in tickers
        }
        results = []
        for f in as_completed(futures, timeout=45):
            try:
                results.append(f.result())
            except Exception as e:
                logger.error("Ticker %s fehlgeschlagen: %s", futures[f], e)

    market_data = [r for r in results if r]
    _enrich_market_data_with_cluster_context(market_data, clusters)

    # === NEU: Hard-Gate Prüfung mit evaluate_trade + Position Sizing ===
    logger.info("[2.5/3] Hard-Gate Prüfung + Position Sizing...")
    executed = []
    skipped = []

    for d in market_data:
        ticker = d["ticker"]
        news_alpha = d.get("news_confidence_score", 50)   # aus Cluster-Kontext
        ticker_info = {"market_cap": 999_999_999, "price": d["price"], "spread_pct": 5.0}  # Platzhalter – später erweitern

        passed, reason = RULES.evaluate_trade(
            ticker_info=ticker_info,
            market_metrics=d,
            news_alpha=news_alpha
        )

        if passed and d.get("score", 0) >= RULES.min_score:
            total_conviction = round(
                (news_alpha * 0.55) + (d.get("score", 50) * 0.45), 2
            )
            pos_size = RULES.calculate_position_size(total_conviction, 250_000)

            logger.info(f"✅ ALARM: {ticker} HIGH CONVICTION | Conviction={total_conviction} | Size=${pos_size:,.0f}")

            executed.append({
                "ticker": ticker,
                "direction": d.get("news_direction"),
                "conviction": total_conviction,
                "position_size": pos_size,
                "reason": "All gates passed"
            })
        else:
            skipped.append({"ticker": ticker, "reason": reason})

    journal.log_signals(parsed_signals, market_data, clusters)

    # STEP 3: Report
    logger.info("[3/3] Report generieren...")
    try:
        market_summary = build_summary(market_data, vix_value, ticker_directions, earnings_list, [], [])
        data = call_claude(market_summary, cfg.get("anthropic_api_key", ""), vix_direct=vix_value)
        journal.log_decision(data)

        html_report = build_html(data, today)
        no_trade = data.get("no_trade", False) or len(executed) == 0
        subject = f"⏸️ No Trade – {today}" if no_trade else f"📊 Trade-Alarm – {today}"
        _send_or_save(html_report, subject, cfg, args.dry_run)
    except Exception as e:
        logger.error("Report-Fehler: %s", e)
        data = {"no_trade": True, "no_trade_grund": f"Report Fehler: {e}"}
        journal.log_decision(data)
        _send_or_save(_error_html(str(e), today), f"⚠️ Report Fehler – {today}", cfg, args.dry_run)

    logger.info("✅ Gesamtlauf beendet in %.1fs | Executed: %d | Skipped: %d",
                time.monotonic() - t_start, len(executed), len(skipped))
    return 0


if __name__ == "__main__":
    sys.exit(main())
