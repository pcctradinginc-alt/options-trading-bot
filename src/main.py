"""
main.py — Daily Options Report Pipeline

v4 Änderungen:
- SEC EDGAR Check nach Marktdaten-Verarbeitung
- Fail-safe: SEC optional, bei Fehler ignoriert
- transformers/torch Logging unterdrückt
- Alle v3 Fixes: VIX direkt, DTE pro Ticker, etc.
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


def setup_logging(verbose: bool) -> None:
    level   = logging.DEBUG if verbose else logging.INFO
    fmt     = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
    datefmt = "%H:%M:%S"
    logging.basicConfig(level=level, format=fmt, datefmt=datefmt)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("transformers").setLevel(logging.ERROR)
    logging.getLogger("torch").setLevel(logging.ERROR)


logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Daily Options Report")
    parser.add_argument("--dry-run", action="store_true",
                        help="Kein Email-Versand — Report als report_preview.html")
    parser.add_argument("--verbose", action="store_true",
                        help="Detaillierte Ausgabe")
    args = parser.parse_args()

    setup_logging(args.verbose)

    cfg = load_config()
    if not validate_config(cfg):
        logger.error("Konfiguration unvollständig — siehe config/config.example.yaml")
        return 1

    today   = datetime.now().strftime("%d.%m.%Y")
    t_start = time.monotonic()

    logger.info("=" * 50)
    logger.info("  Daily Options Report — %s", today)
    logger.info("=" * 50)

    # ══════════════════════════════════════════════════════
    # STEP 1: NEWS-ANALYSE
    # ══════════════════════════════════════════════════════
    logger.info("[1/3] News-Analyse...")
    t1 = time.monotonic()

    articles     = fetch_all_feeds()
    earnings_map = build_earnings_map(cfg.get("finnhub_key",""))
    clusters     = cluster_articles(articles, earnings_map)
    cluster_text = format_clusters_for_claude(clusters)
    market_time, market_status = get_market_context()

    logger.info("  %d Artikel | %d Cluster | %s (%s)",
                len(articles), len(clusters), market_time, market_status)

    if args.verbose:
        for c in clusters[:5]:
            src = c.get("sentiment_source", "keyword")
            logger.debug("  [%.2f] %-8s sent=%+.2f(%s) %s",
                         c["confidence_score"], c["ticker"],
                         c["sentiment_score"], src, c["headline_repr"][:45])

    ticker_signals = run_claude(
        cluster_text, market_time, market_status,
        cfg.get("anthropic_api_key",""),
    )
    logger.info("  Signal: %s  (%.1fs)", ticker_signals, time.monotonic() - t1)

    # VIX direkt holen — autoritativer Wert
    vix_value = get_vix()
    logger.info("  VIX: %s", vix_value)

    # Kein Signal → No-Trade Email
    if ticker_signals in ("TICKER_SIGNALS:NONE", ""):
        logger.info("Keine validen Signale heute")
        html    = _no_trade_html(today, vix_value, market_status, clusters[:3])
        subject = "⏸️ Daily Options Report – Kein Trade heute – " + today
        _send_or_save(html, subject, cfg, args.dry_run)
        logger.info("Fertig in %.1fs", time.monotonic() - t_start)
        return 0

    # ══════════════════════════════════════════════════════
    # STEP 2: MARKTDATEN
    # ══════════════════════════════════════════════════════
    logger.info("[2/3] Marktdaten...")
    t2 = time.monotonic()

    parsed_signals = parse_ticker_signals(ticker_signals)

    if not parsed_signals:
        logger.error("Keine gueltigen Ticker geparst: %s", ticker_signals)
        return 1

    ticker_directions = {s["ticker"]: s["direction"] for s in parsed_signals}
    tickers           = list(ticker_directions.keys())
    dte_map           = {s["ticker"]: s["dte_days"] for s in parsed_signals}

    logger.info("  Geparste Ticker: %s", ", ".join(
        t + ":" + d + "(" + str(dte_map.get(t, 21)) + "DTE)"
        for t, d in ticker_directions.items()
    ))

    finnhub_key = cfg.get("finnhub_key","")
    date_today  = datetime.now().strftime("%Y-%m-%d")
    date_end    = (datetime.now() + timedelta(days=10)).strftime("%Y-%m-%d")

    with ThreadPoolExecutor(max_workers=2) as ex:
        earnings_fut  = ex.submit(get_earnings, date_today, date_end, finnhub_key)
        earnings_list = earnings_fut.result(timeout=12)

    logger.info("  VIX: %s | %d Ticker", vix_value, len(tickers))

    with ThreadPoolExecutor(max_workers=RULES.max_tickers) as ex:
        futures = {
            ex.submit(process_ticker, t, ticker_directions[t],
                      earnings_list, cfg, dte_map.get(t, 21)): t
            for t in tickers
        }
        results = []
        for f in as_completed(futures, timeout=30):
            try:
                results.append(f.result())
            except Exception as e:
                logger.error("Ticker-Future Fehler: %s", e)

    market_data = [r for r in results if r]

    # ── SEC EDGAR Check ───────────────────────────────────
    _run_sec_check(market_data)

    ranked       = sorted(market_data, key=lambda x: x["score"], reverse=True)
    unusual_list = [d["ticker"] for d in market_data if d.get("unusual")]
    failed       = [d["ticker"] for d in market_data if d.get("_src_quote") == "failed"]

    market_summary = build_summary(
        ranked, vix_value, ticker_directions, earnings_list, unusual_list, failed
    )

    if args.verbose:
        logger.debug("\n%s", market_summary)

    logger.info("  Marktdaten fertig  (%.1fs)", time.monotonic() - t2)

    # ══════════════════════════════════════════════════════
    # STEP 3: REPORT + EMAIL
    # ══════════════════════════════════════════════════════
    logger.info("[3/3] Report generieren...")
    t3 = time.monotonic()

    try:
        data        = call_claude(market_summary, cfg.get("anthropic_api_key",""),
                                  vix_direct=vix_value)
        html_report = build_html(data, today)
        no_trade    = data.get("no_trade", False)
        ticker      = data.get("ticker","")
        subject     = (
            "⏸️ Daily Options Report – No Trade – " + today if no_trade
            else "📊 Daily Options Report – " + today + " · " + ticker
        )
        logger.info("  Ergebnis: %s  (%.1fs)",
                    "NO TRADE" if no_trade else "TRADE " + ticker,
                    time.monotonic() - t3)
    except (ValueError, RuntimeError) as e:
        logger.error("Report-Fehler: %s", e)
        html_report = _error_html(str(e), today)
        subject     = "⚠️ Daily Options Report – Fehler – " + today

    _send_or_save(html_report, subject, cfg, args.dry_run)
    logger.info("Fertig in %.1fs", time.monotonic() - t_start)
    return 0


# ══════════════════════════════════════════════════════════
# SEC EDGAR CHECK
# ══════════════════════════════════════════════════════════

def _run_sec_check(market_data: list) -> None:
    """
    SEC EDGAR Check für alle Einzeltitel.
    Fail-safe: bei Fehler/fehlender Library ignoriert.
    Modifiziert market_data in-place.
    """
    try:
        from sec_check import get_sec_signal, ETF_TICKERS as SEC_ETF

        for d in market_data:
            ticker = d["ticker"]
            if ticker in SEC_ETF:
                d["sec_bullish"]    = False
                d["sec_bearish"]    = False
                d["sec_insider"]    = False
                d["sec_reason"]     = "ETF — kein SEC-Check"
                d["sec_confidence"] = 0.0
                continue

            sec = get_sec_signal(ticker, days_back=14)
            d["sec_bullish"]    = sec.get("bullish", False)
            d["sec_bearish"]    = sec.get("bearish", False)
            d["sec_insider"]    = sec.get("insider_buy", False)
            d["sec_reason"]     = sec.get("reason", "")
            d["sec_confidence"] = sec.get("confidence", 0.0)

            # SEC bearish → Score halbieren
            if sec.get("bearish") and d["score"] > 0:
                old_score  = d["score"]
                d["score"] = round(d["score"] * 0.5, 2)
                logger.info("SEC bearish %s: Score %.1f → %.1f | %s",
                            ticker, old_score, d["score"], sec.get("reason",""))

            # SEC Insider-Kauf → Score +10
            if sec.get("insider_buy") and d["score"] > 0:
                old_score  = d["score"]
                d["score"] = round(min(100.0, d["score"] + 10.0), 2)
                logger.info("SEC Insider-Kauf %s: Score %.1f → %.1f | %s",
                            ticker, old_score, d["score"], sec.get("reason",""))

    except ImportError:
        logger.debug("sec_check nicht installiert — SEC übersprungen")
    except Exception as e:
        logger.warning("SEC-Check Pipeline-Fehler: %s", e)


# ══════════════════════════════════════════════════════════
# HILFSFUNKTIONEN
# ══════════════════════════════════════════════════════════

def _send_or_save(html: str, subject: str, cfg: dict, dry_run: bool) -> None:
    if dry_run:
        with open("report_preview.html", "w", encoding="utf-8") as f:
            f.write(html)
        logger.info("Dry-run: report_preview.html gespeichert")
    else:
        send_email(subject, html, cfg)


def _no_trade_html(today: str, vix=None, market_status: str = "",
                   clusters: list = None) -> str:
    vix_str    = str(vix) if vix and vix != "n/v" else "n/v"
    status_str = market_status or "unbekannt"
    clusters   = clusters or []

    cluster_rows = ""
    for c in clusters:
        conf      = c.get("confidence_score", 0)
        tick      = c.get("ticker", "?")
        head      = c.get("headline_repr", "")[:60]
        sent      = c.get("sentiment_score", 0)
        src       = c.get("sentiment_source", "keyword")
        sent_icon = "📈" if sent > 0.1 else ("📉" if sent < -0.1 else "➖")
        src_badge = "🤖" if src == "finbert" else "🔤"
        cluster_rows += (
            f'<tr>'
            f'<td style="padding:6px 8px;font-size:12px;font-weight:600;'
            f'color:#1d1d1f;">{tick}</td>'
            f'<td style="padding:6px 8px;font-size:12px;color:#86868b;'
            f'text-align:center;">{conf:.2f}</td>'
            f'<td style="padding:6px 8px;font-size:12px;color:#86868b;'
            f'text-align:center;">{sent_icon}{src_badge}</td>'
            f'<td style="padding:6px 8px;font-size:12px;color:#86868b;">{head}</td>'
            f'</tr>'
        )

    cluster_section = ""
    if cluster_rows:
        cluster_section = (
            '<div style="margin-top:20px;text-align:left;">'
            '<p style="font-size:11px;font-weight:600;color:#86868b;'
            'text-transform:uppercase;letter-spacing:0.06em;margin:0 0 8px 0;">'
            'Top Cluster heute</p>'
            '<table style="width:100%;border-collapse:collapse;">'
            '<tr style="border-bottom:2px solid #e5e5ea;">'
            '<th style="padding:4px 8px;font-size:10px;color:#86868b;'
            'text-align:left;">Ticker</th>'
            '<th style="padding:4px 8px;font-size:10px;color:#86868b;">Conf</th>'
            '<th style="padding:4px 8px;font-size:10px;color:#86868b;">Sent</th>'
            '<th style="padding:4px 8px;font-size:10px;color:#86868b;'
            'text-align:left;">Headline</th>'
            '</tr>' + cluster_rows + '</table>'
            '<p style="font-size:10px;color:#86868b;margin:6px 0 0 0;">'
            '🤖 = finBERT &nbsp; 🔤 = Keyword</p>'
            '</div>'
        )

    return (
        '<html><head><meta charset="UTF-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1.0"></head>'
        '<body style="margin:0;padding:0;background:#f5f5f7;'
        'font-family:-apple-system,BlinkMacSystemFont,Helvetica Neue,Arial,sans-serif;">'
        '<div style="max-width:520px;margin:0 auto;padding:32px 16px;">'
        '<div style="background:white;border-radius:18px;padding:32px;'
        'box-shadow:0 2px 12px rgba(0,0,0,0.07);">'
        '<div style="text-align:center;margin-bottom:24px;">'
        '<div style="font-size:48px;margin-bottom:12px;">⏸️</div>'
        '<h2 style="color:#1d1d1f;margin:0 0 6px 0;font-size:22px;font-weight:700;">'
        'Daily Options Report</h2>'
        '<h3 style="color:#ff3b30;margin:0 0 6px 0;font-size:16px;font-weight:600;">'
        'Heute kein Trade</h3>'
        f'<p style="color:#86868b;font-size:13px;margin:0;">{today}</p>'
        '</div>'
        '<div style="border-top:1px solid #e5e5ea;padding-top:4px;">'
        '<div style="display:flex;justify-content:space-between;align-items:center;'
        'padding:10px 0;border-bottom:1px solid #e5e5ea;">'
        '<span style="font-size:14px;color:#86868b;">VIX</span>'
        f'<span style="font-size:14px;font-weight:600;color:#1d1d1f;">{vix_str}</span>'
        '</div>'
        '<div style="display:flex;justify-content:space-between;align-items:center;'
        'padding:10px 0;border-bottom:1px solid #e5e5ea;">'
        '<span style="font-size:14px;color:#86868b;">Markt</span>'
        f'<span style="font-size:14px;font-weight:600;color:#1d1d1f;">{status_str}</span>'
        '</div>'
        '<div style="display:flex;justify-content:space-between;align-items:center;'
        'padding:10px 0;">'
        '<span style="font-size:14px;color:#86868b;">Grund</span>'
        '<span style="font-size:14px;color:#1d1d1f;">Kein valides Signal</span>'
        '</div>'
        '</div>'
        + cluster_section +
        '<div style="margin-top:20px;background:#f5f5f7;border-radius:12px;'
        'padding:14px;text-align:center;">'
        '<p style="margin:0;font-size:12px;color:#86868b;">'
        'Morgen läuft die Analyse erneut automatisch.</p>'
        '</div>'
        '</div></div></body></html>'
    )


def _error_html(error: str, today: str) -> str:
    return (
        '<html><head><meta charset="UTF-8"></head>'
        '<body style="font-family:-apple-system,sans-serif;background:#f5f5f7;'
        'padding:40px;text-align:center;">'
        '<div style="background:white;border-radius:18px;padding:32px;'
        'max-width:400px;margin:0 auto;">'
        '<div style="font-size:40px;margin-bottom:16px;">⚠️</div>'
        '<h2 style="color:#1d1d1f;margin:0 0 8px 0;">'
        'Daily Options Report — Fehler</h2>'
        f'<p style="color:#86868b;font-size:14px;">{error}</p>'
        '</div></body></html>'
    )


if __name__ == "__main__":
    sys.exit(main())
