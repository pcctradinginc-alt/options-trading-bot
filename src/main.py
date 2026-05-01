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
from rules import parse_ticker_signals, RULES, merge_reasons
from trading_journal import (
    create_run, update_run_context, log_market_signals,
    log_final_decision, update_due_outcomes,
)


def setup_logging(verbose: bool) -> None:
    level   = logging.DEBUG if verbose else logging.INFO
    fmt     = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
    datefmt = "%H:%M:%S"
    logging.basicConfig(level=level, format=fmt, datefmt=datefmt)
    # Third-party libraries can be extremely noisy in GitHub Actions, especially
    # when FinBERT downloads/checks Hugging Face assets. Keep our app logs useful.
    for noisy in (
        "urllib3", "requests", "httpcore", "httpx", "filelock",
        "huggingface_hub", "huggingface_hub.utils._http",
        "transformers", "transformers.modeling_utils", "torch",
    ):
        logging.getLogger(noisy).setLevel(logging.WARNING)


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

    # Frühere Signale nachträglich bewerten, bevor der neue Lauf startet.
    try:
        update_due_outcomes(cfg)
    except Exception as e:
        logger.warning("Outcome-Update übersprungen: %s", e)

    today   = datetime.now().strftime("%d.%m.%Y")
    t_start = time.monotonic()
    run_id  = create_run()

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
    update_run_context(run_id, market_status=market_status,
                       article_count=len(articles), cluster_count=len(clusters))

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
    update_run_context(run_id, market_status=market_status, vix=vix_value,
                       raw_ticker_signals=ticker_signals,
                       article_count=len(articles), cluster_count=len(clusters))

    # Kein Signal → No-Trade Email
    if ticker_signals in ("TICKER_SIGNALS:NONE", ""):
        logger.info("Keine validen Signale heute")
        log_final_decision(run_id, {"no_trade": True, "no_trade_grund": "Kein valides Signal", "vix": vix_value})
        html    = _no_trade_html(today, vix_value, market_status, clusters[:3], reason="Kein valides Signal")
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

    # Cluster-/Sentiment-Kontext aus Step 1 an Marktdaten hängen.
    # Wichtig fuer spaetere Event-Studies: News-Score und Sentiment-Quelle bleiben erhalten,
    # auch wenn harte Gates den Trade-Score spaeter auf 0 setzen.
    _enrich_market_data_with_cluster_context(market_data, clusters)

    # PRE-/AFTER-MARKET ist Research-only. Marktdaten werden journalisiert,
    # aber kein finaler Trade darf freigegeben werden.
    trade_window_open = (market_status == "OPEN")
    if not trade_window_open:
        _apply_market_status_gate(market_data, market_status)

    # ── SEC EDGAR Check ───────────────────────────────────
    _run_sec_check(market_data)

    # ── Journal: alle Signale/Marktdaten/Options-EV speichern ──
    try:
        log_market_signals(run_id, parsed_signals, market_data, clusters)
    except Exception as e:
        logger.warning("Journal-Signal-Log fehlgeschlagen: %s", e)

    ranked       = sorted(market_data, key=lambda x: x["score"], reverse=True)
    unusual_list = [d["ticker"] for d in market_data if d.get("unusual")]
    failed       = [d["ticker"] for d in market_data if d.get("_src_quote") == "failed"]

    market_summary = build_summary(
        ranked, vix_value, ticker_directions, earnings_list, unusual_list, failed
    )

    if args.verbose:
        logger.debug("\n%s", market_summary)

    logger.info("  Marktdaten fertig  (%.1fs)", time.monotonic() - t2)

    if (not trade_window_open) or not any(
        d.get("score", 0) >= RULES.min_score
        and d.get("_data_quality_ok")
        and d.get("sector_filter_ok", True)
        and not d.get("_liquidity_fail")
        and d.get("options", {}).get("ev_ok")
        for d in ranked
    ):
        if not trade_window_open:
            logger.info("Research-only: Marktstatus %s — keine finale Trade-Freigabe", market_status)
        else:
            logger.info("Kein Ticker besteht Datenqualität+Sektor+Score+Liquidität+EV+Earnings-IV-Gates")
        reject_reasons = []
        for d in ranked[:5]:
            reason = d.get("_no_trade_reason") or d.get("_score_reason") or "Gate nicht bestanden"
            reject_reasons.append(f"{d.get('ticker','?')}: {reason}")
        if not trade_window_open:
            reject_reasons.insert(0, f"Research-only: Marktstatus {market_status}; keine finale Trade-Freigabe")
        no_trade_reason = " | ".join(reject_reasons)[:500] or "Kein Kandidat mit positivem Options EV"
        data = {
            "datum": today, "vix": str(vix_value), "regime": "TRENDING",
            "regime_farbe": "gelb", "no_trade": True,
            "no_trade_grund": no_trade_reason,
            "ticker_tabelle": [],
        }
        log_final_decision(run_id, data)
        html_report = _no_trade_html(today, vix_value, market_status, clusters[:3], reason=no_trade_reason)
        subject = "⏸️ Daily Options Report – No Trade – " + today
        _send_or_save(html_report, subject, cfg, args.dry_run)
        logger.info("Fertig in %.1fs", time.monotonic() - t_start)
        return 0

    # ══════════════════════════════════════════════════════
    # STEP 3: REPORT + EMAIL
    # ══════════════════════════════════════════════════════
    logger.info("[3/3] Report generieren...")
    t3 = time.monotonic()

    try:
        data        = call_claude(market_summary, cfg.get("anthropic_api_key",""),
                                  vix_direct=vix_value)
        log_final_decision(run_id, data)
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
        log_final_decision(run_id, {"no_trade": True, "no_trade_grund": "Report Fehler", "error": str(e)})
        html_report = _error_html(str(e), today)
        subject     = "⚠️ Daily Options Report – Fehler – " + today

    _send_or_save(html_report, subject, cfg, args.dry_run)
    logger.info("Fertig in %.1fs", time.monotonic() - t_start)
    return 0



def _best_cluster_for_ticker(clusters: list, ticker: str) -> dict:
    matches = [c for c in (clusters or []) if c.get("ticker") == ticker]
    if not matches:
        return {}
    return sorted(matches, key=lambda c: c.get("confidence_score", 0), reverse=True)[0]


def _enrich_market_data_with_cluster_context(market_data: list, clusters: list) -> None:
    for d in market_data:
        c = _best_cluster_for_ticker(clusters, d.get("ticker", ""))
        if not c:
            continue
        d["news_confidence_score"] = c.get("confidence_score")
        d["news_sentiment_score"] = c.get("sentiment_score")
        d["news_sentiment_source"] = c.get("sentiment_source", "keyword")
        d["news_headline_repr"] = c.get("headline_repr", "")
        d["news_event_type"] = c.get("event_type", "")


def _apply_market_status_gate(market_data: list, market_status: str) -> None:
    reason = f"Research-only: Marktstatus {market_status}; keine finale Trade-Freigabe"
    for d in market_data:
        d["market_status_trade_allowed"] = False
        d["market_status_gate_reason"] = reason
        d["raw_signal_score"] = d.get("raw_signal_score", d.get("score", 0.0))
        d["gate_adjusted_score"] = d.get("gate_adjusted_score", d.get("score", 0.0))
        d["_no_trade_reason"] = merge_reasons(d.get("_no_trade_reason", ""), reason)
        d["_score_reason"] = merge_reasons(d.get("_score_reason", ""), "market_status_research_only")
        # Finaler Trade-Score wird auf 0 gesetzt, aber raw_signal_score bleibt erhalten.
        d["score"] = 0.0


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

            # v7: SEC verändert den Score nicht mehr direkt.
            # Es wird als Feature journalisiert und im Report erklärt. Score-Gewichte werden später empirisch geprüft.
            if sec.get("bearish") or sec.get("insider_buy"):
                logger.info("SEC Feature %s: bearish=%s insider_buy=%s | %s",
                            ticker, sec.get("bearish"), sec.get("insider_buy"), sec.get("reason", ""))

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
                   clusters: list = None, reason: str = "Kein valides Signal") -> str:
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
        f'<span style="font-size:14px;color:#1d1d1f;text-align:right;max-width:280px;">{reason}</span>'
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
