"""
report_generator.py — HTML-Report + Email-Versand (Step 3)

Fixes gegenüber v1:
- JSON end=0 Ambiguität aufgelöst: explizite found/not-found Logik
- Bare excepts durch spezifische Exceptions ersetzt
- Logging statt print()
"""

import json
import logging
import smtplib
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
from requests.exceptions import RequestException, Timeout

logger = logging.getLogger(__name__)

PROMPT = """Du bist eine regelbasierte Options-KI. Antworte NUR mit JSON - kein Text, kein Markdown.

REGELN:
- VIX >= 25 -> no_trade: true, no_trade_grund: maximal 8 Woerter ohne Satzzeichen
- VIX 20-24.99 -> einsatz: 150
- VIX < 20 -> einsatz: 250
- Ausschluss: Score <50 | change_pct <0 | unter MA50 | Spread >2% | OI <5000
- stop_loss_eur = 30% von einsatz
- kontrakte = round(einsatz / (midpoint * 100)) wenn midpoint bekannt
- bid/ask aus Marktdaten uebernehmen

ETF-SONDERREGEL (wenn ETF-SIGNAL in Marktdaten):
- Empfehlung trotzdem ausgeben wenn VIX + Richtung klar
- strike/delta/iv/bid/ask: n/v, kontrakte: n/v (manuell pruefen)

BEGRUENDUNG (begruendung_detail - 5 Felder, je max 2 Saetze, keine Anfuehrungszeichen):
- ticker_wahl: Warum dieser Ticker? Score-Vergleich.
- option_wahl: Strike, Delta, IV, Spread.
- timing: MA50-Abstand, RelVol, Trend.
- chance_risiko: Einsatz, Ziel, Stop, Break-Even.
- risiko: Hauptrisiko und Fazit.

MARKTSTATUS: markt-Feld 2-3 Saetze. strategie-Feld 1 Satz.
TICKER_TABELLE: ALLE Ticker aus Marktdaten eintragen.
Regime NUR: LOW-VOL, TRENDING oder HIGH-VOL
regime_farbe NUR: gruen, gelb oder rot

JSON-Schema (valides JSON, kein Text davor/danach):
{"datum":"DD.MM.YYYY","vix":"WERT","regime":"TRENDING","regime_farbe":"gelb","no_trade":false,"no_trade_grund":"","vix_warnung":false,"ticker":"SYMBOL","strike":"WERT","laufzeit":"DATUM","delta":"WERT","iv":"WERT%","bid":"WERT","ask":"WERT","midpoint":"WERT","kontrakte":"N","einsatz":150,"stop_loss_eur":45,"unusual":false,"begruendung_detail":{"ticker_wahl":"...","option_wahl":"...","timing":"...","chance_risiko":"...","risiko":"..."},"markt":"...","strategie":"...","ausgeschlossen":"TICKER: GRUND","ticker_tabelle":[{"ticker":"USO","kurs":"120.89","chg":"+2.11%","ma50":"84.88","trend":"ueber MA50","relvol":"1.99","bull":"61.3%","score":"86.65","gewinner":true,"ausgeschlossen":false}]}"""


# ══════════════════════════════════════════════════════════
# JSON REPAIR
# Fix R7: end=0 Ambiguität aufgelöst
# ══════════════════════════════════════════════════════════

def repair_json_quotes(text: str) -> str:
    """Repariert unescapte Anführungszeichen in JSON-String-Werten."""
    result, in_str, escaped, i = [], False, False, 0
    while i < len(text):
        ch = text[i]
        if escaped:
            result.append(ch); escaped = False; i += 1; continue
        if ch == '\\':
            result.append(ch); escaped = True; i += 1; continue
        if ch == '"':
            if not in_str:
                in_str = True; result.append(ch)
            else:
                j = i + 1
                while j < len(text) and text[j] in ' \t\n\r':
                    j += 1
                next_ch = text[j] if j < len(text) else ''
                if next_ch in ',}]:\n' or j >= len(text):
                    in_str = False; result.append(ch)
                else:
                    result.append('\\"')
            i += 1; continue
        if in_str and ch in '\n\r':
            result.append(' '); i += 1; continue
        result.append(ch); i += 1
    return ''.join(result)


def close_fragment(frag: str) -> str:
    """Repariert abgeschnittenes JSON inkl. offener Strings."""
    in_str, i = False, 0
    while i < len(frag):
        if frag[i] == '\\' and in_str and i + 1 < len(frag):
            i += 2; continue
        if frag[i] == '"':
            in_str = not in_str
        i += 1
    if in_str:
        frag += '"'
    last = frag.rfind(",")
    if last > 5:
        frag = frag[:last]
    in_str, i = False, 0
    while i < len(frag):
        if frag[i] == '\\' and in_str and i + 1 < len(frag):
            i += 2; continue
        if frag[i] == '"':
            in_str = not in_str
        i += 1
    if in_str:
        frag += '"'
    frag += "]" * max(0, frag.count("[") - frag.count("]"))
    frag += "}" * max(0, frag.count("{") - frag.count("}"))
    return frag


def extract_json_fragment(text: str) -> str:
    """
    Fix R7: Explizite Suche nach JSON-Grenzen.
    Ambiguität von rfind("}") == -1 → +1 = 0 aufgelöst.
    """
    start = text.find("{")
    if start == -1:
        raise ValueError("Kein öffnendes { im Claude-Response")

    # Suche schließendes } von rechts
    end = text.rfind("}")
    if end == -1:
        # Kein schließendes } gefunden → Fragment reparieren
        logger.debug("Kein schließendes } — close_fragment wird angewendet")
        return text[start:]  # Fragment ohne Ende

    return text[start:end + 1]  # Vollständiges JSON


# ══════════════════════════════════════════════════════════
# CLAUDE CALL
# ══════════════════════════════════════════════════════════

def call_claude(summary: str, api_key: str) -> dict:
    # Summary kürzen
    if len(summary) > 1500:
        lines, kept = summary.splitlines(), []
        for line in lines:
            kept.append(line)
            if "SENTIMENT-FALLBACK" in line:
                break
        summary = "\n".join(kept)
    summary = summary[:1400]

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         api_key,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-sonnet-4-6",
                "max_tokens": 1100,
                "system":     PROMPT,
                "messages":   [{"role": "user", "content": "Marktdaten:\n" + summary}],
            },
            timeout=25,
        )
        r.raise_for_status()
    except (RequestException, Timeout) as e:
        raise RuntimeError("Claude API nicht erreichbar: " + str(e)) from e

    data = r.json()
    if "content" not in data or not data["content"]:
        raise ValueError("Leerer Content in Claude-Response")

    text = data["content"][0]["text"].strip()
    if "```" in text:
        text = text.replace("```json", "").replace("```", "").strip()

    try:
        fragment = extract_json_fragment(text)
    except ValueError as e:
        raise ValueError("JSON-Extraktion fehlgeschlagen: " + str(e)) from e

    # 4 Parse-Versuche mit steigender Reparatur
    parsers = [
        ("direkt",           lambda f: json.loads(f)),
        ("quote_repair",     lambda f: json.loads(repair_json_quotes(f))),
        ("close_fragment",   lambda f: json.loads(close_fragment(f))),
        ("beide_kombiniert", lambda f: json.loads(repair_json_quotes(close_fragment(f)))),
    ]
    last_error = None
    for name, parser in parsers:
        try:
            result = parser(fragment)
            if name != "direkt":
                logger.info("JSON repariert mit Methode: %s", name)
            return result
        except json.JSONDecodeError as e:
            last_error = e
            logger.debug("Parse-Versuch '%s' fehlgeschlagen: %s", name, e)

    raise ValueError("JSON Parse Fehler nach 4 Versuchen: " + str(last_error) +
                     " | Raw: " + text[:300])


# ══════════════════════════════════════════════════════════
# HTML BUILDER
# ══════════════════════════════════════════════════════════

def build_html(d: dict, today: str) -> str:
    G = "#34c759"; R = "#ff3b30"; O = "#ff9500"
    GR = "#86868b"; LG = "#c7c7cc"; DK = "#1d1d1f"
    BG = "#f5f5f7"; WH = "#ffffff"; BD = "#e5e5ea"
    no_trade = d.get("no_trade", False)

    def card(icon, bg, title, content):
        return (f'<div style="background:{WH};border-radius:18px;padding:28px;'
                f'margin-bottom:16px;box-shadow:0 2px 12px rgba(0,0,0,0.07);">'
                f'<div style="display:flex;align-items:center;margin-bottom:20px;">'
                f'<div style="width:36px;height:36px;background:{bg};border-radius:10px;'
                f'text-align:center;line-height:36px;margin-right:12px;font-size:18px;">{icon}</div>'
                f'<h2 style="margin:0;font-size:18px;font-weight:700;color:{DK};">{title}</h2>'
                f'</div>{content}</div>')

    def row(label, val, col=None, last=False):
        c = col or DK
        b = "" if last else f"border-bottom:1px solid {BD};"
        return (f'<div style="display:flex;justify-content:space-between;padding:10px 0;{b}">'
                f'<span style="font-size:14px;color:{GR};">{label}</span>'
                f'<span style="font-size:14px;font-weight:600;color:{c};">{val}</span></div>')

    def section(label, html, border=True):
        b = f"border-bottom:1px solid {BD};" if border else ""
        return (f'<div style="padding:14px 0;{b}">'
                f'<p style="margin:0 0 6px 0;font-size:11px;font-weight:600;color:{GR};'
                f'text-transform:uppercase;letter-spacing:0.06em;">{label}</p>'
                f'<p style="margin:0;font-size:13px;color:{DK};line-height:1.6;">{html}</p></div>')

    # ── Trade Card ────────────────────────────────────────
    if no_trade:
        trade_card = card("❌", "#ffeaea", f'<span style="color:{R};">No Trade</span>',
                          f'<p style="margin:0 0 16px 0;font-size:14px;color:{DK};">'
                          f'{d.get("no_trade_grund","")}</p>'
                          f'<div style="background:{BG};border-radius:12px;padding:16px;">'
                          f'<p style="margin:0;font-size:13px;color:{DK};line-height:1.6;">'
                          f'Kein Trade heute — Kapitalschutz bei erhöhter Volatilität. '
                          f'Morgen läuft die Analyse erneut.</p></div>')
    else:
        einsatz   = d.get("einsatz", 150)
        stop_loss = d.get("stop_loss_eur", round(einsatz * 0.3))
        trade_rows = (
            row("Strike",              d.get("strike","n/v")) +
            row("Laufzeit",            d.get("laufzeit","n/v")) +
            row("Delta",               d.get("delta","n/v")) +
            row("IV",                  d.get("iv","n/v")) +
            row("Bid / Ask",           str(d.get("bid","n/v")) + " / " + str(d.get("ask","n/v"))) +
            row("Einstieg (Midpoint)", d.get("midpoint","n/v")) +
            row("Kontrakte",           str(d.get("kontrakte","n/v"))) +
            row("Einsatz",             str(einsatz) + "€") +
            row("Stop-Loss",           "–30% = max. " + str(stop_loss) + "€", R) +
            row("Take-Profit 1",       "+40% → 50% verkaufen", G) +
            row("Take-Profit 2",       "Rest mit –10% Stop", G) +
            row("Unusual Activity",    "JA 🔥" if d.get("unusual") else "nein",
                O if d.get("unusual") else DK, last=True)
        )
        bd    = d.get("begruendung_detail", {})
        items = [
            ("🏆", "Ticker",        bd.get("ticker_wahl","n/v")),
            ("📐", "Option",        bd.get("option_wahl","n/v")),
            ("⏱",  "Timing",        bd.get("timing","n/v")),
            ("⚖️", "Chance/Risiko", bd.get("chance_risiko","n/v")),
            ("⚠️", "Hauptrisiko",   bd.get("risiko","n/v")),
        ]
        begr = ""
        for i, (icon, label, text) in enumerate(items):
            b = f"border-bottom:1px solid {BD};" if i < len(items) - 1 else ""
            begr += (f'<div style="display:flex;gap:10px;padding:10px 0;{b}">'
                     f'<span style="font-size:16px;min-width:24px;">{icon}</span>'
                     f'<div><p style="margin:0 0 2px 0;font-size:10px;font-weight:700;'
                     f'color:{GR};text-transform:uppercase;">{label}</p>'
                     f'<p style="margin:0;font-size:12px;color:{DK};line-height:1.5;">{text}</p>'
                     f'</div></div>')
        trade_card = card(
            "✅", "#e8f5e9",
            d.get("ticker","") + f' <span style="font-size:14px;color:{GR};">Long Call</span>',
            trade_rows +
            f'<div style="margin-top:20px;background:{BG};border-radius:14px;'
            f'padding:8px 16px 4px 16px;">'
            f'<p style="margin:10px 0 4px 0;font-size:10px;font-weight:700;color:{GR};'
            f'text-transform:uppercase;">Begründung</p>{begr}</div>',
        )

    # ── VIX Warnung ───────────────────────────────────────
    vix_warning = ""
    if d.get("vix_warnung") and not no_trade:
        vix_warning = (f'<div style="background:#fff9e6;border-left:4px solid {O};'
                       f'border-radius:12px;padding:14px 18px;margin-bottom:16px;">'
                       f'<span style="font-size:18px;">⚠️</span>'
                       f'<span style="font-size:13px;font-weight:600;color:{DK};margin-left:8px;">'
                       f'Erhöhte Volatilität (VIX 20–24) – Einsatz auf '
                       f'<strong>{d.get("einsatz",150)}€</strong> reduziert</span></div>')

    # ── Exit Plan ─────────────────────────────────────────
    exit_card = ""
    if not no_trade:
        stop_e    = d.get("stop_loss_eur", round(d.get("einsatz",150) * 0.3))
        exit_card = card("🎯", "#fff3e0", "Exit-Plan",
                         row("Stop-Loss",       "–30% = max. " + str(stop_e) + "€", R) +
                         row("Take-Profit 1",   "+40% → 50% schließen", G) +
                         row("Take-Profit 2",   "Rest mit –10% Stop", G) +
                         row("Zeit-Exit",       "<10 Tage bis Verfall → schließen") +
                         row("Delta Rebalance", "Delta > ±0.30 → prüfen") +
                         row("Vega Exit",       "IV +20% → 50% schließen", last=True))

    # ── Marktstatus ───────────────────────────────────────
    rc    = {"gruen": G, "gelb": O, "rot": R}.get(d.get("regime_farbe","gelb"), O)
    ampel = (f'<span style="display:inline-block;width:11px;height:11px;border-radius:50%;'
             f'background:{rc};margin-right:7px;vertical-align:middle;"></span>')
    try:
        vix_f = float(str(d.get("vix","15")).replace(",","."))
    except (ValueError, TypeError):
        vix_f = 15.0
    vix_pct   = min(100, int((vix_f / 40) * 100))
    vix_color = G if vix_f < 18 else (O if vix_f < 25 else R)

    markt_card = card("🔍", "#e8f0fe", "Marktstatus",
                      f'<div style="display:flex;justify-content:space-between;'
                      f'padding:12px 0;border-bottom:1px solid {BD};">'
                      f'<span style="font-size:14px;color:{GR};">Regime</span>'
                      f'<span style="font-size:15px;font-weight:700;color:{rc};">'
                      f'{ampel}{d.get("regime","n/v")}</span></div>'
                      f'<div style="padding:12px 0;border-bottom:1px solid {BD};">'
                      f'<div style="display:flex;justify-content:space-between;margin-bottom:6px;">'
                      f'<span style="font-size:14px;color:{GR};">VIX</span>'
                      f'<span style="font-size:16px;font-weight:700;color:{vix_color};">'
                      f'{d.get("vix","n/v")}</span></div>'
                      f'<div style="height:5px;background:#e5e5ea;border-radius:3px;">'
                      f'<div style="height:5px;width:{vix_pct}%;background:{vix_color};'
                      f'border-radius:3px;"></div></div></div>' +
                      section("Marktlage", d.get("markt","")) +
                      section("Strategie", d.get("strategie","")) +
                      row("Ausgeschlossen", d.get("ausgeschlossen","–"), last=True))

    # ── Ticker Tabelle ────────────────────────────────────
    def th(label, align="right"):
        return (f'<th style="padding:8px 6px;text-align:{align};font-size:11px;'
                f'font-weight:600;color:{GR};text-transform:uppercase;'
                f'border-bottom:2px solid {BD};">{label}</th>')

    def td(val, align="right", color=DK, bold=False):
        fw = "700" if bold else "500"
        return (f'<td style="padding:10px 6px;text-align:{align};font-size:12px;'
                f'font-weight:{fw};color:{color};border-bottom:1px solid {BD};">{val}</td>')

    rows_html = ""
    for t in d.get("ticker_tabelle", []):
        if t.get("ticker","") in ("X","","SYMBOL"):
            continue
        chg       = t.get("chg","")
        chg_col   = G if "+" in str(chg) else (R if "-" in str(chg) else DK)
        row_color = LG if t.get("ausgeschlossen") else DK
        bold      = bool(t.get("gewinner"))
        rows_html += (f'<tr {"style=background:#f0fff4;" if bold else ""}>' +
                      td(("★ " if bold else "") + t.get("ticker",""), "left",
                         G if bold else row_color, bold) +
                      td(t.get("kurs",""),   "right", row_color, bold) +
                      td(chg,               "right", chg_col,   bold) +
                      td(t.get("ma50",""),  "right", row_color) +
                      td(t.get("trend",""), "center",row_color) +
                      td(t.get("relvol",""),"right", O if t.get("unusual") else row_color) +
                      td(t.get("bull",""),  "right", row_color) +
                      td(t.get("score",""), "right", row_color, bold) + "</tr>")

    if not rows_html:
        rows_html = (f'<tr><td colspan="8" style="padding:16px;text-align:center;'
                     f'font-size:12px;color:{GR};">Keine Daten</td></tr>')

    tabelle_card = card("📋", "#f0f0f5", "Alle analysierten Titel",
                        f'<table style="width:100%;border-collapse:collapse;"><thead><tr>'
                        f'{th("Ticker","left")}{th("Kurs")}{th("Δ%")}{th("MA50")}'
                        f'{th("Trend","center")}{th("RelVol")}{th("Bull%")}{th("Score")}'
                        f'</tr></thead><tbody>{rows_html}</tbody></table>')

    status     = "NO TRADE" if no_trade else "TRADE · " + d.get("ticker","")
    status_col = R if no_trade else G

    return (f'<html><head><meta charset="UTF-8">'
            f'<meta name="viewport" content="width=device-width,initial-scale=1.0"></head>'
            f'<body style="margin:0;padding:0;background:{BG};'
            f"font-family:-apple-system,BlinkMacSystemFont,'Helvetica Neue',Arial,sans-serif;\">"
            f'<div style="max-width:620px;margin:0 auto;padding:32px 16px;">'
            f'<div style="text-align:center;margin-bottom:28px;">'
            f'<p style="margin:0 0 6px 0;font-size:12px;font-weight:600;color:{GR};'
            f'letter-spacing:0.08em;text-transform:uppercase;">Täglicher Report</p>'
            f'<h1 style="margin:0 0 8px 0;font-size:30px;font-weight:700;color:{DK};">'
            f'Options Report</h1>'
            f'<div style="display:inline-block;background:{WH};border-radius:20px;'
            f'padding:6px 18px;box-shadow:0 1px 6px rgba(0,0,0,0.08);">'
            f'<span style="font-size:14px;color:{GR};">'
            f'{d.get("datum",today)} &nbsp;|&nbsp; '
            f'VIX <strong>{d.get("vix","n/v")}</strong> &nbsp;|&nbsp; '
            f'<strong style="color:{status_col};">{status}</strong>'
            f'</span></div></div>'
            f'{trade_card}{vix_warning}{exit_card}{markt_card}{tabelle_card}'
            f'<div style="text-align:center;padding:20px 0;border-top:1px solid {BD};margin-top:8px;">'
            f'<p style="margin:0;font-size:12px;color:{GR};">VIX ✓ · Earnings ✓ · Greeks ✓</p>'
            f'</div></div></body></html>')


# ══════════════════════════════════════════════════════════
# EMAIL
# ══════════════════════════════════════════════════════════

def send_email(subject: str, html_content: str, cfg: dict) -> bool:
    recipient = cfg.get("gmail_recipient","")
    sender    = cfg.get("smtp_sender","")
    password  = cfg.get("smtp_password","")
    host      = cfg.get("smtp_host","smtp.gmail.com")
    port      = int(cfg.get("smtp_port", 587))

    if not all([recipient, sender, password]):
        logger.warning("SMTP nicht vollständig konfiguriert — Email nicht verschickt")
        return False

    msg            = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = recipient
    msg.attach(MIMEText(html_content, "html", "utf-8"))

    try:
        with smtplib.SMTP(host, port, timeout=15) as smtp:
            smtp.starttls()
            smtp.login(sender, password)
            smtp.sendmail(sender, recipient, msg.as_string())
        logger.info("Email verschickt an %s", recipient)
        return True
    except smtplib.SMTPException as e:
        logger.error("SMTP-Fehler: %s", e)
        return False
    except OSError as e:
        logger.error("Netzwerk-Fehler beim Email-Versand: %s", e)
        return False


# ══════════════════════════════════════════════════════════
# DIREKTE AUSFÜHRUNG
# ══════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    from config_loader import load_config, validate_config

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Report Generator")
    parser.add_argument("--summary",      help="Market Summary Text")
    parser.add_argument("--summary-file", help="Datei mit Market Summary")
    parser.add_argument("--output",       help="HTML-Report speichern")
    parser.add_argument("--dry-run",      action="store_true")
    args = parser.parse_args()

    cfg = load_config()
    if not validate_config(cfg):
        raise SystemExit("Konfiguration unvollständig")

    if args.summary:
        market_summary = args.summary
    elif args.summary_file:
        with open(args.summary_file) as f:
            market_summary = f.read().strip()
    else:
        market_summary = sys.stdin.read().strip()

    if not market_summary:
        raise SystemExit("Kein Market Summary angegeben")

    today   = datetime.now().strftime("%d.%m.%Y")
    subject = "📊 Options Report – " + today

    data        = call_claude(market_summary, cfg.get("anthropic_api_key",""))
    html_report = build_html(data, today)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(html_report)
        logger.info("Report gespeichert: %s", args.output)

    if not args.dry_run:
        send_email(subject, html_report, cfg)
    else:
        with open("report_preview.html", "w", encoding="utf-8") as f:
            f.write(html_report)
        logger.info("Dry-run: report_preview.html gespeichert")
