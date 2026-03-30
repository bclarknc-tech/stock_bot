"""
Stock Market Scanner Bot
Scans all US stocks during pre-market, market hours, and after-hours.
Sends a daily 9am email report with top movers, losers, and volume spikes.
"""

import os
import json
import sqlite3
import smtplib
import logging
import schedule
import time
import threading
from datetime import datetime, date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from zoneinfo import ZoneInfo

import alpaca_trade_api as tradeapi
import pandas as pd
from flask import Flask, Response

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("stock_bot.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")

# ── Config (set via environment variables) ─────────────────────────────────
ALPACA_API_KEY    = os.environ["ALPACA_API_KEY"]
ALPACA_SECRET_KEY = os.environ["ALPACA_SECRET_KEY"]
ALPACA_BASE_URL   = os.getenv("ALPACA_BASE_URL", "https://data.alpaca.markets")

SMTP_HOST     = os.environ["SMTP_HOST"]          # e.g. smtp.gmail.com
SMTP_PORT     = int(os.getenv("SMTP_PORT", 587))
SMTP_USER     = os.environ["SMTP_USER"]          # your Gmail address
SMTP_PASSWORD = os.environ["SMTP_PASSWORD"]      # Gmail app password
REPORT_TO     = os.environ["REPORT_TO"]          # recipient email

DB_PATH       = os.getenv("DB_PATH", "market_data.db")

# Volume spike threshold: flag if volume > N × 30-day average
VOLUME_SPIKE_MULTIPLIER = float(os.getenv("VOLUME_SPIKE_MULTIPLIER", 3.0))
TOP_N = int(os.getenv("TOP_N", 10))              # how many gainers/losers to report

# ── Alpaca client ──────────────────────────────────────────────────────────
log.info("KEY CHECK: %s", ALPACA_API_KEY[:8] if ALPACA_API_KEY else "MISSING")
api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url="https://paper-api.alpaca.markets")
data_client = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url=ALPACA_BASE_URL)


# ── Database ───────────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT NOT NULL,
            session     TEXT NOT NULL,   -- premarket | regular | afterhours
            symbol      TEXT NOT NULL,
            price       REAL,
            change_pct  REAL,
            volume      INTEGER,
            avg_volume  REAL
        );
        CREATE INDEX IF NOT EXISTS idx_ts   ON snapshots(ts);
        CREATE INDEX IF NOT EXISTS idx_sym  ON snapshots(symbol);
    """)
    con.commit()
    con.close()
    log.info("Database ready: %s", DB_PATH)


def save_snapshots(rows: list[dict], session: str):
    if not rows:
        return
    ts = datetime.now(ET).isoformat()
    con = sqlite3.connect(DB_PATH)
    con.executemany(
        "INSERT INTO snapshots (ts, session, symbol, price, change_pct, volume, avg_volume) "
        "VALUES (:ts, :session, :symbol, :price, :change_pct, :volume, :avg_volume)",
        [{**r, "ts": ts, "session": session} for r in rows],
    )
    con.commit()
    con.close()


# ── Market data helpers ────────────────────────────────────────────────────
def get_all_assets() -> list[str]:
    """Return all active, tradable US equity symbols."""
    assets = api.list_assets(status="active", asset_class="us_equity")
    symbols = [a.symbol for a in assets if a.tradable and "/" not in a.symbol]
    log.info("Universe: %d symbols", len(symbols))
    return symbols


def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def fetch_snapshots(symbols: list[str]) -> pd.DataFrame:
    """
    Fetch latest quotes/trades for all symbols in batches.
    Returns DataFrame with symbol, price, change_pct, volume, avg_volume.
    """
    records = []
    batch_size = 1000  # Alpaca allows up to 1000 per request

    for batch in chunk(symbols, batch_size):
        try:
            snaps = data_client.get_snapshots(batch)
            for sym, snap in snaps.items():
                try:
                    daily_bar  = snap.daily_bar
                    prev_close = snap.prev_daily_bar.close if snap.prev_daily_bar else None
                    price      = daily_bar.close if daily_bar else None
                    volume     = daily_bar.volume if daily_bar else 0

                    change_pct = None
                    if price and prev_close and prev_close != 0:
                        change_pct = (price - prev_close) / prev_close * 100

                    # 30-day avg volume placeholder (computed from DB history later)
                    records.append(
                        dict(
                            symbol=sym,
                            price=price,
                            change_pct=change_pct,
                            volume=volume,
                            avg_volume=None,
                        )
                    )
                except Exception as e:
                    log.debug("Skipping %s: %s", sym, e)
        except Exception as e:
            log.warning("Batch fetch error: %s", e)

    df = pd.DataFrame(records)
    if df.empty:
        return df

    # Enrich with 30-day average volume from DB
    df["avg_volume"] = df["symbol"].apply(get_avg_volume)
    return df


def get_avg_volume(symbol: str) -> float | None:
    """Pull 30-day average volume from DB snapshots."""
    con = sqlite3.connect(DB_PATH)
    row = con.execute(
        "SELECT AVG(volume) FROM snapshots WHERE symbol=? AND ts >= date('now','-30 days')",
        (symbol,),
    ).fetchone()
    con.close()
    return row[0] if row and row[0] else None


# ── Session detector ───────────────────────────────────────────────────────
def current_session() -> str | None:
    now = datetime.now(ET)
    h   = now.hour + now.minute / 60
    if 4.0 <= h < 9.5:
        return "premarket"
    if 9.5 <= h < 16.0:
        return "regular"
    if 16.0 <= h < 20.0:
        return "afterhours"
    return None  # overnight — don't scan


# ── Scan job ───────────────────────────────────────────────────────────────
_symbols_cache: list[str] = []


def scan():
    global _symbols_cache
    session = current_session()
    if session is None:
        log.debug("Outside trading hours, skipping scan.")
        return

    log.info("Starting scan — session: %s", session)

    if not _symbols_cache:
        _symbols_cache = get_all_assets()

    df = fetch_snapshots(_symbols_cache)
    if df.empty:
        log.warning("No data returned.")
        return

    rows = df.dropna(subset=["price"]).to_dict("records")
    save_snapshots(rows, session)
    log.info("Saved %d snapshots (session=%s)", len(rows), session)


# ── Report builder ─────────────────────────────────────────────────────────
def build_report() -> dict:
    """
    Aggregate today's snapshots into report data:
    - top gainers
    - top losers
    - volume spikes
    - pre-market movers summary
    """
    today = date.today().isoformat()
    con = sqlite3.connect(DB_PATH)

    def query(sql, params=()):
        return pd.read_sql_query(sql, con, params=params)

    # Latest price per symbol today
    latest = query("""
        SELECT symbol, price, change_pct, volume, avg_volume, session
        FROM snapshots
        WHERE date(ts) = ?
        ORDER BY ts DESC
    """, (today,))

    if latest.empty:
        con.close()
        return {}

    # Deduplicate to last record per symbol
    last = latest.drop_duplicates(subset="symbol", keep="first")

    gainers = (
        last.dropna(subset=["change_pct"])
        .sort_values("change_pct", ascending=False)
        .head(TOP_N)
    )
    losers = (
        last.dropna(subset=["change_pct"])
        .sort_values("change_pct", ascending=True)
        .head(TOP_N)
    )

    # Volume spikes: volume > VOLUME_SPIKE_MULTIPLIER × avg
    spikes = last.dropna(subset=["avg_volume"]).copy()
    spikes = spikes[spikes["avg_volume"] > 0].copy()
    spikes["spike_ratio"] = spikes["volume"] / spikes["avg_volume"]
    spikes = spikes[spikes["spike_ratio"] >= VOLUME_SPIKE_MULTIPLIER].sort_values(
        "spike_ratio", ascending=False
    ).head(TOP_N)

    # Pre-market movers
    pm = latest[latest["session"] == "premarket"].drop_duplicates("symbol", keep="last")
    pm_top = pm.dropna(subset=["change_pct"]).sort_values("change_pct", ascending=False).head(5)
    pm_bot = pm.dropna(subset=["change_pct"]).sort_values("change_pct", ascending=True).head(5)

    con.close()
    return dict(
        date=today,
        gainers=gainers.to_dict("records"),
        losers=losers.to_dict("records"),
        spikes=spikes.to_dict("records"),
        pm_top=pm_top.to_dict("records"),
        pm_bot=pm_bot.to_dict("records"),
    )


# ── Email renderer ─────────────────────────────────────────────────────────
def _row_html(r: dict, pct_key="change_pct") -> str:
    pct   = r.get(pct_key)
    price = r.get("price")
    sym   = r.get("symbol", "")
    color = "#16a34a" if (pct or 0) >= 0 else "#dc2626"
    pct_s = f"{pct:+.2f}%" if pct is not None else "N/A"
    price_s = f"${price:.2f}" if price else "N/A"
    return (
        f"<tr>"
        f"<td style='padding:6px 12px;font-weight:600'>{sym}</td>"
        f"<td style='padding:6px 12px'>{price_s}</td>"
        f"<td style='padding:6px 12px;color:{color};font-weight:600'>{pct_s}</td>"
        f"</tr>"
    )


def _spike_row(r: dict) -> str:
    sym   = r.get("symbol", "")
    vol   = r.get("volume", 0)
    avg   = r.get("avg_volume") or 0
    ratio = r.get("spike_ratio", 0)
    return (
        f"<tr>"
        f"<td style='padding:6px 12px;font-weight:600'>{sym}</td>"
        f"<td style='padding:6px 12px'>{int(vol):,}</td>"
        f"<td style='padding:6px 12px'>{int(avg):,}</td>"
        f"<td style='padding:6px 12px;color:#ea580c;font-weight:600'>{ratio:.1f}×</td>"
        f"</tr>"
    )


def _table(headers: list[str], rows_html: str) -> str:
    ths = "".join(f"<th style='padding:6px 12px;text-align:left;border-bottom:1px solid #e5e7eb'>{h}</th>" for h in headers)
    return f"""
    <table style='border-collapse:collapse;width:100%;font-size:14px;margin-bottom:24px'>
      <thead><tr style='background:#f9fafb'>{ths}</tr></thead>
      <tbody>{rows_html}</tbody>
    </table>"""


def build_html_email(report: dict) -> str:
    if not report:
        return "<p>No data collected today.</p>"

    d = report["date"]

    g_rows = "".join(_row_html(r) for r in report["gainers"])
    l_rows = "".join(_row_html(r) for r in report["losers"])
    s_rows = "".join(_spike_row(r) for r in report["spikes"])
    pm_rows = "".join(_row_html(r) for r in report["pm_top"])
    pm_rows += "".join(_row_html(r) for r in report["pm_bot"])

    return f"""
<!DOCTYPE html>
<html>
<body style='font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;max-width:680px;margin:0 auto;padding:24px;color:#111'>
  <h1 style='font-size:22px;margin-bottom:4px'>📈 Daily Market Report</h1>
  <p style='color:#6b7280;margin-top:0'>{d} · All US Equities · Generated 9:00 AM ET</p>
  <hr style='border:none;border-top:1px solid #e5e7eb;margin:16px 0'>

  <h2 style='font-size:16px;color:#16a34a'>🚀 Top {TOP_N} Gainers</h2>
  {_table(["Symbol","Price","Change %"], g_rows)}

  <h2 style='font-size:16px;color:#dc2626'>📉 Top {TOP_N} Losers</h2>
  {_table(["Symbol","Price","Change %"], l_rows)}

  <h2 style='font-size:16px;color:#ea580c'>🔥 Volume Spikes ({VOLUME_SPIKE_MULTIPLIER:.0f}× avg or more)</h2>
  {_table(["Symbol","Today Vol","30d Avg Vol","Spike"], s_rows)}

  <h2 style='font-size:16px;color:#7c3aed'>🌅 Pre-Market Movers</h2>
  {_table(["Symbol","Price","Pre-Mkt Change %"], pm_rows)}

  <hr style='border:none;border-top:1px solid #e5e7eb;margin:24px 0'>
  <p style='font-size:12px;color:#9ca3af'>
    This report is for informational purposes only and does not constitute financial advice.
    Data sourced from Alpaca Markets.
  </p>
</body>
</html>"""


def send_email(html: str, subject: str):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = REPORT_TO
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, REPORT_TO, msg.as_string())
    log.info("Report emailed to %s", REPORT_TO)


# ── Report job ─────────────────────────────────────────────────────────────
def send_daily_report():
    log.info("Building daily report…")
    try:
        report = build_report()
        html   = build_html_email(report)
        subject = f"📈 Market Report — {report.get('date', date.today())}"
        send_email(html, subject)
    except Exception as e:
        log.error("Failed to send report: %s", e)


# ── Flask web server ───────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route("/")
def web_report():
    report = build_report()
    now_et = datetime.now(ET).strftime("%B %d, %Y · %I:%M %p ET")
    session = current_session() or "closed"
    session_colors = {
        "premarket":  ("#1d4ed8", "#dbeafe", "Pre-Market"),
        "regular":    ("#15803d", "#dcfce7", "Market Open"),
        "afterhours": ("#b45309", "#fef3c7", "After-Hours"),
        "closed":     ("#6b7280", "#f3f4f6", "Market Closed"),
    }
    sc, sbg, slabel = session_colors.get(session, session_colors["closed"])

    def fmt_vol(n):
        if not n: return "—"
        n = int(n)
        if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
        if n >= 1_000: return f"{n/1_000:.0f}K"
        return str(n)

    def base_row(r):
        pct = r.get("change_pct")
        color = "#16a34a" if (pct or 0) >= 0 else "#dc2626"
        p = f"{pct:+.2f}%" if pct is not None else "—"
        price = f"${r['price']:.2f}" if r.get("price") else "—"
        return f"<tr><td class='sym'>{r['symbol']}</td><td>{price}</td><td style='color:{color};font-weight:600'>{p}</td><td class='muted'>{fmt_vol(r.get('volume'))}</td></tr>"

    def spike_row(r):
        pct = r.get("change_pct")
        color = "#16a34a" if (pct or 0) >= 0 else "#dc2626"
        p = f"{pct:+.2f}%" if pct is not None else "—"
        price = f"${r['price']:.2f}" if r.get("price") else "—"
        ratio = r.get("spike_ratio", 0)
        return f"<tr><td class='sym'>{r['symbol']}</td><td>{price}</td><td style='color:{color};font-weight:600'>{p}</td><td><span class='badge'>{ratio:.1f}×</span></td><td class='muted'>{fmt_vol(r.get('volume'))}</td></tr>"

    def tbl(rows, cols, render_fn):
        if not rows:
            return "<p style='color:#6b7280;padding:12px'>No data yet — check back after market opens.</p>"
        ths = "".join(f"<th>{c}</th>" for c in cols)
        trs = "".join(render_fn(r) for r in rows)
        return f"<table><thead><tr>{ths}</tr></thead><tbody>{trs}</tbody></table>"

    g_tbl  = tbl(report.get("gainers",[]), ["Symbol","Price","Chg %","Volume"], base_row)
    l_tbl  = tbl(report.get("losers", []), ["Symbol","Price","Chg %","Volume"], base_row)
    s_tbl  = tbl(report.get("spikes", []), ["Symbol","Price","Chg %","Spike","Volume"], spike_row)
    pm_top = tbl(report.get("pm_top",[]), ["Symbol","Price","Pre-Mkt %","Volume"], base_row)
    pm_bot = tbl(report.get("pm_bot",[]), ["Symbol","Price","Pre-Mkt %","Volume"], base_row)

    top_gainer = report["gainers"][0]["symbol"] if report.get("gainers") else "—"
    top_loser  = report["losers"][0]["symbol"]  if report.get("losers")  else "—"
    n_spikes   = len(report.get("spikes", []))

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Market Scanner</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f8fafc;color:#1e293b;font-size:14px}}
  header{{background:#0f172a;color:#f8fafc;padding:16px 28px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px}}
  header h1{{font-size:18px;font-weight:600;letter-spacing:0.04em}}
  .meta{{font-size:12px;color:#94a3b8;margin-top:2px}}
  .badge-session{{padding:4px 12px;border-radius:20px;font-size:11px;font-weight:600;background:{sbg};color:{sc}}}
  main{{max-width:1200px;margin:0 auto;padding:24px 20px}}
  .stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:24px}}
  .stat{{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:16px;border-top:3px solid #e2e8f0}}
  .stat.green{{border-top-color:#16a34a}}.stat.red{{border-top-color:#dc2626}}.stat.blue{{border-top-color:#2563eb}}.stat.amber{{border-top-color:#d97706}}
  .stat-label{{font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:6px}}
  .stat-value{{font-size:26px;font-weight:700}}
  .stat-value.green{{color:#16a34a}}.stat-value.red{{color:#dc2626}}.stat-value.blue{{color:#2563eb}}.stat-value.amber{{color:#d97706}}
  .grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}}
  .panel{{background:#fff;border:1px solid #e2e8f0;border-radius:10px;overflow:hidden}}
  .panel-head{{padding:12px 16px;border-bottom:1px solid #f1f5f9;display:flex;align-items:center;justify-content:space-between}}
  .panel-title{{font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:0.08em}}
  .panel-title.green{{color:#16a34a}}.panel-title.red{{color:#dc2626}}.panel-title.amber{{color:#d97706}}.panel-title.purple{{color:#7c3aed}}
  table{{width:100%;border-collapse:collapse}}
  th{{font-size:10px;text-transform:uppercase;letter-spacing:0.08em;color:#94a3b8;padding:8px 14px;text-align:left;background:#f8fafc;border-bottom:1px solid #f1f5f9;font-weight:500}}
  td{{padding:8px 14px;border-bottom:1px solid #f8fafc}}
  tr:last-child td{{border-bottom:none}}
  tr:hover td{{background:#f8fafc}}
  .sym{{font-weight:600;font-size:13px}}
  .muted{{color:#94a3b8;font-size:12px}}
  .badge{{background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;border:1px solid #fde68a}}
  .pm-grid{{display:grid;grid-template-columns:1fr 1fr}}
  .pm-grid>div+div{{border-left:1px solid #f1f5f9}}
  .pm-label{{padding:8px 14px;font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:0.08em;border-bottom:1px solid #f1f5f9}}
  .send-btn{{background:#2563eb;color:#fff;border:none;padding:8px 18px;border-radius:6px;font-size:12px;cursor:pointer;text-decoration:none;font-weight:500}}
  .send-btn:hover{{background:#1d4ed8}}
  footer{{text-align:center;padding:20px;font-size:11px;color:#94a3b8;border-top:1px solid #e2e8f0;margin-top:24px}}
  @media(max-width:700px){{.grid{{grid-template-columns:1fr}}.pm-grid{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<header>
  <div>
    <h1>📈 Market Scanner</h1>
    <div class="meta">{now_et}</div>
  </div>
  <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">
    <span class="badge-session">{slabel}</span>
    <a href="/send-report" class="send-btn">📧 Email Report Now</a>
  </div>
</header>
<main>
  <div class="stats">
    <div class="stat blue"><div class="stat-label">Today's Date</div><div class="stat-value blue" style="font-size:16px">{date.today().strftime("%b %d")}</div></div>
    <div class="stat green"><div class="stat-label">Top Gainer</div><div class="stat-value green">{top_gainer}</div></div>
    <div class="stat red"><div class="stat-label">Top Loser</div><div class="stat-value red">{top_loser}</div></div>
    <div class="stat amber"><div class="stat-label">Volume Spikes</div><div class="stat-value amber">{n_spikes}</div></div>
  </div>
  <div class="grid">
    <div class="panel">
      <div class="panel-head"><span class="panel-title green">▲ Top Gainers</span></div>
      {g_tbl}
    </div>
    <div class="panel">
      <div class="panel-head"><span class="panel-title red">▼ Top Losers</span></div>
      {l_tbl}
    </div>
    <div class="panel">
      <div class="panel-head"><span class="panel-title amber">⚡ Volume Spikes</span></div>
      {s_tbl}
    </div>
    <div class="panel">
      <div class="panel-head"><span class="panel-title purple">🌅 Pre-Market Movers</span></div>
      <div class="pm-grid">
        <div><div class="pm-label" style="color:#16a34a">Gainers</div>{pm_top}</div>
        <div><div class="pm-label" style="color:#dc2626">Losers</div>{pm_bot}</div>
      </div>
    </div>
  </div>
</main>
<footer>Market Scanner · Data via Alpaca Markets · Auto-reloads every 60s · Not financial advice</footer>
<script>setTimeout(()=>location.reload(), 60000)</script>
</body>
</html>"""
    return Response(html, mimetype="text/html")


@flask_app.route("/send-report")
def trigger_report():
    try:
        send_daily_report()
        return Response("<p style='font-family:sans-serif;padding:20px;font-size:16px'>✅ Report sent! Check your inbox. <a href='/'>← Back to dashboard</a></p>", mimetype="text/html")
    except Exception as e:
        return Response(f"<p style='font-family:sans-serif;padding:20px;color:red;font-size:16px'>❌ Error: {e} <a href='/'>← Back</a></p>", mimetype="text/html")


# ── Scheduler + main ───────────────────────────────────────────────────────
def run_scheduler():
    init_db()
    log.info("Stock bot starting…")
    schedule.every(5).minutes.do(scan)
    schedule.every().day.at("09:00").do(send_daily_report)
    log.info("Scheduler running. Scans every 5 min; report at 09:00 ET daily.")
    scan()
    while True:
        schedule.run_pending()
        time.sleep(30)


def main():
    t = threading.Thread(target=run_scheduler, daemon=True)
    t.start()
    port = int(os.getenv("PORT", 8080))
    log.info("Web server on port %d", port)
    flask_app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
