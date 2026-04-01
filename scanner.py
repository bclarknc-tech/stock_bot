"""
Stock Market Scanner Bot
Scans all US stocks during pre-market, market hours, and after-hours.
Sends a daily 9am email report with top movers, losers, and volume spikes.
"""

import os
import json
import psycopg2
import psycopg2.extras
import smtplib
import logging
from dotenv import load_dotenv
load_dotenv()
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

DATABASE_URL  = os.environ["DATABASE_URL"]        # Postgres connection string

# Volume spike threshold: flag if volume > N × 30-day average
VOLUME_SPIKE_MULTIPLIER = float(os.getenv("VOLUME_SPIKE_MULTIPLIER", 3.0))
TOP_N = int(os.getenv("TOP_N", 10))              # how many gainers/losers to report

# ── Alpaca client ──────────────────────────────────────────────────────────
log.info("KEY CHECK: %s", ALPACA_API_KEY[:8] if ALPACA_API_KEY else "MISSING")
api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url="https://paper-api.alpaca.markets")
data_client = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url=ALPACA_BASE_URL)


# ── Database ───────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    con = get_conn()
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id          SERIAL PRIMARY KEY,
            ts          TIMESTAMPTZ NOT NULL,
            session     TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            price       REAL,
            change_pct  REAL,
            volume      BIGINT,
            avg_volume  REAL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ts  ON snapshots(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sym ON snapshots(symbol)")
    con.commit()
    cur.close()
    con.close()
    log.info("Postgres database ready")


def save_snapshots(rows: list[dict], session: str):
    if not rows:
        return
    ts = datetime.now(ET).isoformat()
    con = get_conn()
    cur = con.cursor()
    psycopg2.extras.execute_batch(cur,
        "INSERT INTO snapshots (ts, session, symbol, price, change_pct, volume, avg_volume) "
        "VALUES (%(ts)s, %(session)s, %(symbol)s, %(price)s, %(change_pct)s, %(volume)s, %(avg_volume)s)",
        [{**r, "ts": ts, "session": session} for r in rows],
    )
    con.commit()
    cur.close()
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
    con = get_conn()
    cur = con.cursor()
    cur.execute(
        "SELECT AVG(volume) FROM snapshots WHERE symbol=%s AND ts >= NOW() - INTERVAL '30 days'",
        (symbol,),
    )
    row = cur.fetchone()
    cur.close()
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
_scan_count: int = 0
_last_scan_success: datetime | None = None


def scan():
    global _symbols_cache, _scan_count, _last_scan_success
    _scan_count += 1
    session = current_session()

    log.info("Scan #%d triggered — session: %s — ET time: %s",
             _scan_count, session or "CLOSED", datetime.now(ET).strftime("%H:%M:%S"))

    if session is None:
        log.info("Scan #%d skipped — outside trading hours (pre-market 04:00, regular 09:30, after-hours 16:00–20:00 ET).", _scan_count)
        return

    # Load symbol universe, retry up to 3 times if it fails
    if not _symbols_cache:
        for attempt in range(1, 4):
            try:
                log.info("Fetching asset universe (attempt %d/3)…", attempt)
                _symbols_cache = get_all_assets()
                log.info("Asset universe loaded: %d symbols", len(_symbols_cache))
                break
            except Exception as e:
                log.error("get_all_assets() attempt %d failed: %s", attempt, e)
                time.sleep(5)
        if not _symbols_cache:
            log.error("Scan #%d aborted — could not load asset universe after 3 attempts.", _scan_count)
            return

    try:
        df = fetch_snapshots(_symbols_cache)
    except Exception as e:
        log.error("Scan #%d — fetch_snapshots() raised an exception: %s", _scan_count, e)
        return

    if df.empty:
        log.warning("Scan #%d — fetch_snapshots() returned empty DataFrame. No data written.", _scan_count)
        return

    rows = df.dropna(subset=["price"]).to_dict("records")
    if not rows:
        log.warning("Scan #%d — all rows dropped (no valid price). Nothing saved.", _scan_count)
        return

    try:
        save_snapshots(rows, session)
        _last_scan_success = datetime.now(ET)
        log.info("Scan #%d SUCCESS — saved %d snapshots (session=%s)", _scan_count, len(rows), session)
    except Exception as e:
        log.error("Scan #%d — save_snapshots() failed: %s", _scan_count, e)


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
    con = get_conn()

    def query(sql, params=()):
        return pd.read_sql_query(sql, con, params=params)

    # Latest price per symbol today
    latest = query("""
        SELECT symbol, price, change_pct, volume, avg_volume, session
        FROM snapshots
        WHERE ts::date = %s
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

    # Embed all data as JSON — filtering is done client-side
    report_json = json.dumps(report)

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
  .filters{{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:14px 18px;margin-bottom:20px;display:flex;align-items:center;gap:20px;flex-wrap:wrap}}
  .filters-label{{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.08em;color:#64748b;margin-right:4px}}
  .toggle-wrap{{display:flex;align-items:center;gap:8px;cursor:pointer;user-select:none}}
  .toggle-wrap input[type=checkbox]{{display:none}}
  .toggle{{width:36px;height:20px;background:#cbd5e1;border-radius:10px;position:relative;transition:background 0.2s}}
  .toggle::after{{content:'';position:absolute;width:16px;height:16px;background:#fff;border-radius:50%;top:2px;left:2px;transition:left 0.2s;box-shadow:0 1px 3px rgba(0,0,0,0.2)}}
  input:checked+.toggle{{background:#2563eb}}
  input:checked+.toggle::after{{left:18px}}
  .toggle-label{{font-size:12px;color:#475569;font-weight:500}}
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
  .panel-count{{font-size:11px;color:#94a3b8}}
  table{{width:100%;border-collapse:collapse}}
  th{{font-size:10px;text-transform:uppercase;letter-spacing:0.08em;color:#94a3b8;padding:8px 14px;text-align:left;background:#f8fafc;border-bottom:1px solid #f1f5f9;font-weight:500}}
  td{{padding:8px 14px;border-bottom:1px solid #f8fafc}}
  tr:last-child td{{border-bottom:none}}
  tr:hover td{{background:#f8fafc}}
  .sym{{font-weight:600;font-size:13px}}
  .muted{{color:#94a3b8;font-size:12px}}
  .badge{{background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;border:1px solid #fde68a}}
  .empty{{color:#94a3b8;padding:16px 14px;font-size:13px}}
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
  <!-- Filter bar -->
  <div class="filters">
    <span class="filters-label">Filters:</span>
    <label class="toggle-wrap">
      <input type="checkbox" id="f-warrants" checked>
      <span class="toggle"></span>
      <span class="toggle-label">Hide Warrants</span>
    </label>
    <label class="toggle-wrap">
      <input type="checkbox" id="f-penny" checked>
      <span class="toggle"></span>
      <span class="toggle-label">Hide Penny Stocks (&lt;$1)</span>
    </label>
    <label class="toggle-wrap">
      <input type="checkbox" id="f-lowvol" checked>
      <span class="toggle"></span>
      <span class="toggle-label">Hide Low Volume (&lt;50K)</span>
    </label>
  </div>

  <div class="stats">
    <div class="stat blue"><div class="stat-label">Today's Date</div><div class="stat-value blue" style="font-size:16px">{date.today().strftime("%b %d")}</div></div>
    <div class="stat green"><div class="stat-label">Top Gainer</div><div class="stat-value green" id="stat-gainer">—</div></div>
    <div class="stat red"><div class="stat-label">Top Loser</div><div class="stat-value red" id="stat-loser">—</div></div>
    <div class="stat amber"><div class="stat-label">Volume Spikes</div><div class="stat-value amber" id="stat-spikes">—</div></div>
  </div>

  <div class="grid">
    <div class="panel">
      <div class="panel-head"><span class="panel-title green">▲ Top Gainers</span><span class="panel-count" id="cnt-gainers"></span></div>
      <div id="tbl-gainers"></div>
    </div>
    <div class="panel">
      <div class="panel-head"><span class="panel-title red">▼ Top Losers</span><span class="panel-count" id="cnt-losers"></span></div>
      <div id="tbl-losers"></div>
    </div>
    <div class="panel">
      <div class="panel-head"><span class="panel-title amber">⚡ Volume Spikes</span><span class="panel-count" id="cnt-spikes"></span></div>
      <div id="tbl-spikes"></div>
    </div>
    <div class="panel">
      <div class="panel-head"><span class="panel-title purple">🌅 Pre-Market Movers</span></div>
      <div class="pm-grid">
        <div><div class="pm-label" style="color:#16a34a">Gainers</div><div id="tbl-pm-top"></div></div>
        <div><div class="pm-label" style="color:#dc2626">Losers</div><div id="tbl-pm-bot"></div></div>
      </div>
    </div>
  </div>
</main>
<footer>Market Scanner · Data via Alpaca Markets · Auto-reloads every 60s · Not financial advice</footer>

<script>
const REPORT = {report_json};
const WARRANT_RE = /W$|WS$|WT$|W[A-Z]$|WARR$/;

function fmtVol(n) {{
  if (!n) return '—';
  n = Math.round(n);
  if (n >= 1e6) return (n/1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n/1e3).toFixed(0) + 'K';
  return String(n);
}}

function isWarrant(sym) {{ return WARRANT_RE.test(sym); }}
function isPenny(price) {{ return price !== null && price < 1.0; }}
function isLowVol(vol) {{ return vol !== null && vol < 50000; }}

function applyFilters(rows) {{
  const hideWarrants = document.getElementById('f-warrants').checked;
  const hidePenny    = document.getElementById('f-penny').checked;
  const hideLowVol   = document.getElementById('f-lowvol').checked;
  return rows.filter(r => {{
    if (hideWarrants && isWarrant(r.symbol)) return false;
    if (hidePenny    && isPenny(r.price))    return false;
    if (hideLowVol   && isLowVol(r.volume))  return false;
    return true;
  }});
}}

function baseRow(r) {{
  const pct = r.change_pct;
  const color = (pct >= 0) ? '#16a34a' : '#dc2626';
  const p = pct !== null ? (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%' : '—';
  const price = r.price !== null ? '$' + r.price.toFixed(2) : '—';
  return `<tr><td class="sym">${{r.symbol}}</td><td>${{price}}</td><td style="color:${{color}};font-weight:600">${{p}}</td><td class="muted">${{fmtVol(r.volume)}}</td></tr>`;
}}

function spikeRow(r) {{
  const pct = r.change_pct;
  const color = (pct >= 0) ? '#16a34a' : '#dc2626';
  const p = pct !== null ? (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%' : '—';
  const price = r.price !== null ? '$' + r.price.toFixed(2) : '—';
  const ratio = (r.spike_ratio || 0).toFixed(1);
  return `<tr><td class="sym">${{r.symbol}}</td><td>${{price}}</td><td style="color:${{color}};font-weight:600">${{p}}</td><td><span class="badge">${{ratio}}×</span></td><td class="muted">${{fmtVol(r.volume)}}</td></tr>`;
}}

function renderTable(elId, rows, rowFn, cols, countElId) {{
  const el = document.getElementById(elId);
  if (!rows || rows.length === 0) {{
    el.innerHTML = '<div class="empty">No data after filters applied.</div>';
    if (countElId) document.getElementById(countElId).textContent = '(0)';
    return;
  }}
  const ths = cols.map(c => `<th>${{c}}</th>`).join('');
  const trs = rows.map(rowFn).join('');
  el.innerHTML = `<table><thead><tr>${{ths}}</tr></thead><tbody>${{trs}}</tbody></table>`;
  if (countElId) document.getElementById(countElId).textContent = `(${{rows.length}})`;
}}

function render() {{
  const gainers = applyFilters(REPORT.gainers || []).slice(0, {TOP_N});
  const losers  = applyFilters(REPORT.losers  || []).slice(0, {TOP_N});
  const spikes  = applyFilters(REPORT.spikes  || []).slice(0, {TOP_N});
  const pmTop   = applyFilters(REPORT.pm_top  || []).slice(0, 5);
  const pmBot   = applyFilters(REPORT.pm_bot  || []).slice(0, 5);

  renderTable('tbl-gainers', gainers, baseRow,  ['Symbol','Price','Chg %','Volume'], 'cnt-gainers');
  renderTable('tbl-losers',  losers,  baseRow,  ['Symbol','Price','Chg %','Volume'], 'cnt-losers');
  renderTable('tbl-spikes',  spikes,  spikeRow, ['Symbol','Price','Chg %','Spike','Volume'], 'cnt-spikes');
  renderTable('tbl-pm-top',  pmTop,   baseRow,  ['Symbol','Price','Pre-Mkt %','Volume'], null);
  renderTable('tbl-pm-bot',  pmBot,   baseRow,  ['Symbol','Price','Pre-Mkt %','Volume'], null);

  document.getElementById('stat-gainer').textContent = gainers.length ? gainers[0].symbol : '—';
  document.getElementById('stat-loser').textContent  = losers.length  ? losers[0].symbol  : '—';
  document.getElementById('stat-spikes').textContent = spikes.length;
}}

// Re-render on any toggle change
document.querySelectorAll('.filters input').forEach(el => el.addEventListener('change', render));

render();
setTimeout(() => location.reload(), 60000);
</script>
</body>
</html>"""
    return Response(html, mimetype="text/html")


@flask_app.route("/health")
def health():
    last_ok = _last_scan_success.isoformat() if _last_scan_success else "never"
    stale = _last_scan_success is None or (datetime.now(ET) - _last_scan_success).seconds > 600
    status = 503 if stale else 200
    return Response(
        f"scans_run={_scan_count} last_success={last_ok} session={current_session() or 'closed'} stale={stale}",
        status=status,
        mimetype="text/plain"
    )


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
    log.info("Stock bot starting — PID %d — %s", os.getpid(), datetime.now(ET).isoformat())

    schedule.every(5).minutes.do(scan)

    # 09:00 ET varies in UTC due to DST — use an ET-aware wrapper instead
    def send_report_if_et_9am():
        now = datetime.now(ET)
        if now.hour == 9 and now.minute < 5:
            send_daily_report()

    schedule.every(5).minutes.do(send_report_if_et_9am)

    log.info("Scheduler armed. Scans every 5 min; report fires at 09:00 ET.")
    scan()  # Run immediately on startup so we know it works

    consecutive_errors = 0
    while True:
        try:
            schedule.run_pending()
            consecutive_errors = 0
        except Exception as e:
            consecutive_errors += 1
            log.error("Scheduler loop error #%d: %s", consecutive_errors, e)
            if consecutive_errors >= 10:
                log.critical("10 consecutive scheduler errors — restarting scheduler loop.")
                consecutive_errors = 0
        time.sleep(30)
        log.debug("Scheduler heartbeat — %s — next scan: %s",
                  datetime.now(ET).strftime("%H:%M:%S"),
                  schedule.next_run())


def main():
    run_scheduler()


if __name__ == "__main__":
    main()
