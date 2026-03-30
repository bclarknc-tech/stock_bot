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
from datetime import datetime, date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from zoneinfo import ZoneInfo

import alpaca_trade_api as tradeapi
import pandas as pd

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
api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url="https://api.alpaca.markets")
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


# ── Scheduler ─────────────────────────────────────────────────────────────
def main():
    init_db()
    log.info("Stock bot starting…")

    # Scan every 5 minutes during active sessions
    schedule.every(5).minutes.do(scan)

    # Send report every day at 9:00 AM ET
    # Note: schedule runs in local time — set your server timezone to America/New_York
    # or adjust the time string accordingly.
    schedule.every().day.at("09:00").do(send_daily_report)

    log.info("Scheduler running. Scans every 5 min; report at 09:00 ET daily.")

    # Run one scan immediately on startup
    scan()

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
