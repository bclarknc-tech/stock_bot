"""
Stock Market Scanner Bot
Scans all US stocks during pre-market, market hours, and after-hours.
Sends a daily 9am email report with top movers, losers, and volume spikes.
"""

import os
import psycopg2
import psycopg2.extras
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
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")

# ── Config ─────────────────────────────────────────────────────────────────
ALPACA_API_KEY    = os.environ["ALPACA_API_KEY"]
ALPACA_SECRET_KEY = os.environ["ALPACA_SECRET_KEY"]
ALPACA_BASE_URL   = os.getenv("ALPACA_BASE_URL", "https://data.alpaca.markets")
SMTP_HOST         = os.environ["SMTP_HOST"]
SMTP_PORT         = int(os.getenv("SMTP_PORT", 587))
SMTP_USER         = os.environ["SMTP_USER"]
SMTP_PASSWORD     = os.environ["SMTP_PASSWORD"]
REPORT_TO         = os.environ["REPORT_TO"]
DATABASE_URL      = os.environ["DATABASE_URL"]
VOLUME_SPIKE_MULTIPLIER = float(os.getenv("VOLUME_SPIKE_MULTIPLIER", 3.0))
TOP_N = int(os.getenv("TOP_N", 25))

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
    assets = api.list_assets(status="active", asset_class="us_equity")
    symbols = [a.symbol for a in assets if a.tradable and "/" not in a.symbol]
    log.info("Universe: %d symbols", len(symbols))
    return symbols


def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def get_avg_volumes_bulk(symbols: list) -> dict:
    """Pull 30-day average volume for all symbols in one query."""
    if not symbols:
        return {}
    con = get_conn()
    cur = con.cursor()
    cur.execute(
        "SELECT symbol, AVG(volume) FROM snapshots "
        "WHERE symbol = ANY(%s) AND ts >= NOW() - INTERVAL '30 days' "
        "GROUP BY symbol",
        (symbols,),
    )
    rows = cur.fetchall()
    cur.close()
    con.close()
    return {row[0]: row[1] for row in rows if row[1] is not None}


def fetch_snapshots(symbols: list[str]) -> pd.DataFrame:
    records = []
    batch_size = 1000

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
                    records.append(dict(symbol=sym, price=price, change_pct=change_pct, volume=volume, avg_volume=None))
                except Exception as e:
                    log.debug("Skipping %s: %s", sym, e)
        except Exception as e:
            log.warning("Batch fetch error: %s", e)

    df = pd.DataFrame(records)
    if df.empty:
        return df

    avg_map = get_avg_volumes_bulk(df["symbol"].tolist())
    df["avg_volume"] = df["symbol"].map(avg_map)
    return df


# ── Session detector ───────────────────────────────────────────────────────
def current_session() -> str | None:
    now = datetime.now(ET)
    h = now.hour + now.minute / 60
    if 4.0 <= h < 9.5:  return "premarket"
    if 9.5 <= h < 16.0: return "regular"
    if 16.0 <= h < 20.0: return "afterhours"
    return None


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
        log.info("Scan #%d skipped — outside trading hours.", _scan_count)
        return

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
            log.error("Scan #%d aborted — could not load asset universe.", _scan_count)
            return

    try:
        df = fetch_snapshots(_symbols_cache)
    except Exception as e:
        log.error("Scan #%d — fetch_snapshots() failed: %s", _scan_count, e)
        return

    if df.empty:
        log.warning("Scan #%d — no data returned.", _scan_count)
        return

    rows = df.dropna(subset=["price"]).to_dict("records")
    if not rows:
        log.warning("Scan #%d — all rows dropped.", _scan_count)
        return

    try:
        save_snapshots(rows, session)
        _last_scan_success = datetime.now(ET)
        log.info("Scan #%d SUCCESS — saved %d snapshots (session=%s)", _scan_count, len(rows), session)
    except Exception as e:
        log.error("Scan #%d — save_snapshots() failed: %s", _scan_count, e)


# ── Email report ───────────────────────────────────────────────────────────
def send_daily_report():
    log.info("Building daily report…")
    try:
        today = date.today().isoformat()
        con = get_conn()
        df = pd.read_sql_query("""
            SELECT DISTINCT ON (symbol) symbol, price, change_pct, volume, avg_volume, session
            FROM snapshots WHERE ts::date = %s ORDER BY symbol, ts DESC
        """, con, params=(today,))
        con.close()

        if df.empty:
            log.warning("No data for report.")
            return

        last = df.drop_duplicates(subset="symbol", keep="first")
        gainers = last.dropna(subset=["change_pct"]).sort_values("change_pct", ascending=False).head(TOP_N)
        losers  = last.dropna(subset=["change_pct"]).sort_values("change_pct", ascending=True).head(TOP_N)

        def rows_html(df):
            html = ""
            for _, r in df.iterrows():
                pct = r.get("change_pct")
                color = "#16a34a" if (pct or 0) >= 0 else "#dc2626"
                html += f"<tr><td style='padding:6px 12px;font-weight:600'>{r['symbol']}</td><td style='padding:6px 12px'>${r['price']:.2f}</td><td style='padding:6px 12px;color:{color};font-weight:600'>{pct:+.2f}%</td></tr>"
            return html

        def tbl(headers, rows):
            ths = "".join(f"<th style='padding:6px 12px;text-align:left;border-bottom:1px solid #e5e7eb'>{h}</th>" for h in headers)
            return f"<table style='border-collapse:collapse;width:100%;font-size:14px;margin-bottom:24px'><thead><tr style='background:#f9fafb'>{ths}</tr></thead><tbody>{rows}</tbody></table>"

        html = f"""<!DOCTYPE html><html><body style='font-family:sans-serif;max-width:680px;margin:0 auto;padding:24px;color:#111'>
        <h1>📈 Daily Market Report — {today}</h1>
        <h2 style='color:#16a34a'>🚀 Top Gainers</h2>{tbl(["Symbol","Price","Change %"], rows_html(gainers))}
        <h2 style='color:#dc2626'>📉 Top Losers</h2>{tbl(["Symbol","Price","Change %"], rows_html(losers))}
        <p style='font-size:12px;color:#9ca3af'>Not financial advice. Data via Alpaca Markets.</p>
        </body></html>"""

        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"📈 Market Report — {today}"
        msg["From"] = SMTP_USER
        msg["To"] = REPORT_TO
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, REPORT_TO, msg.as_string())
        log.info("Report emailed to %s", REPORT_TO)
    except Exception as e:
        log.error("Failed to send report: %s", e)


# ── Scheduler + main ───────────────────────────────────────────────────────
def run_scheduler():
    init_db()
    log.info("Stock bot starting — PID %d — %s", os.getpid(), datetime.now(ET).isoformat())

    schedule.every(1).minutes.do(scan)

    def send_report_if_et_9am():
        now = datetime.now(ET)
        if now.hour == 9 and now.minute < 5:
            send_daily_report()

    schedule.every(5).minutes.do(send_report_if_et_9am)
    log.info("Scheduler armed. Scans every 1 min; report fires at 09:00 ET.")
    scan()

    consecutive_errors = 0
    while True:
        try:
            schedule.run_pending()
            consecutive_errors = 0
        except Exception as e:
            consecutive_errors += 1
            log.error("Scheduler loop error #%d: %s", consecutive_errors, e)
            if consecutive_errors >= 10:
                log.critical("10 consecutive errors — resetting.")
                consecutive_errors = 0
        time.sleep(30)


def main():
    run_scheduler()


if __name__ == "__main__":
    main()
