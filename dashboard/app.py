"""
Stock Bot Web Dashboard — Flask API
Serves the dashboard HTML and exposes JSON endpoints
that read from the same SQLite DB as the scanner.
"""

import os
import psycopg2
import psycopg2.extras
import json
from datetime import date, datetime
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, send_from_directory

app = Flask(__name__, static_folder="static")

DATABASE_URL = os.environ["DATABASE_URL"]
TOP_N   = int(os.getenv("TOP_N", 10))
VOLUME_SPIKE_MULTIPLIER = float(os.getenv("VOLUME_SPIKE_MULTIPLIER", 3.0))
ET = ZoneInfo("America/New_York")


def get_db():
    return psycopg2.connect(DATABASE_URL)


def query(con, sql, params=()):
    cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return [dict(r) for r in rows]


def today():
    return date.today().isoformat()


# ── API routes ─────────────────────────────────────────────────────────────

@app.route("/api/summary")
def summary():
    """High-level stats for the stat cards."""
    con = get_db()
    t = today()

    cur = con.cursor()
    cur.execute("SELECT COUNT(DISTINCT symbol) FROM snapshots WHERE ts::date=%s", (t,))
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT symbol) FROM snapshots WHERE ts::date=%s AND change_pct>0", (t,))
    gainers_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT symbol) FROM snapshots WHERE ts::date=%s AND change_pct<0", (t,))
    losers_count = cur.fetchone()[0]

    cur.execute("SELECT MAX(ts) FROM snapshots WHERE ts::date=%s", (t,))
    last_scan = cur.fetchone()[0]
    if last_scan:
        last_scan = last_scan.isoformat()

    cur.execute("SELECT session FROM snapshots WHERE ts::date=%s ORDER BY ts DESC LIMIT 1", (t,))
    session_row = cur.fetchone()
    session = session_row[0] if session_row else "closed"

    cur.close()
    con.close()
    return jsonify(dict(
        total_symbols=total,
        gainers=gainers_count,
        losers=losers_count,
        last_scan=last_scan,
        session=session,
        date=t,
    ))


@app.route("/api/gainers")
def gainers():
    con = get_db()
    t = today()
    rows = query(con, """
        SELECT DISTINCT ON (symbol) symbol, price, change_pct, volume
        FROM snapshots
        WHERE ts::date=%s AND change_pct IS NOT NULL
        ORDER BY symbol, ts DESC
    """, (t,))
    con.close()
    rows.sort(key=lambda r: r["change_pct"] or 0, reverse=True)
    return jsonify(rows[:TOP_N])


@app.route("/api/losers")
def losers():
    con = get_db()
    t = today()
    rows = query(con, """
        SELECT DISTINCT ON (symbol) symbol, price, change_pct, volume
        FROM snapshots
        WHERE ts::date=%s AND change_pct IS NOT NULL
        ORDER BY symbol, ts DESC
    """, (t,))
    con.close()
    rows.sort(key=lambda r: r["change_pct"] or 0)
    return jsonify(rows[:TOP_N])


@app.route("/api/spikes")
def spikes():
    con = get_db()
    t = today()
    rows = query(con, """
        SELECT DISTINCT ON (symbol) symbol, price, change_pct, volume, avg_volume,
               ROUND(CAST(volume AS NUMERIC)/NULLIF(avg_volume,0), 1) AS spike_ratio
        FROM snapshots
        WHERE ts::date=%s AND avg_volume > 0
        ORDER BY symbol, ts DESC
    """, (t,))
    con.close()
    rows = [r for r in rows if r.get("spike_ratio") and r["spike_ratio"] >= VOLUME_SPIKE_MULTIPLIER]
    rows.sort(key=lambda r: r["spike_ratio"] or 0, reverse=True)
    return jsonify(rows[:TOP_N])


@app.route("/api/premarket")
def premarket():
    con = get_db()
    t = today()
    rows = query(con, """
        SELECT DISTINCT ON (symbol) symbol, price, change_pct, volume
        FROM snapshots
        WHERE ts::date=%s AND session='premarket' AND change_pct IS NOT NULL
        ORDER BY symbol, ts DESC
    """, (t,))
    con.close()
    rows.sort(key=lambda r: abs(r["change_pct"] or 0), reverse=True)
    return jsonify(rows[:20])


@app.route("/api/history/<symbol>")
def history(symbol):
    """Last 48 hours of price snapshots for a symbol (for sparkline)."""
    con = get_db()
    rows = query(con, """
        SELECT ts, price, change_pct, session
        FROM snapshots
        WHERE symbol=%s AND ts >= NOW() - INTERVAL '2 days'
        ORDER BY ts ASC
    """, (symbol.upper(),))
    con.close()
    # Convert datetime to isoformat for JSON
    for r in rows:
        if r.get("ts"):
            r["ts"] = r["ts"].isoformat()
    return jsonify(rows)


# ── Serve dashboard HTML ───────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)
