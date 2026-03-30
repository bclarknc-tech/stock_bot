"""
Stock Bot Web Dashboard — Flask API
Serves the dashboard HTML and exposes JSON endpoints
that read from the same SQLite DB as the scanner.
"""

import os
import sqlite3
import json
from datetime import date, datetime
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, send_from_directory

app = Flask(__name__, static_folder="static")

DB_PATH = os.getenv("DB_PATH", "../market_data.db")
TOP_N   = int(os.getenv("TOP_N", 10))
VOLUME_SPIKE_MULTIPLIER = float(os.getenv("VOLUME_SPIKE_MULTIPLIER", 3.0))
ET = ZoneInfo("America/New_York")


def get_db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def today():
    return date.today().isoformat()


# ── API routes ─────────────────────────────────────────────────────────────

@app.route("/api/summary")
def summary():
    """High-level stats for the stat cards."""
    con = get_db()
    t = today()

    total = con.execute(
        "SELECT COUNT(DISTINCT symbol) FROM snapshots WHERE date(ts)=?", (t,)
    ).fetchone()[0]

    gainers_count = con.execute(
        "SELECT COUNT(DISTINCT symbol) FROM snapshots WHERE date(ts)=? AND change_pct>0", (t,)
    ).fetchone()[0]

    losers_count = con.execute(
        "SELECT COUNT(DISTINCT symbol) FROM snapshots WHERE date(ts)=? AND change_pct<0", (t,)
    ).fetchone()[0]

    last_scan = con.execute(
        "SELECT MAX(ts) FROM snapshots WHERE date(ts)=?",(t,)
    ).fetchone()[0]

    session_row = con.execute(
        "SELECT session FROM snapshots WHERE date(ts)=? ORDER BY ts DESC LIMIT 1", (t,)
    ).fetchone()
    session = session_row[0] if session_row else "closed"

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
    rows = con.execute("""
        SELECT symbol, price, change_pct, volume
        FROM snapshots
        WHERE date(ts)=?
        GROUP BY symbol
        HAVING ts = MAX(ts) AND change_pct IS NOT NULL
        ORDER BY change_pct DESC
        LIMIT ?
    """, (t, TOP_N)).fetchall()
    con.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/losers")
def losers():
    con = get_db()
    t = today()
    rows = con.execute("""
        SELECT symbol, price, change_pct, volume
        FROM snapshots
        WHERE date(ts)=?
        GROUP BY symbol
        HAVING ts = MAX(ts) AND change_pct IS NOT NULL
        ORDER BY change_pct ASC
        LIMIT ?
    """, (t, TOP_N)).fetchall()
    con.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/spikes")
def spikes():
    con = get_db()
    t = today()
    rows = con.execute("""
        SELECT symbol, price, change_pct, volume, avg_volume,
               ROUND(CAST(volume AS REAL)/NULLIF(avg_volume,0),1) AS spike_ratio
        FROM snapshots
        WHERE date(ts)=?
        GROUP BY symbol
        HAVING ts = MAX(ts)
           AND avg_volume > 0
           AND CAST(volume AS REAL)/avg_volume >= ?
        ORDER BY spike_ratio DESC
        LIMIT ?
    """, (t, VOLUME_SPIKE_MULTIPLIER, TOP_N)).fetchall()
    con.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/premarket")
def premarket():
    con = get_db()
    t = today()
    rows = con.execute("""
        SELECT symbol, price, change_pct, volume
        FROM snapshots
        WHERE date(ts)=? AND session='premarket'
        GROUP BY symbol
        HAVING ts = MAX(ts) AND change_pct IS NOT NULL
        ORDER BY ABS(change_pct) DESC
        LIMIT 20
    """, (t,)).fetchall()
    con.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/history/<symbol>")
def history(symbol):
    """Last 48 hours of price snapshots for a symbol (for sparkline)."""
    con = get_db()
    rows = con.execute("""
        SELECT ts, price, change_pct, session
        FROM snapshots
        WHERE symbol=? AND ts >= datetime('now','-2 days')
        ORDER BY ts ASC
    """, (symbol.upper(),)).fetchall()
    con.close()
    return jsonify([dict(r) for r in rows])


# ── Serve dashboard HTML ───────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)
