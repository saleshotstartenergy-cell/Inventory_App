"""
tally_gateway.py

Local gateway for Tally ODBC -> JSON. Run this on the same machine as Tally.
It returns live JSON that matches the fields your ETL/app expect.

Usage:
    # set env vars or export them in Windows
    set TALLY_DSN=TallyODBC64_9000
    set TALLY_API_KEY=some_long_secret

    python tally_gateway.py

Test locally:
    curl http://127.0.0.1:5000/ledgers
    curl -H "X-API-KEY: some_long_secret" http://127.0.0.1:5000/stock_items
"""

import os
import logging
from datetime import date, datetime
from flask import Flask, jsonify, request, abort
try:
    import pyodbc
except Exception as e:
    raise RuntimeError("pyodbc is required on the Tally machine. Install it (pip install pyodbc) and ensure ODBC DSN is configured.") from e

# Basic config
DSN = os.getenv("TALLY_DSN", "TallyODBC64_9000")
API_KEY = os.getenv("TALLY_API_KEY", "")  # set this to a long random string
HOST = os.getenv("GATEWAY_HOST", "127.0.0.1")
PORT = int(os.getenv("GATEWAY_PORT", 5000))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = Flask(__name__)

# Simple auth: require header X-API-KEY if API_KEY set
@app.before_request
def _require_api_key():
    if API_KEY:
        key = request.headers.get("X-API-KEY", "")
        if key != API_KEY:
            logging.warning("Unauthorized request from %s", request.remote_addr)
            abort(401)

def _connect():
    """Open pyodbc connection to Tally via DSN."""
    logging.debug("Connecting to Tally ODBC DSN=%s", DSN)
    return pyodbc.connect(f"DSN={DSN}")

def _to_iso(d):
    if isinstance(d, (datetime, date)):
        return d.isoformat()
    if d is None:
        return None
    return str(d)

def _safe_float(val):
    if val is None:
        return 0.0
    try:
        return float(val)
    except Exception:
        try:
            # pyodbc may return Decimal
            return float(str(val))
        except Exception:
            return 0.0

def _row_to_dict(row, cols):
    out = {}
    for i, c in enumerate(cols):
        v = row[i] if i < len(row) else None
        if isinstance(v, (datetime, date)):
            out[c] = v.isoformat()
        else:
            out[c] = v
    return out

@app.route("/")
def index():
    return jsonify({"ok": True, "service": "tally_gateway", "dsn": DSN})

# Companies
@app.route("/companies")
def companies():
    q = "SELECT $Name, $StartingFrom, $EndingAt FROM Company"
    cols = ["name", "period_start", "period_end"]
    try:
        conn = _connect()
        cur = conn.cursor()
        rows = cur.execute(q).fetchall()
        data = []
        for r in rows:
            data.append({
                "name": str(r[0]).strip() if r[0] is not None else None,
                "period_start": _to_iso(r[1]),
                "period_end": _to_iso(r[2])
            })
        conn.close()
        return jsonify(data)
    except Exception as e:
        logging.exception("companies query failed")
        return jsonify({"error": str(e)}), 500

# Ledgers
@app.route("/ledgers")
def ledgers():
    q = "SELECT $Name, $Parent FROM Ledger"
    try:
        conn = _connect()
        cur = conn.cursor()
        rows = cur.execute(q).fetchall()
        data = [{"name": str(r[0]).strip() if r[0] else None,
                 "parent": str(r[1]).strip() if r[1] else None} for r in rows]
        conn.close()
        return jsonify(data)
    except Exception as e:
        logging.exception("ledgers query failed")
        return jsonify({"error": str(e)}), 500

# Stock items — match pipeline's expected fields:
# name, category, base_unit, opening_qty, opening_rate (closing used here)
@app.route("/stock_items")
def stock_items():
    q = """
        SELECT $Name, $Parent, $BaseUnits, $_ClosingBalance, $_ClosingRate
        FROM StockItem
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        rows = cur.execute(q).fetchall()
        data = []
        for r in rows:
            name = str(r[0]).strip() if r[0] is not None else None
            category = str(r[1]).strip() if r[1] is not None else None
            base_unit = str(r[2]).strip() if r[2] is not None else None
            closing_qty = _safe_float(r[3])
            closing_rate = _safe_float(r[4])
            data.append({
                "name": name,
                "category": category,
                "base_unit": base_unit,
                "closing_qty": closing_qty,
                "closing_rate": closing_rate
            })
        conn.close()
        return jsonify(data)
    except Exception as e:
        logging.exception("stock_items query failed")
        return jsonify({"error": str(e)}), 500

# Stock movements (voucher-level) — match pipeline's VchStockItem fields
@app.route("/stock_movements")
def stock_movements():
    q = """
        SELECT $_LastSaleDate, $_LastSaleParty, $StockItemName, $_LastSalePrice, $_OutwardQuantity, $_OutwardValue
        FROM VchStockItem
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        rows = cur.execute(q).fetchall()
        data = []
        for r in rows:
            # columns: date, party, item_name, rate, qty, amount
            date_val = _to_iso(r[0]) if r[0] is not None else None
            party = str(r[1]).strip() if r[1] is not None else None
            item_name = str(r[2]).strip() if r[2] is not None else None
            rate = _safe_float(r[3])
            qty = _safe_float(r[4])
            amount = _safe_float(r[5])
            movement_type = "OUT" if amount > 0 else "IN"
            data.append({
                "date": date_val,
                "voucher_no": None,
                "company": party,          # pipeline maps 'party' -> company/brand
                "item": item_name,
                "qty": qty,
                "rate": rate,
                "amount": amount,
                "movement_type": movement_type
            })
        conn.close()
        return jsonify(data)
    except Exception as e:
        logging.exception("stock_movements query failed")
        return jsonify({"error": str(e)}), 500

# Simple health endpoint
@app.route("/health")
def health():
    try:
        conn = _connect()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

if __name__ == "__main__":
    logging.info("Starting Tally Gateway (DSN=%s) on %s:%d", DSN, HOST, PORT)
    app.run(host=HOST, port=PORT)
