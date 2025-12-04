from flask import Flask, render_template, request, redirect, url_for, session, jsonify, g
import mysql.connector
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, date
import smtplib
from email.mime.text import MIMEText
import requests
from flask_cors import CORS
import decimal
from urllib.parse import unquote_plus
import calendar
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
import logging
import traceback

# ---------------------------
# Configuration & setup
# ---------------------------
load_dotenv()
app = Flask(__name__)
app.secret_key = os.getenv("APP_SECRET", "supersecret")
CORS(app)

# ---------------------------
# DB Connection
# ---------------------------
def get_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DB", "inventory_db"),
        port=int(os.getenv("MYSQL_PORT", 3306))
    )

# ---------------------------
# Constants
# ---------------------------
CUSTOMER_ALLOWED_COMPANY_KEYWORDS = [
    "novateur electrical & digital systems pvt.ltd",
    "elmeasure",
    "socomec",
    "kei"
]
CUSTOMER_ALLOWED_COMPANY_DISPLAY = [
    "Novateur Electrical & Digital Systems Pvt.Ltd",
    "elmeasure",
    "socomec",
    "kei"
]

JWT_SECRET = os.getenv("JWT_SECRET") or os.getenv("SECRET_KEY") or app.secret_key or "replace-this-in-prod"
JWT_ALGO = "HS256"
JWT_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", 60*24))

TALLY_URL = os.getenv("TALLY_GATEWAY_URL", "")
TALLY_API_KEY = os.getenv("TALLY_API_KEY", "")

# ---------------------------
# Utilities
# ---------------------------
def convert_decimals(obj):
    """
    Recursively convert Decimal and other non-JSON-native types to JSON-native types.
    """
    if obj is None:
        return None
    if isinstance(obj, decimal.Decimal):
        try:
            return float(obj)
        except Exception:
            return 0.0
    if isinstance(obj, (int, float, str, bool)):
        return obj
    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8")
        except Exception:
            return str(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [convert_decimals(v) for v in obj]
    try:
        return float(obj)
    except Exception:
        return str(obj)

# ---------------------------
# Auth helpers
# ---------------------------
def create_token(user_id, username, role, minutes=JWT_EXPIRE_MINUTES):
    payload = {
        "sub": user_id,
        "username": username,
        "role": role,
        "exp": datetime.utcnow() + timedelta(minutes=minutes)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)

def decode_token(token):
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])

def token_or_session_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth.split(" ", 1)[1]
            try:
                payload = decode_token(token)
            except Exception as e:
                return jsonify({"error":"Invalid/expired token", "detail": str(e)}), 401
            g.user = {"id": payload["sub"], "username": payload.get("username"), "role": payload.get("role")}
            return f(*args, **kwargs)
        if "user" in session:
            g.user = {"id": session.get("user_id"), "username": session.get("user"), "role": session.get("role")}
            return f(*args, **kwargs)
        return jsonify({"error":"Authentication required"}), 401
    return wrapped

def requires_role(*roles):
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if not hasattr(g, "user"):
                auth = request.headers.get("Authorization", "")
                if auth.startswith("Bearer "):
                    try:
                        payload = decode_token(auth.split(" ", 1)[1])
                        g.user = {"id": payload["sub"], "username": payload.get("username"), "role": payload.get("role")}
                    except Exception as e:
                        return jsonify({"error":"Invalid/expired token", "detail": str(e)}), 401
                elif "user" in session:
                    g.user = {"id": session.get("user_id"), "username": session.get("user"), "role": session.get("role")}
                else:
                    return jsonify({"error":"Authentication required"}), 401
            if g.user.get("role") not in roles:
                return jsonify({"error":"Forbidden"}), 403
            return f(*args, **kwargs)
        return wrapped
    return decorator

# ---------------------------
# User DB helpers
# ---------------------------
def get_user_by_username(username):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, username, password_hash, role FROM users WHERE username=%s", (username,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    return user

def get_user_by_id(uid):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, username, role FROM users WHERE id=%s", (uid,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    return user

def create_user_db(username, password, role="sales"):
    pwd_hash = generate_password_hash(password)
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s)",
                (username, pwd_hash, role))
    conn.commit()
    new_id = cur.lastrowid
    cur.close()
    conn.close()
    return new_id

def update_user_db(uid, role=None, password=None):
    conn = get_connection()
    cur = conn.cursor()
    if role is not None:
        cur.execute("UPDATE users SET role=%s WHERE id=%s", (role, uid))
    if password:
        cur.execute("UPDATE users SET password_hash=%s WHERE id=%s", (generate_password_hash(password), uid))
    conn.commit()
    cur.close()
    conn.close()

def delete_user_db(uid):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE id=%s", (uid,))
    conn.commit()
    cur.close()
    conn.close()

# ---------------------------
# Permissions / Filters
# ---------------------------
def get_allowed_filters_for_user(user):
    role = user.get("role")
    if role in ("admin", "sales"):
        return None
    clauses = []
    params = []
    for kw in CUSTOMER_ALLOWED_COMPANY_KEYWORDS:
        if kw == "kei":
            clauses.append("LOWER(company) LIKE %s")
            params.append("%kei%")
        else:
            clauses.append("LOWER(company) = %s")
            params.append(kw)
    return {"clauses": clauses, "params": params}

# ---------------------------
# Template helpers
# ---------------------------
@app.context_processor
def inject_globals():
    return {'datetime': datetime, 'timedelta': timedelta}

@app.template_filter("inr")
def inr_format(value):
    """Format number in Indian currency style like ₹12,34,567.89"""
    try:
        value = float(value)
    except (ValueError, TypeError):
        return value
    s = f"{value:.2f}"
    if "." in s:
        int_part, dec_part = s.split(".")
    else:
        int_part, dec_part = s, ""
    if len(int_part) > 3:
        int_part = int_part[-3:] if len(int_part) <= 3 else (
            ",".join([int_part[:-3][::-1][i:i+2][::-1] for i in range(0, len(int_part[:-3]), 2)][::-1]) + "," + int_part[-3:]
        )
    return f"₹{int_part}.{dec_part}"

# ---------------------------
# Small reservation release (minimal, server-side)
# ---------------------------
def simple_release_reservation(item_name: str, billed_qty: float, voucher_no: str = None, movement_id: int = None):
    """
    Minimal server-side reservation release.
    - Try exact-match ACTIVE reservation (qty == billed_qty) and delete it.
    - Else, lock oldest ACTIVE reservation and subtract billed_qty from it (delete if <= 0).
    This opens its own transaction and is appropriate for your 'special case' flow.
    """
    if not item_name or billed_qty <= 0:
        return {"ok": True, "msg": "nothing to do"}

    conn = None
    try:
        conn = get_connection()
        conn.start_transaction()
        cur = conn.cursor()

        # 1) exact-match attempt
        cur.execute("""
            SELECT id, qty FROM stock_reservations
            WHERE item=%s AND status='ACTIVE' AND qty = %s AND (end_date IS NULL OR end_date >= CURDATE())
            ORDER BY start_date ASC, id ASC
            FOR UPDATE
            LIMIT 1
        """, (item_name, billed_qty))
        row = cur.fetchone()

        if row:
            rid = row[0]
            cur.execute("DELETE FROM stock_reservations WHERE id=%s", (rid,))
            conn.commit()
            cur.close()
            return {"ok": True, "mode": "exact", "consumed_reservation_id": rid, "fulfilled": billed_qty}

        # 2) fallback: oldest active reservation
        cur.execute("""
            SELECT id, qty FROM stock_reservations
            WHERE item=%s AND status='ACTIVE' AND (end_date IS NULL OR end_date >= CURDATE())
            ORDER BY start_date ASC, id ASC
            FOR UPDATE
            LIMIT 1
        """, (item_name,))
        row = cur.fetchone()
        if not row:
            conn.commit()
            cur.close()
            return {"ok": True, "mode": "none", "msg": "no active reservations found"}

        rid, rqty = row[0], float(row[1] or 0.0)
        new_qty = rqty - billed_qty
        if new_qty <= 0:
            # remove reservation
            cur.execute("DELETE FROM stock_reservations WHERE id=%s", (rid,))
            conn.commit()
            cur.close()
            return {"ok": True, "mode": "fallback_remove", "consumed_reservation_id": rid, "fulfilled": min(rqty, billed_qty)}
        else:
            cur.execute("UPDATE stock_reservations SET qty=%s WHERE id=%s", (new_qty, rid))
            conn.commit()
            cur.close()
            return {"ok": True, "mode": "fallback_reduce", "reservation_id": rid, "was": rqty, "now": new_qty, "fulfilled": billed_qty}

    except Exception as e:
        try:
            if conn:
                conn.rollback()
        except:
            pass
        logging.exception("simple_release_reservation error: %s", e)
        return {"ok": False, "error": str(e)}
    finally:
        try:
            if conn:
                conn.close()
        except:
            pass

# ---------------------------
# Sync from Tally (keeps your bulk insert behaviour)
# integrated to call simple_release_reservation for OUT movements
# ---------------------------
def sync_from_tally():
    headers = {"X-API-KEY": TALLY_API_KEY} if TALLY_API_KEY else {}
    try:
        items = requests.get(f"{TALLY_URL.rstrip('/')}/stock_items", headers=headers, timeout=15).json()
        moves = requests.get(f"{TALLY_URL.rstrip('/')}/stock_movements", headers=headers, timeout=15).json()
    except Exception as e:
        logging.exception("Tally fetch failed: %s", e)
        return {"ok": False, "error": f"Tally fetch failed: {e}"}

    try:
        conn = get_connection()
        cur = conn.cursor()

        # existing behaviour: clear and insert fresh
        cur.execute("TRUNCATE TABLE stock_items")
        cur.execute("TRUNCATE TABLE stock_movements")

        item_data = []
        for i in items or []:
            item_data.append((
                i.get("name"),
                i.get("category"),
                i.get("base_unit"),
                i.get("closing_qty", 0),
                i.get("closing_rate", 0)
            ))
        if item_data:
            cur.executemany("""
                INSERT INTO stock_items (name, category, base_unit, opening_qty, opening_rate)
                VALUES (%s, %s, %s, %s, %s)
            """, item_data)

        move_data = []
        for m in moves or []:
            move_data.append((
                m.get("date"),
                m.get("voucher_no"),
                m.get("company"),
                m.get("item"),
                m.get("qty", 0),
                m.get("rate", 0),
                m.get("amount", 0),
                m.get("movement_type")
            ))
        if move_data:
            cur.executemany("""
                INSERT INTO stock_movements (date, voucher_no, company, item, qty, rate, amount, movement_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, move_data)

        conn.commit()
        cur.close()
        conn.close()

        # After bulk-insert, attempt reservation release per OUT movement (best-effort, opens own tx per call)
        for m in moves or []:
            try:
                if (m.get("movement_type") or "").upper() == "OUT":
                    itm = m.get("item")
                    qty = float(m.get("qty") or 0)
                    vno = m.get("voucher_no")
                    simple_release_reservation(itm, qty, voucher_no=vno, movement_id=None)
            except Exception:
                logging.exception("release per-move error for movement: %s", m)

        return {"ok": True, "items": len(items or []), "movements": len(moves or [])}
    except Exception as e:
        logging.exception("sync_from_tally MySQL insert failed: %s", e)
        return {"ok": False, "error": f"MySQL insert failed: {e}"}

# ---------------------------
# Routes (UI + debug)
# ---------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        user = request.form["username"]
        pwd = request.form["password"]
        u = get_user_by_username(user)
        if u and check_password_hash(u["password_hash"], pwd):
            session["user"] = u["username"]
            session["user_id"] = u["id"]
            session["role"] = u["role"]
            return redirect(url_for("dashboard"))
        return render_template("login.html", error="Invalid credentials")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/")
def dashboard():
    if "user" not in session:
        return redirect(url_for("login"))
    return render_template("dashboard.html", user=session["user"], role=session["role"])

@app.route("/api/debug/dbtest")
def dbtest():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT NOW();")
        res = cur.fetchone()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "time": str(res[0])})
    except Exception as e:
        logging.exception("dbtest error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/debug/db")
def debug_db():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SHOW TABLES;")
        tables = [r[0] for r in cur.fetchall()]
        cur.close()
        conn.close()
        return jsonify({"ok": True, "tables": tables})
    except Exception as e:
        logging.exception("debug_db error: %s", e)
        return jsonify({"ok": False, "error": str(e)})

# ---------------------------
# Sales / Stock HTML pages (kept original behavior)
# ---------------------------
@app.route("/sales-summary")
def sales_summary():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT SUM(amount) AS total
        FROM stock_movements
        WHERE movement_type='OUT'
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    total = row[0] if row else 0
    return render_template("sales_summary.html", total=total)

@app.route("/sales-summary/brands")
def sales_brands():
    q = request.args.get("q", "").strip()
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if q:
            cur.execute("""
                SELECT 
                    TRIM(IFNULL(m.company, 'Uncategorized')) AS brand,
                    SUM(m.amount) AS value
                FROM stock_movements m
                WHERE m.movement_type = 'OUT'
                  AND LOWER(TRIM(m.company)) LIKE %s
                GROUP BY m.company
                ORDER BY value DESC
            """, (f"%{q}%",))
        else:
            cur.execute("""
                SELECT 
                    TRIM(IFNULL(m.company, 'Uncategorized')) AS brand,
                    SUM(m.amount) AS value
                FROM stock_movements m
                WHERE m.movement_type = 'OUT'
                GROUP BY m.company
                ORDER BY value DESC
            """)
        brands = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()
    return render_template("sales_brands.html", brands=brands, query=q)

@app.route("/sales-summary/brands/<brand>")
def sales_monthly(brand):
    decoded_brand = unquote_plus(brand or "")
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT 
            DATE_FORMAT(m.date, '%M %Y') AS month,
            DATE_FORMAT(m.date, '%Y-%m') AS sort_key,
            SUM(m.amount) AS value
        FROM stock_movements m
        WHERE m.movement_type = 'OUT'
          AND LOWER(TRIM(m.company)) = LOWER(TRIM(%s))
        GROUP BY sort_key, month
        ORDER BY sort_key
    """, (decoded_brand,))
    months = cur.fetchall() or []
    cur.close()
    conn.close()
    return render_template("sales_monthly.html", company=decoded_brand, months=months)

@app.route("/stock-summary", methods=["GET", "POST"])
def stock_summary():
    q = request.args.get("q", "").strip()
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    user = session.get("user", "SalesUser")
    try:
        if request.method == "POST":
            item = request.form["item"]
            qty = float(request.form["qty"])
            days = int(request.form.get("days", 2))
            end_date = date.today() + timedelta(days=days)
            cur.execute("""
                INSERT INTO stock_reservations (item, reserved_by, qty, start_date, end_date, status)
                VALUES (%s, %s, %s, CURDATE(), %s, 'ACTIVE')
            """, (item, user, qty, end_date))
            conn.commit()
            # best-effort email
            try:
                send_reservation_notification(item, qty, user, end_date)
            except Exception:
                logging.exception("reservation email failed")

        # Auto-expire old reservations
        cur.execute("""
            UPDATE stock_reservations
            SET status='EXPIRED'
            WHERE status='ACTIVE' AND end_date < CURDATE()
        """)
        conn.commit()

        if q:
            cur.execute("""
                SELECT name, category
                FROM stock_items
                WHERE name LIKE %s
                LIMIT 1
            """, (f"%{q}%",))
            match = cur.fetchone()
            if match:
                cur.close()
                conn.close()
                return redirect(url_for("stock_items", brand=(match.get("category") or ""), q=match.get("name")))

            cur.execute("""
                SELECT IFNULL(category,'Uncategorized') AS brand, SUM(opening_qty * opening_rate) AS value
                FROM stock_items
                WHERE category LIKE %s OR name LIKE %s
                GROUP BY category ORDER BY category
            """, (f"%{q}%", f"%{q}%"))
            rows = cur.fetchall() or []
            cur.close()
            conn.close()
            return render_template("stock_summary.html", brands=rows, query=q)

        cur.execute("""
            SELECT IFNULL(category,'Uncategorized') AS brand, SUM(opening_qty * opening_rate) AS value
            FROM stock_items
            GROUP BY category ORDER BY category
        """)
        rows = cur.fetchall() or []
        return render_template("stock_summary.html", brands=rows, query=q)
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

@app.route("/stock-summary/<brand>", methods=["GET", "POST"])
def stock_items(brand):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    user = session.get("user", "SalesUser")
    try:
        if request.method == "POST":
            item = request.form["item"]
            qty = float(request.form["qty"])
            reserved_by = request.form.get("reserved_by") or user
            end_date = request.form.get("end_date")
            if not end_date:
                end_date = (date.today() + timedelta(days=3)).strftime("%Y-%m-%d")
            cur.execute("""
                INSERT INTO stock_reservations (item, reserved_by, qty, start_date, end_date, status)
                VALUES (%s, %s, %s, CURDATE(), %s, 'ACTIVE')
            """, (item, reserved_by, qty, end_date))
            conn.commit()
            try:
                send_reservation_notification(item, qty, reserved_by, end_date)
            except Exception:
                logging.exception("reservation email failed")

        # expire old
        cur.execute("""
            UPDATE stock_reservations
            SET status='EXPIRED'
            WHERE status='ACTIVE' AND end_date < CURDATE()
        """)
        conn.commit()

        # cancel reservations that exceed available_qty
        cur.execute("""
            UPDATE stock_reservations r
            JOIN (
                SELECT i.name AS item,
                       i.opening_qty - IFNULL(SUM(m.qty), 0) AS available_qty
                FROM stock_items i
                LEFT JOIN stock_movements m
                  ON i.name = m.item AND m.movement_type='OUT'
                GROUP BY i.name, i.opening_qty
            ) s ON r.item = s.item
            SET r.status='CANCELLED'
            WHERE r.status='ACTIVE' AND r.qty > s.available_qty;
        """)
        conn.commit()

        search_query = request.args.get("q", "").strip()
        if search_query:
            cur.execute("""
                SELECT i.name AS item,
                       i.opening_qty AS total_qty,
                       IFNULL(SUM(r.qty), 0) AS reserved_qty,
                       (i.opening_qty - IFNULL(SUM(r.qty), 0)) AS available_qty,
                       MAX(r.reserved_by) AS reserved_by,
                       DATE_FORMAT(MAX(r.end_date), '%%Y-%%m-%%d') AS end_date
                FROM stock_items i
                LEFT JOIN stock_reservations r
                  ON i.name = r.item AND r.status='ACTIVE'
                WHERE i.category=%s AND i.name LIKE %s
                GROUP BY i.name, i.opening_qty
                ORDER BY i.name
            """, (brand, f"%{search_query}%"))
        else:
            cur.execute("""
                SELECT i.name AS item,
                       i.opening_qty AS total_qty,
                       IFNULL(SUM(r.qty), 0) AS reserved_qty,
                       (i.opening_qty - IFNULL(SUM(r.qty), 0)) AS available_qty,
                       MAX(r.reserved_by) AS reserved_by,
                       DATE_FORMAT(MAX(r.end_date), '%%d-%%m-%%Y') AS end_date
                FROM stock_items i
                LEFT JOIN stock_reservations r
                  ON i.name = r.item AND r.status='ACTIVE'
                WHERE i.category=%s
                GROUP BY i.name, i.opening_qty
                ORDER BY i.name
            """, (brand,))

        rows = cur.fetchall() or []
        # format end_date to dd-mm-yyyy via convert_decimals if needed
        rows = convert_decimals(rows)
        for r in rows:
            ed = r.get("end_date")
            if not ed:
                r["end_date"] = "-"
                continue
            try:
                if isinstance(ed, str):
                    dt = datetime.fromisoformat(ed).date()
                elif isinstance(ed, datetime):
                    dt = ed.date()
                else:
                    dt = ed
                r["end_date"] = dt.strftime("%d-%m-%Y")
            except Exception:
                r["end_date"] = str(ed)

        return render_template("stock_companies.html", brand=brand, items=rows, today=date.today())
    except Exception:
        logging.exception("stock_items error")
        return jsonify({"ok": False, "error": "Internal server error"}), 500
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

# ---------------------------
# Email Notification
# ---------------------------
def send_reservation_notification(item, qty, user, end_date):
    body = f"{user} reserved {qty} units of {item} until {end_date}."
    msg = MIMEText(body)
    msg["Subject"] = "Stock Reserved Notification"
    sender = os.getenv("EMAIL_USER", "yourapp@example.com")
    receivers = os.getenv("EMAIL_NOTIFY", "team@example.com")
    msg["From"] = sender
    msg["To"] = receivers

    rcpts = [r.strip() for r in receivers.split(",") if r.strip()]
    if not rcpts:
        logging.info("send_reservation_notification: no EMAIL_NOTIFY configured, skipping email")
        return

    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=20) as s:
            s.set_debuglevel(0)
            s.starttls()
            user_env = os.getenv("EMAIL_USER")
            pass_env = os.getenv("EMAIL_PASS")
            if not user_env or not pass_env:
                raise RuntimeError("EMAIL_USER or EMAIL_PASS not set in environment")
            s.login(user_env, pass_env)
            s.sendmail(sender, rcpts, msg.as_string())
            logging.info("send_reservation_notification: sent email to %s", rcpts)
    except Exception:
        logging.exception("Email sending failed")

# ---------------------------
# Auto-release simple function (keeps lightweight behaviour)
# ---------------------------
def auto_release_reservations():
    """Auto-cancel reservations that exceed available qty (sold in stock_movements)."""
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE stock_reservations r
            JOIN (
                SELECT i.name AS item,
                       i.opening_qty - IFNULL(SUM(m.qty), 0) AS available_qty
                FROM stock_items i
                LEFT JOIN stock_movements m
                  ON i.name = m.item AND m.movement_type='OUT'
                GROUP BY i.name, i.opening_qty
            ) s ON r.item = s.item
            SET r.status='CANCELLED'
            WHERE r.status='ACTIVE' AND r.qty > s.available_qty;
        """)
        conn.commit()
        cur.close()
    except Exception:
        logging.exception("auto_release_reservations error")
    finally:
        try:
            if conn:
                conn.close()
        except:
            pass

# Use sales table for summaries instead of stock_movements
# Assumes `sales` table includes columns: date, voucher_no, company, item, qty, rate, amount, party_ledger, ledger_name

@app.route("/api/sales-summary")
@requires_role("admin")
def api_sales_summary():
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Use sales.amount (assuming positive sale amounts). If negative signs exist for returns, you may need ABS or filter.
        cur.execute("SELECT SUM(amount) AS total FROM sales")
        row = cur.fetchone()
        # cursor.fetchone() may return tuple or dict depending on cursor setup
        if not row:
            total = 0
        else:
            if isinstance(row, (list, tuple)):
                total = row[0] or 0
            elif isinstance(row, dict):
                total = row.get("total") or 0
            else:
                total = getattr(row, "total", 0) or 0
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass
    total = convert_decimals(total)
    return jsonify({"ok": True, "total_sales": total})



@app.route("/api/sales-summary/brands")
def api_sales_brands():
    q = request.args.get("q", "").strip().lower()
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        # Prefer company from sales, otherwise category from stock_items
        inner = """
            SELECT
              COALESCE(NULLIF(TRIM(s.company), ''), NULLIF(TRIM(i.category), ''), 'Uncategorized') AS brand,
              COALESCE(s.amount, 0) AS amt
            FROM sales s
            LEFT JOIN stock_items i ON s.item = i.name
        """

        if q:
            sql = f"""
                SELECT brand, SUM(amt) AS value
                FROM ({inner}) AS x
                WHERE LOWER(brand) LIKE %s
                GROUP BY brand
                ORDER BY value DESC
            """
            params = (f"%{q}%",)
        else:
            sql = f"""
                SELECT brand, SUM(amt) AS value
                FROM ({inner}) AS x
                GROUP BY brand
                ORDER BY value DESC
            """
            params = ()
        cur.execute(sql, params)
        rows = cur.fetchall() or []
        rows = convert_decimals(rows)
        return jsonify({"ok": True, "brands": rows})
    except Exception as e:
        logging.exception("api_sales_brands error: %s", e)
        tb = traceback.format_exc().splitlines()[-8:]
        return jsonify({"ok": False, "error": "Internal server error", "detail": str(e), "trace": tb}), 500
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except:
            pass


import calendar
from datetime import date, datetime, timedelta
from flask import jsonify, request
from urllib.parse import unquote_plus
import traceback

def _parse_bool(v):
    if v is None:
        return False
    return str(v).strip().lower() in ("1", "true", "yes", "y", "t")

def _build_month_slots(start_date: date, end_date: date):
    """Return list of (sort_key 'YYYY-MM', label 'Month YYYY') from start_date to end_date inclusive by month."""
    slots = []
    cur = date(start_date.year, start_date.month, 1)
    while cur <= end_date:
        sort_key = cur.strftime("%Y-%m")
        label = f"{calendar.month_name[cur.month]} {cur.year}"
        slots.append((sort_key, label))
        # advance one month
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return slots

@app.route("/api/sales-summary/monthly")
@requires_role("admin")
def api_sales_monthly_overallv2():
    """
    Returns monthly sales totals.
    Query params:
      - year: integer. If fiscal=true, interpreted as fiscal start year (Apr year -> Mar year+1).
              If not fiscal, interpreted as calendar year (Jan-Dec).
      - fiscal: 'true'|'1' to use financial year mode (April -> March).
      - start, end: optional 'YYYY-MM' to override and define exact inclusive month range (these override year/fiscal).
    Response:
      { "ok": True, "months": [{"month":"April 2024","sort_key":"2024-04","value":123.45}, ...] }
    """
    year_arg = request.args.get("year", "").strip()
    start_arg = request.args.get("start", "").strip()
    end_arg = request.args.get("end", "").strip()
    fiscal = _parse_bool(request.args.get("fiscal"))

    # determine date range
    try:
        if start_arg and end_arg:
            # expect 'YYYY-MM'
            start_date = datetime.strptime(start_arg + "-01", "%Y-%m-%d").date()
            # end date -> last day of month
            y, m = map(int, end_arg.split("-"))
            end_date = date(y, m, 1)
            # advance to last day
            if end_date.month == 12:
                end_date = date(end_date.year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = date(end_date.year, end_date.month + 1, 1) - timedelta(days=1)
        else:
            year = None
            if year_arg:
                try:
                    year = int(year_arg)
                except Exception:
                    year = None
            today = date.today()
            if fiscal:
                if year is None:
                    # compute current fiscal start year
                    # if month >= April, fiscal start is current year, else previous year
                    year = today.year if today.month >= 4 else today.year - 1
                start_date = date(year, 4, 1)
                end_date = date(year + 1, 3, 31)
            else:
                if year is None:
                    year = today.year
                start_date = date(year, 1, 1)
                end_date = date(year, 12, 31)
    except Exception as e:
        logging.exception("Invalid date filter: %s", e)
        return jsonify({"ok": False, "error": "Invalid date filters"}), 400

    # query aggregated month sums (grouped by YYYY-MM)
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        sql = """
            SELECT DATE_FORMAT(date, '%Y-%m') AS sort_key,
                   DATE_FORMAT(date, '%M %Y') AS month,
                   SUM(amount) AS value
            FROM sales
            WHERE date BETWEEN %s AND %s
            GROUP BY sort_key, month
            ORDER BY sort_key
        """
        params = (start_date.isoformat(), end_date.isoformat())
        cur.execute(sql, params)
        rows = cur.fetchall() or []
        # build lookup
        sums = {r["sort_key"]: float(r["value"] or 0) for r in rows}

        # build month slots and fill zeros for missing months, respecting fiscal ordering if requested
        slots = _build_month_slots(start_date, end_date)
        # If fiscal requested and the span is exactly Apr->Mar, ensure order is Apr->Mar (slots already in chronological order)
        months_out = []
        for sort_key, label in slots:
            months_out.append({"month": label, "sort_key": sort_key, "value": sums.get(sort_key, 0.0)})

        return jsonify({"ok": True, "months": months_out})
    except Exception as e:
        logging.exception("api_sales_monthly_overall error: %s", e)
        tb = traceback.format_exc().splitlines()[-8:]
        return jsonify({"ok": False, "error": "Internal server error", "detail": str(e), "trace": tb}), 500
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass


@app.route("/api/sales-summary/brands/<path:brand>/monthly")
@requires_role("admin")
def api_sales_monthly_brand(brand):
    """
    Monthly sales totals for a specific brand (category or party ledger).
    Query params same as /api/sales-summary/monthly
    'brand' is URL-encoded; matching tries party_ledger/company first, fallback to stock_items.category if needed.
    """
    decoded_brand = unquote_plus(brand or "").strip()
    year_arg = request.args.get("year", "").strip()
    start_arg = request.args.get("start", "").strip()
    end_arg = request.args.get("end", "").strip()
    fiscal = _parse_bool(request.args.get("fiscal"))

    # determine date range (same logic as overall)
    try:
        if start_arg and end_arg:
            start_date = datetime.strptime(start_arg + "-01", "%Y-%m-%d").date()
            y, m = map(int, end_arg.split("-"))
            end_date = date(y, m, 1)
            if end_date.month == 12:
                end_date = date(end_date.year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = date(end_date.year, end_date.month + 1, 1) - timedelta(days=1)
        else:
            year = None
            if year_arg:
                try:
                    year = int(year_arg)
                except Exception:
                    year = None
            today = date.today()
            if fiscal:
                if year is None:
                    year = today.year if today.month >= 4 else today.year - 1
                start_date = date(year, 4, 1)
                end_date = date(year + 1, 3, 31)
            else:
                if year is None:
                    year = today.year
                start_date = date(year, 1, 1)
                end_date = date(year, 12, 31)
    except Exception as e:
        logging.exception("Invalid date filter (brand endpoint): %s", e)
        return jsonify({"ok": False, "error": "Invalid date filters"}), 400

    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        # Prefer matching party_ledger/company fields if you populate them in sales.
        # If you instead want to match stock_items.category, replace the WHERE clause with a JOIN on stock_items.
        sql_inner = """
            SELECT date, amount
            FROM sales
            WHERE (LOWER(TRIM(COALESCE(party_ledger, ''))) = LOWER(TRIM(%s))
                   OR LOWER(TRIM(COALESCE(company, ''))) = LOWER(TRIM(%s)))
              AND date BETWEEN %s AND %s
        """
        params = [decoded_brand, decoded_brand, start_date.isoformat(), end_date.isoformat()]

        sql = f"""
            SELECT DATE_FORMAT(date, '%Y-%m') AS sort_key,
                   DATE_FORMAT(date, '%M %Y') AS month,
                   SUM(amount) AS value
            FROM ({sql_inner}) AS t
            GROUP BY sort_key, month
            ORDER BY sort_key
        """

        cur.execute(sql, tuple(params))
        rows = cur.fetchall() or []
        sums = {r["sort_key"]: float(r["value"] or 0) for r in rows}

        slots = _build_month_slots(start_date, end_date)
        months_out = [{"month": label, "sort_key": sort_key, "value": sums.get(sort_key, 0.0)} for sort_key, label in slots]

        # optional: compute total if you want to return it
        total = sum(m["value"] for m in months_out)

        return jsonify({"ok": True, "brand": decoded_brand, "months": months_out, "total": total})
    except Exception as e:
        logging.exception("api_sales_monthly_brand error: %s", e)
        tb = traceback.format_exc().splitlines()[-8:]
        return jsonify({"ok": False, "error": "Internal server error", "detail": str(e), "trace": tb}), 500
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass


@app.route("/api/stock-summary")
@token_or_session_required
def api_stock_summary():
    q = request.args.get("q", "").strip()
    user = g.user
    allowed = get_allowed_filters_for_user(user)
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        brand_expr = "TRIM(COALESCE(NULLIF(i.brand, ''), i.category))"
        if allowed is None:
            if q:
                like_q = f"%{q}%"
                cur.execute(f"""
                    SELECT {brand_expr} AS brand,
                           COALESCE(SUM(i.opening_qty * i.opening_rate), 0) AS value
                    FROM stock_items i
                    WHERE {brand_expr} LIKE %s OR i.name LIKE %s
                    GROUP BY {brand_expr}
                    ORDER BY {brand_expr}
                """, (like_q, like_q))
            else:
                cur.execute(f"""
                    SELECT {brand_expr} AS brand,
                           COALESCE(SUM(i.opening_qty * i.opening_rate), 0) AS value
                    FROM stock_items i
                    GROUP BY {brand_expr}
                    ORDER BY {brand_expr}
                """)
            data = cur.fetchall() or []
            return jsonify({"ok": True, "brands": convert_decimals(data)})
        # Customer view (hardcoded)
        BRANDS = [
            "Novateur Electrical & Digital Systems Pvt.Ltd",
            "Elemeasure",
            "SOCOMEC",
            "KEI",
            "KEI (100/180 METER)",
            "KEI (CONFLAME)",
            "KEI (HOMECAB)"
        ]
        result = []
        for brand in BRANDS:
            cur.execute(f"""
                SELECT COALESCE(SUM(i.opening_qty * i.opening_rate), 0) AS value
                FROM stock_items i
                WHERE LOWER(TRIM(COALESCE(NULLIF(i.brand, ''), i.category))) = LOWER(TRIM(%s))
            """, (brand,))
            row = cur.fetchone()
            val = float(row["value"]) if row and row.get("value") is not None else 0.0
            result.append({"brand": brand, "value": val})
        return jsonify({"ok": True, "brands": result})
    except Exception:
        logging.exception("api_stock_summary error")
        return jsonify({"ok": False, "error": "Internal server error"}), 500
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

@app.route("/api/stock-summary/<path:brand>")
@token_or_session_required
def api_stock_items(brand):
    decoded_brand = unquote_plus(brand or "").strip()
    user = g.user
    allowed = get_allowed_filters_for_user(user)
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        brand_expr = "LOWER(TRIM(COALESCE(NULLIF(i.brand, ''), i.category)))"
        decoded_norm = decoded_brand.lower().strip()
        if allowed is None:
            cur.execute(f"""
                SELECT i.name AS item,
                       i.opening_qty AS total_qty,
                       IFNULL(SUM(r.qty), 0) AS reserved_qty,
                       (i.opening_qty - IFNULL(SUM(r.qty), 0)) AS available_qty,
                       MAX(r.reserved_by) AS reserved_by,
                       MAX(r.end_date) AS end_date
                FROM stock_items i
                LEFT JOIN stock_reservations r
                  ON i.name = r.item AND r.status='ACTIVE'
                WHERE {brand_expr} = %s
                GROUP BY i.name, i.opening_qty
                ORDER BY i.name
            """, (decoded_norm,))
            rows = cur.fetchall() or []
            rows = convert_decimals(rows)
            for r in rows:
                ed = r.get("end_date")
                if not ed:
                    r["end_date"] = "-"
                    continue
                try:
                    if isinstance(ed, str):
                        dt = datetime.fromisoformat(ed).date()
                    elif isinstance(ed, datetime):
                        dt = ed.date()
                    else:
                        dt = ed
                    r["end_date"] = dt.strftime("%d-%m-%Y")
                except Exception:
                    r["end_date"] = str(ed)
            return jsonify({"ok": True, "brand": decoded_brand, "items": rows})

        ALLOWED_BRANDS = [
            "Novateur Electrical & Digital Systems Pvt.Ltd",
            "Elemeasure",
            "SOCOMEC",
            "KEI",
            "KEI (100/180 METER)",
            "KEI (CONFLAME)",
            "KEI (HOMECAB)"
        ]
        allowed_norm = [b.lower().strip() for b in ALLOWED_BRANDS]
        if decoded_norm in allowed_norm:
            cur.execute(f"""
                SELECT i.name AS item,
                       i.opening_qty AS total_qty,
                       IFNULL(SUM(r.qty), 0) AS reserved_qty,
                       (i.opening_qty - IFNULL(SUM(r.qty), 0)) AS available_qty,
                       MAX(r.reserved_by) AS reserved_by,
                       MAX(r.end_date) AS end_date
                FROM stock_items i
                LEFT JOIN stock_reservations r
                  ON i.name = r.item AND r.status='ACTIVE'
                WHERE {brand_expr} = %s
                GROUP BY i.name, i.opening_qty
                ORDER BY i.name
            """, (decoded_norm,))
            rows = cur.fetchall() or []
            rows = convert_decimals(rows)
            for r in rows:
                ed = r.get("end_date")
                if not ed:
                    r["end_date"] = "-"
                    continue
                try:
                    if isinstance(ed, str):
                        dt = datetime.fromisoformat(ed).date()
                    else:
                        dt = ed
                    r["end_date"] = dt.strftime("%d-%m-%Y")
                except:
                    r["end_date"] = str(ed)
            return jsonify({"ok": True, "brand": decoded_brand, "items": rows})

        # company-restricted customer case
        if not allowed.get("clauses"):
            return jsonify({"ok": True, "brand": decoded_brand, "items": []})

        company_where = " OR ".join(allowed["clauses"])
        params = allowed["params"].copy()
        params.insert(0, decoded_brand)
        sql = f"""
            SELECT DISTINCT i.name AS item,
                   i.opening_qty AS total_qty,
                   IFNULL(SUM(r.qty), 0) AS reserved_qty,
                   (i.opening_qty - IFNULL(SUM(r.qty), 0)) AS available_qty,
                   MAX(r.reserved_by) AS reserved_by,
                   MAX(r.end_date) AS end_date
            FROM stock_items i
            LEFT JOIN stock_reservations r
              ON i.name = r.item AND r.status='ACTIVE'
            JOIN stock_movements m ON m.item = i.name
            WHERE {brand_expr} = LOWER(TRIM(%s)) AND ({company_where})
            GROUP BY i.name, i.opening_qty
            ORDER BY i.name
        """
        cur.execute(sql, tuple(params))
        rows = cur.fetchall() or []
        rows = convert_decimals(rows)
        for r in rows:
            ed = r.get("end_date")
            if not ed:
                r["end_date"] = "-"
                continue
            try:
                if isinstance(ed, str):
                    dt = datetime.fromisoformat(ed).date()
                else:
                    dt = ed
                r["end_date"] = dt.strftime("%d-%m-%Y")
            except:
                r["end_date"] = str(ed)
        return jsonify({"ok": True, "brand": decoded_brand, "items": rows})
    except Exception:
        logging.exception("api_stock_items error")
        return jsonify({"ok": False, "error": "Internal server error"}), 500
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"ok": True, "results": []})
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        like = f"%{q}%"
        cur.execute("""
            SELECT
                i.name AS item,
                i.name AS name,
                i.category,
                i.base_unit,
                i.opening_qty AS total_qty,
                IFNULL(SUM(r.qty), 0) AS reserved_qty,
                (i.opening_qty - IFNULL(SUM(r.qty), 0)) AS available_qty,
                MAX(r.reserved_by) AS reserved_by,
                DATE_FORMAT(MAX(r.end_date), '%d-%m-%y') AS reserve_until,
                (i.opening_qty * COALESCE(i.opening_rate, 0)) AS value
            FROM stock_items i
            LEFT JOIN stock_reservations r
              ON r.item = i.name
              AND r.status = 'ACTIVE'
              AND (r.end_date IS NULL OR r.end_date >= CURDATE())
            WHERE i.name LIKE %s OR i.category LIKE %s
            GROUP BY i.id, i.name, i.category, i.base_unit, i.opening_qty, i.opening_rate
            ORDER BY i.category, i.name
        """, (like, like))
        results = cur.fetchall() or []
        results = convert_decimals(results)
        for row in results:
            row['total_qty'] = row.get('total_qty') or 0
            row['reserved_qty'] = row.get('reserved_qty') or 0
            row['available_qty'] = row.get('available_qty') or max(0, (row['total_qty'] - row['reserved_qty']))
            ru = row.get('reserve_until')
            if ru:
                try:
                    if isinstance(ru, (str,)):
                        dt = datetime.fromisoformat(ru).date()
                    else:
                        dt = ru
                    row['reserve_until'] = dt.strftime("%d-%m-%Y")
                except Exception:
                    pass
        return jsonify({"ok": True, "results": results})
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

@app.route("/api/stock-reserve", methods=["POST"])
def api_stock_reserve():
    data = request.get_json() or {}
    item = data.get("item")
    try:
        qty = float(data.get("qty", 0))
    except Exception:
        return jsonify({"ok": False, "error": "Invalid qty"}), 400
    days = int(data.get("days", 3)) if data.get("days") is not None else 3
    reserved_by = data.get("reserved_by") or session.get("user") or "mobile"
    if not item or qty <= 0:
        return jsonify({"ok": False, "error": "Missing or invalid item/qty"}), 400
    end_date = date.today() + timedelta(days=days)

    conn = None
    try:
        conn = get_connection()
        conn.start_transaction()
        cur = conn.cursor(dictionary=True)
        # lock item
        cur.execute("SELECT name, opening_qty FROM stock_items WHERE name=%s FOR UPDATE", (item,))
        item_row = cur.fetchone()
        if not item_row:
            conn.rollback()
            return jsonify({"ok": False, "error": f"Item '{item}' not found"}), 404
        total_qty = float(item_row.get("opening_qty") or 0)
        cur.execute("""
            SELECT IFNULL(SUM(r.qty), 0) AS reserved_qty
            FROM stock_reservations r
            WHERE r.item = %s AND r.status='ACTIVE' AND (r.end_date IS NULL OR r.end_date >= CURDATE())
            FOR UPDATE
        """, (item,))
        sum_row = cur.fetchone()
        reserved_qty = float(sum_row.get("reserved_qty") or 0)
        available_qty = max(0.0, total_qty - reserved_qty)
        if qty > available_qty:
            conn.rollback()
            return jsonify({"ok": False, "error": f"Only {available_qty} available; cannot reserve {qty}"}), 400

        cur.execute("""
            INSERT INTO stock_reservations (item, reserved_by, qty, start_date, end_date, status)
            VALUES (%s, %s, %s, CURDATE(), %s, 'ACTIVE')
        """, (item, reserved_by, qty, end_date))

        cur.execute("""
            SELECT 
                i.name AS item,
                i.opening_qty AS total_qty,
                IFNULL(SUM(r.qty), 0) AS reserved_qty,
                (i.opening_qty - IFNULL(SUM(r.qty), 0)) AS available_qty,
                MAX(r.reserved_by) AS reserved_by,
                MAX(r.start_date) AS max_start_date,
                MAX(r.end_date) AS max_end_date
            FROM stock_items i
            LEFT JOIN stock_reservations r
              ON r.item = i.name AND r.status='ACTIVE' AND (r.end_date IS NULL OR r.end_date >= CURDATE())
            WHERE i.name=%s
            GROUP BY i.name, i.opening_qty
        """, (item,))
        agg = cur.fetchone() or {}

        def _fmt_date_obj(val):
            if val is None:
                return None
            if isinstance(val, (datetime, date)):
                return val.strftime("%d-%m-%Y")
            try:
                return datetime.fromisoformat(str(val)).date().strftime("%d-%m-%Y")
            except Exception:
                return str(val)

        if agg:
            agg = convert_decimals(agg)
            agg["reserve_until"] = _fmt_date_obj(agg.pop("max_end_date", None))
            agg["last_reserve_start"] = _fmt_date_obj(agg.pop("max_start_date", None))

        conn.commit()
        # best-effort email
        try:
            send_reservation_notification(item, qty, reserved_by, end_date)
        except Exception:
            logging.exception("reservation email failed")
        return jsonify({
            "ok": True,
            "msg": "Reserved successfully",
            "reservation": {"item": item, "qty": qty, "end_date": str(end_date), "reserved_by": reserved_by},
            "aggregates": convert_decimals(agg)
        })
    except Exception as e:
        logging.exception("api_stock_reserve error: %s", e)
        if conn:
            try:
                conn.rollback()
            except:
                pass
        return jsonify({"ok": False, "error": f"DB error: {e}"}), 500
    finally:
        try:
            if conn:
                conn.close()
        except:
            pass

@app.route("/api/reservations")
def api_reservations():
    items_param = request.args.get("items", "").strip()
    if not items_param:
        return jsonify({"ok": True, "reservations": {}})
    raw_items = [unquote_plus(p).strip() for p in items_param.split(",") if p.strip()]
    if not raw_items:
        return jsonify({"ok": True, "reservations": {}})
    only_active = request.args.get("only_active", "").lower() in ("1", "true", "yes")
    placeholders = ",".join(["%s"] * len(raw_items))
    params = raw_items.copy()
    where_clauses = [f"r.item IN ({placeholders})"]
    if only_active:
        where_clauses.append("r.status = 'ACTIVE'")
        where_clauses.append("(r.end_date IS NULL OR r.end_date >= CURDATE())")
    where_sql = " AND ".join(where_clauses)
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        sql = f"""
            SELECT
                r.id,
                r.item,
                r.reserved_by,
                r.qty,
                r.start_date,
                r.end_date,
                r.status,
                r.remarks
            FROM stock_reservations r
            WHERE {where_sql}
            ORDER BY r.item, r.start_date, r.id
        """
        cur.execute(sql, tuple(params))
        rows = cur.fetchall() or []
        cur.close()
        conn.close()
        out = []
        for r in rows:
            r2 = convert_decimals(r)
            sd = r2.get("start_date")
            ed = r2.get("end_date")
            def fmt_date(v):
                if v is None:
                    return None
                if isinstance(v, (str,)):
                    try:
                        dt = datetime.fromisoformat(v).date()
                        return dt.strftime("%d-%m-%Y")
                    except Exception:
                        return v
                if isinstance(v, (datetime, date)):
                    return v.strftime("%d-%m-%Y")
                return str(v)
            r2["start_date"] = fmt_date(sd)
            r2["end_date"] = fmt_date(ed)
            out.append(r2)
        grouped = {}
        for r in out:
            key = r.get("item") or ""
            grouped.setdefault(key, []).append(r)
        return jsonify({"ok": True, "reservations": grouped})
    except Exception:
        logging.exception("api_reservations error")
        try:
            if conn:
                conn.close()
        except:
            pass
        return jsonify({"ok": False, "error": "Internal server error"}), 500

@app.route("/api/me")
@token_or_session_required
def api_me():
    user = g.user
    if user.get("role") in ("admin", "sales"):
        allowed_list = None
    else:
        allowed_list = CUSTOMER_ALLOWED_COMPANY_DISPLAY
    return jsonify({
        "id": user.get("id"),
        "username": user.get("username"),
        "role": user.get("role"),
        "allowed_companies": allowed_list
    })

# User management endpoints
@app.route("/api/users", methods=["GET"])
@requires_role("admin")
def api_list_users():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, username, role, created_at FROM users ORDER BY id")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)

@app.route("/api/users", methods=["POST"])
@requires_role("admin")
def api_create_user():
    data = request.get_json() or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    role = data.get("role") or "sales"
    if not username or not password:
        return jsonify({"error":"username and password required"}), 400
    try:
        new_id = create_user_db(username, password, role)
        return jsonify({"id": new_id, "username": username, "role": role}), 201
    except Exception as e:
        logging.exception("api_create_user error")
        return jsonify({"error":"Could not create user", "detail": str(e)}), 400

@app.route("/api/users/<int:user_id>", methods=["PUT"])
@token_or_session_required
def api_update_user(user_id):
    if (session.get("role") != "admin") and (g.user.get("role") != "admin") and (g.user.get("id") != user_id):
        return jsonify({"error":"Forbidden"}), 403
    data = request.get_json() or {}
    role = data.get("role", None)
    password = data.get("password", None)
    if role and g.user.get("role") != "admin":
        return jsonify({"error":"Only admin can change role"}), 403
    try:
        update_user_db(user_id, role=role, password=password)
        return jsonify({"ok": True})
    except Exception as e:
        logging.exception("api_update_user error")
        return jsonify({"error":"Could not update user", "detail": str(e)}), 400

@app.route("/api/users/<int:user_id>", methods=["DELETE"])
@requires_role("admin")
def api_delete_user(user_id):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT role FROM users WHERE id=%s", (user_id,))
    r = cur.fetchone()
    if not r:
        cur.close()
        conn.close()
        return jsonify({"error":"User not found"}), 404
    if r["role"] == "admin":
        cur.execute("SELECT COUNT(*) AS admins FROM users WHERE role='admin'")
        admins = cur.fetchone().get("admins", 0)
        if admins <= 1:
            cur.close()
            conn.close()
            return jsonify({"error":"Cannot delete the last admin"}), 400
    cur.close()
    conn.close()
    try:
        delete_user_db(user_id)
        return jsonify({"ok": True})
    except Exception as e:
        logging.exception("api_delete_user error")
        return jsonify({"error":"Could not delete user", "detail": str(e)}), 400

# ---------------------------
# Manual sync endpoint (keeps original behaviour)
# ---------------------------
@app.route("/sync")
def manual_sync():
    try:
        from etl_pipeline import ETLPipeline
    except Exception as e:
        logging.exception("manual_sync ETL import failed: %s", e)
        return jsonify({"ok": False, "error": f"ETL import failed: {e}"}), 500
    try:
        etl = ETLPipeline()
        if hasattr(etl, "run_once"):
            etl.run_once()
        else:
            if hasattr(etl, "extract_from_gateway"):
                etl.extract_from_gateway()
            if hasattr(etl, "transform"):
                etl.transform()
            if hasattr(etl, "load"):
                try:
                    etl.load(reset=True)
                except TypeError:
                    etl.load()
        return jsonify({"ok": True, "msg": "ETL sync completed"})
    except Exception:
        logging.exception("manual_sync error")
        return jsonify({"ok": False, "error": "ETL run failed"}), 500

# ---------------------------
# Run Flask
# ---------------------------
if __name__ == "__main__":
    # Set logging level
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=(os.getenv("FLASK_DEBUG", "1") == "1"))
