from flask import Flask, render_template, request, redirect, url_for, session, jsonify
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
from flask import g
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
import logging

# Load env vars
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("APP_SECRET", "supersecret")



# Enable CORS for API access from your Flutter/web clients
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

CUSTOMER_ALLOWED_COMPANY_KEYWORDS = [
    "novateur electrical & digital systems pvt.ltd",
    "elmeasure",
    "socomec",
    "kei"   # match all KEI variants via substring match
]
# human-friendly display list (client-readable)
CUSTOMER_ALLOWED_COMPANY_DISPLAY = [
    "Novateur Electrical & Digital Systems Pvt.Ltd",
    "elmeasure",
    "socomec",
    "kei"
]
@app.route("/api/debug/dbtest")
def dbtest():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT NOW();")
        res = cur.fetchone()
        conn.close()
        return jsonify({"ok": True, "time": str(res[0])})
    except Exception as e:
        import traceback
        print("❌ DBTEST ERROR:", traceback.format_exc())
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------
# Helper: convert non-json-native types
# ---------------------------
def convert_decimals(obj):
    """
    Recursively convert Decimal and other non-JSON-native types to JSON-native types.
    - decimal.Decimal -> float
    - bytes -> utf-8 string
    - datetime/date -> ISO string
    - dict/list/tuple -> converted recursively
    """
    if obj is None:
        return None

    # Decimal -> float
    if isinstance(obj, decimal.Decimal):
        try:
            return float(obj)
        except Exception:
            return 0.0

    # Native JSON types
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

    # fallback: try to convert to float, else string
    try:
        return float(obj)
    except Exception:
        return str(obj)

# ---------------------------
# Auth: DB-backed users + helpers
# ---------------------------
# JWT secret: store it in env, fallback to flask secret key
JWT_SECRET = os.getenv("JWT_SECRET") or os.getenv("SECRET_KEY") or app.secret_key or "replace-this-in-prod"
JWT_ALGO = "HS256"
JWT_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", 60*24))  # default 1 day

def get_user_by_username(username):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, username, password_hash, role FROM users WHERE username=%s", (username,))
    user = cur.fetchone()
    conn.close()
    return user

def get_user_by_id(uid):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, username, role FROM users WHERE id=%s", (uid,))
    user = cur.fetchone()
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
    conn.close()

def delete_user_db(uid):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE id=%s", (uid,))
    conn.commit()
    conn.close()

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

# Decorators for protecting endpoints (session-based or token-based)


def token_or_session_required(f):
    """Accepts either session-based login (web) or Bearer token (API). Sets g.user = {'id', 'username','role'}"""
    @wraps(f)
    def wrapped(*args, **kwargs):
        # 1) check for Authorization header
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth.split(" ", 1)[1]
            try:
                payload = decode_token(token)
            except Exception as e:
                return jsonify({"error":"Invalid/expired token", "detail": str(e)}), 401
            g.user = {"id": payload["sub"], "username": payload.get("username"), "role": payload.get("role")}
            return f(*args, **kwargs)
        # 2) fallback to flask session
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
                # attempt to populate g.user
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

def get_allowed_filters_for_user(user):
    """Return None => no restriction (admin/sales). Otherwise return list of SQL clauses + params."""
    role = user.get("role")
    if role in ("admin", "sales"):
        return None
    # customer => build SQL conditions and params
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
# Environment & Tally Info
# ---------------------------
TALLY_URL = os.getenv("TALLY_GATEWAY_URL", "")
TALLY_API_KEY = os.getenv("TALLY_API_KEY", "")

# ---------------------------
# Frontend & UI routes
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

# ---------------------------
# Dashboard
# ---------------------------
@app.route("/")
def dashboard():
    if "user" not in session:
        return redirect(url_for("login"))
    return render_template("dashboard.html", user=session["user"], role=session["role"])

'''@app.route("/search")
def search():
    query = request.args.get("q", "").strip()
    if not query:
        return redirect(url_for("dashboard"))

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT 
            name,
            category,
            base_unit,
            opening_qty,
            opening_rate,
            (opening_qty * opening_rate) AS value
        FROM stock_items
        WHERE name LIKE %s OR category LIKE %s
        ORDER BY category, name
        LIMIT 50
    """, (f"%{query}%", f"%{query}%"))
    results = cur.fetchall()
    conn.close()
    return render_template("search_results.html", query=query, results=results)
'''
@app.context_processor
def inject_globals():
    return {'datetime': datetime, 'timedelta': timedelta}

# =======================================================
# ⚙️ LIVE SYNC & MANUAL SYNC
# =======================================================
def sync_from_tally():
    """Pull live data from Tally Gateway and push into MySQL"""
    headers = {"X-API-KEY": TALLY_API_KEY}
    try:
        items = requests.get(f"{TALLY_URL}/stock_items", headers=headers, timeout=15).json()
        moves = requests.get(f"{TALLY_URL}/stock_movements", headers=headers, timeout=15).json()
    except Exception as e:
        return {"ok": False, "error": f"Tally fetch failed: {e}"}

    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE stock_items")
        cur.execute("TRUNCATE TABLE stock_movements")

        item_data = []
        for i in items:
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
        for m in moves:
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
        conn.close()
        return {"ok": True, "items": len(items), "movements": len(moves)}
    except Exception as e:
        return {"ok": False, "error": f"MySQL insert failed: {e}"}

@app.route("/sync")
def manual_sync():
    # Try to support both older and newer ETL API shapes:
    try:
        from etl_pipeline import ETLPipeline
    except Exception as e:
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
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/debug/db")
def debug_db():
    """Check MySQL connection"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SHOW TABLES;")
        tables = [r[0] for r in cur.fetchall()]
        conn.close()
        return jsonify({"ok": True, "tables": tables})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

# ---------------------------
# 📊 Sales Summary (overall) - HTML
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
    total = row.get("total") if row and isinstance(row, dict) else (row[0] if row else 0)
    # For the HTML view we keep the template rendering; templates can handle formatting.
    conn.close()
    return render_template("sales_summary.html", total=total)

# -----------------------------------------------
# 📊 SALES SUMMARY - BY BRAND (Total Sales Value Only) - HTML
# -----------------------------------------------
@app.route("/sales-summary/brands")
def sales_brands():
    q = request.args.get("q", "").strip()
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

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
    conn.close()
    return render_template("sales_brands.html", brands=brands, query=q)

# -----------------------------------------------
# 📊 SALES SUMMARY - MONTHLY SALES BY BRAND - HTML
# -----------------------------------------------
@app.route("/sales-summary/brands/<brand>")
def sales_monthly(brand):
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
    """, (brand,))

    months = cur.fetchall() or []
    conn.close()
    return render_template("sales_monthly.html", company=brand, months=months)

# ---------------------------
# 📦 Stock Summary (with smart item redirect + reserve support) - HTML
# ---------------------------
@app.route("/stock-summary", methods=["GET", "POST"])
def stock_summary():
    q = request.args.get("q", "").strip()
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    user = session.get("user", "SalesUser")

    # Reservation submission
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
        send_reservation_notification(item, qty, user, end_date)

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
            conn.close()
            return redirect(url_for("stock_items", brand=(match.get("category") or ""), q=match.get("name")))

        cur.execute("""
            SELECT IFNULL(category,'Uncategorized') AS brand, SUM(opening_qty * opening_rate) AS value
            FROM stock_items
            WHERE category LIKE %s OR name LIKE %s
            GROUP BY category ORDER BY category
        """, (f"%{q}%", f"%{q}%"))
        rows = cur.fetchall() or []
        conn.close()
        return render_template("stock_summary.html", brands=rows, query=q)

    cur.execute("""
        SELECT IFNULL(category,'Uncategorized') AS brand, SUM(opening_qty * opening_rate) AS value
        FROM stock_items
        GROUP BY category ORDER BY category
    """)
    rows = cur.fetchall() or []
    conn.close()
    return render_template("stock_summary.html", brands=rows, query=q)

# ---------------------------
# 📦 Stock Items + Reservations - HTML
# ---------------------------
@app.route("/stock-summary/<brand>", methods=["GET", "POST"])
def stock_items(brand):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    user = session.get("user", "SalesUser")

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

        send_reservation_notification(item, qty, reserved_by, end_date)

    cur.execute("""
        UPDATE stock_reservations
        SET status='EXPIRED'
        WHERE status='ACTIVE' AND end_date < CURDATE()
    """)
    conn.commit()

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
    conn.close()

    return render_template(
        "stock_companies.html",
        brand=brand,
        items=rows,
        today=date.today()
    )

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

    # receivers may be comma-separated
    rcpts = [r.strip() for r in receivers.split(",") if r.strip()]
    if not rcpts:
        print("send_reservation_notification: no EMAIL_NOTIFY configured, skipping email")
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
            print("send_reservation_notification: sent email to", rcpts)
    except Exception as e:
        # print full info to logs (not leaked to client)
        import traceback
        print("Email sending failed:", e)
        print(traceback.format_exc())


def auto_release_reservations():
    """Auto-cancel reservations that exceed available qty (sold in Tally)."""
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
    conn.close()

def auto_release_reservations_smart():
    """
    For each item:
     - compute available_qty = opening_qty - SUM(OUT movements)
     - fetch ACTIVE reservations ordered by start_date ASC (older first)
     - allow reservations cumulatively until they fit within available_qty
     - cancel (status='CANCELLED') the newest reservations when total_reserved > available_qty
    """
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        # iterate items with any active reservations
        cur.execute("""
            SELECT DISTINCT r.item
            FROM stock_reservations r
            WHERE r.status='ACTIVE'
        """)
        items = [row['item'] for row in cur.fetchall() or []]

        for item in items:
            # compute available_qty from stock_items and stock_movements
            cur.execute("""
                SELECT
                  COALESCE(i.opening_qty, 0) AS opening_qty,
                  COALESCE(SUM(m.qty), 0) AS sold_qty
                FROM stock_items i
                LEFT JOIN stock_movements m ON m.item = i.name AND m.movement_type='OUT'
                WHERE i.name = %s
                GROUP BY i.opening_qty
            """, (item,))
            ir = cur.fetchone()
            if not ir:
                # no stock item record — skip or cancel all?
                continue
            opening_qty = float(ir.get('opening_qty') or 0)
            sold_qty = float(ir.get('sold_qty') or 0)
            available_qty = max(0.0, opening_qty - sold_qty)

            # fetch ACTIVE reservations ordered by start_date asc (oldest first)
            cur.execute("""
                SELECT id, qty, start_date
                FROM stock_reservations
                WHERE item = %s AND status='ACTIVE'
                ORDER BY start_date ASC, id ASC
            """, (item,))
            res_rows = cur.fetchall() or []

            # keep older reservations while cumulative <= available_qty
            cum = 0.0
            to_cancel = []  # list of reservation ids to cancel (the newest ones)
            for r in res_rows:
                q = float(r.get('qty') or 0)
                if cum + q <= available_qty + 1e-9:
                    cum += q
                else:
                    # this reservation cannot be fully kept; mark for cancellation
                    to_cancel.append(r['id'])

            if to_cancel:
                # cancel them
                cur.execute(f"""
                    UPDATE stock_reservations
                    SET status='CANCELLED', remarks = CONCAT(IFNULL(remarks,''), ' Auto-cancelled due to shortage on ', CURDATE())
                    WHERE id IN ({','.join(['%s'] * len(to_cancel))})
                """, tuple(to_cancel))
                conn.commit()

    except Exception as e:
        print("auto_release_reservations_smart error:", e)
        import traceback
        print(traceback.format_exc())
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


# =========================================================
# ✅ API ROUTES FOR FLUTTER FRONTEND
# =========================================================

# ---------------------------------------------------------
# 🟢 1️⃣ Login API (optional for Flutter)
# ---------------------------------------------------------
@app.route("/flask/login", methods=["POST"])
def api_login():
    """Flutter API login - returns a JWT token and user info"""
    data = request.get_json()
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    user = get_user_by_username(username)
    if not user or not check_password_hash(user["password_hash"], password):
        return jsonify({"ok": False, "error": "Invalid credentials"}), 401

    token = create_token(user["id"], user["username"], user["role"])
    return jsonify({
        "ok": True,
        "token": token,
        "user": user["username"],
        "role": user["role"]
    }), 200

# ---------------------------------------------------------
# 🟢 2️⃣ Sales Summary (overall)
# ---------------------------------------------------------
@app.route("/api/sales-summary")
@requires_role("admin")
def api_sales_summary():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT SUM(amount) AS total
        FROM stock_movements
        WHERE movement_type='OUT'
    """)
    row = cur.fetchone()
    total = (row[0] if row and isinstance(row, (list, tuple)) else row.get("total")) if row else 0
    conn.close()
    total = convert_decimals(total)
    return jsonify({"ok": True, "total_sales": total})

# ---------------------------------------------------------
# 🟢 2️⃣ 2nd layer Sales Summary (overall)
# ---------------------------------------------------------
@app.route("/api/sales-summary/brands")
def api_sales_brands():
    q = request.args.get("q", "").strip().lower()
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        # inner: compute a brand per movement row (company -> category -> 'Uncategorized')
        inner = """
            SELECT
              COALESCE(NULLIF(TRIM(m.company), ''), NULLIF(TRIM(i.category), ''), 'Uncategorized') AS brand,
              COALESCE(m.amount, 0) AS amt
            FROM stock_movements m
            LEFT JOIN stock_items i ON m.item = i.name
            WHERE m.movement_type = 'OUT'
        """

        if q:
            # outer: filter on computed brand (case-insensitive) then aggregate
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
        import traceback, logging
        tb = traceback.format_exc()
        logging.exception("api_sales_brands error: %s", e)
        # return limited diagnostic info to help debug while avoiding huge traces
        return jsonify({"ok": False, "error": "Internal server error", "detail": str(e), "trace": tb.splitlines()[-8:]}), 500

    finally:
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass

# ---------------------------------------------------------
# 🟢 4️⃣ Monthly Sales for Brand (3rd layer)
# ---------------------------------------------------------
@app.route("/api/sales-summary/brands/<path:brand>")
def api_sales_monthly(brand):
    # decode the incoming path parameter (safer)
    decoded_brand = unquote_plus(brand or "")
    # optional year filter from query param (e.g. ?year=2025)
    year_arg = request.args.get("year", "").strip()
    year = None
    try:
        if year_arg:
            year = int(year_arg)
    except Exception:
        year = None

    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    # Build SQL with optional year filter
    sql = """
        SELECT 
            DATE_FORMAT(m.date, '%M %Y') AS month,
            DATE_FORMAT(m.date, '%Y-%m') AS sort_key,
            SUM(m.amount) AS value
        FROM stock_movements m
        JOIN stock_items i ON m.item = i.name
        WHERE m.movement_type = 'OUT'
          AND LOWER(TRIM(i.category)) = LOWER(TRIM(%s))
    """
    params = [decoded_brand]

    if year is not None:
        sql += " AND YEAR(m.date) = %s"
        params.append(year)

    sql += " GROUP BY sort_key, month ORDER BY sort_key"

    cur.execute(sql, tuple(params))
    data = cur.fetchall()
    data = convert_decimals(data)

    # If monthly rows are empty, also provide a brand-level total (helpful client-side)
    total = None
    if not data:
        total_sql = """
            SELECT SUM(m.amount) AS total
            FROM stock_movements m
            JOIN stock_items i ON m.item = i.name
            WHERE m.movement_type = 'OUT'
              AND LOWER(TRIM(i.category)) = LOWER(TRIM(%s))
        """
        tparams = [decoded_brand]
        # optionally respect year when computing total if you prefer
        # If you want total restricted to same year, uncomment the following:
        if year is not None:
            total_sql += " AND YEAR(m.date) = %s"
            tparams.append(year)

        cur.execute(total_sql, tuple(tparams))
        tr = cur.fetchone()
        if tr and tr.get("total") is not None:
            # convert Decimal -> float if needed (your convert_decimals may already do this)
            total = float(tr["total"])

    cur.close()
    conn.close()

    resp = {"ok": True, "brand": decoded_brand, "months": data}
    if total is not None:
        resp["total"] = total

    return jsonify(resp)

# ---------------------------------------------------------
# 🟢 5️⃣ Stock Summary (1st layer)
# ---------------------------------------------------------
@app.route("/api/stock-summary")
@token_or_session_required
def api_stock_summary():
    """
    Returns stock summary by brand.
    - Admin/Sales: full list from stock_items (grouped by brand)
    - Customer: only the selected hardcoded brands (brand column used)
    """
    q = request.args.get("q", "").strip()
    user = g.user
    allowed = get_allowed_filters_for_user(user)

    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        # Use brand if available, otherwise fall back to category
        brand_expr = "TRIM(COALESCE(NULLIF(i.brand, ''), i.category))"

        # -------------------------------
        # 🟢 Admin / Sales → full list
        # -------------------------------
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
            conn.close()
            return jsonify({"ok": True, "brands": convert_decimals(data)})

        # -------------------------------
        # 🟡 Customer → show only fixed brands
        # -------------------------------
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

        conn.close()
        return jsonify({"ok": True, "brands": result})

    except Exception as e:
        import traceback
        logging.exception("api_stock_summary error: %s", e)
        try:
            conn.close()
        except Exception:
            pass
        return jsonify({"ok": False, "error": "Internal server error"}), 500


# ---------------------------------------------------------
# 🟢 6️⃣ Items under Brand (2nd layer of stock)
# ---------------------------------------------------------
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

        # -------------------------------
        # Admin / sales → full access
        # -------------------------------
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
            conn.close()

            # 🔥 Format date to dd-mm-yyyy exactly
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

        # -------------------------------
        # Customer → allowed brands only
        # -------------------------------
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
            conn.close()

            # 🔥 Apply date formatting patch
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

        # -------------------------------
        # Customer with company restriction (rare case)
        # -------------------------------
        if not allowed.get("clauses"):
            conn.close()
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
        conn.close()

        # 🔥 Apply same date formatting
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

    except Exception as e:
        import traceback
        logging.exception("api_stock_items error: %s", e)
        try:
            conn.close()
        except:
            pass
        return jsonify({"ok": False, "error": "Internal server error"}), 500


# ---------------------------------------------------------
# 🟢 7️⃣ Search Endpoint 
# ---------------------------------------------------------
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
    # convert numeric defaults
            row['total_qty'] = row.get('total_qty') or 0
            row['reserved_qty'] = row.get('reserved_qty') or 0
            row['available_qty'] = row.get('available_qty') or max(0, (row['total_qty'] - row['reserved_qty']))

    # format reserve_until to dd-mm-yyyy
            ru = row.get('reserve_until')
            if ru:
             try:
              if isinstance(ru, (str,)):
                dt = datetime.fromisoformat(ru).date()
              else:
                dt = ru  # already date
              row['reserve_until'] = dt.strftime("%d-%m-%Y")
             except Exception:
            # leave as-is if parse fails
              pass


    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "results": results})


# ---------------------------
# 🟢 API: stock reservation (from Flutter)
# ---------------------------

@app.route("/api/stock-reserve", methods=["POST"])
def api_stock_reserve():
    """Atomic reservation: checks availability, inserts, returns aggregates."""
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

        # Lock item row
        cur.execute("SELECT name, opening_qty FROM stock_items WHERE name=%s FOR UPDATE", (item,))
        item_row = cur.fetchone()
        if not item_row:
            conn.rollback()
            return jsonify({"ok": False, "error": f"Item '{item}' not found"}), 404

        total_qty = float(item_row.get("opening_qty") or 0)

        # Sum active reservations (inside same transaction)
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

        # Insert reservation
        cur.execute("""
            INSERT INTO stock_reservations (item, reserved_by, qty, start_date, end_date, status)
            VALUES (%s, %s, %s, CURDATE(), %s, 'ACTIVE')
        """, (item, reserved_by, qty, end_date))

                # Recompute aggregates for response (fetch raw dates)
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

        # Format dates to dd-mm-yyyy for aggregates
        def _fmt_date_obj(val):
            if val is None:
                return None
            if isinstance(val, (datetime, date)):
                return val.strftime("%d-%m-%Y")
            # if it's a string ISO
            try:
                return datetime.fromisoformat(str(val)).date().strftime("%d-%m-%Y")
            except Exception:
                return str(val)

        if agg:
            agg = convert_decimals(agg)
            agg["reserve_until"] = _fmt_date_obj(agg.pop("max_end_date", None))
            agg["last_reserve_start"] = _fmt_date_obj(agg.pop("max_start_date", None))

        conn.commit()

        # send email (best-effort)
        try:
            send_reservation_notification(item, qty, reserved_by, end_date)
        except Exception as e:
            print("Reservation created but email failed:", e)

        return jsonify({
            "ok": True,
            "msg": "Reserved successfully",
            "reservation": {"item": item, "qty": qty, "end_date": str(end_date), "reserved_by": reserved_by},
            "aggregates": convert_decimals(agg)
        })

    except Exception as e:
        import traceback
        print("api_stock_reserve error:", e)
        print(traceback.format_exc())
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return jsonify({"ok": False, "error": f"DB error: {e}"}), 500
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

# Add near other API routes in your Flask app
from urllib.parse import unquote_plus

@app.route("/api/reservations")
def api_reservations():
    """
    /api/reservations?items=ItemA,ItemB&only_active=true
    Returns reservations grouped by item with dates formatted as dd-mm-yyyy.
    """
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

        # Format dates to dd-mm-yyyy and convert decimals
        out = []
        for r in rows:
            r2 = convert_decimals(r)
            # convert_decimals turns date -> ISO string (YYYY-MM-DD) or leaves string; handle both
            sd = r2.get("start_date")
            ed = r2.get("end_date")
            def fmt_date(v):
                if v is None:
                    return None
                if isinstance(v, (str,)):
                    # if it's ISO 'YYYY-MM-DD', convert; if already formatted, try to parse
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
    except Exception as e:
        import traceback
        print("api_reservations error:", e)
        print(traceback.format_exc())
        try:
            conn.close()
        except Exception:
            pass
        return jsonify({"ok": False, "error": "Internal server error"}), 500


# ---------------------------
# Custom INR filter (template filter)
# ---------------------------
@app.template_filter("inr")
def inr_format(value):
    """Format number in Indian currency style like ₹12,34,567.89"""
    try:
        value = float(value)
    except (ValueError, TypeError):
        return value

    # Split integer and decimal part
    s = f"{value:.2f}"
    if "." in s:
        int_part, dec_part = s.split(".")
    else:
        int_part, dec_part = s, ""

    # Indian grouping (last 3 digits, then 2-2)
    if len(int_part) > 3:
        int_part = int_part[-3:] if len(int_part) <= 3 else (
            ",".join([int_part[:-3][::-1][i:i+2][::-1] for i in range(0, len(int_part[:-3]), 2)][::-1]) + "," + int_part[-3:]
        )

    return f"₹{int_part}.{dec_part}"

    
@app.route("/api/me")
@token_or_session_required
def api_me():
    """
    Return current user info and a client-friendly allowed_companies list.
    The allowed_companies is None for admin/sales, or a list of company display names for customers.
    """
    user = g.user
    # If user is admin/sales, return None to indicate unlimited access
    if user.get("role") in ("admin", "sales"):
        allowed_list = None
    else:
        # for customers return the hardcoded display names
        allowed_list = CUSTOMER_ALLOWED_COMPANY_DISPLAY

    return jsonify({
        "id": user.get("id"),
        "username": user.get("username"),
        "role": user.get("role"),
        "allowed_companies": allowed_list
    })


# List users (admin only)
@app.route("/api/users", methods=["GET"])
@requires_role("admin")
def api_list_users():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, username, role, created_at FROM users ORDER BY id")
    rows = cur.fetchall()
    conn.close()
    return jsonify(rows)

# Create a user (admin)
@app.route("/api/users", methods=["POST"])
@requires_role("admin")
def api_create_user():
    data = request.get_json()
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    role = data.get("role") or "sales"
    if not username or not password:
        return jsonify({"error":"username and password required"}), 400
    try:
        new_id = create_user_db(username, password, role)
        return jsonify({"id": new_id, "username": username, "role": role}), 201
    except Exception as e:
        return jsonify({"error":"Could not create user", "detail": str(e)}), 400

# Update user (admin or user updating own password)
@app.route("/api/users/<int:user_id>", methods=["PUT"])
@token_or_session_required
def api_update_user(user_id):
    # Only admin can change role or update other users; a normal user can change own password
    if (session.get("role") != "admin") and (g.user.get("role") != "admin") and (g.user.get("id") != user_id):
        return jsonify({"error":"Forbidden"}), 403

    data = request.get_json() or {}
    role = data.get("role", None)
    password = data.get("password", None)
    # If non-admin tries to change role -> reject
    if role and g.user.get("role") != "admin":
        return jsonify({"error":"Only admin can change role"}), 403
    try:
        update_user_db(user_id, role=role, password=password)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error":"Could not update user", "detail": str(e)}), 400

# Delete user (admin only)
@app.route("/api/users/<int:user_id>", methods=["DELETE"])
@requires_role("admin")
def api_delete_user(user_id):
    # protect last-admin deletion
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT role FROM users WHERE id=%s", (user_id,))
    r = cur.fetchone()
    if not r:
        conn.close()
        return jsonify({"error":"User not found"}), 404
    if r["role"] == "admin":
        cur.execute("SELECT COUNT(*) AS admins FROM users WHERE role='admin'")
        admins = cur.fetchone().get("admins", 0)
        if admins <= 1:
            conn.close()
            return jsonify({"error":"Cannot delete the last admin"}), 400
    conn.close()
    try:
        delete_user_db(user_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error":"Could not delete user", "detail": str(e)}), 400


# ---------------------------
# Run Flask
# ---------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
