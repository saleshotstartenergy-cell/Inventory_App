from flask import Flask, render_template, request, redirect, url_for, session, jsonify
import mysql.connector
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, date
import smtplib
from email.mime.text import MIMEText
import requests
from flask_cors import CORS

# Load env vars
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
# Auth (hardcoded for now)
# ---------------------------
USERS = {
    "admin": {"password": "admin123", "role": "admin"},
    "sales": {"password": "sales123", "role": "sales"}
}
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
        if user in USERS and USERS[user]["password"] == pwd:
            session["user"] = user
            session["role"] = USERS[user]["role"]
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

@app.route("/search")
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

        # use executemany where possible for speed (but keep individual loop to stay faithful)
        # keep same columns and behavior
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
        # Support both new and old ETL APIs
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
# 📊 Sales Summary
# ---------------------------
@app.route("/sales-summary")
def sales_summary():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT SUM(amount) AS total
        FROM stock_movements
        WHERE movement_type='OUT'
    """)
    row = cur.fetchone()
    total = row.get("total") if row else 0
    conn.close()
    return render_template("sales_summary.html", total=total)

# -----------------------------------------------
# 📊 SALES SUMMARY - BY BRAND (Total Sales Value Only)
# -----------------------------------------------

@app.route("/sales-summary/brands")
def sales_brands():
    """Show total sales value grouped by product brand (category)."""
    q = request.args.get("q", "").strip()
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    if q:
        cur.execute("""
            SELECT 
                TRIM(IFNULL(i.category, 'Uncategorized')) AS name,
                SUM(m.amount) AS value
            FROM stock_movements m
            JOIN stock_items i ON m.item = i.name
            WHERE m.movement_type='OUT' AND i.category LIKE %s
            GROUP BY i.category
            ORDER BY value DESC
        """, (f"%{q}%",))
    else:
        cur.execute("""
            SELECT 
                TRIM(IFNULL(i.category, 'Uncategorized')) AS name,
                SUM(m.amount) AS value
            FROM stock_movements m
            JOIN stock_items i ON m.item = i.name
            WHERE m.movement_type='OUT'
            GROUP BY i.category
            ORDER BY value DESC
        """)

    brands = cur.fetchall() or []
    conn.close()

    return render_template("sales_brands.html", brands=brands, query=q)


# -----------------------------------------------
# 📊 SALES SUMMARY - MONTHLY SALES BY BRAND
# -----------------------------------------------

@app.route("/sales-summary/brands/<brand>")
def sales_monthly(brand):
    """Show monthly total sales value for a specific brand (category)."""
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT 
            DATE_FORMAT(m.date, '%M %Y') AS month,
            DATE_FORMAT(m.date, '%Y-%m') AS sort_key,
            SUM(m.amount) AS value
        FROM stock_movements m
        JOIN stock_items i ON m.item = i.name
        WHERE m.movement_type='OUT' AND TRIM(i.category)=TRIM(%s)
        GROUP BY sort_key, month
        ORDER BY sort_key
    """, (brand,))

    months = cur.fetchall() or []
    conn.close()

    return render_template("sales_monthly.html", company=brand, months=months)


# ---------------------------
# 📦 Stock Summary (with smart item redirect + reserve support)
# ---------------------------
@app.route("/stock-summary", methods=["GET", "POST"])
def stock_summary():
    q = request.args.get("q", "").strip()
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    user = session.get("user", "SalesUser")

    # 🧾 Reservation submission (kept)
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

    # 🕒 Auto-expire old reservations
    cur.execute("""
        UPDATE stock_reservations
        SET status='EXPIRED'
        WHERE status='ACTIVE' AND end_date < CURDATE()
    """)
    conn.commit()

    # 🔍 If user searched for something
    if q:
        # Check if it matches a specific item
        cur.execute("""
            SELECT name, category
            FROM stock_items
            WHERE name LIKE %s
            LIMIT 1
        """, (f"%{q}%",))
        match = cur.fetchone()

        if match:
            # Redirect to brand (2nd layer) page with item query
            conn.close()
            # match['category'] may be None; fallback to empty string
            return redirect(url_for("stock_items", brand=(match.get("category") or ""), q=match.get("name")))

        # Otherwise fallback to filtered brand summary
        cur.execute("""
            SELECT IFNULL(category,'Uncategorized') AS brand, SUM(opening_qty * opening_rate) AS value
            FROM stock_items
            WHERE category LIKE %s OR name LIKE %s
            GROUP BY category ORDER BY category
        """, (f"%{q}%", f"%{q}%"))
        rows = cur.fetchall() or []
        conn.close()
        return render_template("stock_summary.html", brands=rows, query=q)

    # Default view: all brands summary
    cur.execute("""
        SELECT IFNULL(category,'Uncategorized') AS brand, SUM(opening_qty * opening_rate) AS value
        FROM stock_items
        GROUP BY category ORDER BY category
    """)
    rows = cur.fetchall() or []
    conn.close()

    # Template expects brands as list of dicts with keys brand and value
    return render_template("stock_summary.html", brands=rows, query=q)


# ---------------------------
# 📦 Stock Items + Reservations
# ---------------------------
@app.route("/stock-summary/<brand>", methods=["GET", "POST"])
def stock_items(brand):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    user = session.get("user", "SalesUser")

    # 🧾 Handle reservation form submission
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

    # 🕒 Auto-expire old reservations
    cur.execute("""
        UPDATE stock_reservations
        SET status='EXPIRED'
        WHERE status='ACTIVE' AND end_date < CURDATE()
    """)
    conn.commit()

    # 🔄 Auto-release reservations if stock sold in Tally
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

    # 🔍 Handle item-level search
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

    # Template expects items with keys:
    # item, total_qty, reserved_qty, available_qty, reserved_by, end_date
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
    msg["From"] = os.getenv("EMAIL_USER", "yourapp@example.com")
    msg["To"] = os.getenv("EMAIL_NOTIFY", "team@example.com")

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as s:
            s.starttls()
            s.login(os.getenv("EMAIL_USER"), os.getenv("EMAIL_PASS"))
            s.send_message(msg)
    except Exception as e:
        print("Email sending failed:", e)


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

# =========================================================
# ✅ API ROUTES FOR FLUTTER FRONTEND
# =========================================================
from flask_cors import CORS
CORS(app)

# ---------------------------------------------------------
# 🟢 1️⃣ Login API (optional for Flutter)
# ---------------------------------------------------------
@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json()
    user = data.get("username")
    pwd = data.get("password")
    if user in USERS and USERS[user]["password"] == pwd:
        return jsonify({"ok": True, "user": user, "role": USERS[user]["role"]})
    return jsonify({"ok": False, "error": "Invalid credentials"}), 401


# ---------------------------------------------------------
# 🟢 2️⃣ Sales Summary (overall)
# ---------------------------------------------------------
@app.route("/api/sales-summary")
def api_sales_summary():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT SUM(amount) AS total
        FROM stock_movements
        WHERE movement_type='OUT'
    """)
    total = cur.fetchone()[0] or 0
    conn.close()
    return jsonify({"ok": True, "total_sales": total})


# ---------------------------------------------------------
# 🟢 3️⃣ Sales by Brand (2nd layer)
# ---------------------------------------------------------
@app.route("/api/sales-summary/brands")
def api_sales_brands():
    q = request.args.get("q", "").strip()
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    if q:
        cur.execute("""
            SELECT 
                TRIM(IFNULL(i.category, 'Uncategorized')) AS brand,
                SUM(m.amount) AS value
            FROM stock_movements m
            JOIN stock_items i ON m.item = i.name
            WHERE m.movement_type='OUT' AND i.category LIKE %s
            GROUP BY i.category
            ORDER BY value DESC
        """, (f"%{q}%",))
    else:
        cur.execute("""
            SELECT 
                TRIM(IFNULL(i.category, 'Uncategorized')) AS brand,
                SUM(m.amount) AS value
            FROM stock_movements m
            JOIN stock_items i ON m.item = i.name
            WHERE m.movement_type='OUT'
            GROUP BY i.category
            ORDER BY value DESC
        """)

    data = cur.fetchall()
    conn.close()
    return jsonify({"ok": True, "brands": data})


# ---------------------------------------------------------
# 🟢 4️⃣ Monthly Sales for Brand (3rd layer)
# ---------------------------------------------------------
@app.route("/api/sales-summary/brands/<brand>")
def api_sales_monthly(brand):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT 
            DATE_FORMAT(m.date, '%M %Y') AS month,
            DATE_FORMAT(m.date, '%Y-%m') AS sort_key,
            SUM(m.amount) AS value
        FROM stock_movements m
        JOIN stock_items i ON m.item = i.name
        WHERE m.movement_type='OUT' AND TRIM(i.category)=TRIM(%s)
        GROUP BY sort_key, month
        ORDER BY sort_key
    """, (brand,))
    data = cur.fetchall()
    conn.close()
    return jsonify({"ok": True, "brand": brand, "months": data})


# ---------------------------------------------------------
# 🟢 5️⃣ Stock Summary (1st layer)
# ---------------------------------------------------------
@app.route("/api/stock-summary")
def api_stock_summary():
    q = request.args.get("q", "").strip()
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    if q:
        cur.execute("""
            SELECT category AS brand,
                   SUM(opening_qty * opening_rate) AS value
            FROM stock_items
            WHERE category LIKE %s OR name LIKE %s
            GROUP BY category
            ORDER BY category
        """, (f"%{q}%", f"%{q}%"))
    else:
        cur.execute("""
            SELECT category AS brand,
                   SUM(opening_qty * opening_rate) AS value
            FROM stock_items
            GROUP BY category
            ORDER BY category
        """)

    data = cur.fetchall()
    conn.close()
    return jsonify({"ok": True, "brands": data})


# ---------------------------------------------------------
# 🟢 6️⃣ Items under Brand (2nd layer of stock)
# ---------------------------------------------------------
@app.route("/api/stock-summary/<brand>")
def api_stock_items(brand):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT i.name AS item,
               i.opening_qty AS total_qty,
               IFNULL(SUM(r.qty), 0) AS reserved_qty,
               (i.opening_qty - IFNULL(SUM(r.qty), 0)) AS available_qty,
               MAX(r.reserved_by) AS reserved_by,
               DATE_FORMAT(MAX(r.end_date), '%d-%m-%Y') AS end_date
        FROM stock_items i
        LEFT JOIN stock_reservations r
          ON i.name = r.item AND r.status='ACTIVE'
        WHERE i.category=%s
        GROUP BY i.name, i.opening_qty
        ORDER BY i.name
    """, (brand,))
    data = cur.fetchall()
    conn.close()
    return jsonify({"ok": True, "brand": brand, "items": data})
# ---------------------------------------------------------
# 🟢 7️⃣ Search Endpoint
@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"ok": True, "results": []})
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT 
            name, category, base_unit,
            opening_qty, opening_rate,
            (opening_qty * opening_rate) AS value
        FROM stock_items
        WHERE name LIKE %s OR category LIKE %s
        ORDER BY category, name
        LIMIT 50
    """, (f"%{q}%", f"%{q}%"))
    results = cur.fetchall()
    conn.close()
    return jsonify({"ok": True, "results": results})

# ---------------------------
# Custom INR filter
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

# ---------------------------
# Run Flask
# ---------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
