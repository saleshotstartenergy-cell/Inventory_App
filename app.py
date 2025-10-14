from flask import Flask, render_template, request, redirect, url_for, session, jsonify
import mysql.connector
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, date
import smtplib
from email.mime.text import MIMEText
import os, requests, mysql.connector
from flask import Flask, jsonify
# Load env vars
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("APP_SECRET", "supersecret")


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

        for i in items:
            cur.execute("""
                INSERT INTO stock_items (name, category, base_unit, opening_qty, opening_rate)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                i.get("name"),
                i.get("category"),
                i.get("base_unit"),
                i.get("closing_qty", 0),
                i.get("closing_rate", 0)
            ))

        for m in moves:
            cur.execute("""
                INSERT INTO stock_movements (date, voucher_no, company, item, qty, rate, amount, movement_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                m.get("date"),
                m.get("voucher_no"),
                m.get("company"),
                m.get("item"),
                m.get("qty", 0),
                m.get("rate", 0),
                m.get("amount", 0),
                m.get("movement_type")
            ))

        conn.commit()
        conn.close()
        return {"ok": True, "items": len(items), "movements": len(moves)}
    except Exception as e:
        return {"ok": False, "error": f"MySQL insert failed: {e}"}

@app.route("/sync")
def manual_sync():
    from etl_pipeline import ETLPipeline
    etl = ETLPipeline(target="mysql")
    etl.extract_from_gateway()  # new method to fetch from tally_gateway
    etl.transform()
    etl.load(reset=True)
    return jsonify({"ok": True, "msg": "ETL sync completed"})

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
    cur = conn.cursor()
    cur.execute("""
        SELECT SUM(amount)
        FROM stock_movements
        WHERE movement_type='OUT'
    """)
    total = cur.fetchone()[0]
    conn.close()
    return render_template("sales_summary.html", total=total)

@app.route("/sales-summary/brands")
def sales_brands():
    q = request.args.get("q", "").strip()
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    if q:
        cur.execute("""
            SELECT 
                IFNULL(company, 'Unknown') AS company,
                SUM(qty) AS total_qty,
                SUM(amount) AS total_value
            FROM stock_movements
            WHERE movement_type='OUT' AND company LIKE %s
            GROUP BY company
            ORDER BY total_value DESC
        """, (f"%{q}%",))
    else:
        cur.execute("""
            SELECT 
                IFNULL(company, 'Unknown') AS company,
                SUM(qty) AS total_qty,
                SUM(amount) AS total_value
            FROM stock_movements
            WHERE movement_type='OUT'
            GROUP BY company
            ORDER BY total_value DESC
        """)

    brands = cur.fetchall()
    conn.close()

    return render_template("sales_brands.html", brands=brands, query=q)

@app.route("/sales-summary/brands/<company>")
def sales_monthly(company):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT 
            DATE_FORMAT(date, '%M %Y') AS month_name,
            DATE_FORMAT(date, '%Y-%m') AS month_key,
            SUM(amount) AS total_value
        FROM stock_movements
        WHERE movement_type='OUT' AND company=%s
        GROUP BY month_key, month_name
        ORDER BY month_key
    """, (company,))

    months = cur.fetchall()
    conn.close()

    return render_template("sales_monthly.html", company=company, months=months)

# ---------------------------
# 📦 Stock Summary (with smart item redirect + reserve support)
# ---------------------------
@app.route("/stock-summary", methods=["GET", "POST"])
def stock_summary():
    q = request.args.get("q", "").strip()
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    user = session.get("user", "SalesUser")

    # 🧾 Reservation submission (if needed in future on first page)
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
            # ✅ Redirect to brand (2nd layer) page with item query
            conn.close()
            return redirect(url_for("stock_items", brand=match["category"], q=match["name"]))

        # Otherwise fallback to filtered brand summary
        cur.execute("""
            SELECT category AS brand, SUM(opening_qty * opening_rate) AS total_value
            FROM stock_items
            WHERE category LIKE %s OR name LIKE %s
            GROUP BY category ORDER BY category
        """, (f"%{q}%", f"%{q}%"))
        rows = cur.fetchall()
        conn.close()
        return render_template("stock_summary.html", brands=rows, query=q)

    # 🧭 Default view: all brands summary
    cur.execute("""
        SELECT category AS brand, SUM(opening_qty * opening_rate) AS total_value
        FROM stock_items
        GROUP BY category ORDER BY category
    """)
    rows =[{"brand": r[0], "value": r[1]} for r in cur.fetchall()]
    conn.close()

    return render_template("stock_summary.html", brands=rows, query=q)


# ---------------------------
# 📦 Stock Items + Reservations
# ---------------------------
@app.route("/stock-summary/<brand>", methods=["GET", "POST"])
def stock_items(brand):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    user = session.get("user", "SalesUser")

    # 🕒 Auto-expire old reservations
    cur.execute("""
        UPDATE stock_reservations
        SET status='EXPIRED'
        WHERE status='ACTIVE' AND end_date < CURDATE()
    """)
    conn.commit()

    search_query = request.args.get("q", "").strip()

    if search_query:
        cur.execute("""
            SELECT 
                i.name AS item,
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
            SELECT 
                i.name AS item,
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

    rows = cur.fetchall()
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


# ---------------------------
# Custom INR filter
# ---------------------------
@app.template_filter("inr")
def inr_format(value):
    try:
        return f"₹{float(value):,.2f}"
    except:
        return value

# ---------------------------
# Run Flask
# ---------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)




