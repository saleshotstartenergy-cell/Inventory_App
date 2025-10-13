import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

# Use MYSQL_* env vars or fall back to DB_*
DB_HOST = os.getenv("MYSQL_HOST", os.getenv("DB_HOST", "127.0.0.1"))
DB_PORT = int(os.getenv("MYSQL_PORT", os.getenv("DB_PORT", 3306)))
DB_USER = os.getenv("MYSQL_USER", os.getenv("DB_USER", "root"))
DB_PASS = os.getenv("MYSQL_PASSWORD", os.getenv("DB_PASS", ""))
DB_NAME = os.getenv("MYSQL_DB", os.getenv("DB_NAME", "inventory_db"))

def get_connection():
    """Return a MySQL connection using environment variables."""
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        port=DB_PORT,
        cursorclass=pymysql.cursors.DictCursor
    )

def upsert_company(conn, name):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM companies WHERE name=%s", (name,))
        row = cur.fetchone()
        if row:
            return row["id"]
        cur.execute("INSERT INTO companies (name) VALUES (%s)", (name,))
        return cur.lastrowid

def upsert_product_company(conn, name):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM product_companies WHERE name=%s", (name,))
        row = cur.fetchone()
        if row:
            return row["id"]
        cur.execute("INSERT INTO product_companies (name) VALUES (%s)", (name,))
        return cur.lastrowid

def insert_stock_items(conn, company_id, stock_items):
    with conn.cursor() as cur:
        rows = []
        for item in stock_items:
            rows.append((
                company_id,
                item["name"],
                item.get("rate", 0),
                item.get("value", 0),
                item.get("quantity", 0)
            ))
        cur.executemany("""
            INSERT INTO stock_items (company_id, name, rate, value, quantity)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                rate=VALUES(rate),
                value=VALUES(value),
                quantity=VALUES(quantity),
                updated_at=CURRENT_TIMESTAMP
        """, rows)
    logging.info("Inserted/updated %d stock items", len(rows))

def insert_sales(conn, company_id, product_sales, brand_items_map):
    with conn.cursor() as cur:
        for ps in product_sales:
            brand_name = ps["product_company"]
            pc_id = upsert_product_company(conn, brand_name)

            # Upsert summary row
            cur.execute("""
                INSERT INTO sales_product_company (company_id, product_company_id, total_sales)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    total_sales=VALUES(total_sales),
                    last_updated=CURRENT_TIMESTAMP
            """, (company_id, pc_id, ps["sales_amount"]))

            cur.execute("SELECT id FROM sales_product_company WHERE company_id=%s AND product_company_id=%s", (company_id, pc_id))
            spc = cur.fetchone()
            if not spc:
                continue
            sales_pc_id = spc["id"]

            # Reset old items and insert new
            cur.execute("DELETE FROM sales_items WHERE sales_pc_id=%s", (sales_pc_id,))
            items = brand_items_map.get(brand_name, [])
            rows = [(sales_pc_id, i["item_name"], i.get("quantity", 0), i.get("sales_amount", 0)) for i in items]
            if rows:
                cur.executemany("""
                    INSERT INTO sales_items (sales_pc_id, item_name, quantity, sales_amount)
                    VALUES (%s, %s, %s, %s)
                """, rows)
    logging.info("Inserted/updated sales for %d brands", len(product_sales))

def run_loader(companies_data, stock_data_map, sales_data_map):
    conn = get_connection()
    try:
        for comp in companies_data:
            comp_name = comp["name"]
            company_id = upsert_company(conn, comp_name)

            # Stock
            stock_items = stock_data_map.get(comp_name, [])
            if stock_items:
                insert_stock_items(conn, company_id, stock_items)

            # Sales
            product_sales = sales_data_map.get(comp_name, [])
            brand_items_map = {ps["product_company"]: ps.get("items", []) for ps in product_sales}
            if product_sales:
                insert_sales(conn, company_id, product_sales, brand_items_map)

        conn.commit()
        logging.info("✅ All data committed successfully")
    except Exception as e:
        conn.rollback()
        logging.error("❌ Loader failed, rolled back: %s", e)
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES;")
        tables = cur.fetchall()
        print("Tables in DB:", tables)
    conn.close()
