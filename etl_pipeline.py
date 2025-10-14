"""
ETL pipeline for Tally Gateway -> Railway MySQL
Fetches from tally_gateway (via Cloudflare) instead of direct ODBC.
Runs periodically every 30 minutes (or can be called manually via Flask).
"""

import os
import time
import logging
import requests
from dotenv import load_dotenv
import mysql.connector as mysql

# -------------------------
# Config
# -------------------------
load_dotenv()

TALLY_GATEWAY_URL = os.getenv("TALLY_GATEWAY_URL")  # e.g. https://abc.trycloudflare.com
TALLY_API_KEY = os.getenv("TALLY_API_KEY")

MYSQL_CFG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DB", "inventory_db"),
    "port": int(os.getenv("MYSQL_PORT", 3306))
}

HEADERS = {"X-API-KEY": TALLY_API_KEY}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


class ETLPipeline:
    def __init__(self):
        self.items = []
        self.movements = []

    # -------------------------
    # EXTRACT
    # -------------------------
    def extract(self):
        """Pull from Tally Gateway (via Cloudflare tunnel)."""
        logging.info("📡 Fetching from Tally Gateway via %s", TALLY_GATEWAY_URL)
        try:
            items_resp = requests.get(f"{TALLY_GATEWAY_URL}/stock_items", headers=HEADERS, timeout=20)
            moves_resp = requests.get(f"{TALLY_GATEWAY_URL}/stock_movements", headers=HEADERS, timeout=20)
            items_resp.raise_for_status()
            moves_resp.raise_for_status()

            self.items = items_resp.json()
            self.movements = moves_resp.json()

            logging.info("✅ Extracted %d stock_items, %d stock_movements", len(self.items), len(self.movements))

        except Exception as e:
            logging.error("❌ Failed to fetch from Tally Gateway: %s", e)
            self.items, self.movements = [], []

    # -------------------------
    # LOAD → MySQL
    # -------------------------
    def load(self):
        """Truncate and reload MySQL tables with new data (batch insert)."""
        if not (self.items or self.movements):
            logging.warning("⚠️ No data to load, skipping MySQL update.")
            return

        try:
            conn = mysql.connect(**MYSQL_CFG)
            cur = conn.cursor()
            logging.info("🔗 Connected to MySQL @ %s", MYSQL_CFG["host"])

            # Clear old data
            cur.execute("TRUNCATE TABLE stock_items")
            cur.execute("TRUNCATE TABLE stock_movements")

            # -----------------------------
            # Insert Items (batch insert)
            # -----------------------------
            if self.items:
                item_data = [
                    (
                        i.get("name"),
                        i.get("category"),
                        i.get("base_unit"),
                        i.get("closing_qty", 0),
                        i.get("closing_rate", 0),
                    )
                    for i in self.items
                ]

                cur.executemany(
                    """
                    INSERT INTO stock_items (name, category, base_unit, opening_qty, opening_rate)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    item_data,
                )
                logging.info("✅ Inserted %d stock_items", len(item_data))

            # -----------------------------
            # Insert Movements (batch insert)
            # -----------------------------
            if self.movements:
                move_data = [
                    (
                        m.get("date"),
                        m.get("voucher_no"),
                        m.get("company"),
                        m.get("item"),
                        m.get("qty", 0),
                        m.get("rate", 0),
                        m.get("amount", 0),
                        m.get("movement_type"),
                    )
                    for m in self.movements
                ]

                cur.executemany(
                    """
                    INSERT INTO stock_movements
                    (date, voucher_no, company, item, qty, rate, amount, movement_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    move_data,
                )
                logging.info("✅ Inserted %d stock_movements", len(move_data))

            conn.commit()
            logging.info("💾 Data committed to MySQL successfully.")

        except Exception as e:
            logging.error("❌ MySQL load failed: %s", e)

        finally:
            try:
                conn.close()
                logging.info("🔒 MySQL connection closed.")
            except Exception:
                pass

    # -------------------------
    # MAIN
    # -------------------------
    def run_once(self):
        """Run the full ETL cycle once."""
        self.extract()
        if self.items or self.movements:
            self.load()
        else:
            logging.warning("⚠️ No extracted data to load — skipping MySQL update.")


# -------------------------
# Manual execution mode
# -------------------------
if __name__ == "__main__":
    pipeline = ETLPipeline()

    while True:
        logging.info("🚀 Starting ETL cycle...")
        pipeline.run_once()
        logging.info("⏳ Waiting 30 minutes before next sync...\n")
        time.sleep(1800)
