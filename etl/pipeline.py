"""
ETL pipeline for Tally -> MySQL (denormalized: company/item as strings).
"""

import os
import logging
import datetime
import sqlite3
from pathlib import Path
from dotenv import load_dotenv

try:
    import mysql.connector as mysql
except ImportError:
    mysql = None

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQLITE_DB = PROJECT_ROOT / "inventory.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


class ETLPipeline:
    def __init__(self, target="mysql"):
        self.target = target
        self.companies = []
        self.items = []
        self.movements = []

        self.mysql_cfg = {
            "host": os.getenv("MYSQL_HOST", "localhost"),
            "user": os.getenv("MYSQL_USER", "root"),
            "password": os.getenv("MYSQL_PASSWORD", ""),
            "database": os.getenv("MYSQL_DB", "inventory_db"),
            "port": int(os.getenv("MYSQL_PORT", 3306)),
        }

    # -------------------------
    # EXTRACT
    # -------------------------
    def extract(self):
        logging.info("🚀 Starting ETL process")
        self._extract_live()
        logging.info(
            "Extract complete: companies=%d items=%d movements=%d",
            len(self.companies), len(self.items), len(self.movements)
        )

    def _safe_date(self, val):
        if isinstance(val, datetime.date):
            return val.isoformat()
        if not val:
            return datetime.date.today().isoformat()
        return str(val)[:10]

    def _extract_live(self):
        import pyodbc
        DSN = os.getenv("TALLY_DSN", "TallyODBC64_9000")
        logging.info("Starting live extract from Tally ODBC (DSN=%s)", DSN)
        conn = pyodbc.connect(f"DSN={DSN}")
        cur = conn.cursor()

        # --- Stock Items ---
        try:
            rows = cur.execute("""
                SELECT $Name, $Parent, $BaseUnits, $_ClosingBalance, $_ClosingRate
                FROM StockItem
            """).fetchall()
            self.items = []
            for r in rows:
                self.items.append({
                    "name": str(r[0]).strip() if r[0] else "Unknown Item",
                    "category": str(r[1]).strip() if r[1] else None,
                    "base_unit": str(r[2]).strip() if r[2] else None,
                    "opening_qty": float(r[3]) if r[3] else 0.0,
                    "opening_rate": float(r[4]) if r[4] else 0.0,
                })
            logging.info("Fetched %d stock items", len(self.items))
        except Exception as e:
            logging.error("StockItem query failed: %s", e)

        # --- Ledgers ---
        try:
            rows = cur.execute("SELECT $Name FROM Ledger").fetchall()
            self.companies = [{"name": r[0].strip()} for r in rows if r[0]]
            logging.info("Fetched %d ledgers", len(self.companies))
        except Exception as e:
            logging.error("Ledger query failed: %s", e)

        # --- Voucher Movements ---
        try:
            rows = cur.execute("""
                SELECT $_LastSaleDate, $_LastSaleParty,$StockItemName, $_LastSalePrice,
               $_OutwardQuantity, $_OutwardValue
                FROM VchStockItem
            """).fetchall()

            self.movements = []
            for r in rows:
                # Skip header rows like ('Date','Voucher Number',...)
                #if str(r[0]).upper() == "DATE" or str(r[4]).upper() == "ACTUAL QTY":
                 #   continue

                try:
                    date_val = self._safe_date(r[0])
                    #ledger = str(r[1]).strip() if r[1] else "Unknown"
                    party=str(r[1]).strip() if r[1] else "Unknown"
                    item_name=str(r[2]).strip() if r[2] else "Unknown Item"
                    rate = float(r[3]) if r[3] not in (None, "") else 0.0
                    qty = float(r[4]) if r[4] not in (None, "", "Actual Qty") else 0.0
                    amount = float(r[5]) if r[5] not in (None, "") else qty * rate

                    product = next((it for it in self.items if it["name"] == item_name), None)
                    brand = product["category"] if product and product.get("category") else "Unknown"

                    self.movements.append({
                        "date": date_val,
                        "voucher_no": None,
                        "company": brand,
                        "item": "item_name",
                        "qty": qty,
                        "rate": rate,
                        "amount": amount,
                        "movement_type": "OUT" if amount > 0 else "IN"
                    })
                except Exception as e:
                    logging.error("Bad VchStockItem row: %s | error: %s", r, e)

            logging.info("Fetched %d voucher stock movements", len(self.movements))
        except Exception as e:
            logging.error("VchStockItem query failed: %s", e)

        conn.close()

    # -------------------------
    # TRANSFORM
    # -------------------------
    def transform(self):
        logging.info("Transform step (deduplicating)")
        self.companies = list({c["name"]: c for c in self.companies}.values())
        self.items = list({i["name"]: i for i in self.items if i.get("name")}.values())
        logging.info("Transform complete: %d companies, %d items, %d movements",
                     len(self.companies), len(self.items), len(self.movements))

    # -------------------------
    # LOAD
    # -------------------------
    def load(self, reset=False):
        if self.target == "mysql":
            self._load_mysql(reset=reset)
        elif self.target == "sqlite":
            self._load_sqlite(reset=reset)
        else:
            raise ValueError(f"Unknown target {self.target}")

    def _load_sqlite(self, reset=False):
        db_path = Path(SQLITE_DB)
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()

        if reset:
            cur.execute("DROP TABLE IF EXISTS stock_movements")
            cur.execute("DROP TABLE IF EXISTS stock_items")
            conn.commit()

        cur.executescript(open(Path(__file__).parent / "schema.sql").read())

        for m in self.movements:
            cur.execute("""INSERT INTO stock_movements
                (date,voucher_no,company,item,qty,rate,amount,movement_type)
                VALUES (?,?,?,?,?,?,?,?)""",
                (m["date"], m["voucher_no"], m["company"], m["item"],
                 m["qty"], m["rate"], m["amount"], m["movement_type"]))

        for i in self.items:
            cur.execute("""INSERT INTO stock_items
                (name,category,base_unit,opening_qty,opening_rate)
                VALUES (?,?,?,?,?)""",
                (i["name"], i.get("category"), i.get("base_unit"),
                 i.get("opening_qty", 0), i.get("opening_rate", 0)))

        conn.commit()
        conn.close()
        logging.info("SQLite load complete")

    def _load_mysql(self, reset=False):
        if mysql is None:
            raise RuntimeError("mysql-connector-python not installed")

        conn = mysql.connect(**self.mysql_cfg)
        cur = conn.cursor()

        if reset:
            logging.warning("Dropping existing tables before reload (reset=True)")
            cur.execute("DROP TABLE IF EXISTS stock_movements")
            cur.execute("DROP TABLE IF EXISTS stock_items")
            conn.commit()

        schema_path = Path(__file__).parent / "schema.sql"
        with open(schema_path, "r") as f:
            schema_sql = f.read()

        for stmt in schema_sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)

        insert_sql = """INSERT INTO stock_movements
            (date,voucher_no,company,item,qty,rate,amount,movement_type)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
        rows = [(m["date"], m["voucher_no"], m["company"], m["item"],
                 m["qty"], m["rate"], m["amount"], m["movement_type"])
                for m in self.movements]
        if rows:
            cur.executemany(insert_sql, rows)
            logging.info("Inserted %d stock_movements", cur.rowcount)

        insert_items = """INSERT INTO stock_items
            (name,category,base_unit,opening_qty,opening_rate)
            VALUES (%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                category=VALUES(category),
                base_unit=VALUES(base_unit),
                opening_qty=VALUES(opening_qty),
                opening_rate=VALUES(opening_rate)"""
        rows_items = [(i["name"], i.get("category"), i.get("base_unit"),
                       i.get("opening_qty", 0), i.get("opening_rate", 0))
                      for i in self.items]
        if rows_items:
            cur.executemany(insert_items, rows_items)
            logging.info("Inserted/updated %d stock_items", cur.rowcount)

        conn.commit()
        conn.close()
        logging.info("MySQL load complete")
