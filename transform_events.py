import sqlite3
import time
import logging
import argparse

DB_FILE = "ecommerce.db"
AGGREGATION_INTERVAL = 15  # seconds

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def create_aggregates_table(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS aggregates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT UNIQUE,
        product_name TEXT,
        total_sales INTEGER,
        total_revenue REAL
    )
    """)
    conn.commit()

def update_aggregates(conn):
    cur = conn.cursor()
    
    # Compute per-product aggregates from the raw events table
    cur.execute("""
    SELECT product_id, product_name, COUNT(*) as total_sales, SUM(price) as total_revenue
    FROM events
    GROUP BY product_id, product_name
    """)
    
    rows = cur.fetchall()
    
    # Upsert the computed aggregates
    for product_id, product_name, total_sales, total_revenue in rows:
        cur.execute("""
        INSERT INTO aggregates (product_id, product_name, total_sales, total_revenue)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(product_id) DO UPDATE SET
            total_sales=excluded.total_sales,
            total_revenue=excluded.total_revenue
        """, (product_id, product_name, total_sales, total_revenue))
    
    conn.commit()
    logger.info(f"Aggregates updated for {len(rows)} products.")

def main():
    parser = argparse.ArgumentParser(description="Update aggregates from events table")
    parser.add_argument("--once", action="store_true", help="Run a single update and exit")
    parser.add_argument("--interval", type=int, default=AGGREGATION_INTERVAL, help="Seconds between updates in loop mode")
    args = parser.parse_args()

    # Allow concurrent reading while consumer writes
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    create_aggregates_table(conn)
    
    try:
        if args.once:
            update_aggregates(conn)
            return
        
        while True:
            update_aggregates(conn)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Stopping aggregate updater...")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
