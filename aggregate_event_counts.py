import sqlite3
import time
import logging
import argparse
from utils.logger import get_logger

DB_FILE = "ecommerce.db"
AGGREGATION_INTERVAL = 15  # seconds

logger = get_logger(__name__)

def create_event_counts_table(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS event_counts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT,
        product_name TEXT,
        event_type TEXT,
        count INTEGER,
        UNIQUE(product_id, event_type)
    )
    """)
    conn.commit()

def update_event_counts(conn):
    cur = conn.cursor()
    
    # Compute per-product, per-event-type counts from the raw events table
    cur.execute("""
    SELECT 
        product_id,
        product_name,
        event_type,
        COUNT(*) as event_count
    FROM events
    GROUP BY product_id, product_name, event_type
    """)
    
    rows = cur.fetchall()
    
    # Upsert the computed counts
    for product_id, product_name, event_type, count in rows:
        cur.execute("""
        INSERT INTO event_counts (product_id, product_name, event_type, count)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(product_id, event_type) DO UPDATE SET
            count=excluded.count,
            product_name=excluded.product_name
        """, (product_id, product_name, event_type, count))
        logger.info(f"Product {product_name} (ID: {product_id}) -> {event_type}: {count}")
    
    conn.commit()
    logger.info(f"Event counts updated for {len(rows)} product-event combinations.")

def main():
    parser = argparse.ArgumentParser(description="Update event counts from events table")
    parser.add_argument("--once", action="store_true", help="Run a single update and exit")
    parser.add_argument("--interval", type=int, default=AGGREGATION_INTERVAL, help="Seconds between updates in loop mode")
    args = parser.parse_args()

    # Allow concurrent reading while consumer writes
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    create_event_counts_table(conn)
    
    try:
        if args.once:
            update_event_counts(conn)
            return
        
        while True:
            update_event_counts(conn)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Stopping event count updater...")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
