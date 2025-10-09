import sqlite3
import time
import logging
import argparse
from utils.logger import get_logger

DB_FILE = "ecommerce.db"
AGGREGATION_INTERVAL = 15  # seconds
DEFAULT_EVENT_WINDOW = 1000  # last N events to consider

logger = get_logger(__name__)

def create_top_users_table(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS top_users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT UNIQUE,
        total_purchases INTEGER,
        total_spent REAL,
        last_purchase_time TEXT
    )
    """)
    conn.commit()

def update_top_users(conn, event_window):
    cur = conn.cursor()
    
    # Get the latest events within our window and compute user metrics
    cur.execute("""
    WITH recent_events AS (
        SELECT *
        FROM events
        WHERE event_type = 'purchase'
        ORDER BY id DESC
        LIMIT ?
    )
    SELECT 
        user_id,
        COUNT(*) as total_purchases,
        SUM(price) as total_spent,
        MAX(timestamp) as last_purchase_time
    FROM recent_events
    GROUP BY user_id
    ORDER BY total_purchases DESC, total_spent DESC
    """, (event_window,))
    
    rows = cur.fetchall()
    
    # Clear existing data and insert new rankings
    cur.execute("DELETE FROM top_users")
    
    for user_id, total_purchases, total_spent, last_purchase_time in rows:
        cur.execute("""
        INSERT INTO top_users (user_id, total_purchases, total_spent, last_purchase_time)
        VALUES (?, ?, ?, ?)
        """, (user_id, total_purchases, total_spent, last_purchase_time))
        logger.info(f"User {user_id} -> Purchases: {total_purchases}, Spent: ${total_spent:.2f}")
    
    conn.commit()
    logger.info(f"Top users updated based on last {event_window} events. Found {len(rows)} active users.")

def main():
    parser = argparse.ArgumentParser(description="Track top users by purchase activity")
    parser.add_argument("--once", action="store_true", help="Run a single update and exit")
    parser.add_argument("--interval", type=int, default=AGGREGATION_INTERVAL, help="Seconds between updates in loop mode")
    parser.add_argument("--window", type=int, default=DEFAULT_EVENT_WINDOW, help="Number of recent events to consider")
    args = parser.parse_args()

    # Allow concurrent reading while consumer writes
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    create_top_users_table(conn)
    
    try:
        if args.once:
            update_top_users(conn, args.window)
            return
        
        while True:
            update_top_users(conn, args.window)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Stopping top users tracker...")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
