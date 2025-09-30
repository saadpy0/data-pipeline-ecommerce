import sqlite3
import time

DB_FILE = "ecommerce.db"
AGGREGATION_INTERVAL = 15  # seconds

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

def compute_and_save_aggregates(conn):
    cur = conn.cursor()
    
    cur.execute("""
    SELECT product_id, product_name, COUNT(*) as total_sales, SUM(price) as total_revenue
    FROM events
    GROUP BY product_id, product_name
    """)
    
    rows = cur.fetchall()
    
    for product_id, product_name, total_sales, total_revenue in rows:
        cur.execute("""
        INSERT INTO aggregates (product_id, product_name, total_sales, total_revenue)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(product_id) DO UPDATE SET
            total_sales=excluded.total_sales,
            total_revenue=excluded.total_revenue
        """, (product_id, product_name, total_sales, total_revenue))
    
    conn.commit()
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Aggregates updated for {len(rows)} products.")

def main():
    # Allow concurrent reading while consumer writes
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    create_aggregates_table(conn)
    
    try:
        while True:
            compute_and_save_aggregates(conn)
            time.sleep(AGGREGATION_INTERVAL)
    except KeyboardInterrupt:
        print("Stopping aggregate updater...")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
