import sqlite3
import json
import time
from pathlib import Path
from utils.logger import get_logger
logger = get_logger(__name__)


DB_FILE = "ecommerce.db"
EVENT_FILE = "events.jsonl"

# Ensure DB connection is thread-safe
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cur = conn.cursor()

# Make sure the events table exists
cur.execute("""
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT,
    user_id TEXT,
    product_id TEXT,
    product_name TEXT,
    price REAL,
    timestamp TEXT,
    raw TEXT
)
""")
conn.commit()

def save_event(event):
    try:
        cur.execute("""
            INSERT INTO events (event_type, user_id, product_id, product_name, price, timestamp, raw)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            event["event_type"],
            event["user_id"],
            event["product_id"],
            event["product_name"],
            event["price"],
            event["timestamp"],
            json.dumps(event)
        ))
        conn.commit()  # Important: commit after every insert
        logger.debug(f"Saved event to database: {event['event_type']}")
    except Exception as e:
        logger.exception("Failed to save event")

def tail_and_consume():
    logger.info("Tailing events.jsonl ...")
    path = Path(EVENT_FILE)
    path.touch(exist_ok=True)
    with path.open("r") as f:
        f.seek(0, 2)  # Go to end of file
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.5)
                continue
            try:
                event = json.loads(line.strip())
                save_event(event)
                logger.info(f"Processed event: {event['event_type']} for user {event['user_id']}")
            except Exception as e:
                logger.exception("Failed to process line")

if __name__ == "__main__":
    try:
        tail_and_consume()
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        conn.close()
