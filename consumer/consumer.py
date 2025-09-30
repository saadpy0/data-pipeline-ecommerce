#!/usr/bin/env python3
# consumer/consumer_db.py
import sqlite3
import json
import time
import os
import signal
import logging
import sys

DB_PATH = os.getenv("DB_PATH", "ecommerce.db")
EVENT_FILE = os.getenv("EVENT_FILE", "events.jsonl")
SLEEP_SECS = 0.5

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("consumer_db")

stop_requested = False
conn = None

def create_table_if_not_exists():
    global conn
    conn = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type TEXT NOT NULL,
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
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO events (event_type, user_id, product_id, product_name, price, timestamp, raw)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        event.get("event_type"),
        event.get("user_id"),
        event.get("product_id"),
        event.get("product_name"),
        float(event["price"]) if event.get("price") is not None else None,
        event.get("timestamp"),
        json.dumps(event)
    ))
    conn.commit()

def handle_signal(signum, frame):
    global stop_requested
    logger.info("Received stop signal (%s). Shutting down...", signum)
    stop_requested = True

def tail_and_consume():
    # Wait for file if it doesn't exist yet
    if not os.path.exists(EVENT_FILE):
        logger.info("%s not found — waiting for producer to create it...", EVENT_FILE)
        while not os.path.exists(EVENT_FILE) and not stop_requested:
            time.sleep(0.2)

    with open(EVENT_FILE, "r", encoding="utf-8") as f:
        f.seek(0, 2)  # move to EOF
        logger.info("Tailing %s ...", EVENT_FILE)
        while not stop_requested:
            line = f.readline()
            if not line:
                time.sleep(SLEEP_SECS)
                continue
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Skipped invalid JSON line: %s", line[:200])
                continue

            try:
                save_event(event)
                logger.info("Saved event: %s | user=%s product=%s",
                            event.get("event_type"), event.get("user_id"), event.get("product_name"))
            except Exception as e:
                logger.exception("Failed to save event: %s", e)

    logger.info("Consumer loop exiting.")

def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    create_table_if_not_exists()
    try:
        tail_and_consume()
    finally:
        if conn:
            conn.close()
            logger.info("DB connection closed.")

if __name__ == "__main__":
    main()
