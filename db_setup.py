import sqlite3

def create_table():
    conn = sqlite3.connect("ecommerce.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            product_id INTEGER,
            timestamp TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_table()
    print("✅ Database and events table created.")
