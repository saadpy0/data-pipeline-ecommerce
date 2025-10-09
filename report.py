import sqlite3
import argparse
from tabulate import tabulate
from utils.logger import get_logger

DB_FILE = "ecommerce.db"
logger = get_logger(__name__)

def print_product_aggregates(cur):
    print("\n=== Product Sales & Revenue ===")
    cur.execute("""
    SELECT 
        product_name,
        total_sales,
        printf('$%.2f', total_revenue) as revenue
    FROM aggregates 
    ORDER BY total_revenue DESC
    """)
    print(tabulate(cur.fetchall(), headers=['Product', 'Sales', 'Revenue'], tablefmt='grid'))

def print_event_counts(cur):
    print("\n=== Event Counts by Product & Type ===")
    cur.execute("""
    SELECT 
        product_name,
        event_type,
        count
    FROM event_counts
    ORDER BY product_name, count DESC
    """)
    print(tabulate(cur.fetchall(), headers=['Product', 'Event Type', 'Count'], tablefmt='grid'))

def print_top_users(cur, limit=5):
    print(f"\n=== Top {limit} Users by Purchases ===")
    cur.execute("""
    SELECT 
        user_id,
        total_purchases,
        printf('$%.2f', total_spent) as spent,
        last_purchase_time
    FROM top_users 
    ORDER BY total_purchases DESC, total_spent DESC
    LIMIT ?
    """, (limit,))
    print(tabulate(cur.fetchall(), 
                  headers=['User', 'Purchases', 'Total Spent', 'Last Purchase'], 
                  tablefmt='grid'))

def print_summary_stats(cur):
    print("\n=== Summary Statistics ===")
    
    # Total revenue across all products
    cur.execute("SELECT printf('$%.2f', SUM(total_revenue)) FROM aggregates")
    total_revenue = cur.fetchone()[0]
    
    # Total number of purchases
    cur.execute("SELECT SUM(total_sales) FROM aggregates")
    total_sales = cur.fetchone()[0]
    
    # Total number of active users
    cur.execute("SELECT COUNT(*) FROM top_users")
    active_users = cur.fetchone()[0]
    
    # Average revenue per purchase
    cur.execute("""
    SELECT printf('$%.2f', SUM(total_revenue) / SUM(total_sales)) 
    FROM aggregates
    """)
    avg_per_purchase = cur.fetchone()[0]
    
    stats = [
        ["Total Revenue", total_revenue],
        ["Total Purchases", total_sales],
        ["Active Users", active_users],
        ["Avg per Purchase", avg_per_purchase]
    ]
    print(tabulate(stats, headers=['Metric', 'Value'], tablefmt='grid'))

def main():
    parser = argparse.ArgumentParser(description="Generate combined report from all aggregates")
    parser.add_argument("--top", type=int, default=5, help="Number of top users to show")
    args = parser.parse_args()

    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        
        print("\n=== E-commerce Analytics Report ===")
        print_summary_stats(cur)
        print_product_aggregates(cur)
        print_event_counts(cur)
        print_top_users(cur, args.top)
        
    except Exception as e:
        logger.error(f"Failed to generate report: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
