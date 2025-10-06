import json
import random
import time
from datetime import datetime
from utils.logger import get_logger
logger = get_logger(__name__)


# Sample data for simulation
USERS = [f"user_{i}" for i in range(1, 6)]
PRODUCTS = [
    {"id": "p1", "name": "Laptop", "price": 1200},
    {"id": "p2", "name": "Headphones", "price": 150},
    {"id": "p3", "name": "Smartphone", "price": 800},
    {"id": "p4", "name": "Keyboard", "price": 100},
    {"id": "p5", "name": "Mouse", "price": 50},
]

EVENT_TYPES = ["user_signup", "product_view", "add_to_cart", "purchase"]


def generate_event():
    """Generate a random ecommerce event."""
    event_type = random.choice(EVENT_TYPES)
    user = random.choice(USERS)
    product = random.choice(PRODUCTS)

    return {
        "event_type": event_type,
        "user_id": user,
        "product_id": product["id"],
        "product_name": product["name"],
        "price": product["price"],
        "timestamp": datetime.utcnow().isoformat(),
    }


def run_event_stream(output_file="events.jsonl", delay=1):
    """Continuously write events to a file until stopped."""
    with open(output_file, "a") as f:  # append mode so we keep history
        try:
            while True:
                event = generate_event()
                f.write(json.dumps(event) + "\n")
                f.flush()  # force write immediately
                logger.info(f"Generated event: {event}")
                time.sleep(delay)
        except KeyboardInterrupt:
            logger.info("Stopped event generation.")


if __name__ == "__main__":
    run_event_stream()
