import json
import time

def consume_events(input_file="events.jsonl", delay=1):
    """Continuously read and process events from the file."""
    print("📥 Starting event consumer... (Press Ctrl+C to stop)")
    
    try:
        with open(input_file, "r") as f:
            # Move to end of file (so it only reads new events being appended)
            f.seek(0, 2)

            while True:
                line = f.readline()
                if not line:
                    time.sleep(delay)  # wait for new events
                    continue

                try:
                    event = json.loads(line.strip())
                except json.JSONDecodeError:
                    print("⚠️ Skipped invalid line:", line)
                    continue

                process_event(event)

    except KeyboardInterrupt:
        print("\n🛑 Stopped consumer.")


def process_event(event):
    """Custom logic to handle an event."""
    if event["event_type"] == "purchase":
        print(f"💰 PURCHASE: {event['user_id']} bought {event['product_name']} for ${event['price']}")
    elif event["event_type"] == "add_to_cart":
        print(f"🛒 {event['user_id']} added {event['product_name']} to cart")
    elif event["event_type"] == "product_view":
        print(f"👀 {event['user_id']} viewed {event['product_name']}")
    elif event["event_type"] == "user_signup":
        print(f"🙋 New signup: {event['user_id']}")
    else:
        print("🔹 Unknown event:", event)


if __name__ == "__main__":
    consume_events()
