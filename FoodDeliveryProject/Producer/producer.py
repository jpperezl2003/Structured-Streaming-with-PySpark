"""
Kafka Food Delivery Producer
==================
Procesamiento de Datos Masivos | ITESO

Sends random food delivery order events to a Kafka topic one record at a time,
with a random delay between each message.

This simulates a real food delivery application emitting order events over time.

Usage:
  python3 kafka_producer.py --broker localhost:9092 --topic server-logs --records 20

Dependencies:
  pip install kafka-python
"""


import argparse
import random
import json
import time
from datetime import datetime

from kafka import KafkaProducer

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────

EVENT_TYPES = [
    "order_created",
    "order_preparing",
    "order_picked_up",
    "order_delivered",
    "order_cancelled",
]

DELIVERY_ZONES = [
    "Zapopan",
    "Guadalajara",
    "Tlaquepaque",
    "Tonala",
    "Tlajomulco",
]

PAYMENT_METHODS = [
    "card",
    "cash",
    "digital_wallet"]

DELAY_MIN = 5
DELAY_MAX = 15



# ─────────────────────────────────────────────
# Event generator
# ─────────────────────────────────────────────

#We create a function that generates random food delivery event as dictionary, with the same schema as the one defined in the consumer script, so we can send it to kafka and the consumer can read it, it creates random values for each field, and the timestamp is the current time in the format "YYYY-MM-DD HH:MM:SS" , we use the datetime library to get the current time and format it, and we use the random library to generate random values for the other fields.
def generate_delivery_event() -> str:
    """Return a single random food delivery event as a dictionary."""
    timestamp = datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

    event = {
        "order_id": random.randint(1000, 9999),
        "customer_id": random.randint(1, 100),
        "restaurant_id": random.randint(1, 20),
        "delivery_driver_id": random.randint(1, 50),
        "event_type": random.choice(EVENT_TYPES),
        "order_amount": round(random.uniform(80.0, 600.0), 2),
        "delivery_zone": random.choice(DELIVERY_ZONES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "timestamp": timestamp,
    }

    return event

# Create a Kafka producer connected to the broker provided in the command line.
# Each event dictionary is serialized into JSON and encoded as UTF-8 bytes before being sent to Kafka.
def run_producer(args):
    producer = KafkaProducer(
        bootstrap_servers=args.broker,
        value_serializer=lambda event: json.dumps(event).encode("utf-8"),
    )

    print(f"Connected to broker : {args.broker}")
    print(f"Topic               : {args.topic}")
    print(f"Records to send     : {'unlimited' if args.records == 0 else args.records}")
    print(f"Delay between records: {DELAY_MIN}-{DELAY_MAX} seconds")
    print("-" * 55)

    count = 0

    try:
        while args.records == 0 or count < args.records:
            # Generate one synthetic food delivery event.
            event = generate_delivery_event()
            # Send the event to the selected Kafka topic and flush it, we flush it to make sure it is sent immediately and not buffered, which is important for real-time processing and for the consumer to receive it as soon as possible, especially since we are sending one record at a time with a delay in between. 
            producer.send(args.topic, value=event)
            producer.flush()

            count += 1
            print(f"[{count}] Sent: {event}")
            
            # Stop the loop once the requested number of records is reached.
            if args.records != 0 and count >= args.records:
                break
            
            # Wait a random amount of time before sending the next event.
            # This simulates a real streaming source where events arrive over time.
            delay = random.randint(DELAY_MIN, DELAY_MAX)
            print(f"    Next record in {delay}s ...")
            time.sleep(delay)

    except KeyboardInterrupt:
        print("\nStopped by user.")

    finally:
        producer.close()
        print(f"\nDone. Total records sent: {count}")


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

# Create an argument parser to allow the producer to be configured
# from the command line without modifying the source code.

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send random food delivery events to a Kafka topic."
    )

    parser.add_argument(
        "--broker",
        default="localhost:9092",
        help="Kafka broker address (default: localhost:9092).",
    )
    
    # Kafka topic where the food delivery events will be sent.
    # The default topic is the one created for this project.
    
    parser.add_argument(
        "--topic",
        default="food-delivery-events",
        help="Kafka topic name (default: food-delivery-events).",
    )
    
    
    # Number of records to generate and send.
    # If the value is 0, the producer keeps running until the user stops it.
    parser.add_argument(
        "--records",
        type=int,
        default=0,
        help="Number of records to send. 0 means run indefinitely until Ctrl+C.",
    )

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    run_producer(args)

if __name__ == "__main__":
    main()