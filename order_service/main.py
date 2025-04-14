import json
import time
import random
from confluent_kafka import Producer

# Kafka configuration
TOPIC = "orders"  # Topic name for order messages
BROKER = "kafka:9092"  # Kafka broker address

# Initialize Kafka producer
p = Producer({'bootstrap.servers': BROKER})

# List of available products for random selection
products = ["laptop", "phone", "headphones", "keyboard", "mouse"]

def delivery_report(err, msg):
    """
    Callback function to handle message delivery status
    Args:
        err: Error object if delivery failed
        msg: Message object if delivery succeeded
    """
    if err:
        print("Delivery failed:", err)
    else:
        print(f"Produced: {msg.value().decode()}")

def produce_order():
    """
    Generates a random order and sends it to Kafka
    Creates an order with random product, quantity, and order ID
    """
    # Create random order data
    order = {
        "order_id": random.randint(1000, 9999),
        "product": random.choice(products),
        "quantity": random.randint(1, 5),
    }
    # Send order to Kafka topic
    p.produce(TOPIC, json.dumps(order).encode('utf-8'), callback=delivery_report)
    p.flush()  # Ensure message is sent

if __name__ == "__main__":
    print("Starting Order Producer...")
    # Continuously produce orders every 3 seconds
    while True:
        produce_order()
        time.sleep(3)
