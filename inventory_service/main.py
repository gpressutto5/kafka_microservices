import json
from confluent_kafka import Consumer

# Kafka configuration
TOPIC = "orders"  # Topic to consume orders from
BROKER = "kafka:9092"  # Kafka broker address
GROUP_ID = "inventory-service"  # Consumer group ID for load balancing

# Initialize Kafka consumer with configuration
c = Consumer({
    'bootstrap.servers': BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'  # Start reading from the beginning of the topic
})

# Subscribe to the orders topic
c.subscribe([TOPIC])

print("Inventory Service listening for orders...")

try:
    # Main consumer loop
    while True:
        # Poll for new messages with a timeout of 1 second
        msg = c.poll(1.0)
        
        # Skip if no message received
        if msg is None:
            continue
            
        # Handle any consumer errors
        if msg.error():
            print("Consumer error:", msg.error())
            continue

        # Process the received order
        order = json.loads(msg.value().decode('utf-8'))
        print(f"Updating inventory: {order['quantity']}x {order['product']} (Order ID: {order['order_id']})")

# Handle graceful shutdown
except KeyboardInterrupt:
    pass
finally:
    # Clean up consumer resources
    c.close()
