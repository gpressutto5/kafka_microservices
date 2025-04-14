import json
import uuid
import pandas as pd
import os
from confluent_kafka import Consumer
from datetime import datetime

# Kafka configuration
TOPIC = "orders"  # Topic to consume orders from
BROKER = "kafka:9092"  # Kafka broker address
GROUP_ID = "data-sink"  # Consumer group ID for load balancing

# Data storage configuration
DATA_DIR = os.path.join(os.path.dirname(__file__), "data_lake")  # Directory to store parquet files
os.makedirs(DATA_DIR, exist_ok=True)  # Create directory if it doesn't exist

# Initialize Kafka consumer with configuration
c = Consumer({
    'bootstrap.servers': BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'  # Start reading from the beginning of the topic
})

# Subscribe to the orders topic
c.subscribe([TOPIC])

# Buffer to accumulate orders before writing to parquet
buffer = []

def write_to_parquet(data):
    """
    Write accumulated order data to a parquet file with a unique timestamp-based filename
    
    Args:
        data: List of order dictionaries to write to parquet
        
    The function:
    1. Generates a unique filename using timestamp and UUID
    2. Creates a pandas DataFrame from the order data
    3. Writes the DataFrame to a parquet file using pyarrow engine
    4. Prints confirmation message with number of orders written
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    filename = f"orders_{timestamp}_{uuid.uuid4().hex[:6]}.parquet"
    path = os.path.join(DATA_DIR, filename)
    df = pd.DataFrame(data)
    df.to_parquet(path, engine='pyarrow', index=False)
    print(f"Written {len(data)} orders to {path}")

print("Data Sink Service listening for orders...")

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
        buffer.append(order)

        # Write to parquet when buffer reaches 5 orders
        if len(buffer) >= 5:
            write_to_parquet(buffer)
            buffer.clear()

# Handle graceful shutdown
except KeyboardInterrupt:
    pass
finally:
    # Clean up consumer resources
    c.close()
