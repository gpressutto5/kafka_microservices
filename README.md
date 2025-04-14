# Kafka Microservices with Data Lake Sink

This project is a simple Kafka-based microservices architecture built in Python. It simulates an e-commerce system with multiple services and stores events in a simulated data lake using Parquet files.

## 🧱 Architecture

- **Order Service**: Produces random product orders.
- **Inventory Service**: Consumes orders and simulates inventory adjustment.
- **Notification Service**: Sends simulated notifications for each order.
- **Data Sink**: Consumes all orders and stores them as Parquet files in a local folder (`data_lake/`).
- **Kafka UI**: Web interface for exploring topics and consumer groups.
- **Jupyter Notebook**: Explore or clean the Parquet data interactively.

## 🐳 Getting Started

Make sure you have Docker and Docker Compose installed.

### 1. Clone the repo

```bash
git clone <this-repo>
cd kafka_microservices
```

### 2. Run all services

```bash
docker-compose up --build
```

### 3. Access tools

- Kafka UI: [http://localhost:8080](http://localhost:8080)
- Jupyter Notebook: [http://localhost:8888/?token=letmein](http://localhost:8888/?token=letmein)

The Jupyter server opens `parquet_management.ipynb` by default, which allows inspecting and clearing stored Parquet files.

### 4. Watch Orders Flow

The Order Service automatically generates a new random order every 3 seconds with a random ID, product, and quantity.


## 📦 Project Structure

```
kafka_microservices/
├── docker-compose.yml
├── data_sink
│   ├── data_lake
│   │   ├── orders_*.parquet
│   ├── main.py
│   ├── parquet_management.ipynb
│   └── requirements.txt
├── inventory_service
│   ├── main.py
│   └── requirements.txt
├── notification_service
│   ├── main.py
│   └── requirements.txt
└── order_service
    ├── main.py
    └── requirements.txt
```

## 📁 Data Lake Simulation

Each batch of 5 orders is saved as a new `.parquet` file under `data_sink/data_lake/`.

To inspect them use Jupyter notebook.
