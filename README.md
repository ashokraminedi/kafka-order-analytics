# Kafka Order Analytics

A small event processing pipeline built with Apache Kafka, Python, and SQLite. It streams order events, calculates product sales totals, and skips duplicate events so replaying an order does not inflate revenue.

## How it works

`producer.py` → Kafka `orders` topic → `consumer.py` → SQLite `orders.db`

The consumer validates each order and records its `event_id` in SQLite alongside the sales update. If an event with the same ID arrives again, it is skipped.

## Requirements

- Docker Desktop
- Python 3.10+

## Run locally

Start Kafka:

```bash
docker run -d --name kafka-order-analytics -p 9092:9092 apache/kafka:4.1.1
```

Create the `orders` topic:

```bash
docker exec kafka-order-analytics \
  /opt/kafka/bin/kafka-topics.sh \
  --create --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

Set up Python and install the dependency:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

In one terminal, start the consumer:

```bash
python consumer.py
```

In a second terminal, from the same project folder, send sample orders:

```bash
source .venv/bin/activate
python producer.py
```

The consumer prints each processed event. Press **Ctrl+C** to stop it.

## Check the sales totals

```bash
python - <<'PY'
import sqlite3

with sqlite3.connect("orders.db") as connection:
    rows = connection.execute(
        "SELECT product_id, units_sold, revenue_cents "
        "FROM product_sales ORDER BY product_id"
    )
    for product_id, units, cents in rows:
        print(f"{product_id}: {units} units, ${cents / 100:.2f} revenue")
PY
```

Run `python producer.py` again, then restart the consumer. Events with IDs it has already processed are skipped, and the sales totals stay the same.

## Stop Kafka

```bash
docker stop kafka-order-analytics
```

To start the same container again later:

```bash
docker start kafka-order-analytics
```
