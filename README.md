# Kafka Order Analytics

A small event pipeline that reads orders from Kafka and maintains product sales totals in SQLite.

## How it works

`producer.py` → Kafka `orders` topic → `consumer.py` → SQLite `orders.db`

The consumer tracks processed `event_id` values so replayed or duplicate events do not increase sales totals twice.

## Requirements

- Docker
- Python 3.10+

## Run locally

Start Kafka:

```bash
docker run -d --name kafka-order-analytics -p 9092:9092 apache/kafka:4.1.1
```

Create the topic:

```bash
docker exec kafka-order-analytics /opt/kafka/bin/kafka-topics.sh \
  --create --topic orders --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

Install Python dependencies:

```bash
python3 -m venv .venv
source .ven
```
