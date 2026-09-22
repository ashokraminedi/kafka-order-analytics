import json
from kafka import KafkaProducer 

producer = KafkaProducer(
    bootstrap_servers='localhost:9092', 
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))


orders = [
    {
        "event_id": "evt-002",
        "order_id": "ord-102",
        "product_id": "bike",
        "quantity": 2,
        "price": 250.00,
    },
    {
        "event_id": "evt-003",
        "order_id": "ord-103",
        "product_id": "helmet",
        "quantity": 1,
        "price": 45.00,
    }
]

for order in orders:
    producer.send('orders', value=order)
    print(f"Sent order: {order}")

producer.close()