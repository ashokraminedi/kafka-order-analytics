import json
import sqlite3
from decimal import Decimal, InvalidOperation

from kafka import KafkaConsumer

DB_PATH = 'orders.db'

connection = sqlite3.connect(DB_PATH)
connection.execute("""
    CREATE TABLE IF NOT EXISTS processed_events (
        event_id TEXT PRIMARY KEY
    )
""")

connection.execute("""
    CREATE TABLE IF NOT EXISTS product_sales (
        product_id TEXT PRIMARY KEY,
        units_sold INTEGER NOT NULL,
        revenue_cents INTEGER NOT NULL
    )
""")

connection.commit()

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='order_analytics',
    enable_auto_commit=False,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening for orders... Press Ctrl+C to stop.")

try:
    for message in consumer:
        event = message.value

        try:
            event_id = event["event_id"]
            product_id = event["product_id"]
            quantity = int(event["quantity"])
            price = Decimal(str(event["price"]))

            if not event_id or not product_id or quantity <= 0 or price < 0:
                raise ValueError("Invalid order fields")

            revenue_cents = int(price * 100)
            if price * 100 != revenue_cents:
                raise ValueError("Price must have at most two decimal places")

            with connection:
                inserted = connection.execute(
                    "INSERT OR IGNORE INTO processed_events (event_id) VALUES (?)",
                    (event_id,)
                )

                if inserted.rowcount:
                    connection.execute(
                        """
                        INSERT INTO product_sales (product_id, units_sold, revenue_cents)
                        VALUES (?, ?, ?)
                        ON CONFLICT(product_id) DO UPDATE SET
                            units_sold = units_sold + excluded.units_sold,
                            revenue_cents = revenue_cents + excluded.revenue_cents
                        """,
                        (product_id, quantity, quantity *revenue_cents)
                    )

            consumer.commit()
            if inserted.rowcount:
                print(f"Processed {event_id}: {quantity} * {product_id}")
            else:
                print(f"Duplicate event {event_id} skipped.")

        except (KeyError, TypeError,ValueError, InvalidOperation) as error:
            print(f"Invalid event at offset {message.offset}: {error}")
            consumer.commit()

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
    connection.close()
    print("Consumer stopped.")