from kafka import KafkaConsumer
import json
import os

consumer = KafkaConsumer(
    "payment_transactions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

LANDING_DIR = BASE_DIR / "data" / "landing"

LANDING_DIR.mkdir(parents=True, exist_ok=True)

output_file = LANDING_DIR / "payment_batch.json"

print("Listening to Kafka...")

with open(output_file, "a") as f:

    for message in consumer:

        record = message.value

        f.write(json.dumps(record) + "\n")

        print("Saved:", record["transaction_id"])