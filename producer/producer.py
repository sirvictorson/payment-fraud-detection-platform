from kafka import KafkaProducer
from faker import Faker
import json
import random
import uuid
import time
from datetime import datetime

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "payment_transactions"

countries = ["Germany", "UK", "France", "Spain", "USA", "Brazil", "Nigeria"]
merchant_categories = ["Retail", "Travel", "Food", "Electronics", "Finance"]
occupations = ["Engineer", "Doctor", "Teacher", "Consultant", "Analyst"]


def generate_transaction():

    fraud = random.random() < 0.05

    monthly_income = random.randint(2000, 10000)

    amount = random.randint(10, 3000)

    fraud_scenario = "NONE"

    if fraud:

        fraud_type = random.choice([
            "ACCOUNT_TAKEOVER",
            "INCOME_MISMATCH",
            "PROFILE_CHANGE",
            "VELOCITY_FRAUD",
            "NEW_BENEFICIARY"
        ])

        fraud_scenario = fraud_type

        if fraud_type == "ACCOUNT_TAKEOVER":
            amount = random.randint(5000, 15000)

        elif fraud_type == "INCOME_MISMATCH":
            amount = monthly_income * random.randint(3, 10)

        elif fraud_type == "PROFILE_CHANGE":
            amount = random.randint(5000, 10000)

        elif fraud_type == "VELOCITY_FRAUD":
            amount = random.randint(2000, 8000)

        elif fraud_type == "NEW_BENEFICIARY":
            amount = random.randint(5000, 12000)

    event = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.randint(1000, 9999),
        "amount": amount,
        "currency": "USD",
        "merchant_id": random.randint(100, 999),
        "merchant_category": random.choice(merchant_categories),
        "transaction_timestamp": str(datetime.now()),
        "sender_country": random.choice(countries),
        "receiver_country": random.choice(countries),
        "customer_home_country": "Germany",
        "monthly_income": monthly_income,
        "account_age_days": random.randint(30, 2000),
        "device_id": str(uuid.uuid4())[:8],
        "is_new_device": fraud,
        "failed_login_attempts": random.randint(5, 10) if fraud else random.randint(0, 2),
        "recent_profile_change": fraud,
        "new_beneficiary": fraud,
        "is_fraud": int(fraud),
        "fraud_scenario": fraud_scenario
    }

    return event


print("Kafka Producer Started")

while True:

    event = generate_transaction()

    producer.send(TOPIC, value=event)

    print(event)

    time.sleep(1)