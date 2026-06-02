# Payment Fraud Detection Pipeline

## Overview

A real-time fraud detection pipeline built using Kafka, Python, PySpark, Snowflake, Airflow, and NiFi.

## Architecture

Producer → Kafka → Consumer → Landing Zone → PySpark Fraud Engine → Snowflake → Power BI

## Fraud Rules

- Failed login attempts > 5
- New device detected
- Amount greater than monthly income
- Recent profile changes
- New beneficiary added

## Tech Stack

- Python
- Kafka
- Docker
- PySpark
- Snowflake
- SQL
- Airflow
- Apache NiFi

## Pipeline

1. Generate synthetic payment transactions
2. Stream transactions to Kafka
3. Consume Kafka messages
4. Store raw transactions in landing zone
5. Apply fraud scoring in PySpark
6. Split clean and fraudulent transactions
7. Load results to Snowflake
8. Visualize in Power BI

## Results

- Total Transactions Processed: 3936
- Fraud Transactions Detected: 206
- Fraud Rate: 5.23%
