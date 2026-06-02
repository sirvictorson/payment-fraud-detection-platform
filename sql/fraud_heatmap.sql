SELECT
    EXTRACT(HOUR FROM transaction_timestamp) AS txn_hour,
    COUNT(*) AS fraud_count
FROM fraud_transactions
GROUP BY txn_hour
ORDER BY txn_hour;