SELECT
    merchant_id,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_transactions,
    ROUND(
        SUM(is_fraud) * 100.0 / COUNT(*),
        2
    ) AS fraud_rate_pct
FROM fraud_transactions
GROUP BY merchant_id
ORDER BY fraud_rate_pct DESC;