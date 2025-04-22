SELECT 
  DATE(event_time) AS day,
  SUM(price) AS total_sales,
  COUNT(DISTINCT user_id) AS num_customers,
  ROUND(CAST(SUM(price) / COUNT(DISTINCT user_id) AS NUMERIC), 2) AS avg_spend_per_customer
FROM customers
WHERE event_type = 'purchase'
  AND event_time < '2023-02-01'
GROUP BY day
ORDER BY day;