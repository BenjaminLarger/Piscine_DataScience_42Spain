SELECT 
  DATE_TRUNC('month', event_time)::date AS month,
  SUM(price) AS total_revenue
FROM customers
WHERE event_type = 'purchase' 
  AND event_time < '2023-02-01'
GROUP BY month
ORDER BY month;
