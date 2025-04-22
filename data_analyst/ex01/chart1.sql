SELECT 
  DATE(event_time) AS day,
  COUNT(DISTINCT user_id) AS num_customers
FROM customers
WHERE event_type = 'purchase'
AND event_time < '2023-02-01'
GROUP BY day
ORDER BY day;
