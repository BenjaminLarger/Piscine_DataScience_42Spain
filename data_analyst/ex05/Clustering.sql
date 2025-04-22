SELECT 
  user_id,
  COUNT(*) AS num_orders,
  SUM(price) AS total_spent,
  MAX(event_time) AS last_order_date,
  MIN(event_time) AS first_order_date
FROM customers
WHERE event_type = 'purchase'
GROUP BY user_id
ORDER BY num_orders DESC;