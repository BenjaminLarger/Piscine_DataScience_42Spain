SELECT 
  CASE 
    WHEN frequency BETWEEN 0 AND 9.99 THEN '0-10'
    WHEN frequency BETWEEN 10 AND 19.99 THEN '10-20'
    WHEN frequency BETWEEN 20 AND 29.99 THEN '20-30'
    WHEN frequency BETWEEN 30 AND 40 THEN '30-40'
  END AS frequency_range,
  COUNT(*) AS num_users
FROM (
  SELECT 
    user_id,
    COUNT(*) AS frequency
  FROM customers
  WHERE event_type = 'purchase'
  GROUP BY user_id
  HAVING COUNT(*) <= 40
		AND COUNT(*) > 0
) AS user_orders
GROUP BY frequency_range
ORDER BY frequency_range;
