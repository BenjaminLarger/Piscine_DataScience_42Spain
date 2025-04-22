SELECT 
  user_id,
  ROUND(CAST(SUM(price)AS NUMERIC), 2) AS average_basket_price
FROM customers
WHERE event_type = 'purchase'
GROUP BY user_id
ORDER BY average_basket_price DESC;
