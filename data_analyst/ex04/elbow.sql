SELECT user_id, COUNT(*) AS purchases
FROM customers
WHERE event_type = 'purchase'
GROUP BY user_id
ORDER BY purchases DESC;