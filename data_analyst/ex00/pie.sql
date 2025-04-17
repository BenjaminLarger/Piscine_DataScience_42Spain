SELECT event_type, COUNT(*)
FROM customers
GROUP BY event_type
ORDER BY count DESC;