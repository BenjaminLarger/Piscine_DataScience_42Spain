SELECT event_type, COUNT(*)
FROM customers
WHERE event_time < '2023-02-01'
GROUP BY event_type
ORDER BY COUNT(event_type) DESC;