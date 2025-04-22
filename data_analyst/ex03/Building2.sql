WITH spending_data AS (
  SELECT 
    CASE 
      WHEN total_spent BETWEEN 0 AND 39.99 THEN '0-40'
      WHEN total_spent BETWEEN 40 AND 79.99 THEN '40-80'
      WHEN total_spent BETWEEN 80 AND 119.99 THEN '80-120'
      WHEN total_spent BETWEEN 120 AND 159.99 THEN '120-160'
      WHEN total_spent BETWEEN 160 AND 200 THEN '160-200'
    END AS spending_range,
    COUNT(*) AS num_customers
  FROM (
    SELECT user_id,
          SUM(price) AS total_spent
    FROM customers
    WHERE event_type = 'purchase'
    GROUP BY user_id
    HAVING SUM(price) <= 200
          AND SUM(price) > 0
  ) AS spending
  GROUP BY spending_range
)
SELECT spending_range, num_customers
FROM spending_data
ORDER BY 
  CASE 
    WHEN spending_range = '0-40' THEN 1
    WHEN spending_range = '40-80' THEN 2
    WHEN spending_range = '80-120' THEN 3
    WHEN spending_range = '120-160' THEN 4
    WHEN spending_range = '160-200' THEN 5
  END;