/*
Volume (number of  products sold per time period)
*/
-- by the hour of the day
(SELECT
    '09:00 - 11:00' AS time_range,
    b.branch_name,
    SUM(o.quantity) AS products_sold
FROM branches AS b
JOIN orders AS o ON o.branch_id = b.branch_id
JOIN products AS p ON o.product_id = p.product_id
WHERE o.order_date BETWEEN '25-08-2021 9:00:00' AND '25-08-2021 11:00:00'
GROUP BY 1, 2
ORDER BY 1 ASC;)

UNION ALL

(SELECT
    '11:01 - 14:00' AS time_range,
    b.branch_name,
    SUM(o.quantity) AS products_sold
FROM branches AS b
JOIN orders AS o ON o.branch_id = b.branch_id
JOIN products AS p ON o.product_id = p.product_id
WHERE o.order_date BETWEEN '25-08-2021 11:01:00' AND '25-08-2021 14:00:00' 
GROUP BY 1, 2
ORDER BY 1 ASC;)

UNION ALL

(SELECT
    '14:01 - 16:59' AS time_range,
    SUM(o.quantity) AS products_sold
FROM branches AS b
JOIN orders AS o ON o.branch_id = b.branch_id
JOIN products AS p ON o.product_id = p.product_id
WHERE o.order_date BETWEEN '25-08-2021 14:01:00' AND '25-08-2021 16:59:00' 
GROUP BY 1, 2)

/*
Total products sold per branch
*/
SELECT
    b.branch_name,
    SUM(o.quantity) AS products_sold
FROM branches as b
JOIN orders as o ON o.branch_id = b.branch_id
JOIN products as p ON p.product_id = product_id
GROUP BY 1


/*
Revenue (per branch)
*/
SELECT
    b.branch_name,
    SUM(o.total_price) AS revenue
FROM orders o
JOIN branches b ON o.branch_id = b.branch_id
GROUP BY 1
ORDER BY revenue DESC;


