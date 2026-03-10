-- Monthly sales report query
-- Aggregates sales data by month and product category

SELECT
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    category,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    MIN(order_date) AS period_start,
    MAX(order_date) AS period_end
FROM orders o
INNER JOIN order_items oi ON o.order_id = oi.order_id
INNER JOIN products p ON oi.product_id = p.product_id
WHERE
    order_date >= DATEADD(month, -12, GETDATE())
    AND status = 'completed'
GROUP BY
    YEAR(order_date),
    MONTH(order_date),
    category
ORDER BY
    year DESC,
    month DESC,
    total_revenue DESC
