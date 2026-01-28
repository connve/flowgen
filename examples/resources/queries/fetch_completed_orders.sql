-- Fetch completed orders with parameterized filters
-- This query demonstrates how to use BigQuery parameters for SQL injection protection.
-- Parameters are passed from the flow configuration and properly escaped by BigQuery.
SELECT
    id,
    customer_id,
    amount,
    order_date,
    status
FROM `my-project-id.analytics.orders`
WHERE
    status = @status
    AND order_date >= @start_date
ORDER BY order_date DESC
