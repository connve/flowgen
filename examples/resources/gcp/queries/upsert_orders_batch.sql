-- Batch upsert orders from JSON array into BigQuery table
-- Uses MERGE statement with UNNEST to process entire batch in a single query
-- This avoids DML rate limits by doing 1 operation per batch instead of 1 per row
--
-- Performance Benefits:
-- - Single MERGE operation instead of N individual INSERT/UPDATE statements
-- - BigQuery processes the entire batch efficiently with UNNEST
-- - Avoids hitting per-table DML rate limits (e.g., 1,500 operations per minute)
--
-- Parameter:
-- @batch - JSON array parameter passed as native JSON type (no PARSE_JSON needed)
--          Example: [{"id": 1, "name": "Order 1"}, {"id": 2, "name": "Order 2"}]
MERGE `my-project-id.analytics.orders` AS target
USING (
  SELECT
    CAST(JSON_VALUE(item, '$.id') AS INT64) AS id,
    JSON_VALUE(item, '$.order_number') AS order_number,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(item, '$.order_date')) AS order_date,
    JSON_VALUE(item, '$.payment_type') AS payment_type,
    CAST(JSON_VALUE(item, '$.amount') AS FLOAT64) AS amount,
    JSON_VALUE(item, '$.customer_id') AS customer_id,
    JSON_VALUE(item, '$.status') AS status
  FROM UNNEST(JSON_QUERY_ARRAY(@batch)) AS item
) AS source
ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET
    order_number = source.order_number,
    order_date = source.order_date,
    payment_type = source.payment_type,
    amount = source.amount,
    customer_id = source.customer_id,
    status = source.status
WHEN NOT MATCHED THEN
  INSERT (id, order_number, order_date, payment_type, amount, customer_id, status)
  VALUES (source.id, source.order_number, source.order_date, source.payment_type, source.amount, source.customer_id, source.status)
