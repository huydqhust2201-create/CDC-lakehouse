-- Schema evolution demo: add a column at the source.
-- Debezium picks the new column up automatically; the Silver MERGE adds it to
-- the Delta table (spark.databricks.delta.schema.autoMerge.enabled = true).
--
--   docker compose exec -T postgres psql -U shop -d shop < datagen/schema_evolution.sql

ALTER TABLE customers ADD COLUMN IF NOT EXISTS loyalty_tier VARCHAR(20) DEFAULT 'bronze';

-- Touch a few rows so change events carrying the new column are emitted right away.
UPDATE customers SET loyalty_tier = 'gold'
WHERE customer_id IN (SELECT customer_id FROM customers ORDER BY customer_id LIMIT 5);
