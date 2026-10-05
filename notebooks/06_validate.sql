-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 · Validate the models
-- MAGIC
-- MAGIC Databricks does not enforce primary and foreign keys, so the model's promises are tested explicitly.
-- MAGIC Each check uses `assert_true`, which fails the notebook (and the job) with a clear message.
-- MAGIC Run it after every load; in production, the same checks become pipeline expectations or a test suite.

-- COMMAND ----------

-- MAGIC %run ./_config

-- COMMAND ----------

-- Keys are unique
SELECT assert_true(COUNT(*) = COUNT(DISTINCT customer_sk), 'dim_customer.customer_sk is not unique') FROM stagedoor_gold.dim_customer;
SELECT assert_true(COUNT(*) = COUNT(DISTINCT order_id), 'fact_ticket_sales.order_id is not unique') FROM stagedoor_gold.fact_ticket_sales;
SELECT assert_true(COUNT(*) = COUNT(DISTINCT hk_customer), 'hub_customer has duplicate hash keys') FROM stagedoor_raw_vault.hub_customer;

-- COMMAND ----------

-- SCD Type 2: exactly one current version per customer, and no gaps or overlaps between versions
SELECT assert_true(MAX(n) = 1, 'a customer has more than one current version')
FROM (SELECT customer_id, COUNT(*) AS n FROM stagedoor_gold.dim_customer WHERE is_current GROUP BY customer_id);

SELECT assert_true(COUNT(*) = 0, 'dim_customer versions overlap or leave gaps')
FROM (
  SELECT customer_id, valid_to, LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) AS next_from
  FROM stagedoor_gold.dim_customer WHERE customer_sk <> -1
) WHERE next_from IS NOT NULL AND next_from <> valid_to;

-- COMMAND ----------

-- Referential integrity: every fact key resolves to a dimension row
SELECT assert_true(COUNT(*) = 0, 'fact_ticket_sales has orphan foreign keys')
FROM stagedoor_gold.fact_ticket_sales f
LEFT ANTI JOIN stagedoor_gold.dim_customer c ON c.customer_sk = f.customer_sk;

SELECT assert_true(COUNT(*) = 0, 'fact_ticket_sales has orphan event keys')
FROM stagedoor_gold.fact_ticket_sales f
LEFT ANTI JOIN stagedoor_gold.dim_event e ON e.event_sk = f.event_sk;

-- Each fact points at the customer version that was valid when the order was placed
SELECT assert_true(COUNT(*) = 0, 'a fact row points at the wrong customer version')
FROM stagedoor_gold.fact_ticket_sales f
JOIN stagedoor_silver.orders o USING (order_id)
JOIN stagedoor_gold.dim_customer c ON c.customer_sk = f.customer_sk
WHERE c.customer_sk <> -1 AND NOT (o.customer_id = c.customer_id AND o.order_ts >= c.valid_from AND o.order_ts < c.valid_to);

-- COMMAND ----------

-- Completeness: every valid order reaches Gold, and every rejected one is in quarantine with a reason
SELECT assert_true((SELECT COUNT(*) FROM stagedoor_silver.orders) = (SELECT COUNT(*) FROM stagedoor_gold.fact_ticket_sales),
                   'Silver orders and Gold facts have different counts');
SELECT assert_true(COUNT(*) = 0, 'a quarantined record has no reason')
FROM stagedoor_silver.quarantine WHERE reason IS NULL OR reason = '';

-- Satellites never store the same change twice
SELECT assert_true(COUNT(*) = 0, 'sat_customer_details stores a duplicate change')
FROM (SELECT hk_customer, applied_ts, hashdiff, COUNT(*) AS n FROM stagedoor_raw_vault.sat_customer_details GROUP BY ALL HAVING n > 1);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cross-model reconciliation
-- MAGIC The star schema and the vault are built independently from Bronze. If they disagree on a business
-- MAGIC answer, one of them has a bug. This is the strongest single test of the whole pipeline.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW star_revenue AS
SELECT c.city, CAST(SUM(f.gross_amount) AS DECIMAL(18,2)) AS revenue
FROM stagedoor_gold.fact_ticket_sales f
JOIN stagedoor_gold.dim_customer c ON c.customer_sk = f.customer_sk
WHERE f.order_status = 'PAID'
GROUP BY c.city;

CREATE OR REPLACE TEMPORARY VIEW vault_revenue AS
SELECT COALESCE(c.city, 'Unknown') AS city, CAST(SUM(f.gross_amount) AS DECIMAL(18,2)) AS revenue
FROM stagedoor_business_vault.v_fact_ticket_sales f
LEFT JOIN stagedoor_business_vault.v_dim_customer c
  ON c.hk_customer = f.hk_customer AND c.effective_from = f.customer_effective_from
WHERE f.order_status = 'PAID'
GROUP BY ALL;

SELECT assert_true(COUNT(*) = 0, 'star schema and Data Vault disagree on revenue by city')
FROM ((SELECT * FROM star_revenue EXCEPT SELECT * FROM vault_revenue)
      UNION ALL
      (SELECT * FROM vault_revenue EXCEPT SELECT * FROM star_revenue));

-- COMMAND ----------

SELECT 'All checks passed' AS result;
