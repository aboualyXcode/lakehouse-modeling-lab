-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 · Business Vault, and star schemas on top of the vault
-- MAGIC
-- MAGIC The raw vault answers "what did each source say, and when?". It is not built for reporting.
-- MAGIC The **business vault** adds what reporting needs, without changing the raw vault:
-- MAGIC - **soft business rules** (here: an order needs a customer and a positive quantity);
-- MAGIC - **effectivity**: when each version was true, in business time;
-- MAGIC - **point-in-time (PIT) tables**: precomputed "which satellite row was valid on this date", so queries
-- MAGIC   join on equality instead of range lookups.
-- MAGIC
-- MAGIC Finally, **star views** on the business vault give consumers a dimensional model without copying data,
-- MAGIC the usual hybrid: Data Vault in Silver, stars in Gold.

-- COMMAND ----------

-- MAGIC %run ./_config

-- COMMAND ----------

CREATE OR REPLACE VIEW stagedoor_business_vault.bv_customer_effectivity
COMMENT 'One row per customer per effective period of city, country and tier, in business time'
AS
WITH ordered AS (
  SELECT s.*, h.customer_id,
         LAG(struct(s.city, s.country, s.loyalty_tier)) OVER (PARTITION BY s.hk_customer ORDER BY s.applied_ts, s.load_ts) AS prev_tracked,
         ROW_NUMBER() OVER (PARTITION BY s.hk_customer ORDER BY s.applied_ts DESC, s.load_ts DESC) AS recency
  FROM stagedoor_raw_vault.sat_customer_details s
  JOIN stagedoor_raw_vault.hub_customer h USING (hk_customer)
),
latest AS (SELECT hk_customer, full_name, email FROM ordered WHERE recency = 1),
versions AS (
  SELECT hk_customer, customer_id, city, country, loyalty_tier, applied_ts,
         ROW_NUMBER() OVER (PARTITION BY hk_customer ORDER BY applied_ts, load_ts) AS version_no
  FROM ordered
  WHERE prev_tracked IS NULL OR prev_tracked IS DISTINCT FROM struct(city, country, loyalty_tier)
)
SELECT v.hk_customer, v.customer_id, l.full_name, l.email, v.city, v.country, v.loyalty_tier,
       IF(v.version_no = 1, TIMESTAMP'1900-01-01', v.applied_ts)          AS effective_from,
       COALESCE(LEAD(v.applied_ts) OVER w, TIMESTAMP'9999-12-31')          AS effective_to,
       LEAD(v.applied_ts) OVER w IS NULL                                   AS is_current
FROM versions v
JOIN latest l USING (hk_customer)
WINDOW w AS (PARTITION BY v.hk_customer ORDER BY v.applied_ts);

-- COMMAND ----------

CREATE OR REPLACE VIEW stagedoor_business_vault.bv_order_current
COMMENT 'Latest state of each order with soft business rules evaluated, not applied'
AS
SELECT h.order_id, l.hk_order, l.hk_customer, l.hk_event,
       s.quantity, s.unit_price, s.status, s.order_ts,
       l.hk_customer <> repeat('0', 32) AND s.quantity > 0 AS passes_rules,
       NULLIF(concat_ws('; ',
         IF(l.hk_customer = repeat('0', 32), 'customer_id is missing', NULL),
         IF(s.quantity IS NULL OR s.quantity <= 0, 'quantity must be positive', NULL)), '') AS rule_failures
FROM stagedoor_raw_vault.link_order_customer_event l
JOIN stagedoor_raw_vault.hub_order h USING (hk_order)
JOIN stagedoor_raw_vault.sat_order_details s USING (hk_order)
QUALIFY ROW_NUMBER() OVER (PARTITION BY l.hk_order ORDER BY s.applied_ts DESC, s.load_ts DESC) = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Point-in-time table
-- MAGIC One row per customer per snapshot date. A PIT is a performance structure: rebuild it on a schedule,
-- MAGIC and add a column per satellite when a hub has several.

-- COMMAND ----------

CREATE OR REPLACE TABLE stagedoor_business_vault.pit_customer
CLUSTER BY (snapshot_date, hk_customer)
COMMENT 'For each customer and date, the satellite row valid at the end of that day'
AS
SELECT h.hk_customer, d.snapshot_date,
       MAX_BY(s.load_ts, s.applied_ts) AS sat_customer_load_ts,
       MAX(s.applied_ts)              AS sat_customer_applied_ts
FROM stagedoor_raw_vault.hub_customer h
CROSS JOIN (SELECT DISTINCT CAST(order_ts AS DATE) AS snapshot_date FROM stagedoor_business_vault.bv_order_current) d
LEFT JOIN stagedoor_raw_vault.sat_customer_details s
  ON s.hk_customer = h.hk_customer
 AND s.applied_ts < CAST(d.snapshot_date AS TIMESTAMP) + INTERVAL 1 DAY
GROUP BY ALL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A star schema as views over the vault
-- MAGIC Consumers get facts and dimensions; the vault stays the single integrated history underneath.
-- MAGIC If these views get slow, materialize them (a materialized view, or a Gold table loaded from them).

-- COMMAND ----------

CREATE OR REPLACE VIEW stagedoor_business_vault.v_dim_customer
COMMENT 'Customer dimension (SCD Type 2) derived from the vault; key is (hk_customer, effective_from)'
AS SELECT hk_customer, effective_from, customer_id, full_name, email, city, country, loyalty_tier, effective_to, is_current
FROM stagedoor_business_vault.bv_customer_effectivity;

CREATE OR REPLACE VIEW stagedoor_business_vault.v_fact_ticket_sales
COMMENT 'Ticket sales at order grain derived from the vault; only orders that pass business rules'
AS
SELECT o.order_id,
       CAST(date_format(o.order_ts, 'yyyyMMdd') AS INT) AS date_key,
       o.hk_customer, e.effective_from AS customer_effective_from, o.hk_event,
       o.quantity, o.unit_price, CAST(o.quantity * o.unit_price AS DECIMAL(12,2)) AS gross_amount, o.status AS order_status
FROM stagedoor_business_vault.bv_order_current o
LEFT JOIN stagedoor_business_vault.bv_customer_effectivity e
  ON e.hk_customer = o.hk_customer AND o.order_ts >= e.effective_from AND o.order_ts < e.effective_to
WHERE o.passes_rules;

-- COMMAND ----------

-- The same question as in 03, answered from the vault. 06_validate checks the two answers match.
SELECT COALESCE(c.city, 'Unknown') AS city, SUM(f.gross_amount) AS revenue
FROM stagedoor_business_vault.v_fact_ticket_sales f
LEFT JOIN stagedoor_business_vault.v_dim_customer c
  ON c.hk_customer = f.hk_customer AND c.effective_from = f.customer_effective_from
WHERE f.order_status = 'PAID'
GROUP BY ALL
ORDER BY revenue DESC;
