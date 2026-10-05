-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · Silver: validate, deduplicate, conform
-- MAGIC
-- MAGIC **Silver rules**
-- MAGIC - Enforce types, standardize values, and apply data-quality rules. Records that fail go to
-- MAGIC   `quarantine` with the reason; they are never silently dropped.
-- MAGIC - Remove exact duplicates (at-least-once delivery produces them).
-- MAGIC - Keep two shapes per entity: the **change history** (`*_changes`, every distinct valid change)
-- MAGIC   and the **current state** (latest change per key, SCD Type 1).
-- MAGIC - Sequence by **business time** (`updated_at`), never arrival time, so late data cannot move a row backwards.
-- MAGIC
-- MAGIC Every statement is idempotent: rerunning the notebook changes nothing unless Bronze has new data.

-- COMMAND ----------

-- MAGIC %run ./_config

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS stagedoor_silver.quarantine (
  source_table  STRING    NOT NULL,
  record_key    STRING,
  reason        STRING    NOT NULL COMMENT 'Every rule the record broke',
  record        STRING    NOT NULL COMMENT 'The original record as JSON, for replay after a fix',
  _batch_id     INT,
  _ingested_at  TIMESTAMP
) COMMENT 'Records rejected by Silver data-quality rules';

CREATE TABLE IF NOT EXISTS stagedoor_silver.customer_changes (
  customer_id STRING NOT NULL, full_name STRING, email STRING, city STRING, country STRING, loyalty_tier STRING,
  updated_at TIMESTAMP NOT NULL, row_hash STRING NOT NULL, _ingested_at TIMESTAMP
) CLUSTER BY (customer_id, updated_at) COMMENT 'Every distinct, valid customer change';

CREATE TABLE IF NOT EXISTS stagedoor_silver.event_changes (
  event_id STRING NOT NULL, event_name STRING, artist STRING, venue STRING, city STRING, event_date DATE,
  base_price DECIMAL(10,2), updated_at TIMESTAMP NOT NULL, row_hash STRING NOT NULL, _ingested_at TIMESTAMP
) CLUSTER BY (event_id, updated_at) COMMENT 'Every distinct, valid event change';

CREATE TABLE IF NOT EXISTS stagedoor_silver.order_changes (
  order_id STRING NOT NULL, customer_id STRING NOT NULL, event_id STRING NOT NULL, quantity INT, unit_price DECIMAL(10,2),
  status STRING, order_ts TIMESTAMP, updated_at TIMESTAMP NOT NULL, row_hash STRING NOT NULL, _ingested_at TIMESTAMP
) CLUSTER BY (order_id, updated_at) COMMENT 'Every distinct, valid order change';

CREATE TABLE IF NOT EXISTS stagedoor_silver.customers (
  customer_id STRING NOT NULL, full_name STRING, email STRING, city STRING, country STRING, loyalty_tier STRING, updated_at TIMESTAMP,
  CONSTRAINT silver_customers_pk PRIMARY KEY (customer_id)
) CLUSTER BY (customer_id) COMMENT 'Current state of each customer (SCD Type 1)';

CREATE TABLE IF NOT EXISTS stagedoor_silver.events (
  event_id STRING NOT NULL, event_name STRING, artist STRING, venue STRING, city STRING, event_date DATE,
  base_price DECIMAL(10,2), updated_at TIMESTAMP,
  CONSTRAINT silver_events_pk PRIMARY KEY (event_id)
) CLUSTER BY (event_id) COMMENT 'Current state of each event (SCD Type 1)';

CREATE TABLE IF NOT EXISTS stagedoor_silver.orders (
  order_id STRING NOT NULL, customer_id STRING, event_id STRING, quantity INT, unit_price DECIMAL(10,2), status STRING,
  order_ts TIMESTAMP, updated_at TIMESTAMP,
  CONSTRAINT silver_orders_pk PRIMARY KEY (order_id)
) CLUSTER BY (order_id) COMMENT 'Current state of each order (SCD Type 1)';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1 · Quarantine records that break the rules
-- MAGIC Rules are written once, as boolean expressions, and the reason lists every rule a record broke.

-- COMMAND ----------

MERGE INTO stagedoor_silver.quarantine AS q
USING (
  SELECT 'orders' AS source_table, order_id AS record_key,
         concat_ws('; ',
           IF(order_id IS NULL, 'order_id is missing', NULL),
           IF(customer_id IS NULL, 'customer_id is missing', NULL),
           IF(event_id IS NULL, 'event_id is missing', NULL),
           IF(quantity IS NULL OR quantity <= 0, 'quantity must be positive', NULL),
           IF(status IS NULL OR status NOT IN ('PLACED', 'PAID', 'REFUNDED'), 'unknown status', NULL)) AS reason,
         to_json(struct(*)) AS record, _batch_id, _ingested_at
  FROM stagedoor_bronze.orders
  WHERE order_id IS NULL OR customer_id IS NULL OR event_id IS NULL OR quantity IS NULL OR quantity <= 0
     OR status IS NULL OR status NOT IN ('PLACED', 'PAID', 'REFUNDED')
  UNION ALL
  SELECT 'customers', customer_id,
         concat_ws('; ', IF(customer_id IS NULL, 'customer_id is missing', NULL),
                         IF(email IS NULL OR email NOT LIKE '%@%', 'email is invalid', NULL)),
         to_json(struct(*)), _batch_id, _ingested_at
  FROM stagedoor_bronze.customers
  WHERE customer_id IS NULL OR email IS NULL OR email NOT LIKE '%@%'
  UNION ALL
  SELECT 'events', event_id,
         concat_ws('; ', IF(event_id IS NULL, 'event_id is missing', NULL),
                         IF(base_price < 0, 'base_price is negative', NULL)),
         to_json(struct(*)), _batch_id, _ingested_at
  FROM stagedoor_bronze.events
  WHERE event_id IS NULL OR base_price < 0
) AS s
ON q.source_table = s.source_table AND q.record = s.record
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2 · Change history: valid, standardized, deduplicated
-- MAGIC `row_hash` identifies the payload, so the same change delivered twice is stored once,
-- MAGIC while a genuine correction with the same `updated_at` is still kept.

-- COMMAND ----------

MERGE INTO stagedoor_silver.customer_changes AS t
USING (
  SELECT customer_id, full_name, email, city, country, loyalty_tier, updated_at, row_hash, MIN(_ingested_at) AS _ingested_at
  FROM (
    SELECT trim(customer_id) AS customer_id, trim(full_name) AS full_name, lower(trim(email)) AS email,
           trim(city) AS city, upper(trim(country)) AS country, trim(loyalty_tier) AS loyalty_tier, updated_at,
           md5(to_json(struct(full_name, email, city, country, loyalty_tier))) AS row_hash, _ingested_at
    FROM stagedoor_bronze.customers
    WHERE customer_id IS NOT NULL AND email LIKE '%@%'
  )
  GROUP BY ALL
) AS s
ON t.customer_id = s.customer_id AND t.updated_at = s.updated_at AND t.row_hash = s.row_hash
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO stagedoor_silver.event_changes AS t
USING (
  SELECT event_id, event_name, artist, venue, city, event_date, base_price, updated_at, row_hash, MIN(_ingested_at) AS _ingested_at
  FROM (
    SELECT trim(event_id) AS event_id, trim(event_name) AS event_name, trim(artist) AS artist, trim(venue) AS venue,
           trim(city) AS city, event_date, base_price, updated_at,
           md5(to_json(struct(event_name, artist, venue, city, event_date, base_price))) AS row_hash, _ingested_at
    FROM stagedoor_bronze.events
    WHERE event_id IS NOT NULL AND (base_price IS NULL OR base_price >= 0)
  )
  GROUP BY ALL
) AS s
ON t.event_id = s.event_id AND t.updated_at = s.updated_at AND t.row_hash = s.row_hash
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO stagedoor_silver.order_changes AS t
USING (
  SELECT order_id, customer_id, event_id, quantity, unit_price, status, order_ts, updated_at, row_hash, MIN(_ingested_at) AS _ingested_at
  FROM (
    SELECT trim(order_id) AS order_id, trim(customer_id) AS customer_id, trim(event_id) AS event_id, quantity, unit_price,
           upper(trim(status)) AS status, order_ts, updated_at,
           md5(to_json(struct(customer_id, event_id, quantity, unit_price, status, order_ts))) AS row_hash, _ingested_at
    FROM stagedoor_bronze.orders
    WHERE order_id IS NOT NULL AND customer_id IS NOT NULL AND event_id IS NOT NULL
      AND quantity > 0 AND status IN ('PLACED', 'PAID', 'REFUNDED')
  )
  GROUP BY ALL
) AS s
ON t.order_id = s.order_id AND t.updated_at = s.updated_at AND t.row_hash = s.row_hash
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3 · Current state, with a sequence guard
-- MAGIC `WHEN MATCHED AND s.updated_at > t.updated_at` is the guard: a late or replayed change older than
-- MAGIC the current row never overwrites it. Without it, the last file to arrive wins, which is wrong.

-- COMMAND ----------

MERGE INTO stagedoor_silver.customers AS t
USING (
  SELECT * EXCEPT (row_hash, _ingested_at) FROM stagedoor_silver.customer_changes
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC, _ingested_at DESC) = 1
) AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO stagedoor_silver.events AS t
USING (
  SELECT * EXCEPT (row_hash, _ingested_at) FROM stagedoor_silver.event_changes
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY updated_at DESC, _ingested_at DESC) = 1
) AS s
ON t.event_id = s.event_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO stagedoor_silver.orders AS t
USING (
  SELECT * EXCEPT (row_hash, _ingested_at) FROM stagedoor_silver.order_changes
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, _ingested_at DESC) = 1
) AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

SELECT source_table, record_key, reason, _batch_id FROM stagedoor_silver.quarantine ORDER BY _ingested_at DESC;
