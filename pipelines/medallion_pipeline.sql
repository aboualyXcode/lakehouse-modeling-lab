-- Lakeflow pipelines (Lakeflow Spark Declarative Pipelines, formerly Delta Live Tables)
-- The whole Medallion flow for Stagedoor, declared rather than orchestrated.
--
-- Compare with notebooks 01–03: no checkpoints to manage, no MERGE statements, no run order.
-- You declare each dataset and its query; the pipeline works out dependencies, runs incrementally,
-- enforces the expectations, and handles out-of-order change data with AUTO CDC.
--
-- Pipeline configuration (set in databricks.yml):
--   landing_path   /Volumes/<catalog>/stagedoor_landing/files/raw
-- Target catalog and schema come from the pipeline settings (default schema: stagedoor_ldp).

-- ============================================================================
-- BRONZE: streaming tables over the landing files. Append-only, schema pinned,
-- anything unexpected kept in _rescued_data.
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_customers
COMMENT 'Raw customer change records, exactly as delivered'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${landing_path}/customers', format => 'json', rescuedDataColumn => '_rescued_data',
  schema => 'op STRING, customer_id STRING, full_name STRING, email STRING, city STRING, country STRING, loyalty_tier STRING, updated_at TIMESTAMP');

CREATE OR REFRESH STREAMING TABLE bronze_events
COMMENT 'Raw event change records, exactly as delivered'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${landing_path}/events', format => 'json', rescuedDataColumn => '_rescued_data',
  schema => 'op STRING, event_id STRING, event_name STRING, artist STRING, venue STRING, city STRING, event_date DATE, base_price DECIMAL(10,2), updated_at TIMESTAMP');

CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT 'Raw order change records, exactly as delivered'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${landing_path}/orders', format => 'json', rescuedDataColumn => '_rescued_data',
  schema => 'op STRING, order_id STRING, customer_id STRING, event_id STRING, quantity INT, unit_price DECIMAL(10,2), status STRING, order_ts TIMESTAMP, updated_at TIMESTAMP');

-- ============================================================================
-- SILVER: expectations enforce data quality. Failing rows are dropped here and
-- counted in the pipeline's data-quality metrics. Use ON VIOLATION FAIL UPDATE for
-- rules that must stop the pipeline, or plain EXPECT to only record violations.
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE silver_customer_changes (
  CONSTRAINT has_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_email     EXPECT (email LIKE '%@%')       ON VIOLATION DROP ROW
)
COMMENT 'Valid, standardized customer changes'
AS SELECT trim(customer_id) AS customer_id, trim(full_name) AS full_name, lower(trim(email)) AS email,
          trim(city) AS city, upper(trim(country)) AS country, trim(loyalty_tier) AS loyalty_tier, updated_at
FROM STREAM(bronze_customers);

CREATE OR REFRESH STREAMING TABLE silver_event_changes (
  CONSTRAINT has_event_id       EXPECT (event_id IS NOT NULL)                 ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_price EXPECT (base_price IS NULL OR base_price >= 0) ON VIOLATION DROP ROW
)
COMMENT 'Valid event changes'
AS SELECT trim(event_id) AS event_id, event_name, artist, venue, city, event_date, base_price, updated_at
FROM STREAM(bronze_events);

CREATE OR REFRESH STREAMING TABLE silver_order_changes (
  CONSTRAINT has_order_id      EXPECT (order_id IS NOT NULL)                       ON VIOLATION DROP ROW,
  CONSTRAINT has_customer      EXPECT (customer_id IS NOT NULL)                    ON VIOLATION DROP ROW,
  CONSTRAINT positive_quantity EXPECT (quantity > 0)                               ON VIOLATION DROP ROW,
  CONSTRAINT known_status      EXPECT (status IN ('PLACED', 'PAID', 'REFUNDED'))   ON VIOLATION DROP ROW
)
COMMENT 'Valid order changes'
AS SELECT trim(order_id) AS order_id, trim(customer_id) AS customer_id, trim(event_id) AS event_id,
          quantity, unit_price, upper(trim(status)) AS status, order_ts, updated_at
FROM STREAM(bronze_orders);

-- Current state per key (SCD Type 1). AUTO CDC sequences by updated_at, so a late change
-- never overwrites a newer one.
CREATE OR REFRESH STREAMING TABLE silver_orders COMMENT 'Current state of each order';
CREATE FLOW silver_orders_cdc AS AUTO CDC INTO silver_orders
FROM STREAM(silver_order_changes)
KEYS (order_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 1;

CREATE OR REFRESH STREAMING TABLE silver_events COMMENT 'Current state of each event';
CREATE FLOW silver_events_cdc AS AUTO CDC INTO silver_events
FROM STREAM(silver_event_changes)
KEYS (event_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 1;

-- ============================================================================
-- GOLD: the star schema.
-- ============================================================================

-- SCD Type 2 in one statement. History is kept for city, country and tier only;
-- name and email changes update the current version in place.
-- AUTO CDC adds __START_AT and __END_AT (the SEQUENCE BY values) and handles out-of-order changes.
CREATE OR REFRESH STREAMING TABLE dim_customer_history COMMENT 'Customer history, SCD Type 2';
CREATE FLOW dim_customer_scd2 AS AUTO CDC INTO dim_customer_history
FROM STREAM(silver_customer_changes)
KEYS (customer_id)
SEQUENCE BY updated_at
COLUMNS * EXCEPT (updated_at)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (full_name, email);

-- Consumer-facing dimension: a stable surrogate key, the first version open to the beginning of
-- time (so early-arriving orders still match), and an explicit Unknown member.
CREATE OR REFRESH MATERIALIZED VIEW dim_customer
COMMENT 'Customer dimension, SCD Type 2, with surrogate keys and an Unknown member'
AS
SELECT xxhash64(customer_id, __START_AT) AS customer_sk, customer_id, full_name, email, city, country, loyalty_tier,
       IF(__START_AT = MIN(__START_AT) OVER (PARTITION BY customer_id), TIMESTAMP'1900-01-01', __START_AT) AS valid_from,
       COALESCE(__END_AT, TIMESTAMP'9999-12-31') AS valid_to,
       __END_AT IS NULL AS is_current
FROM dim_customer_history
UNION ALL
SELECT -1, 'UNKNOWN', 'Unknown customer', NULL, 'Unknown', NULL, NULL, TIMESTAMP'1900-01-01', TIMESTAMP'9999-12-31', true;

CREATE OR REFRESH MATERIALIZED VIEW dim_event
COMMENT 'Event dimension, SCD Type 1'
AS
SELECT xxhash64(event_id) AS event_sk, event_id, event_name, artist, venue, city, event_date, base_price FROM silver_events
UNION ALL
SELECT -1, 'UNKNOWN', 'Unknown event', NULL, NULL, NULL, NULL, NULL;

CREATE OR REFRESH MATERIALIZED VIEW fact_ticket_sales
COMMENT 'Ticket sales at order grain'
AS
SELECT o.order_id,
       CAST(date_format(o.order_ts, 'yyyyMMdd') AS INT) AS date_key,
       COALESCE(c.customer_sk, -1) AS customer_sk,
       COALESCE(e.event_sk, -1)    AS event_sk,
       o.quantity, o.unit_price,
       CAST(o.quantity * o.unit_price AS DECIMAL(12,2)) AS gross_amount,
       o.status AS order_status
FROM silver_orders o
LEFT JOIN dim_customer c
  ON c.customer_id = o.customer_id AND o.order_ts >= c.valid_from AND o.order_ts < c.valid_to
LEFT JOIN dim_event e ON e.event_id = o.event_id;

-- A ready-made answer for dashboards. Materialized views refresh incrementally where possible.
CREATE OR REFRESH MATERIALIZED VIEW revenue_by_city
COMMENT 'Paid revenue by the customer''s city at the time of purchase'
AS
SELECT c.city, SUM(f.gross_amount) AS revenue, SUM(f.quantity) AS tickets
FROM fact_ticket_sales f
JOIN dim_customer c ON c.customer_sk = f.customer_sk
WHERE f.order_status = 'PAID'
GROUP BY c.city;
