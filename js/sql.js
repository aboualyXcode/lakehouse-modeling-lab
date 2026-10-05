/* sql.js: how each playground table is built on Databricks. These are excerpts of the notebooks in
   /notebooks, trimmed to the statement that matters. Schemas are shown without the stagedoor_ prefix. */
(function (root) {
  'use strict';
  root.SQL = {
    bronze: { lang: 'python', file: 'notebooks/01_bronze_ingest.py', code: `# Auto Loader: incremental, exactly-once file ingestion into an append-only Delta table.
# The schema is pinned; anything unexpected is kept in _rescued_data instead of failing or vanishing.
(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(EXPECTED[entity])
    .option("rescuedDataColumn", "_rescued_data")
    .load(f"{LANDING_PATH}/{entity}")
    .select("*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.regexp_extract("_metadata.file_name", r"batch_(\\d+)", 1).cast("int").alias("_batch_id"),
            F.current_timestamp().alias("_ingested_at"))
    .writeStream
    .option("checkpointLocation", f"{CHECKPOINTS}/{entity}/bronze")
    .trigger(availableNow=True)
    .toTable(f"{BRONZE}.{entity}"))` },
    silver: { lang: 'sql', file: 'notebooks/02_silver_conform.sql', code: `-- Latest valid state per key. The sequence guard (updated_at >) stops
-- late or replayed records from moving the current row backwards.
MERGE INTO silver.customers AS t
USING (
  SELECT * FROM silver.customer_changes
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1
) AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;` },
    quarantine: { lang: 'sql', file: 'notebooks/02_silver_conform.sql', code: `-- Records that break a rule are kept with every reason, never silently dropped.
-- MERGE makes the statement idempotent: rerunning it adds nothing new.
MERGE INTO silver.quarantine AS q
USING (
  SELECT 'orders' AS source_table, order_id AS record_key,
         concat_ws('; ',
           IF(customer_id IS NULL, 'customer_id is missing', NULL),
           IF(quantity IS NULL OR quantity <= 0, 'quantity must be positive', NULL),
           IF(status NOT IN ('PLACED','PAID','REFUNDED'), 'unknown status', NULL)) AS reason,
         to_json(struct(*)) AS record, _batch_id, _ingested_at
  FROM bronze.orders
  WHERE customer_id IS NULL OR quantity IS NULL OR quantity <= 0
     OR status NOT IN ('PLACED','PAID','REFUNDED')
) AS s
ON q.source_table = s.source_table AND q.record = s.record
WHEN NOT MATCHED THEN INSERT *;` },
    dim_customer: { lang: 'sql', file: 'notebooks/03_gold_star_schema.sql', code: `-- SCD Type 2 that survives late data: derive versions from the full change
-- history in business time, then merge on (customer_id, valid_from).
CREATE OR REPLACE TEMPORARY VIEW customer_versions AS
WITH ordered AS (
  SELECT *, LAG(struct(city, country, loyalty_tier))
              OVER (PARTITION BY customer_id ORDER BY updated_at) AS prev_tracked
  FROM silver.customer_changes
), starts AS (                              -- a version starts when a tracked column changes
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at) AS version_no
  FROM ordered
  WHERE prev_tracked IS NULL OR prev_tracked IS DISTINCT FROM struct(city, country, loyalty_tier)
)
SELECT s.customer_id, c.full_name, c.email, s.city, s.country, s.loyalty_tier,
       IF(version_no = 1, TIMESTAMP'1900-01-01', s.updated_at)          AS valid_from,
       COALESCE(LEAD(s.updated_at) OVER w, TIMESTAMP'9999-12-31')        AS valid_to,
       LEAD(s.updated_at) OVER w IS NULL                                 AS is_current
FROM starts s JOIN silver.customers c USING (customer_id)    -- Type 1 columns: latest value
WINDOW w AS (PARTITION BY s.customer_id ORDER BY s.updated_at);

MERGE INTO gold.dim_customer AS d
USING customer_versions AS v
ON d.customer_id = v.customer_id AND d.valid_from = v.valid_from
WHEN MATCHED THEN UPDATE SET d.valid_to = v.valid_to, d.is_current = v.is_current,
                             d.full_name = v.full_name, d.email = v.email
WHEN NOT MATCHED THEN INSERT (customer_id, full_name, email, city, country, loyalty_tier,
                              valid_from, valid_to, is_current)
  VALUES (v.customer_id, v.full_name, v.email, v.city, v.country, v.loyalty_tier,
          v.valid_from, v.valid_to, v.is_current)
WHEN NOT MATCHED BY SOURCE AND d.customer_sk <> -1 THEN DELETE;
-- customer_sk is an IDENTITY column: assigned on insert, never changed.
-- In a declarative pipeline, AUTO CDC ... STORED AS SCD TYPE 2 does all of this.` },
    dim_event: { lang: 'sql', file: 'notebooks/03_gold_star_schema.sql', code: `-- SCD Type 1: keep only the latest attributes.
MERGE INTO gold.dim_event AS d
USING silver.events AS s
ON d.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET
  d.event_name = s.event_name, d.artist = s.artist, d.venue = s.venue,
  d.city = s.city, d.event_date = s.event_date, d.base_price = s.base_price
WHEN NOT MATCHED THEN INSERT (event_id, event_name, artist, venue, city, event_date, base_price)
  VALUES (s.event_id, s.event_name, s.artist, s.venue, s.city, s.event_date, s.base_price);` },
    dim_date: { lang: 'sql', file: 'notebooks/03_gold_star_schema.sql', code: `-- A calendar dimension, generated once.
CREATE OR REPLACE TABLE gold.dim_date AS
SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
       d AS full_date, date_format(d, 'EEEE') AS day_name,
       date_format(d, 'yyyy-MM') AS month,
       concat(year(d), '-Q', quarter(d)) AS quarter,
       dayofweek(d) IN (1, 7) AS is_weekend
FROM (SELECT explode(sequence(DATE'2025-01-01', DATE'2027-12-31')) AS d);` },
    fact: { lang: 'sql', file: 'notebooks/03_gold_star_schema.sql', code: `-- Transaction fact at order grain. The customer key is the version valid
-- at order time; missing customers point to the Unknown member (-1).
MERGE INTO gold.fact_ticket_sales AS f
USING (
  SELECT o.order_id,
         CAST(date_format(o.order_ts, 'yyyyMMdd') AS INT) AS date_key,
         COALESCE(c.customer_sk, -1) AS customer_sk,
         COALESCE(e.event_sk, -1)    AS event_sk,
         o.quantity, o.unit_price, o.quantity * o.unit_price AS gross_amount,
         o.status AS order_status
  FROM silver.orders o
  LEFT JOIN gold.dim_customer c
    ON c.customer_id = o.customer_id
   AND o.order_ts >= c.valid_from AND o.order_ts < c.valid_to
  LEFT JOIN gold.dim_event e ON e.event_id = o.event_id
) AS s
ON f.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *        -- status changes, late customers re-pointed
WHEN NOT MATCHED THEN INSERT *;` },
    hub: { lang: 'sql', file: 'notebooks/04_raw_vault.py', code: `-- Hubs hold each business key once, forever. Insert-only.
-- Orders mention customers too, so the order feed also loads hub_customer.
MERGE INTO raw_vault.hub_customer AS h
USING (
  SELECT * FROM (
    SELECT COALESCE(md5(upper(NULLIF(trim(customer_id), ''))), repeat('0', 32)) AS hk_customer,
           NULLIF(trim(customer_id), '') AS customer_id,
           _ingested_at AS load_ts, 'stagedoor.ticketing.customers' AS record_source
    FROM bronze.customers
    UNION ALL
    SELECT COALESCE(md5(upper(NULLIF(trim(customer_id), ''))), repeat('0', 32)),
           NULLIF(trim(customer_id), ''), _ingested_at, 'stagedoor.ticketing.orders'
    FROM bronze.orders
  )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY hk_customer ORDER BY load_ts) = 1
) AS s
ON h.hk_customer = s.hk_customer
WHEN NOT MATCHED THEN INSERT *;
-- The notebook generates this statement for every hub from metadata.` },
    link: { lang: 'sql', file: 'notebooks/04_raw_vault.py', code: `-- A link records that business keys occurred together: one unit of work.
MERGE INTO raw_vault.link_order_customer_event AS l
USING (
  SELECT md5(concat_ws('||', COALESCE(upper(NULLIF(trim(order_id), '')), '-1'),
                             COALESCE(upper(NULLIF(trim(customer_id), '')), '-1'),
                             COALESCE(upper(NULLIF(trim(event_id), '')), '-1'))) AS hk_order_customer_event,
         COALESCE(md5(upper(NULLIF(trim(order_id), ''))), repeat('0', 32))    AS hk_order,
         COALESCE(md5(upper(NULLIF(trim(customer_id), ''))), repeat('0', 32)) AS hk_customer,
         COALESCE(md5(upper(NULLIF(trim(event_id), ''))), repeat('0', 32))    AS hk_event,
         _ingested_at AS load_ts, 'stagedoor.ticketing.orders' AS record_source
  FROM bronze.orders
  QUALIFY ROW_NUMBER() OVER (PARTITION BY hk_order_customer_event ORDER BY load_ts) = 1
) AS s
ON l.hk_order_customer_event = s.hk_order_customer_event
WHEN NOT MATCHED THEN INSERT *;` },
    sat: { lang: 'sql', file: 'notebooks/04_raw_vault.py', code: `-- Satellites are insert-only. A new row is kept when its hashdiff differs from the row
-- just BEFORE it in applied (business) time. Comparing with the latest LOADED row
-- instead silently drops late-arriving changes.
INSERT INTO raw_vault.sat_customer_details
SELECT hk_customer, load_ts, applied_ts, hashdiff,
       full_name, email, city, country, loyalty_tier, record_source
FROM (
  SELECT *, LAG(hashdiff) OVER (PARTITION BY hk_customer
                                ORDER BY applied_ts, is_new, load_ts) AS prev_hashdiff
  FROM (
    SELECT *, true AS is_new FROM staged_customer_sat
    UNION ALL
    SELECT *, false AS is_new FROM raw_vault.sat_customer_details
    WHERE hk_customer IN (SELECT hk_customer FROM staged_customer_sat)
  )
)
WHERE is_new AND prev_hashdiff IS DISTINCT FROM hashdiff;
-- staged_customer_sat computes
--   hashdiff = md5(concat_ws('||', COALESCE(trim(full_name),''), COALESCE(trim(email),''),
--                  COALESCE(trim(city),''), COALESCE(trim(country),''), COALESCE(trim(loyalty_tier),'')))` },
    effectivity: { lang: 'sql', file: 'notebooks/05_business_vault.sql', code: `-- Effective periods derived from the satellite, in business time.
CREATE OR REPLACE VIEW business_vault.bv_customer_effectivity AS
WITH changes AS (
  SELECT s.*, h.customer_id,
         LAG(struct(city, country, loyalty_tier)) OVER w AS prev_tracked
  FROM raw_vault.sat_customer_details s
  JOIN raw_vault.hub_customer h USING (hk_customer)
  WINDOW w AS (PARTITION BY hk_customer ORDER BY applied_ts, load_ts)
), versions AS (
  SELECT * FROM changes
  WHERE prev_tracked IS NULL
     OR prev_tracked IS DISTINCT FROM struct(city, country, loyalty_tier)
)
SELECT hk_customer, customer_id, city, country, loyalty_tier,
       IF(prev_tracked IS NULL, TIMESTAMP'1900-01-01', applied_ts) AS effective_from,
       COALESCE(LEAD(applied_ts) OVER (PARTITION BY hk_customer ORDER BY applied_ts),
                TIMESTAMP'9999-12-31') AS effective_to,
       LEAD(applied_ts) OVER (PARTITION BY hk_customer ORDER BY applied_ts) IS NULL AS is_current
FROM versions;` },
    bv_order: { lang: 'sql', file: 'notebooks/05_business_vault.sql', code: `-- Soft business rules live here, not in the raw vault.
CREATE OR REPLACE VIEW business_vault.bv_order_current AS
SELECT h.order_id, l.hk_order, l.hk_customer, l.hk_event,
       s.quantity, s.unit_price, s.status, s.order_ts,
       l.hk_customer <> repeat('0', 32) AND s.quantity > 0 AS passes_rules
FROM raw_vault.link_order_customer_event l
JOIN raw_vault.hub_order h USING (hk_order)
JOIN raw_vault.sat_order_details s USING (hk_order)
QUALIFY ROW_NUMBER() OVER (PARTITION BY l.hk_order ORDER BY s.applied_ts DESC, s.load_ts DESC) = 1;` },
    pit: { lang: 'sql', file: 'notebooks/05_business_vault.sql', code: `-- Point-in-time table: for each key and snapshot date, which satellite row was valid.
-- Queries join on equality instead of running range lookups every time.
CREATE OR REPLACE TABLE business_vault.pit_customer AS
SELECT h.hk_customer, d.snapshot_date,
       MAX_BY(s.load_ts, s.applied_ts)  AS sat_customer_load_ts,
       MAX(s.applied_ts)                AS sat_customer_applied_ts
FROM raw_vault.hub_customer h
CROSS JOIN (SELECT DISTINCT CAST(order_ts AS DATE) AS snapshot_date
            FROM business_vault.bv_order_current) d
LEFT JOIN raw_vault.sat_customer_details s
  ON s.hk_customer = h.hk_customer
 AND s.applied_ts < d.snapshot_date + INTERVAL 1 DAY
GROUP BY ALL;` },
  };
})(typeof window !== 'undefined' ? (window.LML = window.LML || {}) : (global.LML = global.LML || {}));
