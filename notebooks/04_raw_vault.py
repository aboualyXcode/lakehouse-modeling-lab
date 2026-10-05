# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Raw Vault: hubs, links and satellites (Data Vault 2.0)
# MAGIC
# MAGIC ```
# MAGIC   hub_customer ──┐                       ┌── sat_customer_details
# MAGIC   hub_event ─────┼── link_order_customer_event
# MAGIC   hub_order ─────┘                       └── sat_order_details, sat_event_details
# MAGIC ```
# MAGIC
# MAGIC **Raw vault rules**
# MAGIC - Load from **Bronze**, not Silver. Only hard rules apply (types, hashing); soft business rules belong in
# MAGIC   the business vault. Bad records are kept: a missing business key maps to the **zero key** (`000…0`).
# MAGIC - **Insert-only.** Nothing is updated or deleted, so the vault is a complete, auditable history.
# MAGIC - **Hash keys** are `md5(upper(trim(business_key)))`: deterministic, so every source and every load
# MAGIC   computes the same key independently, in parallel, without lookups.
# MAGIC - **Satellites** store a row only when the **hashdiff** (a hash of the descriptive columns) changes.
# MAGIC   The comparison is against the row just before it in *business* time (`applied_ts`). Comparing against the
# MAGIC   latest *loaded* row drops late-arriving changes, a well-known pitfall that the tests in this repo catch.
# MAGIC - Every row carries `load_ts` (when the warehouse learned it) and `record_source` (where it came from).
# MAGIC
# MAGIC The loads are generated from metadata. Real vaults have hundreds of these tables; nobody writes them by hand.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

ZERO_KEY = "repeat('0', 32)"

def bk(col):
    """Normalized business key: trimmed, upper-cased, empty treated as missing."""
    return f"upper(NULLIF(trim({col}), ''))"

def hash_key(col):
    return f"COALESCE(md5({bk(col)}), {ZERO_KEY})"

def link_key(cols):
    return "md5(concat_ws('||', " + ", ".join(f"COALESCE({bk(c)}, '-1')" for c in cols) + "))"

def hash_diff(cols):
    return "md5(concat_ws('||', " + ", ".join(f"COALESCE(trim(CAST({c} AS STRING)), '')" for c in cols) + "))"

HUBS = {
    "hub_customer": {"key": "hk_customer", "bk": "customer_id", "sources": [("customers", "customer_id"), ("orders", "customer_id")]},
    "hub_event":    {"key": "hk_event",    "bk": "event_id",    "sources": [("events", "event_id"), ("orders", "event_id")]},
    "hub_order":    {"key": "hk_order",    "bk": "order_id",    "sources": [("orders", "order_id")]},
}
LINKS = {
    "link_order_customer_event": {
        "key": "hk_order_customer_event", "source": "orders",
        "parts": [("hk_order", "hub_order", "order_id"), ("hk_customer", "hub_customer", "customer_id"), ("hk_event", "hub_event", "event_id")],
    },
}
SATS = {
    "sat_customer_details": {"hub": "hub_customer", "source": "customers",
        "cols": {"full_name": "STRING", "email": "STRING", "city": "STRING", "country": "STRING", "loyalty_tier": "STRING"}},
    "sat_event_details": {"hub": "hub_event", "source": "events",
        "cols": {"event_name": "STRING", "artist": "STRING", "venue": "STRING", "city": "STRING", "event_date": "DATE", "base_price": "DECIMAL(10,2)"}},
    "sat_order_details": {"hub": "hub_order", "source": "orders",
        "cols": {"quantity": "INT", "unit_price": "DECIMAL(10,2)", "status": "STRING", "order_ts": "TIMESTAMP"}},
}
RV = RAW_VAULT
record_source = lambda src: f"'stagedoor.ticketing.{src}'"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables
# MAGIC Primary and foreign keys are informational, but they document the model and BI tools use them.

# COMMAND ----------

def run(sql):
    spark.sql(sql)
    return sql

for name, h in HUBS.items():
    run(f"""CREATE TABLE IF NOT EXISTS {RV}.{name} (
      {h['key']} STRING NOT NULL COMMENT 'md5(upper(trim({h['bk']}))); zero key when missing',
      {h['bk']} STRING COMMENT 'Business key as delivered (trimmed)',
      load_ts TIMESTAMP NOT NULL COMMENT 'When the warehouse first saw this key',
      record_source STRING NOT NULL,
      CONSTRAINT {name}_pk PRIMARY KEY ({h['key']}) RELY
    ) CLUSTER BY ({h['key']}) COMMENT 'Hub: one row per business key, insert-only'""")

for name, l in LINKS.items():
    fks = ",\n      ".join(f"CONSTRAINT {name}_{hub}_fk FOREIGN KEY ({col}) REFERENCES {RV}.{hub}" for col, hub, _ in l["parts"])
    run(f"""CREATE TABLE IF NOT EXISTS {RV}.{name} (
      {l['key']} STRING NOT NULL,
      {", ".join(f"{col} STRING NOT NULL" for col, _, _ in l['parts'])},
      load_ts TIMESTAMP NOT NULL, record_source STRING NOT NULL,
      CONSTRAINT {name}_pk PRIMARY KEY ({l['key']}) RELY,
      {fks}
    ) CLUSTER BY ({l['parts'][0][0]}) COMMENT 'Link: one row per unique combination of business keys'""")

for name, s in SATS.items():
    key = HUBS[s["hub"]]["key"]
    run(f"""CREATE TABLE IF NOT EXISTS {RV}.{name} (
      {key} STRING NOT NULL,
      load_ts TIMESTAMP NOT NULL COMMENT 'When the warehouse learned this version',
      applied_ts TIMESTAMP NOT NULL COMMENT 'When it happened in the source (business time)',
      hashdiff STRING NOT NULL,
      {", ".join(f"{c} {t}" for c, t in s['cols'].items())},
      record_source STRING NOT NULL,
      CONSTRAINT {name}_pk PRIMARY KEY ({key}, load_ts, applied_ts),
      CONSTRAINT {name}_hub_fk FOREIGN KEY ({key}) REFERENCES {RV}.{s['hub']}
    ) CLUSTER BY ({key}, applied_ts) COMMENT 'Satellite: descriptive history, insert-only'""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hubs: every business key, once
# MAGIC Orders mention customers and events, so the order feed loads those hubs too. That is why an order for a
# MAGIC customer the CRM has not sent yet still has a customer hub row: the vault knows the customer exists.

# COMMAND ----------

def load_hub(name):
    h = HUBS[name]
    staged = " UNION ALL ".join(
        f"SELECT {hash_key(col)} AS {h['key']}, NULLIF(trim({col}), '') AS {h['bk']}, _ingested_at AS load_ts, {record_source(src)} AS record_source FROM {BRONZE}.{src}"
        for src, col in h["sources"])
    return run(f"""MERGE INTO {RV}.{name} AS t
      USING (SELECT * FROM ({staged}) QUALIFY ROW_NUMBER() OVER (PARTITION BY {h['key']} ORDER BY load_ts) = 1) AS s
      ON t.{h['key']} = s.{h['key']}
      WHEN NOT MATCHED THEN INSERT *""")

for name in HUBS:
    load_hub(name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Links: which keys occurred together

# COMMAND ----------

def load_link(name):
    l = LINKS[name]
    cols = ", ".join(f"{hash_key(bkcol)} AS {col}" for col, _, bkcol in l["parts"])
    return run(f"""MERGE INTO {RV}.{name} AS t
      USING (
        SELECT {link_key([bkcol for _, _, bkcol in l['parts']])} AS {l['key']}, {cols},
               _ingested_at AS load_ts, {record_source(l['source'])} AS record_source
        FROM {BRONZE}.{l['source']}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {l['key']} ORDER BY load_ts) = 1
      ) AS s
      ON t.{l['key']} = s.{l['key']}
      WHEN NOT MATCHED THEN INSERT *""")

for name in LINKS:
    load_link(name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Satellites: insert a row when the hashdiff changes, in business time
# MAGIC Staged rows are unioned with the existing rows for the same keys and ordered by `applied_ts`.
# MAGIC A staged row is inserted only if its hashdiff differs from the row before it. Existing rows sort first on
# MAGIC ties, so re-delivered duplicates and reruns insert nothing.

# COMMAND ----------

def load_sat(name):
    s = SATS[name]
    h = HUBS[s["hub"]]
    key, cols = h["key"], list(s["cols"])
    staged = f"""SELECT DISTINCT {hash_key(h['bk'])} AS {key}, _ingested_at AS load_ts, updated_at AS applied_ts,
                 {hash_diff(cols)} AS hashdiff, {", ".join(cols)}, {record_source(s['source'])} AS record_source
               FROM {BRONZE}.{s['source']}"""
    out = ", ".join([key, "load_ts", "applied_ts", "hashdiff"] + cols + ["record_source"])
    return run(f"""INSERT INTO {RV}.{name}
      SELECT {out} FROM (
        SELECT *, LAG(hashdiff) OVER (PARTITION BY {key} ORDER BY applied_ts, is_new, load_ts) AS prev_hashdiff
        FROM (
          SELECT *, true AS is_new FROM ({staged})
          UNION ALL
          SELECT {out}, false AS is_new FROM {RV}.{name}
        )
      )
      WHERE is_new AND prev_hashdiff IS DISTINCT FROM hashdiff""")

for name in SATS:
    load_sat(name)

# COMMAND ----------

# The generated expressions, for reference
print("hk_customer =", hash_key("customer_id"))
print("hk_order_customer_event =", link_key(["order_id", "customer_id", "event_id"]))
print("sat_customer_details.hashdiff =", hash_diff(list(SATS["sat_customer_details"]["cols"])))

# COMMAND ----------

display(spark.sql(f"""
  SELECT 'hubs' AS kind, 'hub_customer' AS table_name, COUNT(*) AS row_count FROM {RV}.hub_customer UNION ALL
  SELECT 'hubs', 'hub_event', COUNT(*) FROM {RV}.hub_event UNION ALL
  SELECT 'hubs', 'hub_order', COUNT(*) FROM {RV}.hub_order UNION ALL
  SELECT 'links', 'link_order_customer_event', COUNT(*) FROM {RV}.link_order_customer_event UNION ALL
  SELECT 'satellites', 'sat_customer_details', COUNT(*) FROM {RV}.sat_customer_details UNION ALL
  SELECT 'satellites', 'sat_event_details', COUNT(*) FROM {RV}.sat_event_details UNION ALL
  SELECT 'satellites', 'sat_order_details', COUNT(*) FROM {RV}.sat_order_details
"""))
