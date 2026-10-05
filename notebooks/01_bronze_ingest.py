# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: ingest raw change files with Auto Loader
# MAGIC
# MAGIC **Bronze rules**
# MAGIC - Append only. Never update, never delete, never filter. Bronze is the replayable record of what arrived.
# MAGIC - Keep the source shape. Add metadata (`_source_file`, `_batch_id`, `_ingested_at`), not business logic.
# MAGIC - Never lose data to schema changes: unknown fields go to `_rescued_data`.
# MAGIC
# MAGIC Auto Loader tracks which files it has processed in the checkpoint, so rerunning this notebook only
# MAGIC ingests new files (exactly-once). `trigger(availableNow=True)` processes everything available and stops,
# MAGIC which makes it suitable for scheduled jobs.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F

# Pin the expected columns and types. Anything the source sends that is not in this schema, such as a new
# field, a wrongly typed value or a case mismatch, is captured in _rescued_data instead of failing the stream
# or disappearing silently. Bronze stays stable even when sources drift.
EXPECTED = {
    "customers": "op STRING, customer_id STRING, full_name STRING, email STRING, city STRING, country STRING, loyalty_tier STRING, updated_at TIMESTAMP",
    "events": "op STRING, event_id STRING, event_name STRING, artist STRING, venue STRING, city STRING, event_date DATE, base_price DECIMAL(10,2), updated_at TIMESTAMP",
    "orders": "op STRING, order_id STRING, customer_id STRING, event_id STRING, quantity INT, unit_price DECIMAL(10,2), status STRING, order_ts TIMESTAMP, updated_at TIMESTAMP",
}

def ingest(entity: str):
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(EXPECTED[entity])
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{LANDING_PATH}/{entity}")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.regexp_extract("_metadata.file_name", r"batch_(\d+)", 1).cast("int").alias("_batch_id"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
    return (
        stream.writeStream
        .option("checkpointLocation", f"{CHECKPOINTS}/{entity}/bronze")
        .trigger(availableNow=True)
        .toTable(f"{BRONZE}.{entity}")
    )

for entity in ENTITIES:
    ingest(entity).awaitTermination()
    print(f"{BRONZE}.{entity}: {spark.table(f'{BRONZE}.{entity}').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect
# MAGIC After batch 2, `O-9007` shows its unexpected fields in `_rescued_data`, and Amara's move appears twice.

# COMMAND ----------

display(spark.sql(f"""
  SELECT _batch_id, op, order_id, customer_id, quantity, status, _rescued_data
  FROM {BRONZE}.orders ORDER BY _batch_id, order_id
"""))
