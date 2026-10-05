# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup: schemas, volume and the first batch of source data
# MAGIC
# MAGIC Creates one schema per layer and a volume for landing files, then writes **batch 1**: the initial
# MAGIC replication of Stagedoor's ticketing database as JSON change records (CDC), one file per source table.
# MAGIC
# MAGIC Run order: `00_setup` → `01_bronze_ingest` → `02_silver_conform` → `03_gold_star_schema` →
# MAGIC `04_raw_vault` → `05_business_vault` → `06_validate`. Then land more batches with
# MAGIC `07_land_next_batch` and run 01–06 again to watch the models react.
# MAGIC
# MAGIC **Requirements:** a Unity Catalog workspace, serverless or DBR 15.4 LTS+ compute, and
# MAGIC `CREATE SCHEMA` and `CREATE VOLUME` on the chosen catalog.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

for schema in [LANDING, BRONZE, SILVER, GOLD, RAW_VAULT, BUSINESS_VAULT]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}` COMMENT 'Lakehouse Modeling Lab'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{LANDING}`.files COMMENT 'Landing zone for raw change files'")
for entity in ENTITIES:
    dbutils.fs.mkdirs(f"{LANDING_PATH}/{entity}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The source data
# MAGIC Every record is a change event: an operation (`op`), the full row after the change, and `updated_at`,
# MAGIC the time the change happened in the source system. `updated_at` is what we sequence by. Arrival time is not.

# COMMAND ----------

import json

def write_batch(batch_id: int, records: list) -> None:
    """Write one JSON-lines file per source table, like a CDC connector dropping files."""
    for entity in ENTITIES:
        rows = [r for r in records if r["_table"] == entity]
        if not rows:
            continue
        body = "\n".join(json.dumps({k: v for k, v in r.items() if k != "_table"}) for r in rows)
        dbutils.fs.put(f"{LANDING_PATH}/{entity}/batch_{batch_id:03d}.json", body, overwrite=True)
        print(f"batch {batch_id}: {len(rows):>2} {entity} records")

def customer(cid, name, email, city, country, tier, ts, op="INSERT"):
    return dict(_table="customers", op=op, customer_id=cid, full_name=name, email=email, city=city,
                country=country, loyalty_tier=tier, updated_at=ts)

def event(eid, name, artist, venue, city, date, price, ts, op="INSERT"):
    return dict(_table="events", op=op, event_id=eid, event_name=name, artist=artist, venue=venue,
                city=city, event_date=date, base_price=price, updated_at=ts)

def order(oid, cid, eid, qty, price, status, order_ts, ts=None, op="INSERT", **extra):
    return dict(_table="orders", op=op, order_id=oid, customer_id=cid, event_id=eid, quantity=qty,
                unit_price=price, status=status, order_ts=order_ts, updated_at=ts or order_ts, **extra)

batch_1 = [
    customer("C-1001", "Amara Okafor", "amara.okafor@example.com", "Berlin", "DE", "Silver", "2026-01-31T09:00:00Z"),
    customer("C-1002", "Diego Ramos", "diego.ramos@example.com", "Lisbon", "PT", "Bronze", "2026-01-31T10:00:00Z"),
    customer("C-1003", "Hana Sato", "hana.sato@example.com", "Osaka", "JP", "Gold", "2026-01-31T11:00:00Z"),
    customer("C-1004", "Layla Hassan", "layla.hassan@example.com", "Cairo", "EG", "Bronze", "2026-01-31T12:00:00Z"),
    event("E-501", "Northern Lights Tour", "Aurora Vale", "Olympia Hall", "Berlin", "2026-05-14", 89.00, "2026-02-07T13:00:00Z"),
    event("E-502", "Midnight Static", "The Ferrymen", "Coliseu", "Lisbon", "2026-06-02", 65.00, "2026-02-07T14:00:00Z"),
    event("E-503", "Harbor Nights", "Kaito & Mira", "Zepp Bayside", "Osaka", "2026-07-19", 120.00, "2026-02-07T15:00:00Z"),
    order("O-9001", "C-1001", "E-501", 2, 89.00, "PAID", "2026-02-28T09:00:00Z"),
    order("O-9002", "C-1002", "E-502", 1, 65.00, "PAID", "2026-02-28T17:00:00Z"),
    order("O-9003", "C-1003", "E-503", 4, 120.00, "PAID", "2026-03-01T03:00:00Z"),
    order("O-9004", "C-1001", "E-502", 2, 65.00, "PLACED", "2026-03-02T03:00:00Z"),
    order("O-9005", "C-1004", "E-501", 3, 89.00, "REFUNDED", "2026-03-01T13:00:00Z"),
]
write_batch(1, batch_1)
