# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Land the next batch: production happens
# MAGIC
# MAGIC Writes batch 2 or batch 3 to the landing volume. Each record is a realistic problem; after landing,
# MAGIC rerun `01` to `06` and compare the tables with the expectations below. The playground lets you try
# MAGIC the same events interactively.
# MAGIC
# MAGIC | Batch | Record | What should happen |
# MAGIC |---|---|---|
# MAGIC | 2 | Amara moves Berlin → Lisbon | New `dim_customer` version; Berlin version closed; old orders stay in Berlin |
# MAGIC | 2 | Amara's move is delivered twice | Bronze keeps both; Silver and the satellite store it once |
# MAGIC | 2 | A delayed change: Amara lived in Austin two hours before the move | Inserted into history; current city stays Lisbon |
# MAGIC | 2 | O-9004 is paid | Fact row updated in place (status PAID) |
# MAGIC | 2 | O-9006 with no customer and quantity −2 | Quarantined in Silver; kept in the raw vault on the zero key, failed in the business vault |
# MAGIC | 2 | O-9007 carries `promo_code` and `sales_channel` | New fields land in `_rescued_data` |
# MAGIC | 2 | O-9008 for customer C-2001, who doesn't exist yet | Fact points to the Unknown member; `hub_customer` already has C-2001 |
# MAGIC | 3 | C-2001's customer record arrives | The O-9008 fact row is re-pointed to the real customer |

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

dbutils.widgets.dropdown("batch", "2", ["2", "3"], "Batch to land")
batch = int(dbutils.widgets.get("batch"))

import json

def put(entity, records):
    body = "\n".join(json.dumps(r) for r in records)
    dbutils.fs.put(f"{LANDING_PATH}/{entity}/batch_{batch:03d}.json", body, overwrite=True)
    print(f"batch {batch}: {len(records):>2} {entity} records")

if batch == 2:
    amara = dict(customer_id="C-1001", full_name="Amara Okafor", email="amara.okafor@example.com", loyalty_tier="Silver")
    put("customers", [
        dict(op="UPDATE", **amara, city="Lisbon", country="PT", updated_at="2026-03-02T13:15:00Z"),
        dict(op="UPDATE", **amara, city="Lisbon", country="PT", updated_at="2026-03-02T13:15:00Z"),  # duplicate delivery
        dict(op="UPDATE", **amara, city="Austin", country="US", updated_at="2026-03-02T11:15:00Z"),  # late: older than the move
    ])
    put("orders", [
        dict(op="UPDATE", order_id="O-9004", customer_id="C-1001", event_id="E-502", quantity=2, unit_price=65.00,
             status="PAID", order_ts="2026-03-02T03:00:00Z", updated_at="2026-03-02T15:00:00Z"),
        dict(op="INSERT", order_id="O-9006", customer_id=None, event_id="E-503", quantity=-2, unit_price=120.00,
             status="PAID", order_ts="2026-03-02T16:00:00Z", updated_at="2026-03-02T16:00:00Z"),
        dict(op="INSERT", order_id="O-9007", customer_id="C-1003", event_id="E-501", quantity=2, unit_price=89.00,
             status="PLACED", order_ts="2026-03-02T17:00:00Z", updated_at="2026-03-02T17:00:00Z",
             promo_code="SPRING10", sales_channel="mobile_app"),
        dict(op="INSERT", order_id="O-9008", customer_id="C-2001", event_id="E-503", quantity=1, unit_price=120.00,
             status="PAID", order_ts="2026-03-02T18:00:00Z", updated_at="2026-03-02T18:00:00Z"),
    ])
else:
    put("customers", [
        dict(op="INSERT", customer_id="C-2001", full_name="Noah Fischer", email="noah.fischer@example.com",
             city="Berlin", country="DE", loyalty_tier="Bronze", updated_at="2026-03-02T17:00:00Z"),
    ])
