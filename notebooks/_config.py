# Databricks notebook source
# MAGIC %md
# MAGIC # Shared configuration
# MAGIC Every notebook runs this first with `%run ./_config`. It reads the `catalog` widget (or job parameter),
# MAGIC sets it as the current catalog, and defines the schema names used everywhere:
# MAGIC
# MAGIC | Schema | Layer | Contents |
# MAGIC |---|---|---|
# MAGIC | `stagedoor_landing` | Landing | A Unity Catalog volume with the raw JSON change files |
# MAGIC | `stagedoor_bronze` | Bronze | Append-only Delta tables, one per source table |
# MAGIC | `stagedoor_silver` | Silver | Validated change history, current state, quarantine |
# MAGIC | `stagedoor_gold` | Gold | The star schema |
# MAGIC | `stagedoor_raw_vault` | Silver | Data Vault 2.0 hubs, links and satellites |
# MAGIC | `stagedoor_business_vault` | Silver/Gold | Business rules, effectivity, PIT, and star views over the vault |

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog catalog")
CATALOG = dbutils.widgets.get("catalog").strip()
PREFIX = "stagedoor"
LANDING, BRONZE, SILVER, GOLD, RAW_VAULT, BUSINESS_VAULT = (
    f"{PREFIX}_{name}" for name in ["landing", "bronze", "silver", "gold", "raw_vault", "business_vault"]
)
LANDING_PATH = f"/Volumes/{CATALOG}/{LANDING}/files/raw"
CHECKPOINTS = f"/Volumes/{CATALOG}/{LANDING}/files/_checkpoints"
ENTITIES = ["customers", "events", "orders"]

spark.sql(f"USE CATALOG `{CATALOG}`")
print(f"Using catalog {CATALOG}; landing files at {LANDING_PATH}")
