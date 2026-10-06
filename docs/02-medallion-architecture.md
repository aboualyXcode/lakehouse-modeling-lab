# 2. The Medallion Architecture

The Medallion Architecture organizes a lakehouse into layers of increasing quality: **Bronze**, **Silver** and **Gold**. It is a *data quality and processing* pattern, not a data model. It tells you what each layer is responsible for, not how tables inside it are shaped.

```
  sources ──▶  BRONZE  ──▶  SILVER  ──▶  GOLD  ──▶  BI, ML, apps
               raw          trusted      consumable
               as received  integrated   shaped for use
```

## The layers

### Bronze: what arrived

| | |
|---|---|
| **Contains** | Source data exactly as delivered, plus ingestion metadata (`_source_file`, `_ingested_at`, `_batch_id`). |
| **Rules** | Append only. No filtering, no deduplication, no business logic. Unexpected fields are captured in `_rescued_data`, not dropped. |
| **Why** | Bronze is your replay log. When a Silver rule turns out to be wrong, you fix it and reprocess from Bronze instead of asking the source to resend a year of data. |
| **Stagedoor** | `stagedoor_bronze.customers`, `.events`, `.orders`, loaded by Auto Loader (`notebooks/01_bronze_ingest.py`). |

### Silver: what is true

| | |
|---|---|
| **Contains** | Validated, typed, standardized and deduplicated data, integrated across sources. Usually both change history and current state. |
| **Rules** | Every rule is explicit and testable. Records that fail go to a quarantine table, or are counted by pipeline expectations, never silently dropped. Sequence by business time. |
| **Why** | This is the trusted, reusable foundation. Most teams build from Silver, and the same customer table serves many Gold products. |
| **Stagedoor** | `*_changes` (history), `customers`/`events`/`orders` (current), `quarantine` (`notebooks/02_silver_conform.sql`). The raw and business vault also live at Silver level. |

### Gold: what people use

| | |
|---|---|
| **Contains** | Data shaped for a purpose: star schemas, aggregates, feature tables, data products. |
| **Rules** | Named and documented for consumers. Stable contracts: renaming a Gold column is a breaking change. |
| **Why** | Consumers should never need to know how sources work. |
| **Stagedoor** | `stagedoor_gold.dim_customer`, `dim_event`, `dim_date`, `fact_ticket_sales` (`notebooks/03_gold_star_schema.sql`). |

## Where the models go

The layers say nothing about modeling, so here is how the common models map onto them:

| Pattern | Bronze | Silver | Gold | Use when |
|---|---|---|---|---|
| **Medallion + star** | Raw | Cleansed entities, current state and change history | Star schemas | The default for analytics. One or a few sources per domain. |
| **Medallion + Data Vault + star** | Raw | Raw vault, then business vault | Star schemas or views over the vault | Many sources, frequent change, audit requirements. |
| **Medallion + wide tables** | Raw | Cleansed entities | One wide table per use case | Small scope, speed matters, few consumers. |

This repository implements the first two side by side from the same Bronze tables, so you can compare them directly. The playground shows both paths in its lineage diagram.

## Organizing it in Unity Catalog

Unity Catalog has a three-level namespace: `catalog.schema.table`. Common layouts:

| Layout | Example | Good for |
|---|---|---|
| Catalog per environment, schema per layer | `prod.silver.customers` | Small platforms, one domain |
| Catalog per environment, schema per layer and domain | `prod.sales_gold.fact_orders` | Most organizations |
| Catalog per domain, schema per layer | `sales.gold.fact_orders` | Data mesh, strong domain ownership |

This repository uses a prefix (`stagedoor_bronze`, `stagedoor_silver`, …) so it can run in any existing catalog without colliding with your tables.

Grant access by layer: engineers write everywhere, analysts read Gold, data scientists read Silver and Gold. Few people should ever query Bronze directly.

## Reprocessing

The main operational benefit of Medallion is that every layer can be rebuilt from the one below it:

- A Silver rule was wrong → fix it, rebuild Silver from Bronze, then Gold from Silver.
- A new Gold product is needed → build it from Silver; nothing upstream changes.
- A source sent bad data for a week → it is in Bronze, tagged with `_batch_id`; quarantine or replay those batches.

Design for this: keep Bronze long enough (months, or forever for small sources), make Silver and Gold idempotent, and avoid manual fixes in Gold that a rebuild would erase.

## Anti-patterns

- **Cleaning in Bronze.** Filtering or deduplicating on ingest destroys the replay log. Bronze should be boring.
- **Silver as a copy of Bronze.** If Silver adds no validation or integration, the layer is overhead.
- **Gold as a dumping ground.** Hundreds of one-off Gold tables with overlapping logic recreate the problem Gold was meant to solve. Conform dimensions and reuse them.
- **Too many layers.** "Silver 1", "Silver 2" and "Platinum" usually mean responsibilities were never defined. Name layers by what they guarantee.
- **Skipping Silver for speed.** Gold built straight from Bronze duplicates cleaning logic in every product and makes numbers disagree.

## In the playground

Open the **Pipeline** tab and queue a duplicate delivery, a bad record and schema drift, then run the pipeline. Bronze grows by every record. Silver absorbs the duplicate and quarantines the bad record. The unexpected fields appear in Bronze's `_rescued_data`. Gold never sees any of it.
