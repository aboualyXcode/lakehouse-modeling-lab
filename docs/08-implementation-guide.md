# 8. Implementation guide

This chapter has two parts: how to run this repository on Databricks, and how to apply the same approach to a real domain of your own.

## Part 1: run this repository

### Requirements

- A Databricks workspace with **Unity Catalog**.
- **Serverless compute**, or a cluster on Databricks Runtime 15.4 LTS or later.
- On the catalog you use: `USE CATALOG`, `CREATE SCHEMA` and `CREATE VOLUME`. Everything is created in schemas prefixed `stagedoor_`, so an existing catalog such as `main` works without collisions.

### Option A: from the workspace UI

1. In your workspace, open **Workspace → Create → Git folder** and clone this repository.
2. Open `notebooks/00_setup.py`, set the **catalog** widget, attach compute and run it. It creates the schemas, a volume, and the first batch of source files.
3. Run the notebooks in order: `01_bronze_ingest` → `02_silver_conform` → `03_gold_star_schema` → `04_raw_vault` → `05_business_vault` → `06_validate`. Each notebook explains what it does and why.
4. Open `07_land_next_batch`, choose batch **2**, and run it. This lands the production problems: a move, a duplicate, a late change, a bad record, schema drift and an early-arriving order.
5. Run `01` to `06` again and compare the tables with the table at the top of notebook 07. Then land batch **3** (the late customer arrives) and run them once more.

### Option B: with Databricks Asset Bundles

```sh
# Install the Databricks CLI and authenticate once
databricks auth login --host https://<your-workspace>

databricks bundle validate
databricks bundle deploy -t dev --var catalog=<your_catalog>
databricks bundle run lab_notebooks -t dev          # notebooks 00–06 as one job
```

The job runs the star schema and the vault in parallel after Bronze, then validates both. In `dev` mode, deployed resources are prefixed with your user name, so several people can deploy the same bundle.

To land more batches, run `notebooks/07_land_next_batch.py` from the workspace, then run the job again (task `setup` is idempotent).

### The declarative version

`pipelines/medallion_pipeline.sql` builds the same Medallion flow as a Lakeflow pipeline, with expectations and `AUTO CDC`. After running `00_setup` once to create the landing files:

```sh
databricks bundle run medallion_pipeline -t dev
```

It writes to the schema `stagedoor_ldp`. Land batch 2 and 3 and update the pipeline again; compare `dim_customer_history` with `stagedoor_gold.dim_customer` from the notebooks.

### What to look at

| After | Look at | You should see |
|---|---|---|
| Batch 1 | `stagedoor_gold.fact_ticket_sales` | 5 orders, all linked to real customers |
| Batch 2 | `stagedoor_bronze.orders` | O-9007 with `promo_code` in `_rescued_data` |
| Batch 2 | `stagedoor_silver.quarantine` | O-9006 with two reasons |
| Batch 2 | `stagedoor_gold.dim_customer` for C-1001 | Three versions: Berlin, Austin (the late change), Lisbon |
| Batch 2 | `stagedoor_gold.fact_ticket_sales` for O-9008 | `customer_sk = -1` |
| Batch 2 | `stagedoor_raw_vault.hub_customer` | C-2001 already present, and the zero key |
| Batch 3 | `stagedoor_gold.fact_ticket_sales` for O-9008 | Re-pointed to Noah Fischer |
| Every batch | `06_validate` | All checks pass, including star vs vault reconciliation |

### Clean up

```sql
DROP SCHEMA IF EXISTS stagedoor_business_vault CASCADE;
DROP SCHEMA IF EXISTS stagedoor_raw_vault CASCADE;
DROP SCHEMA IF EXISTS stagedoor_gold CASCADE;
DROP SCHEMA IF EXISTS stagedoor_silver CASCADE;
DROP SCHEMA IF EXISTS stagedoor_bronze CASCADE;
DROP SCHEMA IF EXISTS stagedoor_ldp CASCADE;
DROP SCHEMA IF EXISTS stagedoor_landing CASCADE;   -- also deletes the volume and its files
```

Then `databricks bundle destroy -t dev` if you deployed the bundle.

## Part 2: apply it to your own domain

### Step 1: start from the questions

List the ten questions the business most needs answered, and who asks them. They define the Gold layer, and therefore the facts, the grains and the conformed dimensions. Draw a **bus matrix**.

### Step 2: inventory the sources

For each source: what entities it describes, its business keys, whether it provides CDC or only snapshots, how late and how duplicated its data can be, and who owns it. Overlapping keys across sources are the signal for a Data Vault ([chapter 6](06-choosing-an-approach.md)).

### Step 3: decide the architecture and the models

Write a one-page decision record:

- Unity Catalog layout (catalogs, schemas, volumes).
- What goes in Silver: cleansed entities, or a raw and business vault.
- What goes in Gold: which star schemas, which wide tables.
- Which attributes are SCD Type 2, and which are Type 1.
- Notebooks and jobs, or declarative pipelines. For new work, prefer declarative pipelines unless you need control they do not offer.

### Step 4: build Bronze for everything

Ingest every source raw, with metadata columns and a rescued data column, before modeling anything. It is cheap, it starts history accumulating, and it lets you explore the real data rather than the documented data.

### Step 5: build Silver with tests

Typed, standardized, deduplicated, with quality rules, quarantine, change history and current state. Write the tests as you go: uniqueness, completeness, sequence guards.

### Step 6: build one Gold product end to end

One business process, one fact table, its dimensions, and one real dashboard used by real people. Resist building every dimension first. A complete thin slice teaches you more than a broad unfinished layer.

### Step 7: automate and deploy

- Put everything in Git, with an Asset Bundle per project and `dev` / `staging` / `prod` targets.
- Run tests in CI on every pull request: `databricks bundle validate`, unit tests for transformation logic, and a deployment to `dev` that runs the validation notebook.
- Deploy to `prod` from the main branch only, using a service principal, never a personal account.

### Step 8: operate it

- Alerts on job failures, expectation violations and quarantine growth.
- Freshness SLAs per Gold table, monitored.
- An owner for every table, and a postmortem for every data incident.

## Project structure for your own repository

```
my-domain/
├── databricks.yml               bundle: jobs, pipelines, targets
├── resources/                   one YAML file per job or pipeline (for larger projects)
├── src/
│   ├── bronze/                  ingestion
│   ├── silver/                  cleansing, or the raw and business vault
│   ├── gold/                    stars and data products
│   └── pipelines/               declarative pipeline sources
├── tests/
│   ├── unit/                    transformation logic, runnable locally
│   └── validation/              data tests run after each load
└── docs/
    ├── decisions/               one record per modeling decision
    └── bus-matrix.md
```

## Checklist before going to production

- [ ] Every table has a declared grain, an owner and a comment.
- [ ] Every key is tested for uniqueness; every foreign key for integrity.
- [ ] Late, duplicate and malformed data have been tested, not just assumed.
- [ ] Every load is idempotent.
- [ ] Bronze retention allows a full rebuild of Silver.
- [ ] Rejected records are quarantined with reasons and monitored.
- [ ] Sensitive columns are tagged and masked; deletion requests have a process.
- [ ] Deployment is automated and runs as a service principal.
- [ ] Consumers know which Gold tables are the contract, and how changes are announced.
