# Glossary

**Accumulating snapshot.** A fact table with one row per process instance (such as an order), updated as milestones happen.

**Applied timestamp (`applied_ts`).** In a Data Vault satellite, when a change was true in the source. Business time.

**Auto Loader.** Databricks' incremental file ingestion (`cloudFiles`), which processes each new file exactly once.

**AUTO CDC.** A declarative pipeline API that applies change data to a target as SCD Type 1 or 2, handling out-of-order records. Replaces `APPLY CHANGES`.

**Bitemporal.** Tracking both business time and system time for every change.

**Bridge table.** A Data Vault query helper that precomputes paths across several links.

**Bronze.** The Medallion layer holding raw data exactly as received, append-only.

**Business key.** The identifier the business uses, such as `C-1001`.

**Business vault.** The Data Vault layer that applies soft business rules and adds query helpers on top of the raw vault.

**Bus matrix.** A grid of business processes against the conformed dimensions they use.

**CDC (change data capture).** Delivering every insert, update and delete from a source as a stream of change records.

**Conformed dimension.** A dimension shared, with one meaning, by every fact table that uses it.

**Degenerate dimension.** A dimension attribute stored directly on the fact, with no dimension table, such as `order_id`.

**Early-arriving fact.** A fact whose dimension record has not arrived yet. Pointed to the Unknown member until it does.

**Effectivity satellite.** A satellite recording when a relationship or record was active.

**Expectation.** A data-quality rule on a declarative pipeline dataset: record, drop the row, or fail.

**Fact table.** A table of measurements at a declared grain, with keys to dimensions.

**Gold.** The Medallion layer of consumption-ready data: star schemas, aggregates, data products.

**Grain.** What one row of a table represents.

**Hash key.** A deterministic key computed by hashing a business key, used by Data Vault.

**Hashdiff.** A hash of a satellite's descriptive columns, used to detect changes.

**Hub.** A Data Vault table holding each business key once.

**Identity column.** A Delta column that generates unique values on insert; used for surrogate keys.

**Late-arriving data.** Records that arrive after records describing later events. Must be sequenced by business time.

**Link.** A Data Vault table recording relationships between business keys.

**Liquid clustering.** Delta's data layout optimization (`CLUSTER BY`), replacing partitioning and Z-order for most tables.

**Load timestamp (`load_ts`).** When the platform learned about a record. System time.

**Materialized view.** A stored query result, refreshed incrementally where possible.

**Medallion Architecture.** Organizing a lakehouse into Bronze, Silver and Gold layers of increasing quality.

**Metric view.** A Unity Catalog object defining measures and dimensions once for all consumers.

**One big table (OBT).** A wide, denormalized table that pre-joins facts and dimensions for a use case.

**PIT (point-in-time) table.** A Data Vault helper listing, per key and snapshot date, which satellite row was valid.

**Quarantine.** A table holding rejected records with the reasons they failed.

**Raw vault.** The Data Vault layer of hubs, links and satellites loaded with hard rules only.

**Record source.** The system a Data Vault row came from.

**Rescued data column.** A column holding fields that did not match the expected schema, instead of failing or dropping them.

**Satellite.** A Data Vault table holding descriptive attributes and their history.

**SCD (slowly changing dimension).** A strategy for handling change in dimension attributes. Type 1 overwrites; Type 2 adds a version.

**Sequence guard.** A condition that stops older changes from overwriting newer ones, such as `WHEN MATCHED AND s.updated_at > t.updated_at`.

**Silver.** The Medallion layer of validated, deduplicated, integrated data.

**Star schema.** A fact table surrounded by denormalized dimension tables.

**Streaming table.** A Delta table fed incrementally by a stream, typically in a declarative pipeline.

**Surrogate key.** A meaningless, stable key for a dimension row, one per version in SCD Type 2.

**Unknown member.** A dimension row (key `-1`) that facts point to when their real dimension row is missing.

**Unity Catalog.** Databricks' governance layer with the `catalog.schema.table` namespace.

**Zero key.** The Data Vault hash key (`000…0`) used when a business key is missing.
