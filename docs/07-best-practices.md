# 7. Best practices

These are the practices that most often separate a model people trust from one they work around. Each one is applied somewhere in this repository; the file is named so you can see it in context.

## Naming

Pick conventions once, document them, and never mix styles.

| Object | Convention | Example |
|---|---|---|
| Schemas | `<domain>_<layer>` or `<layer>` | `stagedoor_silver` |
| Bronze tables | Source table name | `bronze.orders` |
| Silver tables | Business entity, plural | `silver.customers`, `silver.customer_changes` |
| Dimensions | `dim_<entity>`, singular | `dim_customer` |
| Facts | `fact_<process>` | `fact_ticket_sales` |
| Hubs, links, satellites | `hub_<entity>`, `link_<a>_<b>`, `sat_<entity>_<descriptor>` | `sat_customer_details` |
| Business vault | `bv_<name>`, `pit_<hub>`, `bridge_<name>` | `pit_customer` |
| Surrogate keys | `<entity>_sk` | `customer_sk` |
| Hash keys | `hk_<entity>` | `hk_customer` |
| Technical columns | Leading underscore | `_ingested_at`, `_batch_id` |
| Timestamps | `_ts` or `_at` suffix, always UTC | `order_ts`, `updated_at` |
| Booleans | `is_` or `has_` prefix | `is_current` |

Use `snake_case` everywhere. Avoid abbreviations nobody outside your team knows.

## Keys and grain

- **Declare every table's grain** in its table comment (`03_gold_star_schema.sql`).
- **Test uniqueness** at the grain, every load (`06_validate.sql`). Unenforced keys that are never tested will eventually be wrong.
- **Standardize business keys before using them:** trim, consistent case, empty as missing.
- **Never let a missing key drop a row.** Map it to the Unknown member (`-1`) or the zero key, and decide in a business rule whether it counts.
- **Never reuse or change a surrogate key.** Identity columns guarantee this; manual `ROW_NUMBER()` keys do not.

## Time

- **Store timestamps in UTC**, and convert to local time only in presentation.
- **Sequence by business time**, never by arrival order: `SEQUENCE BY updated_at`, `ORDER BY applied_ts`.
- **Keep system time too** (`_ingested_at`, `load_ts`) so you can explain what a report showed on a given day.
- **Use half-open intervals** for validity: `valid_from <= t < valid_to`.
- **Open the first version to the beginning of time** (`1900-01-01`) so early facts still join.
- **Expect late and duplicate data.** Every load in this repository is tested against both.

## Idempotency and reprocessing

- Every load should be safe to run twice. Use `MERGE` with explicit match conditions, insert-only `WHEN NOT MATCHED`, and checkpoints for streams.
- Keep Bronze long enough to rebuild Silver from it.
- Never patch Gold by hand. A rebuild erases manual fixes; fix the rule upstream instead.
- Make every derived table reproducible from the layer below.

## Data quality

- **Write rules as code** next to the model: expectations in pipelines, explicit predicates in notebooks.
- **Quarantine, don't drop.** Keep rejected rows with the rule they broke and the original record (`silver.quarantine`), so they can be fixed and replayed.
- **Separate hard and soft rules.** Types and keys are hard rules, applied early. Business validity is a soft rule, applied in Silver or the business vault, where it can change without reloading history.
- **Reconcile across layers:** row counts from Silver to Gold, totals from source to report, and, where you have two models, their answers against each other.
- **Monitor the trend.** A quarantine table that grows by 2% a day is a source problem, even if every individual load passes.

## Performance

- **Liquid clustering** on the columns used for filters and joins: dates and dimension keys on facts, business keys on dimensions, hash keys on vault tables. Or `CLUSTER BY AUTO`.
- **Right-size types:** `DECIMAL(10,2)` for money, `INT` date keys, `DATE` rather than `TIMESTAMP` where time of day is meaningless.
- **Avoid exploding joins:** always join SCD Type 2 dimensions on the validity window, never on the business key alone.
- **Use PIT tables** when vault queries repeatedly look up "the version valid at time t".
- **Materialize** star views over a vault once they are queried heavily.
- **Let the platform maintain tables:** predictive optimization for managed tables; otherwise schedule `OPTIMIZE` and `VACUUM`.

## Governance and security

- **Grant by layer and domain** at schema level. Most users never need Bronze.
- **Tag sensitive columns** (`pii`) and apply column masks or row filters in Unity Catalog, especially on email, names and addresses in `dim_customer` and the customer satellite.
- **Plan for deletion requests.** Insert-only models (Bronze, Data Vault) make "forget this customer" harder. Common approaches: keep personal data in separate satellites or tables that can be deleted or crypto-shredded, and keep Bronze retention short for personal data.
- **Rely on lineage.** Unity Catalog captures table and column lineage automatically; descriptive names and comments make it useful.

## Documentation

- Table and column **comments** for every Gold object, written for consumers.
- A **bus matrix** of business processes and conformed dimensions.
- For each table: grain, keys, refresh frequency, owner and SLA.
- **Postmortems** for data incidents, recorded with the rule or test that would have caught them.

## Testing

| Test | Example in this repository |
|---|---|
| Uniqueness at the grain | `customer_sk`, `order_id`, `hk_customer` in `06_validate.sql` |
| Referential integrity | Every fact key resolves to a dimension row |
| SCD integrity | One current version per key; no gaps or overlaps |
| Completeness | Silver order count equals Gold fact count; every rejection has a reason |
| Idempotency | Rerunning notebooks 02–05 changes nothing |
| Cross-model reconciliation | Star schema and Data Vault agree on revenue by city |
| Randomized scenarios | `tests/model.test.js`: hundreds of random event sequences, invariants checked after every run |

The last row is worth copying. Randomized scenario tests found a real out-of-order bug in this repository that hand-written tests had missed ([chapter 4](04-data-vault-2.md#the-out-of-order-pitfall)).

## Cost

- Prefer incremental processing (streaming tables, `availableNow` triggers, `MERGE` on changed keys) over full rebuilds once data grows.
- Use serverless compute for jobs and pipelines unless you have a reason not to; it removes idle-cluster cost.
- Watch the Data Vault table count: every satellite is a table to load, cluster and maintain.
- Retire unused Gold tables. Lineage and query history show which ones nobody reads.
