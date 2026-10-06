# 1. Foundations

Before choosing between a star schema and a Data Vault, it helps to separate two questions that are often mixed up:

- **Architecture:** how data moves and is refined. The Medallion Architecture (Bronze, Silver, Gold) is an architecture.
- **Model:** how data is shaped inside a layer. A star schema and a Data Vault are models.

You will almost always use both: an architecture to organize the platform, and one or more models inside its layers. The rest of this guide keeps the two apart.

## The running example

Stagedoor sells concert tickets. Its ticketing database has three tables, and a change-data-capture (CDC) connector sends every insert and update as a JSON record:

| Source table | Business key | Changes over time |
|---|---|---|
| `customers` | `customer_id` | Customers move, change tier, change email |
| `events` | `event_id` | Prices change |
| `orders` | `order_id` | Status goes PLACED → PAID → REFUNDED |

Everything in this repository, from the playground to the notebooks, models these three tables.

## Grain: the most important decision

The **grain** of a table is what one row represents. Declare it in one sentence before anything else:

- `fact_ticket_sales`: one row per order.
- `dim_customer`: one row per customer per version of their city, country and tier.
- `sat_customer_details`: one row per change in a customer's descriptive attributes.

Most modeling bugs are grain bugs: a join that silently multiplies rows, a "daily" table that sometimes has two rows per day, an aggregate that double counts. If you cannot state a table's grain, you cannot test it.

## Keys

| Kind | Example | Use it for |
|---|---|---|
| **Business (natural) key** | `C-1001` | Identifying things the way the business does. Can change, collide across systems, or arrive malformed. |
| **Surrogate key** | `customer_sk = 42` | Joining facts to dimensions in a star schema. Meaningless, stable, one per dimension row (so one per *version* in SCD Type 2). On Databricks: an identity column. |
| **Hash key** | `md5(upper(trim('C-1001')))` | Data Vault. Deterministic, so any process can compute it without a lookup, which allows parallel loading. |
| **Hashdiff** | `md5(name‖email‖city…)` | Detecting whether descriptive attributes changed, by comparing one value instead of every column. |

Rules of thumb:

- Never use a surrogate key as a business identifier, and never expose it to users.
- Standardize business keys before hashing (trim, upper-case, treat empty as missing). `'c-1001 '` and `'C-1001'` must produce the same key.
- Decide explicitly what a missing key becomes: the Unknown member (`-1`) in a star, the zero key (`000…0`) in a vault.

## Normalized and denormalized

- **Normalized** models (third normal form, or a Data Vault) store each fact once. They are good for integrity and integration, and need many joins to query.
- **Denormalized** models (a star schema, or a one-big-table) repeat descriptive data to make queries simple and fast.

On Databricks, storage is cheap and joins on well-clustered Delta tables are fast, so the trade-off is less about performance than it used to be. It is now mostly about **change**: how hard it is to add a source, fix history, or answer a new question.

## History

Data changes. The question is which history you need:

| Approach | What it keeps | Typical use |
|---|---|---|
| **SCD Type 0** | The original value forever | Date of birth, signup channel |
| **SCD Type 1** | Only the latest value | Corrections, email, name |
| **SCD Type 2** | A new row per change, with validity dates | Attributes that affect reporting: city, tier, segment |
| **SCD Type 3** | Current and one previous value in columns | Rare; "previous region" style reports |
| **Insert-only history** | Every change, from every source | Data Vault satellites, Bronze |

In Stagedoor, a customer who moves from Berlin to Lisbon must still have last month's Berlin orders counted in Berlin. That makes `city` a Type 2 attribute. Their email is Type 1: nobody reports by old email addresses.

## Two kinds of time

Every change has at least two timestamps, and confusing them causes most late-data bugs:

- **Business time** (`updated_at`, `applied_ts`): when the change happened in the real world or the source system.
- **System time** (`_ingested_at`, `load_ts`): when the platform learned about it.

They differ whenever data arrives late, which happens in production all the time. Sequence changes by business time. Keep system time for auditing and for reproducing what a report showed on a given day. Tracking both deliberately is called **bitemporal** modeling.

## Conformance

A dimension is **conformed** when it means the same thing everywhere it is used. If marketing's "customer" and finance's "customer" are different tables with different rules, every cross-team number becomes an argument. Conformed dimensions, shared keys and shared definitions are what make a lakehouse one platform rather than many disconnected projects.

## Where to go next

- [2. The Medallion Architecture](02-medallion-architecture.md) organizes the layers.
- [3. Star schema](03-star-schema.md) and [4. Data Vault 2.0](04-data-vault-2.md) are the two models compared in this repository.
- [6. Choosing an approach](06-choosing-an-approach.md) puts them side by side.
