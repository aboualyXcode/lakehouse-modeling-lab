# 4. Data Vault 2.0

Data Vault is an **integration** model. Where a star schema is shaped for querying, a Data Vault is shaped for absorbing many sources and constant change, while keeping a complete, auditable history of everything every source ever said. It was created by Dan Linstedt; version 2.0 added hash keys, which make parallel loading possible, along with a methodology for building it.

It is rarely the layer people query. The usual architecture is a Data Vault in Silver and star schemas in Gold, and that is what this repository builds.

## The three building blocks

```
  hub_customer ──┐                              ┌── sat_customer_details
  hub_event ─────┼── link_order_customer_event ─┤
  hub_order ─────┘                              └── sat_order_details
```

| Component | Holds | Grain | Changes |
|---|---|---|---|
| **Hub** | A business key, and nothing else | One row per unique key, ever | Insert-only |
| **Link** | A relationship between business keys (a unit of work) | One row per unique combination of keys | Insert-only |
| **Satellite** | Descriptive attributes and their history | One row per change, per parent key | Insert-only |

Every row also carries:

- **`load_ts`**: when the warehouse learned this, in system time.
- **`record_source`**: where it came from, for example `stagedoor.ticketing.orders`.
- In satellites, **`applied_ts`**: when it was true in the source, in business time. Not every Data Vault includes it, but you need it whenever data can arrive late (see below).

### Hubs

A hub answers "which customers exist?". It stores the business key and a **hash key** computed from it:

```sql
COALESCE(md5(upper(NULLIF(trim(customer_id), ''))), repeat('0', 32)) AS hk_customer
```

- Standardize before hashing (trim, upper-case, empty as missing), or the same customer gets two keys.
- A missing business key maps to the **zero key** (`000…0`) instead of being dropped. The raw vault keeps everything; business rules decide later what counts.
- Hubs are loaded from every source that mentions the key. In Stagedoor, `hub_customer` is loaded from both `customers` and `orders`, so an order for a customer the CRM has not synced yet still produces a hub row. The vault knows the customer exists before it knows anything about them.

### Links

A link records that keys occurred together. `link_order_customer_event` says "this order involved this customer and this event". Its hash key hashes all the participating business keys in a fixed order:

```sql
md5(concat_ws('||', COALESCE(upper(NULLIF(trim(order_id), '')), '-1'),
                     COALESCE(upper(NULLIF(trim(customer_id), '')), '-1'),
                     COALESCE(upper(NULLIF(trim(event_id), '')), '-1')))
```

Links are always many-to-many, even when the business says the relationship is one-to-many today. That is deliberate: when the business rule changes, the model does not.

### Satellites

Satellites hold everything descriptive, with full history. A new row is inserted only when the attributes change, which is detected with a **hashdiff**:

```sql
md5(concat_ws('||', COALESCE(trim(full_name), ''), COALESCE(trim(email), ''),
                     COALESCE(trim(city), ''),      COALESCE(trim(country), ''),
                     COALESCE(trim(loyalty_tier), '')))
```

Use a fixed column order, a delimiter, explicit null replacement and consistent casting. Changing the hashdiff recipe later makes every row look changed.

Split satellites by **source** and by **rate of change**: attributes from the CRM and attributes from the billing system go in separate satellites on the same hub, and fast-changing attributes are kept apart from slow ones.

## The out-of-order pitfall

The simplest satellite load compares each incoming row with the **latest loaded row** for its key and inserts it if the hashdiff differs. That is wrong as soon as data arrives late.

Suppose Amara lived in Berlin and the vault has already loaded her move to Austin, effective 13:15. Then a delayed record arrives saying she was already in Austin from 11:15:

| Arrives | applied_ts | City | Compared with the latest loaded row | Result |
|---|---|---|---|---|
| 1st | 13:15 | Austin | Berlin | Inserted |
| 2nd (late) | 11:15 | Austin | Austin (13:15) | Judged unchanged and **dropped** |

The vault now says she lived in Berlin until 13:15. An order she placed at 12:00 is attributed to Berlin in the vault and to Austin in the star schema, which sequences by business time. The two models disagree, and nothing fails.

**The fix:** compare with the row just before the incoming one in **business time** (`applied_ts`), not with the latest row loaded. `notebooks/04_raw_vault.py` does this with a window over the staged and existing rows:

```sql
LAG(hashdiff) OVER (PARTITION BY hk_customer ORDER BY applied_ts, is_new, load_ts)
```

This repository's randomized tests found exactly this bug in an early version of the playground: the star schema and the vault disagreed on historical revenue in 5 of 840 simulated pipeline runs. The **Star vs Vault** tab and `notebooks/06_validate.sql` both reconcile the two models, which is how bugs like this get caught.

## Raw vault and business vault

| | Raw vault | Business vault |
|---|---|---|
| **Applies** | Hard rules only: types, hashing, zero keys | Soft rules: validity, derivations, business logic |
| **Changes when** | Sources change | Business rules change |
| **Stagedoor** | Hubs, links, satellites (`04_raw_vault.py`) | `bv_order_current`, `bv_customer_effectivity`, `pit_customer` (`05_business_vault.sql`) |

Keeping soft rules out of the raw vault means a rule change never requires reloading history: you rebuild the business vault from the raw vault. In the playground, *Bad record* shows the difference: the order is kept in the raw vault on the zero key, flagged `passes_rules = false` in the business vault, and excluded from reporting.

## Query helpers

A vault needs help to be queried efficiently:

- **Point-in-time (PIT) tables:** for each hub key and snapshot date, which satellite row was valid. Queries then join on equality instead of running range lookups over every satellite.
- **Bridge tables:** precomputed paths across several links, for example order → customer → household.
- **Effectivity satellites:** when a relationship in a link was active, for example which account manager owned a customer when.
- **Star views:** dimensional views over the business vault (`v_dim_customer`, `v_fact_ticket_sales`). Start virtual; materialize when performance requires it.

## Other satellite and link types

- **Multi-active satellites:** several valid rows at once per key, such as one per phone number type. Add the distinguishing column to the key.
- **Same-as links:** record that two business keys are the same real-world entity (customer deduplication across systems).
- **Hierarchical links:** parent-child relationships within one hub.
- **Status-tracking satellites:** record when a key appears in or disappears from a source's full extract, which is how deletes are detected without CDC.

## Hash choice and collisions

MD5 produces 128-bit keys; SHA-1 and SHA-256 are longer. For keys, the question is collision probability, not cryptographic security. With MD5, a collision becomes plausible only at around 2⁶⁴ keys, far beyond any real hub. Pick one function, use it everywhere, and store keys as strings (or `BINARY` to halve storage). Spark's `md5()`, `sha1()` and `sha2(x, 256)` are all built in.

## Automation

Data Vault is repetitive by design: every hub load looks like every other hub load. That is a strength only if it is automated. Options:

- **Metadata-driven notebooks**, as in `04_raw_vault.py`, where one config generates every load.
- **dbt packages** such as AutomateDV and datavault4dbt.
- **Commercial vault automation tools.**

A hand-written Data Vault with hundreds of tables is the most common way Data Vault projects fail.

## When Data Vault is worth it

Choose it when several of these are true:

- Many sources describe the same business entities, with overlapping or conflicting keys.
- Sources are added or change shape frequently (acquisitions, SaaS sprawl).
- You must be able to show what each source said at any point in time (regulation, audit).
- The platform is long-lived and the team can invest in automation and discipline.

Avoid it when one or two stable sources feed a domain, the team is small or new to modeling, or results are needed in weeks. A well-built Silver layer with change history gives you most of the auditability for far less effort.

**Try it:** queue *Late-arriving change* and *Duplicate delivery* in the playground. In `raw_vault.sat_customer_details`, the late row is kept with its earlier `applied_ts`, and the duplicate adds nothing.
