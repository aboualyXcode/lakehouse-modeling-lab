# 6. Choosing an approach

There is no best model, only a best fit for a given set of sources, consumers, constraints and team. This chapter gives a comparison, a decision guide, worked scenarios and the mistakes to avoid. The **Choose an approach** tab in the playground asks the same questions interactively.

## First, the right question

"Medallion or Data Vault or star schema?" is not a real choice, because they solve different problems:

- **Medallion** decides how data is refined: raw, trusted, consumable. You will almost certainly use it.
- **Data Vault** decides how sources are integrated, with history and audit.
- **Star schema** decides how data is presented for analysis.

The real decision is **what goes in Silver and what goes in Gold**.

## Side by side

| | Star schema | Data Vault 2.0 | Wide tables (OBT) |
|---|---|---|---|
| **Optimized for** | Querying and BI | Integration, change, audit | Speed of delivery |
| **Typical layer** | Gold | Silver (raw and business vault) | Gold |
| **History** | SCD Type 2 on chosen attributes | Everything, insert-only, per source | Usually none, or snapshots |
| **Adding a source** | Rework ETL, conform to existing dimensions | Add satellites and links; existing tables untouched | Rebuild the affected tables |
| **A business rule changes** | Rebuild the affected tables | Rebuild the business vault; the raw vault never changes | Rebuild the affected tables |
| **Query complexity** | Low: one join per dimension | High: needs PITs, bridges or marts | Lowest |
| **Number of tables** | Moderate | High (3–5× a star) | Low, but grows with use cases |
| **Skills needed** | Widely known | Specialized; needs automation | Minimal |
| **Time to first value** | Weeks | Months | Days |
| **Biggest risk** | Rework when sources multiply | Overengineering, unqueryable without marts | Inconsistent numbers across tables |

## Decision guide

Walk through these in order. Stop at the first strong signal.

1. **Do you have more than a handful of sources describing the same entities, or must you prove what each source said at any time?**
   Yes → **Data Vault in Silver, star schemas in Gold.**
2. **Are the consumers mainly analysts, dashboards and semantic layers?**
   Yes → **Cleansed entities in Silver, star schemas in Gold.** This is the default for most teams.
3. **Is this a single use case, needed in days, with few consumers?**
   Yes → **Cleansed entities in Silver, a wide table in Gold.** Plan to refactor into a star once a second use case appears.
4. **Is it a data science or application workload?**
   Feature tables and application tables are often wide by nature. Build them from Silver entities or from Gold dimensions, so they reuse the same definitions.

Whatever you choose, keep Bronze raw and keep Silver's change history. Those two decisions keep every other option open.

## Scenarios

**A ticketing startup with one database and a BI tool.** One source, analysts as consumers, a small team. → Medallion with a star schema. A vault would triple the table count and add nothing the change history in Silver does not already give.

**Stagedoor after acquiring two competitors.** Three ticketing systems with different customer IDs, plus a CRM, a payments provider and a marketing platform; finance needs auditable revenue. → Data Vault in Silver: one `hub_customer` integrating all systems through same-as links, one satellite per source. Star schemas in Gold for reporting, built from the business vault.

**A bank's regulatory reporting.** Regulators may ask what a customer's risk rating was, according to which system, on any past date. → Data Vault, with bitemporal satellites (`load_ts` and `applied_ts`) and point-in-time tables. Gold marts on top for the reports themselves.

**A marketing team needs a campaign dashboard by Friday.** → A wide Gold table from Silver entities. If more campaign questions follow, promote it to a fact table with conformed dimensions.

**An ML team building churn features.** → Feature tables built from Silver change history (for point-in-time correct features) and Gold dimensions (for shared definitions). Neither replaces the star schema analysts use.

## The hybrid in this repository

```
              ┌──▶ Silver: cleansed entities ──▶ Gold: star schema
  Bronze ─────┤
              └──▶ Silver: raw vault ──▶ business vault ──▶ star views
```

Both paths are built from the same Bronze tables, and `notebooks/06_validate.sql` checks that they produce the same answers. In practice you would pick one path per domain; building both is a teaching device, and a powerful test.

A common evolution is:

1. Start with Medallion and a star schema.
2. When a domain gains sources and history requirements, introduce a vault in that domain's Silver layer.
3. Keep the Gold star schemas unchanged, now fed by the vault. Consumers never notice.

Because Gold is the contract, you can change the integration model underneath it without breaking anyone.

## Anti-patterns

- **Vault everything.** Building a Data Vault for a domain with one stable source creates complexity with no integration benefit.
- **Query the raw vault directly.** Analysts writing six-way joins across hubs, links and satellites will get wrong answers. Always provide marts or star views.
- **One big table forever.** Each new question produces another wide table with slightly different logic, and the numbers drift apart. Wide tables are a Gold product, not a modeling strategy.
- **Skip Silver history "for now".** Without change history, you cannot add a Type 2 attribute later or answer "what did we know then?". Keeping history is cheap; reconstructing it is often impossible.
- **Model without consumers.** A beautiful model nobody queries is a cost center. Start from the questions in Gold, then design upwards.
- **Choosing by fashion.** Pick the approach your team can build, test and operate well.

## A one-page summary

| If you need… | Choose |
|---|---|
| Fast, simple analytics on a few sources | Star schema in Gold |
| Integration of many, changing sources with full audit | Data Vault in Silver + stars in Gold |
| One dashboard by Friday | A wide Gold table, refactored later |
| Reprocessing, quality layers, governance | Medallion, always |
| Incremental SCD Type 2 with minimal code | `AUTO CDC` in a declarative pipeline |
