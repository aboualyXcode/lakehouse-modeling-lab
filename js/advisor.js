/* advisor.js: recommends a modeling approach from a few questions about your context.
   Scores are deliberately simple and explainable: each answer adds points and a reason.
   The same reasoning is written out in docs/06-choosing-an-approach.md. */
(function (root) {
  'use strict';
  const APPROACHES = {
    star: { name: 'Medallion with star schemas in Gold', short: 'Star schema',
      summary: 'Clean and conform data in Silver, then publish dimensional models (facts and dimensions) in Gold. The default for analytics on Databricks.' },
    hybrid: { name: 'Medallion with a Data Vault in Silver and star schemas in Gold', short: 'Data Vault + Star',
      summary: 'Integrate many sources into a raw and business vault in Silver, then publish star schemas or views in Gold for consumers.' },
    vault: { name: 'Data Vault 2.0 as the integration layer', short: 'Data Vault',
      summary: 'Hubs, links and satellites as the auditable system of record. Consumers still need marts on top; on its own it is rarely the whole answer.' },
    wide: { name: 'Medallion with wide, denormalized Gold tables', short: 'One big table',
      summary: 'Skip formal modeling in Gold and publish wide tables per use case. Fast to build; costly to keep consistent as use cases multiply.' },
  };
  const QUESTIONS = [
    { id: 'sources', q: 'How many source systems feed this domain?', options: [
      { a: 'One or two', s: { star: 2, wide: 2, vault: -2, hybrid: -1 }, why: { star: 'Few sources means little integration work, so a vault adds overhead without much benefit.' } },
      { a: 'Three to ten', s: { star: 1, hybrid: 2, vault: 1 }, why: { hybrid: 'Several sources describing the same customers and orders benefit from hubs that integrate on business keys.' } },
      { a: 'More than ten, with overlapping keys', s: { hybrid: 3, vault: 3, star: -1, wide: -2 }, why: { hybrid: 'Many overlapping sources is the problem Data Vault was designed for: hubs integrate keys, satellites keep each source separate.' } },
    ] },
    { id: 'change', q: 'How often do sources change shape, or new sources arrive?', options: [
      { a: 'Rarely', s: { star: 2, wide: 1 }, why: { star: 'Stable sources keep dimensional ETL cheap to maintain.' } },
      { a: 'A few times a year', s: { star: 1, hybrid: 1 } },
      { a: 'Constantly (acquisitions, new SaaS tools)', s: { hybrid: 3, vault: 2, wide: -1 }, why: { hybrid: 'In a vault, a new source usually means new satellites and links, without rework of existing tables.' } },
    ] },
    { id: 'audit', q: 'Do you need a complete, auditable history of every source value?', options: [
      { a: 'No, current values and some history are enough', s: { star: 2, wide: 2, vault: -2, hybrid: -1 } },
      { a: 'History for selected attributes', s: { star: 2, hybrid: 1 }, why: { star: 'SCD Type 2 dimensions keep history for the attributes that matter to reporting.' } },
      { a: 'Yes, it is a regulatory requirement', s: { hybrid: 3, vault: 3 }, why: { vault: 'Insert-only satellites with load timestamps and record sources give a full audit trail by design.', hybrid: 'The vault keeps the audit trail while the stars keep reporting simple.' } },
    ] },
    { id: 'consumers', q: 'Who consumes the data?', options: [
      { a: 'Analysts and BI dashboards', s: { star: 3, hybrid: 1, vault: -2 }, why: { star: 'BI tools, Databricks SQL and metric views work best with facts and dimensions.' } },
      { a: 'Data scientists and applications', s: { wide: 2, star: 1 }, why: { wide: 'Feature and application tables are often wide by nature; a star can still feed them.' } },
      { a: 'Both, across many teams', s: { star: 2, hybrid: 2, wide: -1 }, why: { star: 'Conformed dimensions give every team the same definition of customer, event and date.' } },
    ] },
    { id: 'team', q: 'What is your team\'s modeling experience?', options: [
      { a: 'Small team, new to modeling', s: { star: 2, wide: 1, vault: -3, hybrid: -2 }, why: { star: 'Dimensional modeling is the easiest formal approach to learn and explain to stakeholders.' } },
      { a: 'Experienced with dimensional modeling', s: { star: 2, hybrid: 1 } },
      { a: 'A dedicated platform team with Data Vault experience', s: { hybrid: 2, vault: 2 }, why: { hybrid: 'Data Vault needs discipline and automation; an experienced team can supply both.' } },
    ] },
    { id: 'speed', q: 'How soon do people need results?', options: [
      { a: 'Within weeks', s: { wide: 2, star: 1, vault: -2, hybrid: -2 }, why: { wide: 'Wide tables deliver the first dashboard fastest; plan to refactor once patterns repeat.' } },
      { a: 'Within a quarter', s: { star: 2, hybrid: 1 } },
      { a: 'This is a multi-year platform', s: { hybrid: 2, vault: 1, star: 1, wide: -2 }, why: { hybrid: 'Over years, the cost of reworking integrations usually outweighs the up-front cost of a vault.' } },
    ] },
  ];
  function recommend(answers) {
    const score = { star: 0, hybrid: 0, vault: 0, wide: 0 }, reasons = { star: [], hybrid: [], vault: [], wide: [] };
    QUESTIONS.forEach(q => {
      const opt = q.options[answers[q.id]];
      if (!opt) return;
      Object.keys(opt.s).forEach(k => { score[k] += opt.s[k]; });
      Object.keys(opt.why || {}).forEach(k => reasons[k].push(opt.why[k]));
    });
    return Object.keys(score).map(k => Object.assign({ id: k, score: score[k], reasons: reasons[k] }, APPROACHES[k])).sort((a, b) => b.score - a.score);
  }
  const MATRIX = {
    cols: ['Star schema', 'Data Vault 2.0', 'Wide tables', 'Medallion'],
    rows: [
      ['What it is', 'A dimensional model: facts and dimensions', 'An integration model: hubs, links, satellites', 'Denormalized, use-case-shaped tables', 'A layering pattern for data quality, not a model'],
      ['Optimized for', 'Querying and BI', 'Integrating and auditing many sources', 'Speed of delivery', 'Progressive refinement and reprocessing'],
      ['History', 'SCD Type 2 on chosen attributes', 'Everything, insert-only', 'Usually none, or snapshots', 'Bronze keeps the raw history'],
      ['Adding a source', 'Rework ETL and conform', 'Add satellites and links', 'Rebuild affected tables', 'Add Bronze tables and flows'],
      ['Query complexity', 'Low: one join per dimension', 'High: needs marts or PITs', 'Lowest: often no joins', 'Depends on the model in each layer'],
      ['Typical layer', 'Gold', 'Silver (raw and business vault)', 'Gold', 'Bronze → Silver → Gold'],
      ['Databricks fit', 'Liquid clustering, PK/FK RELY, metric views', 'Insert-only Delta tables, MERGE, hash keys', 'Materialized views', 'Lakeflow Spark Declarative Pipelines, Unity Catalog'],
    ],
  };
  root.Advisor = { APPROACHES, QUESTIONS, recommend, MATRIX };
})(typeof window !== 'undefined' ? (window.LML = window.LML || {}) : (global.LML = global.LML || {}));
