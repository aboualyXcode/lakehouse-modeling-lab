#!/usr/bin/env node
/* Tests the playground's lakehouse model with plain Node (no dependencies).
   1. MD5 matches Node's crypto, the same function Spark's md5() implements.
   2. Hand-written scenarios: duplicates, late data, early-arriving facts, bad records, schema drift.
   3. Hundreds of random event sequences, checking invariants after every pipeline run.
   Usage: node tests/model.test.js */
'use strict';
const path = require('path');
const crypto = require('crypto');
global.LML = {};
require(path.join(__dirname, '..', 'js', 'md5.js'));
require(path.join(__dirname, '..', 'js', 'model.js'));
require(path.join(__dirname, '..', 'js', 'sql.js'));
require(path.join(__dirname, '..', 'js', 'advisor.js'));
const { Lab, util, ACTIONS, CATALOG, QUESTIONS } = global.LML;

let checks = 0, failures = 0;
function ok(cond, msg) { checks++; if (!cond) { failures++; console.log('  FAIL ' + msg); } }
const T = lab => lab.out.tables;

/* ---------- 1. hashing ---------- */
console.log('Hashing');
for (const s of ['', 'C-1001', 'O-9001||C-1001||E-501', 'Tomás Silva', '東京', 'x'.repeat(200)]) ok(global.LML.md5(s) === crypto.createHash('md5').update(s, 'utf8').digest('hex'), `md5("${s.slice(0, 20)}")`);
ok(util.hashKey('  c-1001 ') === util.hashKey('C-1001'), 'hash keys are computed on UPPER(TRIM(business key))');
ok(util.hashKey(null) === util.ZERO_KEY, 'a missing business key maps to the zero key');
ok(util.hashLink('O-1', null, 'E-1') === global.LML.md5('O-1||-1||E-1'), 'link hash keys replace missing keys with -1');
console.log(`  ${checks} checks`);

/* ---------- invariants, checked after every run ---------- */
function invariants(lab, ctx) {
  const t = T(lab);
  // Bronze is append-only and complete
  ok(t['bronze.customers'].length + t['bronze.events'].length + t['bronze.orders'].length === lab.bronze.length, `${ctx}: bronze holds every record`);
  // Silver: one row per key, the latest valid change, never a bad record
  for (const [tbl, key] of [['customers', 'customer_id'], ['events', 'event_id'], ['orders', 'order_id']]) {
    const rows = t['silver.' + tbl];
    ok(new Set(rows.map(r => r[key])).size === rows.length, `${ctx}: silver.${tbl} has one row per ${key}`);
    for (const r of rows) {
      const valid = lab.bronze.filter(b => b.table === tbl && b.data[key] === r[key] && !lab.violations(tbl, b.data));
      const newest = lab.bronze.filter(b => b.table === tbl && b.data[key] === r[key] && lab.violations(tbl, b.data).length === 0).reduce((m, b) => (b.data.updated_at > m ? b.data.updated_at : m), '');
      ok(r.updated_at === newest, `${ctx}: silver.${tbl} ${r[key]} is the latest change (${r.updated_at} vs ${newest})`);
      void valid;
    }
  }
  ok(t['silver.orders'].every(o => o.customer_id && o.quantity > 0), `${ctx}: no invalid order reaches silver`);
  // Star schema: SCD2 periods are contiguous, single current row, stable keys
  const dim = t['gold.dim_customer'].filter(d => d.customer_sk !== -1);
  const byId = new Map(); dim.forEach(d => { if (!byId.has(d.customer_id)) byId.set(d.customer_id, []); byId.get(d.customer_id).push(d); });
  for (const [id, v] of byId) {
    v.sort((a, b) => (a.valid_from < b.valid_from ? -1 : 1));
    ok(v[0].valid_from === util.BEGIN, `${ctx}: ${id} first version starts at the beginning of time`);
    ok(v[v.length - 1].valid_to === util.END && v.filter(x => x.is_current).length === 1 && v[v.length - 1].is_current, `${ctx}: ${id} has exactly one current version, the last`);
    for (let i = 1; i < v.length; i++) ok(v[i - 1].valid_to === v[i].valid_from, `${ctx}: ${id} versions are contiguous`);
    for (let i = 1; i < v.length; i++) ok(['city', 'country', 'loyalty_tier'].some(c => v[i - 1][c] !== v[i][c]), `${ctx}: ${id} has no redundant version`);
    ok(new Set(v.map(x => x.email)).size === 1, `${ctx}: ${id} email is Type 1 (same on every version)`);
  }
  ok(new Set(t['gold.dim_customer'].map(d => d.customer_sk)).size === t['gold.dim_customer'].length, `${ctx}: customer_sk is unique`);
  const sks = new Set(t['gold.dim_customer'].map(d => d.customer_sk)), esks = new Set(t['gold.dim_event'].map(d => d.event_sk)), dks = new Set(t['gold.dim_date'].map(d => d.date_key));
  ok(t['gold.fact_ticket_sales'].length === t['silver.orders'].length, `${ctx}: one fact row per valid order`);
  ok(t['gold.fact_ticket_sales'].every(f => sks.has(f.customer_sk) && esks.has(f.event_sk) && dks.has(f.date_key)), `${ctx}: every fact foreign key resolves`);
  for (const f of t['gold.fact_ticket_sales']) {
    const o = t['silver.orders'].find(x => x.order_id === f.order_id);
    const d = t['gold.dim_customer'].find(x => x.customer_sk === f.customer_sk);
    if (f.customer_sk !== -1) ok(d.customer_id === o.customer_id && d.valid_from <= o.order_ts && o.order_ts < d.valid_to, `${ctx}: ${f.order_id} points at the customer version valid at order time`);
    else ok(!byId.has(o.customer_id), `${ctx}: ${f.order_id} is Unknown only while the customer is missing`);
  }
  // Raw vault: unique hubs, correct hashes, insert-only satellites without redundant rows
  for (const h of ['customer', 'event', 'order']) {
    const hub = t[`raw_vault.hub_${h}`];
    ok(new Set(hub.map(x => x['hk_' + h])).size === hub.length, `${ctx}: hub_${h} keys are unique`);
    ok(hub.every(x => x['hk_' + h] === util.hashKey(x[h + '_id'])), `${ctx}: hub_${h} hash keys match their business keys`);
    const seen = new Set();
    for (const s of t[`raw_vault.sat_${h}_details`]) {
      const k = s['hk_' + h] + '|' + s.applied_ts + '|' + s.hashdiff;
      ok(!seen.has(k), `${ctx}: sat_${h}_details never stores the same change twice`);
      seen.add(k);
      ok(hub.some(x => x['hk_' + h] === s['hk_' + h]), `${ctx}: every sat_${h}_details row has a hub parent`);
    }
  }
  ok(t['raw_vault.link_order_customer_event'].every(l => l.hk_order_customer_event === util.hashLink(t['raw_vault.hub_order'].find(o => o.hk_order === l.hk_order).order_id,
    (t['raw_vault.hub_customer'].find(c => c.hk_customer === l.hk_customer) || {}).customer_id, (t['raw_vault.hub_event'].find(e => e.hk_event === l.hk_event) || {}).event_id)), `${ctx}: link hash keys match their parts`);
  // Both models answer every question identically
  for (const q of QUESTIONS) ok(JSON.stringify(lab.out.answers.star[q.id]) === JSON.stringify(lab.out.answers.vault[q.id]),
    `${ctx}: star and vault agree on ${q.id}\n    star  ${JSON.stringify(lab.out.answers.star[q.id])}\n    vault ${JSON.stringify(lab.out.answers.vault[q.id])}`);
  // Every table the model builds is described in the catalogue
  ok(Object.keys(t).every(k => CATALOG[k]), `${ctx}: every table has a catalogue entry`);
}

/* ---------- 2. scenarios ---------- */
console.log('Scenarios');
const before = checks;
{
  const lab = new Lab(1); invariants(lab, 'initial load');
  ok(T(lab)['gold.dim_customer'].length === 5 && T(lab)['silver.orders'].length === 5, 'initial load: 4 customers + Unknown, 5 orders');

  const sat0 = T(lab)['raw_vault.sat_customer_details'].length, silver0 = T(lab)['silver.customers'].length, b0 = lab.bronze.length;
  lab.act('duplicate'); lab.run(); invariants(lab, 'duplicate');
  ok(lab.bronze.length === b0 + 1, 'duplicate: bronze grows by one');
  ok(T(lab)['raw_vault.sat_customer_details'].length === sat0 && T(lab)['silver.customers'].length === silver0, 'duplicate: no new silver or satellite rows');

  const sk = new Map(T(lab)['gold.dim_customer'].map(d => [d.customer_id + d.valid_from, d.customer_sk]));
  lab.act('move'); lab.run(); invariants(lab, 'move');
  const moved = T(lab)['gold.dim_customer'].filter(d => d.customer_id === 'C-1001');
  ok(moved.length === 2 && moved[0].city === 'Berlin' && !moved[0].is_current, 'move: Berlin version closed, new version current');
  ok(T(lab)['gold.dim_customer'].every(d => !sk.has(d.customer_id + d.valid_from) || sk.get(d.customer_id + d.valid_from) === d.customer_sk), 'move: existing surrogate keys did not change');
  ok(lab.out.answers.star.revenue_by_city.some(r => r.key === 'Berlin'), 'move: Berlin keeps the revenue from before the move');

  lab.act('email'); lab.run(); invariants(lab, 'email');
  const nVersions = T(lab)['gold.dim_customer'].length;
  lab.act('late'); lab.run(); invariants(lab, 'late');
  const lateCust = lab.bronze[lab.bronze.length - 1].data;
  const versions = T(lab)['gold.dim_customer'].filter(d => d.customer_id === lateCust.customer_id);
  const cur = versions.find(v => v.is_current), mid = versions.find(v => v.city === lateCust.city);
  ok(mid && !mid.is_current && mid.valid_from === lateCust.updated_at, 'late: the late change became a historical version');
  ok(cur.city === T(lab)['silver.customers'].find(c => c.customer_id === lateCust.customer_id).city && cur.city !== lateCust.city, 'late: the current row did not go backwards');
  ok(T(lab)['gold.dim_customer'].length > nVersions, 'late: history gained the late version (full-row CDC can also restore the old value afterwards)');

  lab.act('early'); lab.run(); invariants(lab, 'early-arriving fact');
  ok(T(lab)['gold.fact_ticket_sales'].some(f => f.customer_sk === -1), 'early: the order points to the Unknown member');
  ok(T(lab)['raw_vault.hub_customer'].some(h => h.customer_id && h.customer_id.startsWith('C-2')), 'early: the hub already knows the customer key from the order');
  lab.run(); invariants(lab, 'customer arrives');
  ok(!T(lab)['gold.fact_ticket_sales'].some(f => f.customer_sk === -1), 'early: after the customer arrives, no fact points to Unknown');

  lab.act('bad'); lab.run(); invariants(lab, 'bad record');
  ok(T(lab)['silver.quarantine'].some(q => /customer_id is missing/.test(q.reason) && /quantity/.test(q.reason)), 'bad: quarantined with both reasons');
  ok(T(lab)['raw_vault.link_order_customer_event'].some(l => l.hk_customer === util.ZERO_KEY), 'bad: the raw vault keeps it, linked to the zero key');
  ok(T(lab)['business_vault.bv_order_current'].some(o => !o.passes_rules), 'bad: the business vault flags it');

  lab.act('drift'); lab.run(); invariants(lab, 'schema drift');
  ok(T(lab)['bronze.orders'].some(o => o._rescued_data && /promo_code/.test(o._rescued_data)), 'drift: unknown fields land in _rescued_data');

  lab.act('price'); lab.run(); invariants(lab, 'price');
  ok(T(lab)['gold.fact_ticket_sales'].filter(f => f.order_id === 'O-9001')[0].unit_price === '89.00', 'price: old orders keep the price they paid');
  ['order', 'pay', 'refund', 'tier', 'signup'].forEach(a => { lab.act(a); lab.run(); invariants(lab, a); });
  ok(lab.log.length === lab.batch, 'every run writes a log entry');
}
{
  // Regression: a late change with the same values as a newer, already-loaded change must not be dropped.
  const lab = new Lab(3);
  const base = Date.parse(lab.clock), at = h => new Date(base + h * 3600e3).toISOString().replace('.000Z', 'Z');
  const c = Object.assign({}, lab.src.customers.get('C-1003'));
  lab.pending.push({ table: 'customers', op: 'UPDATE', data: Object.assign({}, c, { city: 'Austin', country: 'US', updated_at: at(50) }), updated_at: at(50) });
  lab.pending.push({ table: 'orders', op: 'INSERT', data: { order_id: 'O-7777', customer_id: 'C-1003', event_id: 'E-501', quantity: 2, unit_price: 89, status: 'PAID', order_ts: at(40), updated_at: at(40) }, updated_at: at(40) });
  lab.run(); invariants(lab, 'out-of-order setup');
  lab.pending.push({ table: 'customers', op: 'UPDATE', data: Object.assign({}, c, { city: 'Austin', country: 'US', updated_at: at(30) }), updated_at: at(30), late: true });
  lab.run(); invariants(lab, 'late change equal to a newer one');
  const f = T(lab)['gold.fact_ticket_sales'].find(x => x.order_id === 'O-7777');
  ok(T(lab)['gold.dim_customer'].find(d => d.customer_sk === f.customer_sk).city === 'Austin', 'out-of-order: the order placed after the late move counts in Austin');
  ok(T(lab)['raw_vault.sat_customer_details'].filter(s => s.city === 'Austin').length === 2, 'out-of-order: the satellite kept the late row');
}
console.log(`  ${checks - before} checks`);

/* ---------- 3. random event sequences ---------- */
console.log('Random sequences');
const before2 = checks;
let runs = 0;
for (let seed = 1; seed <= 60; seed++) {
  const lab = new Lab(seed);
  let r = seed * 2654435761 >>> 0;
  const next = () => { r = (Math.imul(r, 1664525) + 1013904223) >>> 0; return r / 4294967296; };
  for (let step = 0; step < 14; step++) {
    const n = 1 + Math.floor(next() * 3);
    for (let i = 0; i < n; i++) lab.act(ACTIONS[Math.floor(next() * ACTIONS.length)].id);
    lab.run(); runs++;
    invariants(lab, `seed ${seed} run ${lab.batch}`);
    if (failures > 20) break;
  }
  if (failures > 20) break;
}
console.log(`  ${checks - before2} checks across ${runs} pipeline runs`);

/* ---------- 4. advisor and content ---------- */
console.log('Advisor and content');
const before3 = checks;
const fs = require('fs');
const { Advisor, SQL } = global.LML;
const answerSets = [[0, 0, 0, 0, 0, 0], [2, 2, 2, 2, 2, 2], [1, 1, 1, 1, 1, 1], [0, 0, 0, 1, 0, 0]];
const expectTop = ['star', 'hybrid', null, 'wide'];
answerSets.forEach((set, i) => {
  const ans = {}; Advisor.QUESTIONS.forEach((q, j) => { ans[q.id] = set[j]; });
  const ranked = Advisor.recommend(ans);
  ok(ranked.length === 4 && ranked.every((r, k) => k === 0 || ranked[k - 1].score >= r.score), `advisor ranking ${i} is complete and sorted`);
  if (expectTop[i] && i !== 3) ok(ranked[0].id === expectTop[i], `advisor: answers ${set.join('')} recommend ${expectTop[i]} (got ${ranked[0].id})`);
});
{
  const ans = {}; Advisor.QUESTIONS.forEach(q => { ans[q.id] = 0; }); ans.speed = 0; ans.consumers = 1; ans.team = 0; ans.sources = 0;
  ok(['wide', 'star'].includes(Advisor.recommend(ans)[0].id), 'advisor: a quick single-source data science need never recommends a vault');
}
ok(Advisor.recommend({}).every(r => r.score === 0), 'advisor: no answers gives a neutral ranking');
for (const [name, meta] of Object.entries(CATALOG)) {
  const s = SQL[meta.sql];
  ok(!!s, `${name} has a SQL snippet`);
  if (s) ok(fs.existsSync(path.join(__dirname, '..', s.file)), `${name}: snippet source ${s.file} exists`);
}
console.log(`  ${checks - before3} checks`);

console.log(failures ? `\n${failures} of ${checks} checks failed` : `\nAll ${checks} checks passed.`);
process.exit(failures ? 1 : 0);
