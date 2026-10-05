/* model.js: the playground's lakehouse, with no DOM code.
   A source system emits change records (CDC). Each pipeline run lands them in Bronze, then rebuilds
   Silver, the Gold star schema, and the Raw and Business Vault from Bronze, the same way the
   Databricks notebooks in /notebooks do. Rebuilding from Bronze each run is what a declarative
   pipeline guarantees too: every downstream table is a function of the raw history. */
(function (root) {
  'use strict';
  const md5 = root.md5;

  /* ---------- helpers ---------- */
  function rng(seed) { let a = seed | 0; return () => { a = (a + 0x6D2B79F5) | 0; let t = Math.imul(a ^ (a >>> 15), 1 | a); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; }; }
  const iso = ms => new Date(ms).toISOString().replace('.000Z', 'Z');
  const addH = (ts, h) => iso(Date.parse(ts) + h * 3600e3);
  const money = n => Math.round(n * 100) / 100;
  const clone = o => JSON.parse(JSON.stringify(o));
  const BEGIN = '1900-01-01T00:00:00Z', END = '9999-12-31T00:00:00Z';
  const ZERO_KEY = '00000000000000000000000000000000';
  const bk = v => (v == null || String(v).trim() === '' ? null : String(v).trim().toUpperCase());
  const hashKey = v => (bk(v) == null ? ZERO_KEY : md5(bk(v)));
  const hashLink = (...vals) => md5(vals.map(v => (bk(v) == null ? '-1' : bk(v))).join('||'));
  const hashDiff = (row, cols) => md5(cols.map(c => (row[c] == null ? '' : String(row[c]).trim())).join('||'));
  const fmtMoney = n => (n == null ? null : Number(n).toFixed(2));

  /* ---------- source system: Stagedoor's ticketing database ---------- */
  const SCHEMA = {
    customers: { key: 'customer_id', cols: ['customer_id', 'full_name', 'email', 'city', 'country', 'loyalty_tier', 'updated_at'] },
    events: { key: 'event_id', cols: ['event_id', 'event_name', 'artist', 'venue', 'city', 'event_date', 'base_price', 'updated_at'] },
    orders: { key: 'order_id', cols: ['order_id', 'customer_id', 'event_id', 'quantity', 'unit_price', 'status', 'order_ts', 'updated_at'] },
  };
  const CITIES = [['Berlin', 'DE'], ['Lisbon', 'PT'], ['Osaka', 'JP'], ['Cairo', 'EG'], ['Austin', 'US'], ['Toronto', 'CA'], ['Nairobi', 'KE'], ['Madrid', 'ES']];
  const TIERS = ['Bronze', 'Silver', 'Gold', 'Platinum'];
  const NEW_PEOPLE = [['Noah Fischer', 'Berlin', 'DE'], ['Priya Nair', 'Toronto', 'CA'], ['Tomás Silva', 'Lisbon', 'PT'], ['Wanjiru Kamau', 'Nairobi', 'KE'], ['Yuki Tanaka', 'Osaka', 'JP'], ['Elena Ruiz', 'Madrid', 'ES'], ['Sam Carter', 'Austin', 'US']];

  class Lab {
    constructor(seed) {
      this.r = rng(seed == null ? 7 : seed);
      this.clock = '2026-03-02T09:00:00Z';
      this.src = { customers: new Map(), events: new Map(), orders: new Map() };
      this.pending = []; this.deferred = []; this.bronze = []; this.batch = 0;
      this.nextOrder = 9001; this.nextCustomer = 1005; this.nextEarly = 2001; this.newPerson = 0; this.moveTurn = 0;
      this.sk = { customer: new Map(), event: new Map(), nextCustomer: 1, nextEvent: 1 };
      this.out = null; this.prev = null; this.log = [];
      this.seed();
      this.run('Initial load: the ticketing database is replicated for the first time.');
    }
    tick(h) { this.clock = addH(this.clock, h == null ? 4 : h); return this.clock; }
    emit(table, op, row, extra) {
      const rec = { table, op, data: clone(row), updated_at: row.updated_at };
      if (extra) rec.extra = extra;
      this.pending.push(rec);
      return rec;
    }
    upsert(table, row, op) { this.src[table].set(row[SCHEMA[table].key], clone(row)); return this.emit(table, op || 'UPDATE', row); }
    seed() {
      const t = this.clock;
      [['C-1001', 'Amara Okafor', 'amara.okafor@example.com', 'Berlin', 'DE', 'Silver'], ['C-1002', 'Diego Ramos', 'diego.ramos@example.com', 'Lisbon', 'PT', 'Bronze'],
        ['C-1003', 'Hana Sato', 'hana.sato@example.com', 'Osaka', 'JP', 'Gold'], ['C-1004', 'Layla Hassan', 'layla.hassan@example.com', 'Cairo', 'EG', 'Bronze']]
        .forEach(([id, n, e, city, cc, tier], i) => this.upsert('customers', { customer_id: id, full_name: n, email: e, city, country: cc, loyalty_tier: tier, updated_at: addH(t, -720 + i) }, 'INSERT'));
      [['E-501', 'Northern Lights Tour', 'Aurora Vale', 'Olympia Hall', 'Berlin', '2026-05-14', 89], ['E-502', 'Midnight Static', 'The Ferrymen', 'Coliseu', 'Lisbon', '2026-06-02', 65],
        ['E-503', 'Harbor Nights', 'Kaito & Mira', 'Zepp Bayside', 'Osaka', '2026-07-19', 120]]
        .forEach(([id, name, artist, venue, city, date, price], i) => this.upsert('events', { event_id: id, event_name: name, artist, venue, city, event_date: date, base_price: price, updated_at: addH(t, -500 + i) }, 'INSERT'));
      [['C-1001', 'E-501', 2, 'PAID', -48], ['C-1002', 'E-502', 1, 'PAID', -40], ['C-1003', 'E-503', 4, 'PAID', -30], ['C-1001', 'E-502', 2, 'PLACED', -6], ['C-1004', 'E-501', 3, 'REFUNDED', -20]]
        .forEach(([c, e, q, s, h]) => this.order(c, e, q, s, addH(t, h)));
    }
    order(customerId, eventId, qty, status, ts, extra) {
      const ev = this.src.events.get(eventId);
      const row = { order_id: 'O-' + this.nextOrder++, customer_id: customerId, event_id: eventId, quantity: qty, unit_price: ev ? ev.base_price : 50, status, order_ts: ts, updated_at: ts };
      this.src.orders.set(row.order_id, clone(row));
      return this.emit('orders', 'INSERT', row, extra);
    }
    pick(arr) { return arr[Math.floor(this.r() * arr.length)]; }

    /* ---------- things that happen in production ---------- */
    act(kind) {
      const t = this.tick();
      const cust = () => Array.from(this.src.customers.values());
      switch (kind) {
        case 'order': {
          const c = this.pick(cust()), e = this.pick(Array.from(this.src.events.values()));
          const rec = this.order(c.customer_id, e.event_id, 1 + Math.floor(this.r() * 4), 'PLACED', t);
          return `${c.full_name} ordered ${rec.data.quantity} × ${e.event_name} (${rec.data.order_id}, PLACED).`;
        }
        case 'pay': {
          const open = Array.from(this.src.orders.values()).filter(o => o.status === 'PLACED' && o.customer_id);
          if (!open.length) return null;
          open.forEach(o => this.upsert('orders', Object.assign(o, { status: 'PAID', updated_at: t })));
          return `Payment captured for ${open.map(o => o.order_id).join(', ')} (status PLACED → PAID).`;
        }
        case 'refund': {
          const paid = Array.from(this.src.orders.values()).filter(o => o.status === 'PAID').sort((a, b) => (a.updated_at < b.updated_at ? 1 : -1));
          if (!paid.length) return null;
          const o = paid[0]; this.upsert('orders', Object.assign(o, { status: 'REFUNDED', updated_at: t }));
          return `${o.order_id} was refunded (PAID → REFUNDED).`;
        }
        case 'move': {
          const list = cust().sort((a, b) => (a.customer_id < b.customer_id ? -1 : 1));
          const c = list[this.moveTurn++ % list.length];
          const choices = CITIES.filter(x => x[0] !== c.city), [city, cc] = this.pick(choices);
          const from = c.city;
          this.upsert('customers', Object.assign(c, { city, country: cc, updated_at: t }));
          return `${c.full_name} (${c.customer_id}) moved from ${from} to ${city}. City is a Type 2 attribute, so history matters.`;
        }
        case 'tier': {
          const c = cust().filter(x => x.loyalty_tier !== 'Platinum').sort((a, b) => (a.customer_id < b.customer_id ? -1 : 1))[0];
          if (!c) return null;
          const from = c.loyalty_tier; this.upsert('customers', Object.assign(c, { loyalty_tier: TIERS[TIERS.indexOf(from) + 1], updated_at: t }));
          return `${c.full_name} was upgraded from ${from} to ${c.loyalty_tier}.`;
        }
        case 'email': {
          const c = this.pick(cust()); const old = c.email;
          this.upsert('customers', Object.assign(c, { email: c.email.replace('@example.com', '@mail.example.org'), updated_at: t }));
          return `${c.full_name} changed email (${old} → ${c.email}). Email is a Type 1 attribute: no new version.`;
        }
        case 'signup': {
          const [n, city, cc] = NEW_PEOPLE[this.newPerson++ % NEW_PEOPLE.length];
          const id = 'C-' + this.nextCustomer++;
          this.upsert('customers', { customer_id: id, full_name: n, email: n.toLowerCase().replace(/[^a-z]+/g, '.') + '@example.com', city, country: cc, loyalty_tier: 'Bronze', updated_at: t }, 'INSERT');
          const e = this.pick(Array.from(this.src.events.values()));
          this.order(id, e.event_id, 2, 'PLACED', addH(t, 0.25));
          return `New customer ${n} (${id}) signed up and ordered 2 × ${e.event_name}.`;
        }
        case 'price': {
          const e = this.pick(Array.from(this.src.events.values())); const old = e.base_price;
          this.upsert('events', Object.assign(e, { base_price: money(e.base_price * 1.15), updated_at: t }));
          return `${e.event_name} base price rose from ${fmtMoney(old)} to ${fmtMoney(e.base_price)}. Past orders keep the price they paid.`;
        }
        case 'duplicate': {
          const last = this.pending.length ? this.pending[this.pending.length - 1] : this.bronze[this.bronze.length - 1];
          if (!last) return null;
          this.pending.push({ table: last.table, op: last.op, data: clone(last.data), updated_at: last.updated_at, duplicate: true });
          return `The connector re-delivered a record it already sent (${last.table} ${last.data[SCHEMA[last.table].key]}). At-least-once delivery does this.`;
        }
        case 'late': {
          // The customer whose record changed most recently gets a change dated just before that one.
          const c = cust().sort((a, b) => (a.updated_at > b.updated_at ? -1 : a.updated_at < b.updated_at ? 1 : a.customer_id < b.customer_id ? -1 : 1))[0];
          const [city, cc] = CITIES.filter(x => x[0] !== c.city)[3];
          const when = addH(c.updated_at, -2);
          this.pending.push({ table: 'customers', op: 'UPDATE', data: Object.assign(clone(c), { city, country: cc, updated_at: when }), updated_at: when, late: true });
          return `A delayed record arrived: ${c.full_name} lived in ${city} from ${when.slice(0, 16).replace('T', ' ')}, two hours before their latest change. It belongs in history; the current address must not go backwards.`;
        }
        case 'early': {
          const id = 'C-' + this.nextEarly++;
          const e = this.pick(Array.from(this.src.events.values()));
          const rec = this.order(id, e.event_id, 1, 'PAID', t);
          const [n, city, cc] = NEW_PEOPLE[this.newPerson++ % NEW_PEOPLE.length];
          this.deferred.push({ table: 'customers', op: 'INSERT', data: { customer_id: id, full_name: n, email: n.toLowerCase().replace(/[^a-z]+/g, '.') + '@example.com', city, country: cc, loyalty_tier: 'Bronze', updated_at: addH(t, -1) }, updated_at: addH(t, -1) });
          return `A guest checkout created ${rec.data.order_id} for ${id} before the CRM synced that customer. The customer record arrives in the next batch.`;
        }
        case 'bad': {
          const e = this.pick(Array.from(this.src.events.values()));
          const row = { order_id: 'O-' + this.nextOrder++, customer_id: null, event_id: e.event_id, quantity: -2, unit_price: e.base_price, status: 'PAID', order_ts: t, updated_at: t };
          this.pending.push({ table: 'orders', op: 'INSERT', data: row, updated_at: t });
          return `A buggy client sent ${row.order_id} with no customer_id and quantity −2.`;
        }
        case 'drift': {
          const c = this.pick(cust()), e = this.pick(Array.from(this.src.events.values()));
          const rec = this.order(c.customer_id, e.event_id, 2, 'PLACED', t, { promo_code: 'SPRING10', sales_channel: 'mobile_app' });
          return `The app team started sending new fields (promo_code, sales_channel) on ${rec.data.order_id} without telling anyone.`;
        }
        default: return null;
      }
    }

    /* ---------- a pipeline run ---------- */
    run(reason) {
      this.batch++;
      const ingested = this.tick(0.25);
      const recs = this.pending.splice(0);
      recs.forEach((r, i) => {
        const known = SCHEMA[r.table].cols;
        const data = {}; known.forEach(c => { data[c] = r.data[c] === undefined ? null : r.data[c]; });
        this.bronze.push({ table: r.table, op: r.op, data, _rescued_data: r.extra ? JSON.stringify(r.extra) : null,
          _batch_id: this.batch, _ingested_at: ingested, _source_file: `landing/${r.table}/batch_${String(this.batch).padStart(3, '0')}.json`, _seq: this.bronze.length + i * 0 });
      });
      this.bronze.forEach((b, i) => { b._seq = i; });
      this.pending = this.deferred.splice(0);
      this.prev = this.out;
      this.out = this.build();
      this.log.unshift({ batch: this.batch, at: ingested, reason: reason || null, notes: this.explain(recs) });
      return this.log[0];
    }

    /* Everything downstream is a pure function of Bronze. */
    build() {
      const o = { tables: {} };
      const T = o.tables;
      /* Bronze: append-only, every record exactly as delivered */
      for (const t of Object.keys(SCHEMA)) {
        T['bronze.' + t] = this.bronze.filter(b => b.table === t).map(b => Object.assign({ _op: b.op }, b.data, { _rescued_data: b._rescued_data, _batch_id: b._batch_id, _ingested_at: b._ingested_at }));
      }
      /* Silver: typed, validated, deduplicated change history plus the current state per key */
      const quarantine = [];
      const changes = {};
      for (const t of Object.keys(SCHEMA)) {
        const seen = new Set(), list = [];
        for (const b of this.bronze.filter(x => x.table === t)) {
          const d = b.data, why = this.violations(t, d);
          if (why.length) { quarantine.push({ source_table: t, record_key: d[SCHEMA[t].key], reason: why.join('; '), _batch_id: b._batch_id, _ingested_at: b._ingested_at }); continue; }
          const sig = JSON.stringify([d[SCHEMA[t].key], d.updated_at, d]);
          if (seen.has(sig)) continue;
          seen.add(sig);
          list.push(Object.assign({}, d, { _seq: b._seq, _ingested_at: b._ingested_at }));
        }
        list.sort((a, b) => (a.updated_at < b.updated_at ? -1 : a.updated_at > b.updated_at ? 1 : a._seq - b._seq));
        changes[t] = list;
        const cur = new Map();
        list.forEach(r => cur.set(r[SCHEMA[t].key], r));
        T['silver.' + t] = Array.from(cur.values()).map(r => { const x = Object.assign({}, r); delete x._seq; delete x._ingested_at; return x; })
          .sort((a, b) => (a[SCHEMA[t].key] < b[SCHEMA[t].key] ? -1 : 1));
      }
      T['silver.quarantine'] = quarantine;
      o.changes = changes;

      /* Gold, star schema */
      const tracked = ['city', 'country', 'loyalty_tier'];
      const dimC = [];
      const byCust = new Map();
      changes.customers.forEach(r => { if (!byCust.has(r.customer_id)) byCust.set(r.customer_id, []); byCust.get(r.customer_id).push(r); });
      for (const [id, rows] of byCust) {
        const latest = rows[rows.length - 1];
        const versions = [];
        rows.forEach(r => {
          const last = versions[versions.length - 1];
          if (!last || tracked.some(c => last.r[c] !== r[c])) versions.push({ r, from: last ? r.updated_at : BEGIN });
        });
        versions.forEach((v, i) => {
          const key = id + '|' + v.from;
          if (!this.sk.customer.has(key)) this.sk.customer.set(key, this.sk.nextCustomer++);
          dimC.push({ customer_sk: this.sk.customer.get(key), customer_id: id, full_name: latest.full_name, email: latest.email, city: v.r.city, country: v.r.country,
            loyalty_tier: v.r.loyalty_tier, valid_from: v.from, valid_to: i + 1 < versions.length ? versions[i + 1].from : END, is_current: i + 1 === versions.length });
        });
      }
      dimC.sort((a, b) => (a.customer_id < b.customer_id ? -1 : a.customer_id > b.customer_id ? 1 : a.valid_from < b.valid_from ? -1 : 1));
      T['gold.dim_customer'] = [{ customer_sk: -1, customer_id: 'UNKNOWN', full_name: 'Unknown customer', email: null, city: 'Unknown', country: null, loyalty_tier: null, valid_from: BEGIN, valid_to: END, is_current: true }].concat(dimC);
      T['gold.dim_event'] = [{ event_sk: -1, event_id: 'UNKNOWN', event_name: 'Unknown event', artist: null, venue: null, city: null, event_date: null, base_price: null }]
        .concat(T['silver.events'].map(e => {
          if (!this.sk.event.has(e.event_id)) this.sk.event.set(e.event_id, this.sk.nextEvent++);
          return { event_sk: this.sk.event.get(e.event_id), event_id: e.event_id, event_name: e.event_name, artist: e.artist, venue: e.venue, city: e.city, event_date: e.event_date, base_price: fmtMoney(e.base_price) };
        }));
      const dates = new Set(T['silver.orders'].map(x => x.order_ts.slice(0, 10)));
      T['gold.dim_date'] = Array.from(dates).sort().map(d => {
        const dt = new Date(d + 'T00:00:00Z');
        return { date_key: Number(d.replace(/-/g, '')), full_date: d, day_name: dt.toLocaleDateString('en-GB', { weekday: 'long', timeZone: 'UTC' }), month: d.slice(0, 7), quarter: `${d.slice(0, 4)}-Q${Math.floor((dt.getUTCMonth()) / 3) + 1}`, is_weekend: [0, 6].includes(dt.getUTCDay()) };
      });
      const asOf = (id, ts) => { const v = dimC.find(x => x.customer_id === id && x.valid_from <= ts && ts < x.valid_to); return v ? v.customer_sk : -1; };
      T['gold.fact_ticket_sales'] = T['silver.orders'].map(x => ({
        order_id: x.order_id, date_key: Number(x.order_ts.slice(0, 10).replace(/-/g, '')), customer_sk: asOf(x.customer_id, x.order_ts),
        event_sk: this.sk.event.has(x.event_id) ? this.sk.event.get(x.event_id) : -1, quantity: x.quantity, unit_price: fmtMoney(x.unit_price),
        gross_amount: fmtMoney(x.quantity * x.unit_price), order_status: x.status,
      }));

      /* Raw Vault: loaded from Bronze, insert-only, hard rules only. Bad data is kept. */
      const RS = t => 'stagedoor.ticketing.' + t;
      const hubs = { customer: new Map(), event: new Map(), order: new Map() };
      const links = new Map();
      const sats = { customer: [], event: [], order: [] };
      const satCols = { customer: ['full_name', 'email', 'city', 'country', 'loyalty_tier'], event: ['event_name', 'artist', 'venue', 'city', 'event_date', 'base_price'], order: ['quantity', 'unit_price', 'status', 'order_ts'] };
      /* Satellite history per hash key, in business (applied) time. A record is inserted unless it is an exact
         repeat, or it matches the row just before it in applied time. Comparing against the latest *loaded* row
         instead would silently drop late-arriving changes, a classic Data Vault pitfall with out-of-order data. */
      const hist = { customer: new Map(), event: new Map(), order: new Map() };
      const addHub = (h, id, b) => { const k = hashKey(id); if (!hubs[h].has(k)) hubs[h].set(k, { ['hk_' + h]: k, [h + '_id']: bk(id) == null ? null : String(id).trim(), load_ts: b._ingested_at, record_source: RS(b.table) }); return k; };
      for (const b of this.bronze) {
        const d = b.data;
        const ent = { customers: 'customer', events: 'event', orders: 'order' }[b.table];
        const hk = addHub(ent, d[SCHEMA[b.table].key], b);
        if (b.table === 'orders') {
          const hc = addHub('customer', d.customer_id, b), he = addHub('event', d.event_id, b);
          const lk = hashLink(d.order_id, d.customer_id, d.event_id);
          if (!links.has(lk)) links.set(lk, { hk_order_customer_event: lk, hk_order: hk, hk_customer: hc, hk_event: he, load_ts: b._ingested_at, record_source: RS(b.table) });
        }
        const cols = satCols[ent];
        const row = {}; cols.forEach(c => { row[c] = c === 'base_price' || c === 'unit_price' ? fmtMoney(d[c]) : d[c]; });
        const hd = hashDiff(row, cols);
        const h = hist[ent].get(hk) || [];
        if (h.some(x => x.applied === d.updated_at && x.hd === hd)) continue;
        const pred = h.filter(x => x.applied <= d.updated_at).pop();
        if (pred && pred.hd === hd) continue;
        h.push({ applied: d.updated_at, hd });
        h.sort((x, y) => (x.applied < y.applied ? -1 : x.applied > y.applied ? 1 : 0));
        hist[ent].set(hk, h);
        sats[ent].push(Object.assign({ ['hk_' + ent]: hk, load_ts: b._ingested_at, applied_ts: d.updated_at, hashdiff: hd }, row, { record_source: RS(b.table) }));
      }
      ['customer', 'event', 'order'].forEach(h => { T[`raw_vault.hub_${h}`] = Array.from(hubs[h].values()); T[`raw_vault.sat_${h}_details`] = sats[h]; });
      T['raw_vault.link_order_customer_event'] = Array.from(links.values());

      /* Business Vault: soft rules, effectivity and point-in-time structures */
      const custSat = sats.customer.slice().sort((a, b) => (a.hk_customer < b.hk_customer ? -1 : a.hk_customer > b.hk_customer ? 1 : a.applied_ts < b.applied_ts ? -1 : a.applied_ts > b.applied_ts ? 1 : a.load_ts < b.load_ts ? -1 : 1));
      const eff = [];
      const byHk = new Map();
      custSat.forEach(s => { if (!byHk.has(s.hk_customer)) byHk.set(s.hk_customer, []); byHk.get(s.hk_customer).push(s); });
      for (const [hk, rows] of byHk) {
        const versions = [];
        rows.forEach(s => { const last = versions[versions.length - 1]; if (!last || tracked.some(c => last.s[c] !== s[c])) versions.push({ s, from: last ? s.applied_ts : BEGIN }); });
        const latest = rows[rows.length - 1];
        versions.forEach((v, i) => eff.push({ hk_customer: hk, customer_id: hubs.customer.get(hk).customer_id, full_name: latest.full_name, email: latest.email, city: v.s.city, country: v.s.country, loyalty_tier: v.s.loyalty_tier,
          effective_from: v.from, effective_to: i + 1 < versions.length ? versions[i + 1].from : END, is_current: i + 1 === versions.length }));
      }
      T['business_vault.bv_customer_effectivity'] = eff;
      const orderSat = new Map();
      sats.order.slice().sort((a, b) => (a.applied_ts < b.applied_ts ? -1 : a.applied_ts > b.applied_ts ? 1 : a.load_ts < b.load_ts ? -1 : 1)).forEach(s => orderSat.set(s.hk_order, s));
      const validOrders = [];
      for (const l of links.values()) {
        const s = orderSat.get(l.hk_order); if (!s) continue;
        const why = [];
        if (l.hk_customer === ZERO_KEY) why.push('customer_id is missing');
        if (!(Number(s.quantity) > 0)) why.push('quantity must be positive');
        validOrders.push({ hk_order: l.hk_order, order_id: hubs.order.get(l.hk_order).order_id, hk_customer: l.hk_customer, hk_event: l.hk_event, quantity: s.quantity, unit_price: s.unit_price, status: s.status, order_ts: s.order_ts, passes_rules: !why.length, rule_failures: why.join('; ') || null });
      }
      T['business_vault.bv_order_current'] = validOrders.sort((a, b) => (a.order_id < b.order_id ? -1 : 1));
      const snapshots = Array.from(new Set(validOrders.map(x => x.order_ts.slice(0, 10)))).sort();
      const pit = [];
      for (const hk of hubs.customer.keys()) for (const d of snapshots) {
        const asof = d + 'T23:59:59Z';
        const rows = byHk.get(hk) || [];
        const before = rows.filter(s => s.applied_ts <= asof);
        const pick = before.length ? before[before.length - 1] : null;
        pit.push({ hk_customer: hk, snapshot_date: d, sat_customer_load_ts: pick ? pick.load_ts : null, sat_customer_applied_ts: pick ? pick.applied_ts : null });
      }
      T['business_vault.pit_customer'] = pit;
      o.vault = { hubs, links, sats, eff, validOrders, byHk };
      o.answers = answerAll(T, o.vault);
      return o;
    }
    violations(t, d) {
      const why = [];
      if (d[SCHEMA[t].key] == null) why.push(`${SCHEMA[t].key} is missing`);
      if (t === 'orders') {
        if (d.customer_id == null) why.push('customer_id is missing');
        if (!(Number(d.quantity) > 0)) why.push('quantity must be positive');
        if (!['PLACED', 'PAID', 'REFUNDED'].includes(d.status)) why.push('unknown status');
      }
      if (t === 'customers' && (!d.email || !d.email.includes('@'))) why.push('email is invalid');
      return why;
    }

    /* ---------- what changed, in plain words ---------- */
    explain(recs) {
      const notes = [];
      const T = this.out.tables, P = this.prev ? this.prev.tables : {};
      const n = (name, f) => (T[name] || []).filter(f || (() => true)).length;
      const pn = (name, f) => (P[name] || []).filter(f || (() => true)).length;
      const dupes = recs.filter(r => r.duplicate).length, late = recs.filter(r => r.late).length, drift = recs.filter(r => r.extra).length;
      notes.push({ layer: 'bronze', text: `Appended ${recs.length} record${recs.length === 1 ? '' : 's'} exactly as delivered${dupes ? `, including ${dupes} duplicate${dupes > 1 ? 's' : ''}` : ''}. Bronze never updates or deletes.` });
      if (drift) notes.push({ layer: 'bronze', text: `${drift} record${drift > 1 ? 's' : ''} carried unexpected fields. Auto Loader's rescued data column kept them in _rescued_data instead of failing or silently dropping them.` });
      const q = n('silver.quarantine') - pn('silver.quarantine');
      if (q) notes.push({ layer: 'silver', text: `${q} record${q > 1 ? 's' : ''} failed data-quality rules and went to silver.quarantine with the reason. They never reach Gold.` });
      if (dupes) notes.push({ layer: 'silver', text: 'Duplicates were dropped: same key, same updated_at, same payload.' });
      if (late) notes.push({ layer: 'silver', text: 'The late record was sequenced by updated_at, not arrival time: it changed history but not the current row.' });
      const newVersions = n('gold.dim_customer') - pn('gold.dim_customer');
      if (this.prev && newVersions > 0) notes.push({ layer: 'gold', text: `dim_customer gained ${newVersions} version${newVersions > 1 ? 's' : ''}. The previous version's valid_to was closed, so orders keep the city they were placed from.` });
      const unknown = n('gold.fact_ticket_sales', f => f.customer_sk === -1);
      if (unknown) notes.push({ layer: 'gold', text: `${unknown} fact row${unknown > 1 ? 's' : ''} point${unknown > 1 ? '' : 's'} to the Unknown customer (customer_sk −1) because the customer record has not arrived yet. They will re-point when it does.` });
      else if (P['gold.fact_ticket_sales'] && P['gold.fact_ticket_sales'].some(f => f.customer_sk === -1)) notes.push({ layer: 'gold', text: 'The late customer arrived, and the fact rows that pointed to Unknown now point to the real customer.' });
      const fact = n('gold.fact_ticket_sales') - pn('gold.fact_ticket_sales');
      if (fact > 0 && this.prev) notes.push({ layer: 'gold', text: `fact_ticket_sales gained ${fact} order${fact > 1 ? 's' : ''}; status changes were merged into existing rows.` });
      ['customer', 'event', 'order'].forEach(h => {
        const hub = n(`raw_vault.hub_${h}`) - pn(`raw_vault.hub_${h}`), sat = n(`raw_vault.sat_${h}_details`) - pn(`raw_vault.sat_${h}_details`);
        if (this.prev && (hub || sat)) notes.push({ layer: 'vault', text: `${h}: ${hub ? `${hub} new business key${hub > 1 ? 's' : ''} in hub_${h}, ` : ''}${sat} new satellite row${sat === 1 ? '' : 's'} (only rows whose hashdiff changed).` });
      });
      if (dupes) notes.push({ layer: 'vault', text: 'The duplicate produced no new satellite row: the same hashdiff already existed at that applied_ts.' });
      if (late) notes.push({ layer: 'vault', text: 'The late change was compared with the satellite row before it in applied time, not with the latest row loaded, so it was kept.' });
      if (this.prev && recs.some(r => r.table === 'orders' && r.data.customer_id == null)) notes.push({ layer: 'vault', text: 'The raw vault kept the bad order and linked it to the zero key (000…0). Business rules in the business vault exclude it from reporting, without losing the evidence.' });
      return notes;
    }
  }

  /* ---------- the same business questions, answered from both models ---------- */
  function answerAll(T, V) {
    const star = {}, vault = {};
    const dimC = new Map(T['gold.dim_customer'].map(d => [d.customer_sk, d]));
    const dimE = new Map(T['gold.dim_event'].map(d => [d.event_sk, d]));
    const paid = T['gold.fact_ticket_sales'].filter(f => f.order_status === 'PAID');
    const sum = (rows, keyF, valF) => { const m = new Map(); rows.forEach(r => { const k = keyF(r); m.set(k, money((m.get(k) || 0) + valF(r))); }); return Array.from(m, ([k, v]) => ({ key: k, value: v })).sort((a, b) => b.value - a.value || (a.key < b.key ? -1 : 1)); };
    star.revenue_by_city = sum(paid, f => dimC.get(f.customer_sk).city, f => Number(f.gross_amount));
    star.tickets_by_event = sum(paid, f => dimE.get(f.event_sk).event_name, f => f.quantity);
    star.customers_by_tier = sum(T['gold.dim_customer'].filter(d => d.is_current && d.customer_sk !== -1), d => d.loyalty_tier, () => 1);

    const vpaid = V.validOrders.filter(o => o.passes_rules && o.status === 'PAID');
    const cityAt = (hk, ts) => { const v = V.eff.find(e => e.hk_customer === hk && e.effective_from <= ts && ts < e.effective_to); return v ? v.city : 'Unknown'; };
    vault.revenue_by_city = sum(vpaid, o => cityAt(o.hk_customer, o.order_ts), o => Number(o.quantity) * Number(o.unit_price));
    const evName = new Map();
    V.sats.event.slice().sort((a, b) => (a.applied_ts < b.applied_ts ? -1 : 1)).forEach(s => evName.set(s.hk_event, s.event_name));
    vault.tickets_by_event = sum(vpaid, o => evName.get(o.hk_event) || 'Unknown event', o => Number(o.quantity));
    vault.customers_by_tier = sum(V.eff.filter(e => e.is_current), e => e.loyalty_tier, () => 1);
    return { star, vault };
  }

  /* ---------- table catalogue: what each table is, its keys, and how Databricks builds it ---------- */
  const CATALOG = {
    'bronze.customers': { layer: 'bronze', grain: 'One row per change record received, duplicates included.', keys: [], sql: 'bronze' },
    'bronze.events': { layer: 'bronze', grain: 'One row per change record received.', keys: [], sql: 'bronze' },
    'bronze.orders': { layer: 'bronze', grain: 'One row per change record received.', keys: [], sql: 'bronze' },
    'silver.customers': { layer: 'silver', grain: 'One row per customer: the latest valid state (SCD Type 1).', keys: ['customer_id'], sql: 'silver' },
    'silver.events': { layer: 'silver', grain: 'One row per event: the latest valid state.', keys: ['event_id'], sql: 'silver' },
    'silver.orders': { layer: 'silver', grain: 'One row per order: the latest valid state.', keys: ['order_id'], sql: 'silver' },
    'silver.quarantine': { layer: 'silver', grain: 'One row per rejected record, with the rule it broke.', keys: [], sql: 'quarantine' },
    'gold.dim_customer': { layer: 'gold', grain: 'One row per customer per version of city, country and tier (SCD Type 2).', keys: ['customer_sk'], sql: 'dim_customer' },
    'gold.dim_event': { layer: 'gold', grain: 'One row per event (SCD Type 1).', keys: ['event_sk'], sql: 'dim_event' },
    'gold.dim_date': { layer: 'gold', grain: 'One row per calendar day.', keys: ['date_key'], sql: 'dim_date' },
    'gold.fact_ticket_sales': { layer: 'gold', grain: 'One row per order (transaction fact), with the order\'s current status.', keys: ['order_id'], fks: ['date_key', 'customer_sk', 'event_sk'], sql: 'fact' },
    'raw_vault.hub_customer': { layer: 'vault', grain: 'One row per unique customer business key, ever.', keys: ['hk_customer'], sql: 'hub' },
    'raw_vault.hub_event': { layer: 'vault', grain: 'One row per unique event business key.', keys: ['hk_event'], sql: 'hub' },
    'raw_vault.hub_order': { layer: 'vault', grain: 'One row per unique order business key.', keys: ['hk_order'], sql: 'hub' },
    'raw_vault.link_order_customer_event': { layer: 'vault', grain: 'One row per unique order, customer and event combination.', keys: ['hk_order_customer_event'], fks: ['hk_order', 'hk_customer', 'hk_event'], sql: 'link' },
    'raw_vault.sat_customer_details': { layer: 'vault', grain: 'One row per change in customer attributes (insert-only).', keys: ['hk_customer', 'load_ts', 'applied_ts'], sql: 'sat' },
    'raw_vault.sat_event_details': { layer: 'vault', grain: 'One row per change in event attributes.', keys: ['hk_event', 'load_ts', 'applied_ts'], sql: 'sat' },
    'raw_vault.sat_order_details': { layer: 'vault', grain: 'One row per change in order attributes.', keys: ['hk_order', 'load_ts', 'applied_ts'], sql: 'sat' },
    'business_vault.bv_customer_effectivity': { layer: 'bvault', grain: 'One row per customer per effective period, derived from the satellite.', keys: ['hk_customer', 'effective_from'], sql: 'effectivity' },
    'business_vault.bv_order_current': { layer: 'bvault', grain: 'One row per order with soft business rules applied.', keys: ['hk_order'], sql: 'bv_order' },
    'business_vault.pit_customer': { layer: 'bvault', grain: 'One row per customer per snapshot date, pointing at the satellite row valid then.', keys: ['hk_customer', 'snapshot_date'], sql: 'pit' },
  };
  const QUESTIONS = [
    { id: 'revenue_by_city', title: 'Paid revenue by the customer\'s city at the time of purchase', unit: 'money',
      why: 'Needs history: a customer who moved must count in the city they lived in when they ordered.',
      star: `SELECT c.city, SUM(f.gross_amount) AS revenue
FROM gold.fact_ticket_sales f
JOIN gold.dim_customer c ON c.customer_sk = f.customer_sk   -- the version valid at order time
WHERE f.order_status = 'PAID'
GROUP BY c.city
ORDER BY revenue DESC;`,
      vault: `SELECT COALESCE(e.city, 'Unknown') AS city, SUM(o.quantity * o.unit_price) AS revenue
FROM business_vault.bv_order_current o
LEFT JOIN business_vault.bv_customer_effectivity e
  ON e.hk_customer = o.hk_customer
 AND o.order_ts >= e.effective_from AND o.order_ts < e.effective_to
WHERE o.passes_rules AND o.status = 'PAID'
GROUP BY ALL
ORDER BY revenue DESC;` },
    { id: 'tickets_by_event', title: 'Paid tickets per event', unit: 'count',
      why: 'A plain aggregate over a conformed dimension. Easy in a star; in a vault you first have to pick the current satellite row per event.',
      star: `SELECT e.event_name, SUM(f.quantity) AS tickets
FROM gold.fact_ticket_sales f
JOIN gold.dim_event e USING (event_sk)
WHERE f.order_status = 'PAID'
GROUP BY e.event_name
ORDER BY tickets DESC;`,
      vault: `WITH event_now AS (
  SELECT hk_event, event_name
  FROM raw_vault.sat_event_details
  QUALIFY ROW_NUMBER() OVER (PARTITION BY hk_event ORDER BY applied_ts DESC, load_ts DESC) = 1
)
SELECT COALESCE(e.event_name, 'Unknown event') AS event_name, SUM(o.quantity) AS tickets
FROM business_vault.bv_order_current o
LEFT JOIN event_now e USING (hk_event)
WHERE o.passes_rules AND o.status = 'PAID'
GROUP BY ALL
ORDER BY tickets DESC;` },
    { id: 'customers_by_tier', title: 'Customers in each loyalty tier today', unit: 'count',
      why: 'Current state only: the star filters is_current, the vault filters the open effective period.',
      star: `SELECT loyalty_tier, COUNT(*) AS customers
FROM gold.dim_customer
WHERE is_current AND customer_sk <> -1
GROUP BY loyalty_tier
ORDER BY customers DESC;`,
      vault: `SELECT loyalty_tier, COUNT(*) AS customers
FROM business_vault.bv_customer_effectivity
WHERE is_current
GROUP BY loyalty_tier
ORDER BY customers DESC;` },
  ];
  const ACTIONS = [
    { id: 'order', label: 'New order', note: 'A fan buys tickets.' },
    { id: 'pay', label: 'Capture payments', note: 'PLACED orders become PAID.' },
    { id: 'refund', label: 'Refund an order', note: 'A PAID order is refunded.' },
    { id: 'move', label: 'Customer moves', note: 'City changes: Type 2 history.' },
    { id: 'tier', label: 'Loyalty upgrade', note: 'Tier changes: Type 2 history.' },
    { id: 'email', label: 'Email change', note: 'Type 1: overwrite, no history.' },
    { id: 'signup', label: 'New customer', note: 'Sign-up plus a first order.' },
    { id: 'price', label: 'Price increase', note: 'Event price rises 15%.' },
    { id: 'duplicate', label: 'Duplicate delivery', note: 'The connector resends a record.' },
    { id: 'late', label: 'Late-arriving change', note: 'An old address change shows up now.' },
    { id: 'early', label: 'Early-arriving fact', note: 'An order before its customer.' },
    { id: 'bad', label: 'Bad record', note: 'No customer, negative quantity.' },
    { id: 'drift', label: 'Schema drift', note: 'New fields appear unannounced.' },
  ];

  root.Lab = Lab;
  root.CATALOG = CATALOG; root.QUESTIONS = QUESTIONS; root.ACTIONS = ACTIONS; root.SCHEMA = SCHEMA;
  root.util = { hashKey, hashLink, hashDiff, ZERO_KEY, BEGIN, END, bk };
})(typeof window !== 'undefined' ? (window.LML = window.LML || {}) : (global.LML = global.LML || {}));
