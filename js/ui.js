/* ui.js: renders the playground. All modeling logic lives in model.js. */
(function () {
  'use strict';
  const L = window.LML;
  const $ = s => document.querySelector(s);
  const h = (tag, attrs, ...kids) => {
    const el = document.createElement(tag);
    Object.entries(attrs || {}).forEach(([k, v]) => {
      if (v == null || v === false) return;
      if (k === 'class') el.className = v; else if (k === 'text') el.textContent = v; else if (k === 'html') el.innerHTML = v;
      else if (k.startsWith('on')) el.addEventListener(k.slice(2), v); else el.setAttribute(k, v === true ? '' : v);
    });
    kids.flat().forEach(c => { if (c != null && c !== false) el.append(c.nodeType ? c : document.createTextNode(c)); });
    return el;
  };

  let lab = new L.Lab(7);
  let selected = 'gold.dim_customer', pane = 'rows', queued = [];

  const NODES = {
    bronze: { title: 'Bronze', sub: 'Raw, append-only, exactly as received', chip: 'bronze' },
    silver: { title: 'Silver', sub: 'Validated, deduplicated, current state', chip: 'silver' },
    gold: { title: 'Gold · Star schema', sub: 'Facts and dimensions for BI', chip: 'gold' },
    raw_vault: { title: 'Silver · Raw Vault', sub: 'Hubs, links, satellites. Insert-only', chip: 'vault' },
    business_vault: { title: 'Business Vault', sub: 'Rules, effectivity, point-in-time', chip: 'bvault' },
  };
  const LAYER_CHIP = { bronze: ['bronze', 'Bronze'], silver: ['silver', 'Silver'], gold: ['gold', 'Gold'], vault: ['vault', 'Raw vault'], bvault: ['bvault', 'Business vault'] };
  const TRICKY = new Set(['duplicate', 'late', 'early', 'bad', 'drift']);

  /* ---------- actions and the pending batch ---------- */
  function renderActions() {
    $('#actions').replaceChildren(...L.ACTIONS.map(a => h('button', { type: 'button', class: 'act' + (TRICKY.has(a.id) ? ' is-tricky' : ''), title: a.note,
      onclick: () => { const text = lab.act(a.id); if (text) queued.push({ text, kind: a.id }); renderPending(); } }, h('b', { text: a.label }), h('span', { text: a.note }))));
  }
  function renderPending() {
    const recs = lab.pending;
    $('#pend-count').textContent = recs.length ? `${recs.length} change record${recs.length === 1 ? '' : 's'}` : '';
    let items = queued.map(q => h('li', null, h('span', { class: 'op', text: '+' }), h('span', { text: q.text })));
    if (!items.length && recs.length) items = [h('li', null, h('span', { class: 'op', text: 'HELD' }), h('span', { text: 'The late customer record from the guest checkout will arrive with this batch.' }))];
    if (!items.length) items = [h('li', null, h('span', { class: 'empty', text: 'Nothing queued. Pick some production events above, or run as is.' }))];
    $('#pending').replaceChildren(...items);
  }
  function run() {
    const reason = queued.map(q => q.text).join(' ');
    lab.run(reason || null);
    queued = [];
    renderPending(); renderLineage(true); renderViewer(); renderLog(); renderQuestions();
  }

  /* ---------- lineage ---------- */
  function tablesOf(node) { return Object.keys(lab.out.tables).filter(k => k.startsWith(node + '.')); }
  function renderLineage(flash) {
    const T = lab.out.tables, P = lab.prev ? lab.prev.tables : {};
    document.querySelectorAll('.node').forEach(el => {
      const node = el.dataset.node, meta = NODES[node];
      const rows = tablesOf(node).map(name => {
        const n = T[name].length, d = n - (P[name] ? P[name].length : n);
        const changed = flash && lab.prev && JSON.stringify(T[name]) !== JSON.stringify(P[name] || []);
        const b = h('button', { type: 'button', class: 'tbl' + (changed ? ' flash' : ''), 'aria-pressed': String(name === selected), title: `${name}: ${L.CATALOG[name].grain}`,
          onclick: () => { selected = name; renderLineage(false); renderViewer(); } },
          h('span', { class: 'tbl-name', text: name.split('.')[1] }), h('span', { class: 'tbl-n', text: String(n) }),
          h('span', { class: 'tbl-d' + (d ? '' : ' is-zero'), text: d > 0 ? '+' + d : d < 0 ? String(d) : '0' }));
        return b;
      });
      el.replaceChildren(h('div', { class: 'node-head' }, h('div', { class: 'node-title', text: meta.title }), h('div', { class: 'node-sub', text: meta.sub })), h('div', { class: 'node-tables' }, rows));
    });
  }

  /* ---------- table viewer ---------- */
  const isHash = v => typeof v === 'string' && /^[0-9a-f]{32}$/.test(v);
  const isTs = v => typeof v === 'string' && /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/.test(v);
  function cell(col, v) {
    if (v == null) return h('td', { class: 'null', text: 'null' });
    if (isHash(v)) return h('td', { class: 'hash', title: v, text: v === L.util.ZERO_KEY ? '0000…0000 (zero key)' : v.slice(0, 10) + '…' });
    if (isTs(v)) return h('td', { text: v.startsWith('1900') ? '1900-01-01 (start)' : v.startsWith('9999') ? '9999-12-31 (open)' : v.slice(0, 16).replace('T', ' ') });
    if (typeof v === 'boolean') return h('td', { text: String(v) });
    if (typeof v === 'number' || /^-?\d+\.\d\d$/.test(v)) return h('td', { class: 'num' + (Number(v) < 0 ? ' bad' : ''), text: String(v) });
    return h('td', { text: String(v), title: String(v).length > 30 ? String(v) : null });
  }
  function rowId(name, r) {
    const keys = (L.CATALOG[name].keys || []);
    if (!keys.length) return JSON.stringify(r);
    return keys.map(k => r[k]).join('|');
  }
  function renderViewer() {
    const meta = L.CATALOG[selected], [chipCls, chipText] = LAYER_CHIP[meta.layer];
    $('#tv-name').replaceChildren(h('span', { text: selected }), h('span', { class: 'chip chip-' + chipCls, text: chipText }));
    $('#tv-grain').textContent = meta.grain;
    const keys = meta.keys || [], fks = meta.fks || [];
    $('#tv-keys').replaceChildren(...(keys.length ? [h('b', { text: 'Key: ' }), keys.join(' + ')] : ['No key: every record is kept.']), ...(fks.length ? ['   ', h('b', { text: 'References: ' }), fks.join(', ')] : []));
    const rows = lab.out.tables[selected];
    const prev = lab.prev ? lab.prev.tables[selected] || [] : null;
    const prevById = prev ? new Map(prev.map(r => [rowId(selected, r), JSON.stringify(r)])) : null;
    const cols = rows.length ? Object.keys(rows[0]) : [];
    const shown = rows.slice(-80);
    const tbody = shown.map(r => {
      const id = rowId(selected, r), was = prevById && prevById.get(id);
      const cls = prevById && was == null ? 'is-new' : prevById && was !== JSON.stringify(r) ? 'is-chg' : r.is_current === false ? 'is-closed' : null;
      return h('tr', { class: cls }, cols.map(c => cell(c, r[c])));
    });
    const table = h('table', { class: 'grid' },
      h('thead', null, h('tr', null, cols.map(c => h('th', { class: keys.includes(c) ? 'is-pk' : fks.includes(c) ? 'is-fk' : null, scope: 'col', text: c })))),
      h('tbody', null, tbody.length ? tbody : h('tr', null, h('td', { text: 'No rows yet.' }))));
    $('#tv-rows').replaceChildren(table, h('div', { class: 'tv-legend' }, h('span', { class: 'lg-new', text: 'New in the last run' }), h('span', { class: 'lg-chg', text: 'Changed in the last run' }),
      h('span', { class: 'lg-closed', text: 'Closed version (history)' }), rows.length > shown.length ? h('span', { text: `Showing the last ${shown.length} of ${rows.length} rows` }) : null));
    const s = L.SQL[meta.sql];
    $('#tv-sql').replaceChildren(h('pre', { class: 'code', html: highlight(s.code, s.lang) }), h('p', { class: 'src-note', html: `From <code>${s.file}</code>. On Databricks the schemas are prefixed, for example <code>stagedoor_gold</code>.` }));
    $('#tv-rows').hidden = pane !== 'rows'; $('#tv-sql').hidden = pane !== 'sql';
    document.querySelectorAll('.seg-btn').forEach(b => b.setAttribute('aria-pressed', String(b.dataset.pane === pane)));
  }
  function highlight(code, lang) {
    const esc = s => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const kw = lang === 'python' ? /\b(from|import|def|return|for|in|if|else|True|False|None)\b/g
      : /\b(SELECT|FROM|WHERE|JOIN|LEFT|ON|AND|OR|NOT|IN|IS|NULL|DISTINCT|AS|MERGE|INTO|USING|WHEN|MATCHED|THEN|UPDATE|SET|INSERT|VALUES|UNION|ALL|GROUP|BY|ORDER|DESC|QUALIFY|OVER|PARTITION|CREATE|OR|REPLACE|VIEW|TABLE|WITH|CASE|END|LATERAL|LIMIT|CROSS|WINDOW|EXISTS|INTERVAL|DATE|TIMESTAMP|IF|COALESCE|true|false)\b/g;
    return code.split('\n').map(line => {
      const ci = lang === 'python' ? line.indexOf('#') : line.indexOf('--');
      const body = ci >= 0 ? line.slice(0, ci) : line, comment = ci >= 0 ? line.slice(ci) : '';
      const parts = esc(body).split(/('[^']*'|"[^"]*")/g).map((p, i) => (i % 2 ? `<span class="s">${p}</span>` : p.replace(kw, '<span class="k">$1</span>')));
      return parts.join('') + (comment ? `<span class="c">${esc(comment)}</span>` : '');
    }).join('\n');
  }

  /* ---------- log ---------- */
  function renderLog() {
    $('#log').replaceChildren(...lab.log.slice(0, 6).map((b, i) => h('div', { class: 'log-batch' + (i ? ' is-old' : '') },
      h('h3', { text: `Batch ${b.batch} · ${b.at.slice(0, 16).replace('T', ' ')}` }),
      h('ul', null, b.notes.map(n => { const [cls, txt] = LAYER_CHIP[n.layer] || LAYER_CHIP.bronze; return h('li', null, h('span', { class: 'chip chip-' + cls, text: txt.replace('Raw vault', 'Vault') }), h('span', { text: n.text })); })))));
  }

  /* ---------- star vs vault ---------- */
  function resultTable(rows, unit) {
    const max = Math.max(1, ...rows.map(r => r.value));
    return h('table', { class: 'res' }, h('tbody', null, rows.length ? rows.map(r => h('tr', null,
      h('td', null, r.key, h('span', { class: 'bar' }, h('i', { style: `width:${(r.value / max) * 100}%` }))),
      h('td', { text: unit === 'money' ? r.value.toFixed(2) : String(r.value) }))) : h('tr', null, h('td', { text: 'No rows' }))));
  }
  function joins(sql) { return (sql.match(/\bJOIN\b/g) || []).length; }
  function renderQuestions() {
    const A = lab.out.answers;
    $('#qa').replaceChildren(...L.QUESTIONS.map(q => {
      const same = JSON.stringify(A.star[q.id]) === JSON.stringify(A.vault[q.id]);
      return h('article', { class: 'q-card' },
        h('div', { class: 'q-head' }, h('h2', { text: q.title }), h('span', { class: 'match ' + (same ? 'ok' : 'no'), text: same ? '✓ Both models agree' : '✗ The models disagree' })),
        h('p', { class: 'q-why', text: q.why }),
        h('div', { class: 'q-cols' },
          h('div', { class: 'q-col' }, h('h3', null, h('span', { class: 'chip chip-gold', text: 'Gold' }), 'Star schema'), h('pre', { class: 'code', html: highlight(q.star, 'sql') }),
            h('p', { class: 'joins', text: `${joins(q.star)} join${joins(q.star) === 1 ? '' : 's'}` }), resultTable(A.star[q.id], q.unit)),
          h('div', { class: 'q-col' }, h('h3', null, h('span', { class: 'chip chip-bvault', text: 'Business vault' }), 'Data Vault'), h('pre', { class: 'code', html: highlight(q.vault, 'sql') }),
            h('p', { class: 'joins', text: `${joins(q.vault)} join${joins(q.vault) === 1 ? '' : 's'}, on top of views that already join hubs, links and satellites` }), resultTable(A.vault[q.id], q.unit))));
    }));
  }

  /* ---------- advisor ---------- */
  const answers = {};
  function renderAdvisor() {
    const A = L.Advisor;
    $('#adv-form').replaceChildren(...A.QUESTIONS.map(q => h('fieldset', { class: 'adv-q' }, h('legend', { text: q.q }),
      q.options.map((o, i) => h('label', { class: 'adv-opt' }, h('input', { type: 'radio', name: q.id, value: String(i), checked: answers[q.id] === i, onchange: () => { answers[q.id] = i; renderAdvice(); } }), h('span', { text: o.a }))))));
    const m = A.MATRIX;
    $('#matrix').replaceChildren(h('thead', null, h('tr', null, h('th', { scope: 'col', text: '' }), m.cols.map(c => h('th', { scope: 'col', text: c })))),
      h('tbody', null, m.rows.map(r => h('tr', null, h('th', { scope: 'row', text: r[0] }), r.slice(1).map(c => h('td', { text: c }))))));
    renderAdvice();
  }
  function renderAdvice() {
    const A = L.Advisor, n = Object.keys(answers).length;
    if (!n) { $('#adv-result').replaceChildren(h('h2', { text: 'Recommendation' }), h('p', { class: 'adv-summary', text: 'Answer a question to start. Most teams answering honestly end up with a star schema in Gold; the questions show when that is not enough.' })); return; }
    const ranked = A.recommend(answers), top = ranked[0], lo = Math.min(...ranked.map(r => r.score)), hi = Math.max(...ranked.map(r => r.score));
    $('#adv-result').replaceChildren(
      h('h2', { text: `Recommendation (${n} of ${A.QUESTIONS.length} answered)` }),
      h('p', { class: 'adv-top', text: top.name }), h('p', { class: 'adv-summary', text: top.summary }),
      h('ol', { class: 'rank' }, ranked.map((r, i) => h('li', { class: 'rank-row' + (i ? '' : ' is-top') }, h('span', { text: r.short }),
        h('span', { class: 'rank-bar' }, h('i', { style: `width:${hi === lo ? 50 : 8 + ((r.score - lo) / (hi - lo)) * 92}%` })), h('b', { text: (r.score > 0 ? '+' : '') + r.score })))),
      top.reasons.length ? h('ul', { class: 'why' }, top.reasons.map(t => h('li', { text: t }))) : null);
  }

  /* ---------- wiring ---------- */
  function show(view) {
    document.querySelectorAll('.view').forEach(v => { v.hidden = v.id !== 'view-' + view; });
    document.querySelectorAll('.tab').forEach(t => t.setAttribute('aria-pressed', String(t.dataset.view === view)));
    if (location.hash.slice(1) !== view) history.replaceState(null, '', '#' + view);
  }
  document.querySelectorAll('.tab').forEach(t => t.addEventListener('click', () => show(t.dataset.view)));
  document.querySelectorAll('.seg-btn').forEach(b => b.addEventListener('click', () => { pane = b.dataset.pane; renderViewer(); }));
  $('#run').addEventListener('click', run);
  $('#reset').addEventListener('click', () => { lab = new L.Lab(7); queued = []; renderAll(); });
  function renderAll() { renderActions(); renderPending(); renderLineage(false); renderViewer(); renderLog(); renderQuestions(); renderAdvisor(); }
  renderAll();
  show(['pipeline', 'questions', 'advisor'].includes(location.hash.slice(1)) ? location.hash.slice(1) : 'pipeline');
  window.lab = () => lab;
})();
