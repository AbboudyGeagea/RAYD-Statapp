/*
 * static/js/report_composer.js — the Report Composer page (templates/worklists.html,
 * routes/worklists.py), a pivot builder since 2026-11-21 (operator: "more flexible"; asked
 * for: more than 2 groupings, more chart types, a live result, work in the result table).
 *
 *   left        the report, what to count, every field (searchable, grouped), the measures
 *               (or, for a list, its columns); drag one onto a bar, or click it for its menu
 *   builder     Show as (table, list, bar, stacked, line, pie, heatmap); Filters (chips: the
 *               dataset's date and usual filters always there, each opens its editor);
 *               Rows (up to 4 groupings), Columns (one split), Values (measures); a list's
 *               Columns and sort
 *   result      live (re-run as you build, the switch at the top) or on Generate; a table
 *               sorts on a header click, moves or hides columns from the header, and opens
 *               the rows behind any number (drill-down, /worklists/api/drill); charts too
 *   SQL         a drawer, on the SQL button (hand-written SQL for the settings capability)
 *
 * A report's definition: {dataset, criteria, ask, output: {kind: list|table|chart, chart,
 * rows: [grouping, ...], cols, measures, columns, sort, desc}, sql}. Reports saved with one
 * "rows" grouping open as before.
 */
(function () {
'use strict';
const $ = id => document.getElementById(id);
const jq = window.jQuery;
const hasS2 = () => !!(jq && jq.fn && jq.fn.select2);
const clone = o => JSON.parse(JSON.stringify(o));
const esc = s => String(s ?? '').replace(/[&<>"']/g, ch => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'})[ch]);
const enc = encodeURIComponent;
const BLANK = '(blank)';
const MAX_ROWS = 4;
const MAIL_SERVER_URL = $('rc').dataset.mailUrl;
const TYPE_WORDS = {date: 'date', multi: 'choice', text: 'text', yesno: 'yes / no', range: 'number'};
const TYPE_ICONS = {date: 'bi-calendar3', multi: 'bi-ui-checks', text: 'bi-fonts', yesno: 'bi-toggle-on', range: 'bi-123'};
const SHOW_AS = [['table', 'Table', 'bi-table'], ['list', 'List', 'bi-list-ul'], ['bar', 'Bar', 'bi-bar-chart'],
                 ['stacked', 'Stacked', 'bi-bar-chart-steps'], ['line', 'Line', 'bi-graph-up'],
                 ['pie', 'Pie', 'bi-pie-chart'], ['heatmap', 'Heatmap', 'bi-grid-3x3']];
// Where the fields of a group come from, when it is not PACS.
const TAB_NOTES = {
  exam: 'From the RIS (the nightly Oracle ETL): its status history, performed steps and reports, read as Report 35 reads them.',
  nlp: 'From the NLP worker\'s reading of the ORU reports: negations and history removed, benign findings left out.',
  crn: 'From Critical Result Notification (Admin > CRN), matched by accession.',
};
let CAT, FIELD = {}, LIB = [], S = null, extra = [], sqlInfo = null, dirty = false, compileTimer = null,
    runTimer = null, runSeq = 0, chart = null, lastResult = null, model = null, quietLib = false,
    LIVE = true, stale = false, pivotSort = null, DRAG = null;
let popKind = null, popFid = null, popAnchor = null;
const openGroups = new Set(['_meas', '_cols', 'primary']);
const optionPromise = {}, optionList = {};
const pop = $('rc-pop');

async function api(url, body) {
  const opt = body === undefined ? {} : {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)};
  const r = await fetch(url, opt);
  const d = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(d.error || ('HTTP ' + r.status));
  return d;
}
// Confirmations use SweetAlert2, like the rest of RAYD; the browser's own dialog only if the
// library could not load, so a question is never skipped.
async function askYesNo(title, text, {yes = 'Yes', danger = false, icon = 'warning'} = {}) {
  if (!window.Swal) return window.confirm(title + (text ? '\n\n' + text : ''));
  const r = await Swal.fire({title, text, icon, showCancelButton: true,
    confirmButtonText: yes, cancelButtonText: 'Cancel',
    confirmButtonColor: danger ? '#dc2626' : '#2563eb', cancelButtonColor: '#64748b',
    focusCancel: danger,
    // The page is a fixed-height shell with its own scrolling areas; SweetAlert2's default
    // heightAuto resets html/body height and jumps the page.
    heightAuto: false});
  return r.isConfirmed;
}
function explain(title, text) {
  if (window.Swal) Swal.fire({title, text, icon: 'info', heightAuto: false, confirmButtonColor: '#2563eb'});
  else toast(text);
}
let toastTimer;
function toast(msg) { const t = $('toast'); t.textContent = msg; t.hidden = false; clearTimeout(toastTimer); toastTimer = setTimeout(() => t.hidden = true, 3600); }
function markDirty(on = true) { dirty = on; renderStatus(); }
function isoToday() { const d = new Date(); return new Date(d - d.getTimezoneOffset() * 60000).toISOString().slice(0, 10); }
function storeGet(k) { try { return localStorage.getItem(k); } catch (e) { return null; } }
function storeSet(k, v) { try { localStorage.setItem(k, v); } catch (e) { /* not needed */ } }

// ── State ────────────────────────────────────────────────────────────────────
const D = () => S.definition;
const ds = () => CAT.datasets[D().dataset] || null;
const inDataset = f => !!f && f.datasets.includes(D().dataset);
const handMode = () => D().sql != null;
const showValue = () => { const o = D().output || {}; return o.kind === 'chart' ? (o.chart || 'bar') : (o.kind || 'table'); };
// A new report starts EMPTY (operator, 2026-10-25): what to count is chosen first.
function emptyDefinition() {
  return {dataset: null, criteria: {}, ask: [], output: {kind: 'table', chart: 'bar', rows: [], cols: null, measures: [], columns: [], sort: null, desc: true}, sql: null};
}
// What a dataset starts with: a count (gigabytes for Storage) grouped by modality, its usual
// list columns, and the last 30 days (set by the dataset change), so the first live result is quick.
function blankDefinition(dataset, out = {}) {
  const d = CAT.datasets[dataset];
  const rows = d.dimensions.some(x => x.id === 'modality') ? ['modality'] : [];
  return {dataset, criteria: {}, ask: [], sql: null,
          output: {kind: out.kind || 'table', chart: out.chart || 'bar', rows, cols: null, measures: [d.default_measure],
                   columns: d.list_default.slice(), sort: d.default_date, desc: true}};
}
// Rows, columns, measures and list columns the dataset offers (a saved report may name a
// grouping its dataset no longer has); a report saved with one grouping gets a list.
function normalizeOutput() {
  const o = D().output = D().output || {}, d = ds();
  o.rows = Array.isArray(o.rows) ? o.rows : (o.rows ? [o.rows] : []);
  if (!o.kind) o.kind = 'table';
  if (!o.chart) o.chart = 'bar';
  if (!d) return;
  const dims = new Set(d.dimensions.map(x => x.id));
  o.rows = [...new Set(o.rows.filter(x => dims.has(x)))].slice(0, MAX_ROWS);
  if (o.cols && (!dims.has(o.cols) || o.rows.includes(o.cols))) o.cols = null;
  o.measures = (o.measures || []).filter(m => d.measures.some(x => x.id === m));
  if (!o.measures.length) o.measures = [d.default_measure];
  o.columns = (o.columns || []).filter(c => d.columns.some(x => x.id === c));
  if (!o.columns.length) o.columns = d.list_default.slice();
  if (!d.columns.some(x => x.id === o.sort)) o.sort = d.default_date;
}
// The Filters bar: the dataset's date (the date range) and its usual filters, then those added.
function baseFields() {
  const d = ds(); if (!d) return [];
  return [d.default_date, ...(d.pinned || [])].map(id => FIELD[id]).filter(inDataset);
}
function shownFields() {
  const base = baseFields(), ids = new Set(base.map(f => f.id));
  return base.concat(extra.filter(id => !ids.has(id)).map(id => FIELD[id]).filter(inDataset));
}
function initialExtra() {
  if (!ds()) return [];
  const base = new Set(baseFields().map(f => f.id)), used = new Set([...Object.keys(D().criteria || {}), ...D().ask]);
  return CAT.fields.filter(f => inDataset(f) && !base.has(f.id) && used.has(f.id)).map(f => f.id);
}
function load(w) {
  closePop();
  S = clone(w);
  S.definition.ask = S.definition.ask || [];
  S.definition.criteria = S.definition.criteria || {};
  normalizeOutput();
  extra = initialExtra(); pivotSort = null;
  markDirty(false); lastResult = null; resetResults();
  renderAll(); scheduleCompile(0); syncUrl();
  if (LIVE) scheduleRun(0);
}
function blank() {
  load({id: null, name: '', description: '', folder: '', visibility: 'personal',
        can_edit: true, version: 0, versions: [], definition: emptyDefinition()});
}
function syncUrl() {
  try { history.replaceState(null, '', S && S.id ? '?id=' + S.id : location.pathname); } catch (e) { /* not needed */ }
}

// ── Required fields and the status line ──────────────────────────────────────
function missingFields() {
  if (!S) return [];
  const out = [];
  if (!String(S.name || '').trim()) out.push(['name', 'Name']);
  if (handMode()) { if (!String(D().sql || '').trim()) out.push(['sql', 'SQL']); return out; }
  if (!D().dataset) out.push(['ds', 'What to count']);
  return out;
}
function renderStatus() {
  if (!S) return;
  const miss = missingFields(), keys = miss.map(m => m[0]);
  $('f-name').classList.toggle('missing', keys.includes('name'));
  $('f-ds').classList.toggle('missing', keys.includes('ds'));
  $('sql-wrap').classList.toggle('missing', keys.includes('sql'));
  const st = $('bar-status');
  st.textContent = dirty ? 'Unsaved changes' : (S.id ? `Saved · version ${S.version}` : 'New report · not saved yet');
  st.classList.toggle('dirty', dirty);
  $('bar-missing').textContent = miss.length ? 'Still to fill: ' + miss.map(m => m[1]).join(', ') : '';
}
// Scroll the panel or the main area, never the page (a fixed-height shell).
function scrollWithin(el) {
  const box = el && el.closest('aside .rc-fields, main'); if (!box) return;
  const top = el.getBoundingClientRect().top - box.getBoundingClientRect().top;
  box.scrollTo({top: box.scrollTop + top - box.clientHeight / 3, behavior: 'smooth'});
}
function focusMissing(key) {
  if (key === 'sql') { openDrawer(); $('sql-edit').focus(); return; }
  const el = {name: $('f-name'), ds: $('f-ds')}[key];
  if (el) { scrollWithin(el); el.focus({preventScroll: true}); }
}
const runBlockers = () => missingFields().filter(m => m[0] !== 'name');

// ── Library: the Report dropdown ─────────────────────────────────────────────
async function loadLibrary(selectId) {
  LIB = await api('/worklists/api/list');
  renderLibrary();
  $('folders').innerHTML = [...new Set(LIB.map(w => w.folder))].map(f => `<option value="${esc(f)}">`).join('');
  if (selectId) await open(selectId);
}
function renderLibrary() {
  const sel = $('rc-report'), groups = {};
  LIB.forEach(w => { const g = (w.visibility === 'shared' ? 'Shared' : 'Personal') + ' · ' + w.folder; (groups[g] = groups[g] || []).push(w); });
  const shared = g => g.startsWith('Shared');
  const keys = Object.keys(groups).sort((a, b) => shared(a) === shared(b) ? a.localeCompare(b) : (shared(a) ? -1 : 1));
  let html = '';
  if (S && !S.id) html += `<option value="" selected>${esc(S.name || 'New report')} · not saved</option>`;
  else if (!LIB.length) html += '<option value="">No saved reports yet</option>';
  html += keys.map(g => `<optgroup label="${esc(g)}">${groups[g].map(w => `<option value="${w.id}"${S && S.id === w.id ? ' selected' : ''}>${esc(w.name)}</option>`).join('')}</optgroup>`).join('');
  quietLib = true;
  if (hasS2() && jq(sel).data('select2')) jq(sel).select2('destroy');
  sel.innerHTML = html;
  if (hasS2()) jq(sel).select2({width: '100%'});
  sel.onchange = onLibraryPick;           // jQuery's trigger calls it too (Select2)
  quietLib = false;
}
async function onLibraryPick() {
  if (quietLib) return;
  const id = +$('rc-report').value;
  if (!id || (S && S.id === id)) return;
  if (!(await open(id))) renderLibrary();
}
async function open(id) {
  if (dirty && !(await askYesNo('Discard your changes?', 'This report has changes that are not saved.', {yes: 'Discard changes'}))) return false;
  try { load(await api('/worklists/api/' + id)); return true; } catch (e) { toast(e.message); return false; }
}

// ── Header: name, description, who sees it, folder, history, what to count ───
function renderHeader() {
  if (document.activeElement !== $('f-name')) $('f-name').value = S.name;
  if (document.activeElement !== $('f-desc')) $('f-desc').value = S.description || '';
  if (document.activeElement !== $('f-folder')) $('f-folder').value = S.folder;
  [...$('f-vis').children].forEach(b => {
    b.setAttribute('aria-pressed', String(b.dataset.v === S.visibility));
    b.disabled = b.dataset.v === 'shared' && !CAT.me.can_edit_shared && S.visibility !== 'shared';
  });
  $('owner-hint').textContent = S.id ? `${S.owner_name ? 'Owner ' + S.owner_name : 'Shared example'}${S.updated_by_name ? ' · last saved by ' + S.updated_by_name : ''}${S.updated_at ? ' · ' + S.updated_at : ''}` : '';
  $('f-history').innerHTML = `<option value="">History…</option>` + (S.versions || []).map(v => `<option value="${v.version}">v${v.version} · ${esc(v.saved_at)}${v.saved_by ? ' · ' + esc(v.saved_by) : ''}</option>`).join('');
  $('f-history').hidden = !(S.versions || []).length;
  $('btn-save').disabled = !S.can_edit; $('btn-del').disabled = !S.id || !S.can_edit; $('btn-dup').disabled = !S.id && !D().dataset && !handMode();
  $('btn-save').title = S.can_edit ? 'Save (Ctrl+S)' : 'Read-only for you: duplicate it into Personal to change it';
  $('f-ds').innerHTML = (D().dataset ? '' : '<option value="" selected>Choose…</option>') +
    Object.entries(CAT.datasets).map(([k, d]) => `<option value="${k}"${k === D().dataset ? ' selected' : ''}>${esc(d.label)}</option>`).join('');
  $('f-ds').disabled = handMode();
  $('ds-hint').textContent = handMode() ? 'Not used: the SQL names its own views.'
    : ds() ? ds().hint : 'What one row is: an exam, a study, a series, a report, an NLP finding, a storage day or a CRN.';
}

// ── Criteria: what each filter says (days and times included) ────────────────
const DAY_SHORT = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
const DAY_SETS = [['1,2,3,4,5', 'Mon–Fri'], ['6,7', 'Weekend'], ['1,2,3,4,5,6,7', 'Every day']];
const windowsOf = c => (c && Array.isArray(c.windows)) ? c.windows : [];
function daysText(days) {
  const d = [...new Set(days)].filter(x => x >= 1 && x <= 7).sort((a, b) => a - b);
  if (d.length === 7) return 'Every day';
  const runs = [];
  d.forEach(x => { const last = runs[runs.length - 1]; if (last && x === last[1] + 1) last[1] = x; else runs.push([x, x]); });
  return runs.map(([a, b]) => a === b ? DAY_SHORT[a - 1]
    : b === a + 1 ? `${DAY_SHORT[a - 1]}, ${DAY_SHORT[b - 1]}` : `${DAY_SHORT[a - 1]}–${DAY_SHORT[b - 1]}`).join(', ');
}
function windowText(w) {
  const days = (w.days || []).length ? daysText(w.days) : 'no day picked';
  if (!w.from && !w.to) return `${days}, all day`;
  const a = w.from || '00:00', b = w.to || '00:00';
  if (b === '00:00') return `${days} ${a} → midnight`;
  return `${days} ${a} → ${b}${b <= a ? ' next day' : ''}`;
}
function isActive(f, c) {
  if (!c) return false;
  if (f.type === 'date') return (!!c.mode && c.mode !== 'none') || windowsOf(c).length > 0;
  if (f.type === 'multi') return (c.values || []).length > 0;
  if (f.type === 'text') return !!String(c.value || '').trim();
  if (f.type === 'yesno') return c.value === 'yes' || c.value === 'no';
  if (f.type === 'range') return (c.min !== '' && c.min != null) || (c.max !== '' && c.max != null);
  return false;
}
function optionsNow(f) { return Array.isArray(f.options) ? f.options : (optionList[D().dataset + '|' + f.id] || null); }
function optLabel(f, v) {
  const hit = (optionsNow(f) || []).find(x => x.value === v);
  return hit ? hit.label : String(v);
}
function summary(f, c) {
  if (!isActive(f, c)) return '';
  if (f.type === 'date') {
    const parts = [];
    if (c.mode === 'quick') parts.push((CAT.quick_dates.find(q => q.id === c.quick) || {}).label || c.quick);
    if (c.mode === 'relative') parts.push(`${c.dir === 'next' ? 'Next' : 'Previous'} ${c.n} ${c.unit}`);
    if (c.mode === 'fixed') parts.push(c.from === c.to ? c.from : `${c.from} → ${c.to}`);
    windowsOf(c).forEach(w => parts.push(windowText(w)));
    return parts.join(' · ');
  }
  if (f.type === 'multi') {
    const names = c.values.map(v => optLabel(f, v));
    return names.length > 3 ? names.slice(0, 2).join(', ') + ` +${names.length - 2}` : names.join(', ');
  }
  if (f.type === 'text') return `${c.op || 'contains'} “${c.value}”`;
  if (f.type === 'yesno') return c.value === 'yes' ? 'Yes' : 'No';
  if (f.type === 'range') return [c.min !== '' && c.min != null ? `≥ ${c.min}` : '', c.max !== '' && c.max != null ? `≤ ${c.max}` : ''].filter(Boolean).join(' and ') + (f.unit ? ' ' + f.unit : '');
  return '';
}

// ── The values a filter offers: from the data, with their counts ─────────────
function cachedOptions(f) {
  const key = D().dataset + '|' + f.id;
  if (!optionPromise[key]) {
    optionPromise[key] = api(`/worklists/api/options?dataset=${enc(D().dataset)}&field=${enc(f.id)}`)
      .then(list => (optionList[key] = list), e => { delete optionPromise[key]; throw e; });
  }
  return optionPromise[key];
}
// The first OPTIONS_CAP values (most frequent first) are kept; a field with more asks the
// server for the typed text.
async function loadOptions(f, term) {
  const q = String(term || '').trim().toLowerCase();
  let list = Array.isArray(f.options) ? f.options : await cachedOptions(f);
  if (q) {
    if (!Array.isArray(f.options) && list.length >= (CAT.options_cap || 300)) {
      list = await api(`/worklists/api/options?dataset=${enc(D().dataset)}&field=${enc(f.id)}&q=${enc(q)}`);
    } else {
      list = list.filter(o => String(o.label).toLowerCase().includes(q));
    }
  }
  if (!q || BLANK.includes(q)) list = list.concat([{value: BLANK, label: BLANK}]);
  return list;
}

// ── A filter's controls, in its editor and in the "before it runs" dialog ────
function control(f, c, where) {
  const a = `data-f="${f.id}"`;
  if (f.type === 'date') {
    const mode = c.mode || 'none';
    const chip = (act, v, label, on) => `<button type="button" class="rc-chip" data-act="${act}" data-v="${v}" ${a} aria-pressed="${on}">${esc(label)}</button>`;
    let html = '<div class="rc-chips">' + chip('quick', 'none', 'Any time', mode === 'none')
      + CAT.quick_dates.map(q => chip('quick', q.id, q.label, mode === 'quick' && c.quick === q.id)).join('')
      + chip('mode', 'fixed', 'From – to', mode === 'fixed') + chip('mode', 'relative', 'Previous / next N', mode === 'relative') + '</div>';
    if (mode === 'fixed') html += `<div class="rc-row"><input type="date" class="rfp-input" data-k="from" ${a} value="${esc(c.from || '')}" aria-label="From">
      <input type="date" class="rfp-input" data-k="to" ${a} value="${esc(c.to || '')}" aria-label="To, included"></div><p class="rc-hint mt-1">Both days included.</p>`;
    if (mode === 'relative') html += `<div class="rc-row"><select class="rfp-input" data-k="dir" ${a} aria-label="Previous or next"><option value="prev"${c.dir !== 'next' ? ' selected' : ''}>Previous</option><option value="next"${c.dir === 'next' ? ' selected' : ''}>Next</option></select>
      <input type="number" min="1" class="rfp-input fixed" data-k="n" ${a} value="${esc(c.n || 7)}" style="width:4.5rem" aria-label="How many">
      <select class="rfp-input" data-k="unit" ${a} aria-label="Days or hours"><option${c.unit !== 'hours' ? ' selected' : ''}>days</option><option${c.unit === 'hours' ? ' selected' : ''}>hours</option></select></div>`;
    if (mode === 'quick') html += '<p class="rc-hint mt-2">Worked out on the day it runs, on the hospital clock.</p>';
    const wins = windowsOf(c), dayOnly = (f.day_only || []).includes(D().dataset);
    html += `<div class="rc-win">${wins.map((w, i) => windowRow(f, w, i, dayOnly)).join('')}
      <button type="button" class="rc-link" data-act="win-add" ${a} style="align-self:flex-start"><i class="bi bi-plus-lg" aria-hidden="true"></i> ${
        wins.length ? 'Another window' : dayOnly ? 'Days of the week' : 'Days of the week and times'}</button>
      ${wins.length && !dayOnly ? `<p class="rc-hint">A window that ends earlier than it starts runs on to the next morning: Sat 13:00 → 07:00 is Saturday 13:00 to Sunday 07:00.${
        (f.time_known || []).includes(D().dataset) ? ' A study PACS sent without a time is left out of a window with times.' : ''}</p>` : ''}</div>`;
    return html;
  }
  if (f.type === 'multi') {
    const sel = c.values || [];
    if (where === 'panel') {
      return `<select multiple class="rc-s2 w-full" id="rc-sel-${f.id}" ${a} aria-label="${esc(f.label)}">${
        sel.map(v => `<option value="${esc(JSON.stringify(v))}" selected>${esc(optLabel(f, v))}</option>`).join('')}</select>
        <p class="rc-hint mt-2">Each value shows its rows in the last year; type to find one.</p>`;
    }
    const opts = optionsNow(f);
    if (!opts) return '<span class="hint">The values could not be loaded.</span>';
    const all = opts.concat([{value: BLANK, label: BLANK}]);
    const more = sel.filter(v => !all.some(o => o.value === v)).map(v => ({value: v, label: String(v)}));
    return `<div class="rc-chips" style="max-height:220px;overflow-y:auto">${all.concat(more).map(o =>
      `<button type="button" class="rc-chip" data-act="chip" ${a} data-v="${esc(JSON.stringify(o.value))}" aria-pressed="${sel.includes(o.value)}">${esc(o.label)}${o.n != null ? `<span class="cnt">${o.n.toLocaleString()}</span>` : ''}</button>`).join('')}</div>`;
  }
  if (f.type === 'text') return `<select class="rfp-input" data-k="op" ${a} aria-label="${esc(f.label)}: how to match">${['contains', 'equals', 'starts with', 'does not contain'].map(o => `<option${(c.op || 'contains') === o ? ' selected' : ''}>${o}</option>`).join('')}</select>
    <input class="rfp-input mt-2" data-k="value" ${a} value="${esc(c.value || '')}" placeholder="Type the text…" aria-label="${esc(f.label)}">`;
  if (f.type === 'yesno') return `<div class="rc-seg" role="group" aria-label="${esc(f.label)}">${[['any', 'Any'], ['yes', 'Yes'], ['no', 'No']].map(([v, l]) =>
    `<button type="button" data-act="yn" ${a} data-v="${v}" aria-pressed="${(c.value || 'any') === v}">${l}</button>`).join('')}</div>`;
  if (f.type === 'range') return `<div class="rc-row" style="margin-top:0"><input class="rfp-input" type="number" step="any" data-k="min" ${a} value="${esc(c.min ?? '')}" placeholder="From" aria-label="${esc(f.label)}: from">
    <span class="lbl">to</span><input class="rfp-input" type="number" step="any" data-k="max" ${a} value="${esc(c.max ?? '')}" placeholder="To" aria-label="${esc(f.label)}: to"></div>${f.unit ? `<p class="rc-hint mt-1">${esc(f.unit)}</p>` : ''}`;
  return '';
}
function windowRow(f, w, i, dayOnly) {
  const a = `data-f="${f.id}" data-i="${i}"`, days = w.days || [];
  return `<div class="rc-win-row">
    <div class="rc-days" role="group" aria-label="Days">${DAY_SHORT.map((d, j) =>
      `<button type="button" class="rc-day" data-act="win-day" ${a} data-v="${j + 1}" aria-pressed="${days.includes(j + 1)}">${d}</button>`).join('')}</div>
    <div class="rc-win-tools">${DAY_SETS.map(([v, l]) => `<button type="button" class="rc-link" data-act="win-days" ${a} data-v="${v}">${l}</button>`).join('')}
      <span class="flex-1"></span><button type="button" class="rc-icon danger" data-act="win-del" ${a} title="Remove this window" aria-label="Remove this window"><i class="bi bi-x-lg"></i></button></div>
    ${dayOnly ? '' : `<div class="rc-row"><input type="time" class="rfp-input" data-k="wfrom" ${a} value="${esc(w.from || '')}" aria-label="From time">
      <span class="lbl">→</span><input type="time" class="rfp-input" data-k="wto" ${a} value="${esc(w.to || '')}" aria-label="To time"></div>`}
    <p class="rc-hint rc-win-hint${days.length ? '' : ' bad'}">${esc(days.length ? windowText(w) : 'Pick at least one day')}</p>
  </div>`;
}
function dateDefaults(c) {
  if (c.mode === 'relative') { c.dir = c.dir || 'prev'; c.n = c.n || 7; c.unit = c.unit || 'days'; }
  if (c.mode === 'fixed') { const today = isoToday(); c.from = c.from || CAT.report_start_date || today; c.to = c.to || today; }
}
// A control's click or typing, on a set of criteria: the report's, or the dialog's copy.
function applyClick(criteria, b) {
  const fid = b.dataset.f, v = b.dataset.v, act = b.dataset.act;
  if (!fid || !FIELD[fid]) return false;
  const c = criteria[fid] = criteria[fid] || {};
  if (act.startsWith('win-')) {
    const wins = c.windows = windowsOf(c), w = wins[+b.dataset.i];
    if (act === 'win-add') wins.push({days: [1, 2, 3, 4, 5], from: '', to: ''});
    else if (act === 'win-del') wins.splice(+b.dataset.i, 1);
    else if (act === 'win-day' && w) { const d = +v; w.days = w.days.includes(d) ? w.days.filter(x => x !== d) : [...w.days, d].sort((x, y) => x - y); }
    else if (act === 'win-days' && w) w.days = v.split(',').map(Number);
    if (!wins.length) delete c.windows;
    return true;
  }
  if (act === 'quick') { if (v === 'none') c.mode = 'none'; else { c.mode = 'quick'; c.quick = v; } return true; }
  if (act === 'mode') { c.mode = v; dateDefaults(c); return true; }
  if (act === 'yn') { c.value = v; return true; }
  if (act === 'chip') {
    const val = JSON.parse(v);
    c.values = (c.values || []).includes(val) ? c.values.filter(x => x !== val) : [...(c.values || []), val];
    return true;
  }
  return false;
}
function applyInput(criteria, t) {
  const fid = t.dataset.f, k = t.dataset.k;
  if (!fid || !k || !FIELD[fid]) return false;
  const c = criteria[fid] = criteria[fid] || {};
  if (k === 'wfrom' || k === 'wto') {          // a window's time: its line below says what it means
    const w = windowsOf(c)[+t.dataset.i]; if (!w) return false;
    w[k === 'wfrom' ? 'from' : 'to'] = t.value;
    const hint = t.closest('.rc-win-row') && t.closest('.rc-win-row').querySelector('.rc-win-hint');
    if (hint && (w.days || []).length) hint.textContent = windowText(w);
    return true;
  }
  c[k] = k === 'n' ? Math.max(1, parseInt(t.value || '1', 10)) : t.value;
  return true;
}

// ── Select2 dropdowns of values ──────────────────────────────────────────────
function destroySelects(root) {
  if (!root || !hasS2()) return;
  root.querySelectorAll('select.rc-s2').forEach(sel => { if (jq(sel).data('select2')) jq(sel).select2('destroy'); });
}
function optTemplate(item) {
  if (item.loading || item.n == null) return item.text;
  return jq('<span class="rc-opt"></span>').append(jq('<span></span>').text(item.text), jq('<span class="cnt"></span>').text(Number(item.n).toLocaleString()));
}
function initSelects(root) {
  if (!root) return;
  root.querySelectorAll('select.rc-s2').forEach(sel => {
    const f = FIELD[sel.dataset.f];
    sel.onchange = () => onMultiChange(sel, f);        // jQuery's trigger calls it too (Select2)
    if (!hasS2()) { sel.size = 8; fillNative(sel, f); return; }
    jq(sel).select2({
      width: '100%', placeholder: 'All', closeOnSelect: false, templateResult: optTemplate,
      ajax: {
        delay: 150,
        transport: (params, ok, fail) => {
          loadOptions(f, params.data && params.data.term)
            .then(list => ok({results: list.map(o => ({id: JSON.stringify(o.value), text: String(o.label), n: o.n}))}))
            .catch(fail);
          return {abort() {}};
        },
        processResults: d => d,
      },
      language: {searching: () => 'Loading the values in the data…', noResults: () => 'No value matches',
                 errorLoading: () => 'The values could not be loaded'},
    });
  });
}
// Select2 could not load (a blocked CDN): a plain list of the values, still with counts.
async function fillNative(sel, f) {
  try {
    const chosen = new Set([...sel.options].map(o => o.value));
    (await loadOptions(f, '')).forEach(o => {
      const id = JSON.stringify(o.value); if (chosen.has(id)) return;
      sel.add(new Option(o.n != null ? `${o.label} · ${o.n.toLocaleString()}` : String(o.label), id));
    });
  } catch (e) { toast(e.message); }
}
const crit = id => (D().criteria[id] = D().criteria[id] || {});
function onMultiChange(sel, f) {
  const vals = [...sel.selectedOptions].map(o => { try { return JSON.parse(o.value); } catch (e) { return o.value; } });
  const c = crit(f.id);
  if (JSON.stringify(vals) === JSON.stringify(c.values || [])) return;
  c.values = vals;
  criteriaChanged();
}
// A filter changed: its chip, the field list's tags, the SQL, the live result.
function criteriaChanged() {
  markDirty(); renderFilterZone(); renderFields(); scheduleCompile(); scheduleRun();
}
// The groupings, measures or kind changed: the whole builder, the field tags, the result.
function outputChanged() {
  pivotSort = null;
  markDirty(); normalizeOutput(); renderZones(); renderFields(); scheduleCompile(0); scheduleRun();
}

// ── What a field can become ──────────────────────────────────────────────────
const dimIds = () => new Set(ds().dimensions.map(d => d.id));
function dimFor(f) {
  if (!f || !ds()) return null;
  const ids = dimIds();
  if (f.type === 'date') return ids.has(f.id + ':month') ? f.id + ':month' : null;
  return ids.has(f.id) ? f.id : null;
}
const colFor = f => (f && ds() && ds().columns.some(c => c.id === f.col)) ? f.col : null;
const dimField = id => FIELD[String(id).split(':')[0]];
function dimLabel(id) { const d = ds() && ds().dimensions.find(x => x.id === id); return d ? d.label : id; }
const measureLabel = id => ((ds().measures.find(m => m.id === id) || {}).label || id);
const columnLabel = id => ((ds().columns.find(c => c.id === id) || {}).label || id);
function inGrouping(f) {
  const o = D().output, mine = id => !!id && dimField(id) === f;
  return {row: o.rows.some(mine), col: mine(o.cols)};
}

// ── Left: the field list ─────────────────────────────────────────────────────
function fieldItem(it) {
  return `<div class="rc-fitem" draggable="true" tabindex="0" role="button" data-fkind="${it.kind}" data-fid="${esc(it.id)}" title="${esc(it.title || it.label)}">
    <i class="bi ${it.icon}" aria-hidden="true"></i><span class="l">${esc(it.label)}</span>
    <span class="tags">${it.tags.map(t => `<span class="tag">${t}</span>`).join('')}</span><i class="bi bi-three-dots-vertical more" aria-hidden="true"></i></div>`;
}
function renderFields() {
  const box = $('rc-fields');
  if (!S || !ds() || handMode()) {
    box.innerHTML = `<p class="rc-hint">${S && handMode() ? 'Written in SQL: the fields are not used.' : 'Choose what to count first.'}</p>`;
    return;
  }
  const q = $('rc-fq').value.trim().toLowerCase(), o = D().output, list = o.kind === 'list';
  const groups = [];
  if (list) groups.push({id: '_cols', label: 'List columns', items: ds().columns.map(c => ({kind: 'column', id: c.id, label: c.label,
    icon: 'bi-layout-three-columns', tags: o.columns.includes(c.id) ? ['shown'] : []}))});
  else groups.push({id: '_meas', label: 'Measures', items: ds().measures.map(m => ({kind: 'measure', id: m.id, label: m.label,
    icon: 'bi-hash', tags: o.measures.includes(m.id) ? ['value'] : []}))});
  CAT.tabs.forEach(t => {
    const items = CAT.fields.filter(f => f.tab === t.id && inDataset(f)).map(f => {
      const tags = [], g = inGrouping(f);
      if (isActive(f, D().criteria[f.id])) tags.push('filter');
      if (!list && g.row) tags.push('row');
      if (!list && g.col) tags.push('col');
      return {kind: 'field', id: f.id, label: f.label, title: f.hint ? `${f.label}: ${f.hint}` : f.label, icon: TYPE_ICONS[f.type] || 'bi-dot', tags};
    });
    if (items.length) groups.push({id: t.id, label: t.label, items});
  });
  box.innerHTML = groups.map(g => {
    const items = q ? g.items.filter(it => (it.label + ' ' + (it.title || '')).toLowerCase().includes(q)) : g.items;
    if (!items.length) return '';
    const isOpen = q ? true : openGroups.has(g.id);
    return `<div class="rc-fgroup" data-open="${isOpen}"><button type="button" class="rc-fhead" data-fgroup="${g.id}" aria-expanded="${isOpen}"><i class="bi bi-chevron-down" aria-hidden="true"></i>${esc(g.label)}<span class="n">${items.length}</span></button>
      <div class="rc-fitems">${items.map(fieldItem).join('')}</div></div>`;
  }).join('') || '<p class="rc-hint">No field matches.</p>';
}
$('rc-fq').addEventListener('input', renderFields);
$('rc-fields').addEventListener('click', e => {
  const head = e.target.closest('[data-fgroup]');
  if (head) { const g = head.dataset.fgroup; if (openGroups.has(g)) openGroups.delete(g); else openGroups.add(g); renderFields(); return; }
  const item = e.target.closest('.rc-fitem'); if (item) openFieldMenu(item);
});
$('rc-fields').addEventListener('keydown', e => {
  const item = e.target.closest('.rc-fitem');
  if (item && (e.key === 'Enter' || e.key === ' ')) { e.preventDefault(); openFieldMenu(item); }
});
$('rc-fields').addEventListener('dragstart', e => {
  const item = e.target.closest('.rc-fitem'); if (!item) return;
  DRAG = {src: item.dataset.fkind, id: item.dataset.fid};
  e.dataTransfer.setData('text/plain', item.dataset.fid); e.dataTransfer.effectAllowed = 'copyMove';
  item.classList.add('dragging'); closePop();
});
document.addEventListener('dragend', () => {
  DRAG = null;
  document.querySelectorAll('.dragging').forEach(el => el.classList.remove('dragging'));
  document.querySelectorAll('.rc-zone.over, .rc-zone.refuse').forEach(z => z.classList.remove('over', 'refuse'));
});
function openFieldMenu(el) {
  const kind = el.dataset.fkind, id = el.dataset.fid, o = D().output, list = o.kind === 'list';
  const items = [];
  if (kind === 'measure') {
    const on = o.measures.includes(id);
    items.push({act: 'values', icon: 'bi-plus-lg', label: on ? 'In Values' : 'Add to Values', disabled: on});
  } else if (kind === 'column') {
    const on = o.columns.includes(id);
    items.push({act: 'listcol', icon: 'bi-plus-lg', label: on ? 'Shown in the list' : 'Add to the list', disabled: on});
  } else {
    const f = FIELD[id];
    items.push({act: 'filter', icon: 'bi-funnel', label: 'Filter by…'});
    if (!list && dimFor(f)) {
      items.push({act: 'rows', icon: 'bi-list-nested', label: 'Add to Rows', disabled: o.rows.length >= MAX_ROWS && !inGrouping(f).row});
      if (showValue() !== 'pie') items.push({act: 'cols', icon: 'bi-layout-three-columns', label: 'Split Columns by'});
    }
    if (list && colFor(f)) { const on = o.columns.includes(colFor(f)); items.push({act: 'listcol', icon: 'bi-plus-lg', label: on ? 'Shown in the list' : 'Add to the list', disabled: on}); }
    if (f.hint) items.push({sep: true}, {act: 'about', icon: 'bi-question-circle', label: 'What it holds'});
  }
  const label = kind === 'measure' ? measureLabel(id) : kind === 'column' ? columnLabel(id) : FIELD[id].label;
  openMenu(el, label, items, act => {
    if (act === 'filter') return addFilterField(id);
    if (act === 'about') return explain(FIELD[id].label, FIELD[id].hint);
    if (act === 'rows' || act === 'cols') return handleDrop(act, {src: 'field', id});
    if (act === 'values') return handleDrop('values', {src: 'measure', id});
    if (act === 'listcol') return handleDrop('listcols', {src: kind === 'column' ? 'column' : 'field', id});
  });
}

// ── The builder: Show as and the bars ────────────────────────────────────────
function renderZones() {
  if (!S) return;
  const hand = handMode();
  $('rc-builder').hidden = hand;
  if (hand) return;
  const v = showValue(), list = v === 'list', pie = v === 'pie', has = !!ds();
  $('rc-show').innerHTML = SHOW_AS.map(([k, l, i]) =>
    `<button type="button" data-show="${k}" aria-pressed="${v === k}"${has ? '' : ' disabled'}><i class="bi ${i}" aria-hidden="true"></i> ${l}</button>`).join('');
  const shown = {rows: !list, cols: !list && !pie, values: !list, listcols: list};
  Object.entries(shown).forEach(([z, on]) => {
    $('z-' + z).hidden = !on;
    const lab = document.querySelector(`.rc-zlabel[data-for="${z}"]`); if (lab) lab.hidden = !on;
  });
  renderFilterZone();
  if (!has) { ['rows', 'cols', 'values', 'listcols'].forEach(z => { $('z-' + z).innerHTML = ''; }); return; }
  if (list) renderListZone(); else { renderRowsZone(); if (!pie) renderColsZone(); renderValuesZone(); }
}
function renderFilterZone() {
  const box = $('z-filters');
  if (!S || !ds()) { box.innerHTML = '<span class="empty">Choose what to count on the left first.</span>'; return; }
  const base = new Set(baseFields().map(f => f.id));
  box.innerHTML = shownFields().map(f => {
    const c = D().criteria[f.id], on = isActive(f, c), ask = D().ask.includes(f.id);
    if (!on && !ask && base.has(f.id)) {
      return `<button type="button" class="rc-chipz ghost" data-filter="${f.id}" title="Filter by ${esc(f.label)}">${esc(f.label)} <i class="bi bi-chevron-down" aria-hidden="true"></i></button>`;
    }
    const val = on ? summary(f, c) : 'any';
    return `<span class="rc-chipz${ask ? ' ask' : ''}" style="cursor:default" title="${esc(f.label + ': ' + val + (ask ? ' (asked when run)' : ''))}">
      <button type="button" class="t" data-filter="${f.id}"><b>${esc(f.label)}</b><span class="v">${esc(val)}</span>${ask ? '<span class="crit-ask">asked</span>' : ''}</button>
      <button type="button" class="x" data-unfilter="${f.id}" aria-label="${base.has(f.id) ? 'Clear' : 'Remove'} the ${esc(f.label)} filter"><i class="bi bi-x"></i></button></span>`;
  }).join('') + '<button type="button" class="rc-zadd" data-act="add-filter" title="Filter by another field"><i class="bi bi-plus-lg" aria-hidden="true"></i> Filter</button>';
}
function dimChip(zone, i, id) {
  const f = dimField(id), bucket = String(id).includes(':') ? id.split(':')[1] : null;
  const buckets = bucket ? ds().dimensions.filter(d => d.id.startsWith(f.id + ':')).map(d => d.id.split(':')[1]) : [];
  return `<span class="rc-chipz" draggable="true" data-zone="${zone}" data-i="${i}" title="Drag to reorder or move"><b>${esc(f ? f.label : dimLabel(id))}</b>${
    bucket ? `<select data-bucket="${zone}" data-i="${i}" aria-label="Group ${esc(f.label)} by">${buckets.map(b => `<option value="${b}"${b === bucket ? ' selected' : ''}>by ${b}</option>`).join('')}</select>` : ''}
    <button type="button" class="x" data-rm="${zone}" data-i="${i}" aria-label="Remove ${esc(f ? f.label : id)}"><i class="bi bi-x"></i></button></span>`;
}
function renderRowsZone() {
  const o = D().output;
  $('z-rows').innerHTML = (o.rows.length ? o.rows.map((id, i) => dimChip('rows', i, id)).join('') : '<span class="empty">Drop fields here to group the rows (up to 4): modality, then room…</span>')
    + (o.rows.length < MAX_ROWS ? '<button type="button" class="rc-zadd" data-add="rows" title="Group the rows by"><i class="bi bi-plus-lg" aria-hidden="true"></i></button>' : '');
}
function renderColsZone() {
  const o = D().output, heat = showValue() === 'heatmap';
  $('z-cols').innerHTML = o.cols ? dimChip('cols', 0, o.cols)
    : `<span class="empty">${heat ? 'A heatmap needs a field here (e.g. hour, with weekday in Rows)' : 'Drop a field here to split the values into columns (e.g. month)'}</span>`
      + '<button type="button" class="rc-zadd" data-add="cols" title="Split the columns by"><i class="bi bi-plus-lg" aria-hidden="true"></i></button>';
}
function renderValuesZone() {
  const o = D().output, one = !!o.cols || ['pie', 'heatmap'].includes(showValue());
  $('z-values').innerHTML = o.measures.map((id, i) => `<span class="rc-chipz" draggable="true" data-zone="values" data-i="${i}"${one && i ? ' style="opacity:.5"' : ''} title="${one && i ? 'Not shown: with columns split, a pie or a heatmap, only the first value is shown' : 'Drag to reorder'}"><b>${esc(measureLabel(id))}</b>
      <button type="button" class="x" data-rm="values" data-i="${i}" aria-label="Remove ${esc(measureLabel(id))}"><i class="bi bi-x"></i></button></span>`).join('')
    + '<button type="button" class="rc-zadd" data-add="values" title="Add a measure"><i class="bi bi-plus-lg" aria-hidden="true"></i></button>'
    + (one && o.measures.length > 1 ? '<span class="empty">only the first is shown</span>' : '');
}
function renderListZone() {
  const o = D().output, d = ds();
  $('z-listcols').innerHTML = o.columns.map((id, i) => `<span class="rc-chipz" draggable="true" data-zone="listcols" data-i="${i}" title="Drag to reorder"><b>${esc(columnLabel(id))}</b>
      <button type="button" class="x" data-rm="listcols" data-i="${i}" aria-label="Remove ${esc(columnLabel(id))}"><i class="bi bi-x"></i></button></span>`).join('')
    + '<button type="button" class="rc-zadd" data-add="listcols" title="Add a column"><i class="bi bi-plus-lg" aria-hidden="true"></i></button>'
    + `<span class="rc-zsort">Sort <select id="o-sort" aria-label="Sort by">${d.columns.map(c => `<option value="${c.id}"${c.id === o.sort ? ' selected' : ''}>${esc(c.label)}</option>`).join('')}</select>
       <select id="o-dir" aria-label="Order"><option value="desc"${o.desc !== false ? ' selected' : ''}>newest / largest first</option><option value="asc"${o.desc === false ? ' selected' : ''}>oldest / smallest first</option></select></span>`;
}
function setShow(v) {
  const o = D().output;
  if (v === 'table' || v === 'list') o.kind = v; else { o.kind = 'chart'; o.chart = v; }
  if (v === 'pie' && o.cols) { o.cols = null; toast('A pie shows one value per row: the column split was taken off.'); }
  outputChanged();
}
// Dropped, or picked from a menu: a field, a measure, a list column, or a chip moved.
function handleDrop(zone, p, at = Infinity) {
  const o = D().output;
  if (zone === 'filters') { if (p.src === 'field') addFilterField(p.id); return; }
  const move = (arr, id) => { const a = arr.slice(), from = a.indexOf(id); if (from >= 0) { a.splice(from, 1); if (from < at) at--; } a.splice(Math.min(at, a.length), 0, id); return a; };
  if (zone === 'rows' || zone === 'cols') {
    let dim = p.src === 'field' ? dimFor(FIELD[p.id]) : p.src === 'chip' ? (p.zone === 'rows' ? o.rows[p.i] : p.zone === 'cols' ? o.cols : null) : null;
    if (!dim) return toast(p.src === 'field' ? `${FIELD[p.id].label} cannot be grouped: filter by it instead.` : 'Only a grouping goes here.');
    if (p.src === 'field' && FIELD[p.id].type === 'date') {      // a date already grouped keeps its period
      const have = [...o.rows, o.cols].find(x => x && dimField(x) === FIELD[p.id]); if (have) dim = have;
    }
    const wasCol = o.cols === dim;
    if (wasCol) o.cols = null;
    if (zone === 'rows') {
      if (!o.rows.includes(dim) && o.rows.length >= MAX_ROWS) { if (wasCol) o.cols = dim; return toast(`Rows take up to ${MAX_ROWS} groupings.`); }
      o.rows = move(o.rows, dim);
    } else {
      if (showValue() === 'pie') return toast('A pie has no columns: choose Bar or Table to split by columns.');
      o.rows = o.rows.filter(x => x !== dim); o.cols = dim;
    }
  } else if (zone === 'values') {
    const id = p.src === 'measure' ? p.id : (p.src === 'chip' && p.zone === 'values') ? o.measures[p.i] : null;
    if (!id) return toast('Only a measure goes in Values.');
    o.measures = move(o.measures, id);
  } else if (zone === 'listcols') {
    const id = p.src === 'column' ? p.id : p.src === 'field' ? colFor(FIELD[p.id]) : (p.src === 'chip' && p.zone === 'listcols') ? o.columns[p.i] : null;
    if (!id) return toast('Only a list column goes here.');
    o.columns = move(o.columns, id);
  }
  outputChanged();
}
function accepts(zone, p) {
  if (!p || !ds()) return false;
  if (zone === 'filters') return p.src === 'field';
  if (zone === 'rows' || zone === 'cols') return (p.src === 'field' && !!dimFor(FIELD[p.id])) || (p.src === 'chip' && (p.zone === 'rows' || p.zone === 'cols'));
  if (zone === 'values') return p.src === 'measure' || (p.src === 'chip' && p.zone === 'values');
  if (zone === 'listcols') return p.src === 'column' || (p.src === 'field' && !!colFor(FIELD[p.id])) || (p.src === 'chip' && p.zone === 'listcols');
  return false;
}
const builder = $('rc-builder');
builder.addEventListener('dragstart', e => {
  const chip = e.target.closest && e.target.closest('.rc-chipz[data-zone]'); if (!chip) return;
  DRAG = {src: 'chip', zone: chip.dataset.zone, i: +chip.dataset.i};
  e.dataTransfer.setData('text/plain', chip.dataset.zone); e.dataTransfer.effectAllowed = 'move';
  chip.classList.add('dragging'); closePop();
});
builder.addEventListener('dragover', e => {
  const zone = e.target.closest('.rc-zone'); if (!zone || !DRAG) return;
  const ok = accepts(zone.dataset.zone, DRAG);
  zone.classList.toggle('over', ok); zone.classList.toggle('refuse', !ok);
  if (ok) { e.preventDefault(); e.dataTransfer.dropEffect = DRAG.src === 'chip' ? 'move' : 'copy'; }
});
builder.addEventListener('dragleave', e => {
  const zone = e.target.closest('.rc-zone');
  if (zone && !zone.contains(e.relatedTarget)) zone.classList.remove('over', 'refuse');
});
builder.addEventListener('drop', e => {
  const zone = e.target.closest('.rc-zone'); if (!zone || !DRAG) return;
  e.preventDefault(); zone.classList.remove('over', 'refuse');
  const before = e.target.closest('.rc-chipz[data-i]');
  const at = before && before.dataset.zone === zone.dataset.zone ? +before.dataset.i : Infinity;
  const p = DRAG; DRAG = null;
  if (accepts(zone.dataset.zone, p)) handleDrop(zone.dataset.zone, p, at);
});
builder.addEventListener('click', e => {
  const t = e.target, o = D().output;
  const show = t.closest('[data-show]'); if (show && !show.disabled) return setShow(show.dataset.show);
  const unf = t.closest('[data-unfilter]');
  if (unf) {
    const fid = unf.dataset.unfilter;
    delete D().criteria[fid]; D().ask = D().ask.filter(x => x !== fid); extra = extra.filter(x => x !== fid);
    if (popFid === fid) closePop();
    return criteriaChanged();
  }
  const filt = t.closest('[data-filter]'); if (filt) return openFilter(filt.dataset.filter, filt.closest('.rc-chipz') || filt);
  if (t.closest('[data-act="add-filter"]')) return openAddFilter();
  const rm = t.closest('[data-rm]');
  if (rm) {
    const z = rm.dataset.rm, i = +rm.dataset.i;
    if (z === 'rows') o.rows.splice(i, 1);
    else if (z === 'cols') o.cols = null;
    else if (z === 'values') { if (o.measures.length === 1) return toast('Values need at least one measure.'); o.measures.splice(i, 1); }
    else if (z === 'listcols') { if (o.columns.length === 1) return toast('A list needs at least one column.'); o.columns.splice(i, 1); }
    return outputChanged();
  }
  const add = t.closest('[data-add]'); if (add) return openZonePicker(add.dataset.add, add);
});
builder.addEventListener('change', e => {
  const t = e.target, o = D().output;
  if (t.dataset.bucket) {
    const f = dimField(t.dataset.bucket === 'rows' ? o.rows[+t.dataset.i] : o.cols), id = `${f.id}:${t.value}`;
    if (t.dataset.bucket === 'rows') o.rows[+t.dataset.i] = id; else o.cols = id;
    return outputChanged();
  }
  if (t.id === 'o-sort') { o.sort = t.value; return outputChanged(); }
  if (t.id === 'o-dir') { o.desc = t.value !== 'asc'; return outputChanged(); }
});
// The + of a bar: the groupings, measures or columns it can take, searchable.
function openZonePicker(zone, anchor) {
  const o = D().output, d = ds();
  let items = [], title = '';
  if (zone === 'rows' || zone === 'cols') {
    title = zone === 'rows' ? 'Group the rows by' : 'Split the columns by';
    const used = new Set([...o.rows, o.cols].filter(Boolean).map(x => dimField(x) && dimField(x).id));
    items = CAT.fields.filter(f => inDataset(f) && dimFor(f) && !used.has(f.id))
      .map(f => ({id: f.id, label: f.label, group: (CAT.tabs.find(t => t.id === f.tab) || {}).label}));
  } else if (zone === 'values') {
    title = 'Add a measure';
    items = d.measures.filter(m => !o.measures.includes(m.id)).map(m => ({id: m.id, label: m.label}));
  } else {
    title = 'Add a column';
    items = d.columns.filter(c => !o.columns.includes(c.id)).map(c => ({id: c.id, label: c.label}));
  }
  openPicker(anchor, title, items, id => {
    if (zone === 'rows' || zone === 'cols') handleDrop(zone, {src: 'field', id});
    else if (zone === 'values') handleDrop('values', {src: 'measure', id});
    else handleDrop('listcols', {src: 'column', id});
  });
}

// ── Popovers: a filter's editor, a field's menu, a picker ────────────────────
function placePop(anchor) {
  pop.hidden = false;
  const r = anchor.getBoundingClientRect(), w = pop.offsetWidth, h = pop.offsetHeight;
  const left = Math.min(r.left, window.innerWidth - w - 12);
  let top = r.bottom + 6;
  if (top + h > window.innerHeight - 12) top = Math.max(12, r.top - h - 6);
  pop.style.left = Math.max(12, left) + 'px'; pop.style.top = top + 'px';
}
function closePop() {
  if (!popKind) return;
  destroySelects(pop);
  pop.hidden = true; pop.innerHTML = ''; pop.className = 'rc';
  popKind = null; popFid = null; popAnchor = null;
}
function openMenu(anchor, title, items, onPick) {
  closePop();
  popKind = 'menu'; popAnchor = anchor; pop.className = 'rc menu';
  pop.innerHTML = `<div class="rc-menu-head">${esc(title)}</div>` + items.map(it => it.sep ? '<div class="rc-menu-sep"></div>'
    : `<button type="button" class="rc-menu-item" data-menu="${it.act}"${it.disabled ? ' disabled' : ''}><i class="bi ${it.icon}" aria-hidden="true"></i>${esc(it.label)}</button>`).join('');
  pop._pick = onPick;
  placePop(anchor);
  const first = pop.querySelector('.rc-menu-item:not(:disabled)'); if (first) first.focus();
}
function openPicker(anchor, title, items, onPick) {
  closePop();
  popKind = 'picker'; popAnchor = anchor; pop.className = 'rc menu';
  pop.innerHTML = `<div class="rc-menu-head">${esc(title)}</div><input type="search" class="rfp-input" data-pickq placeholder="Find…" style="margin:0 0 4px">
    <div class="rc-pick-list" style="max-height:300px;overflow-y:auto"></div>`;
  const draw = q => {
    const hit = items.filter(it => !q || it.label.toLowerCase().includes(q));
    let last = null;
    pop.querySelector('.rc-pick-list').innerHTML = hit.map(it => {
      const head = it.group && it.group !== last ? `<div class="rc-menu-head">${esc(it.group)}</div>` : '';
      last = it.group;
      return head + `<button type="button" class="rc-menu-item" data-pick="${esc(it.id)}">${esc(it.label)}</button>`;
    }).join('') || '<p class="rc-hint" style="padding:8px">Nothing left to add.</p>';
  };
  pop._pick = onPick; pop._draw = draw;
  draw('');
  placePop(anchor);
  pop.querySelector('[data-pickq]').focus();
}
function openFilter(fid, anchor) {
  if (popKind === 'filter' && popFid === fid) return closePop();
  closePop();
  popKind = 'filter'; popFid = fid; popAnchor = anchor; pop.className = 'rc';
  renderFilterPop();
  placePop(anchor);
}
function renderFilterPop() {
  const f = FIELD[popFid], c = D().criteria[f.id] || {}, ask = D().ask.includes(f.id), isExtra = extra.includes(f.id);
  destroySelects(pop);
  pop.innerHTML = `<div class="rc-pophead"><b>${esc(f.label)}${f.id === ds().default_date ? ' <span class="rc-hint">· the date range</span>' : ''}</b>
      ${f.hint ? `<button type="button" class="rc-icon" data-act="hint" data-f="${f.id}" title="${esc(f.hint)}" aria-label="About ${esc(f.label)}"><i class="bi bi-question-circle"></i></button>` : ''}
      <button type="button" class="rc-icon" data-act="pop-close" aria-label="Close"><i class="bi bi-x-lg"></i></button></div>
    ${control(f, c, 'panel')}
    <div class="rc-popfoot"><label class="rc-ask" title="Ask for this filter each time the report is generated"><input type="checkbox" class="rfp-tick" data-ask="${f.id}"${ask ? ' checked' : ''}><span>Ask when run</span></label>
      <span class="flex-1"></span>
      ${isActive(f, c) ? `<button type="button" class="rc-link" data-act="clear" data-f="${f.id}">Clear</button>` : ''}
      ${isExtra ? `<button type="button" class="rc-link" style="color:var(--rc-bad)" data-act="remove" data-f="${f.id}">Remove</button>` : ''}
      <button type="button" class="rc-btn small primary" data-act="pop-close">Done</button></div>`;
  initSelects(pop);
}
pop.addEventListener('click', e => {
  const t = e.target;
  const m = t.closest('[data-menu]'); if (m) { const pick = pop._pick; closePop(); return pick(m.dataset.menu); }
  const pk = t.closest('[data-pick]'); if (pk) { const pick = pop._pick; closePop(); return pick(pk.dataset.pick); }
  const b = t.closest('[data-act]'); if (!b || popKind !== 'filter') return;
  const act = b.dataset.act, fid = popFid;
  if (act === 'pop-close') return closePop();
  if (act === 'hint') return explain(FIELD[fid].label, FIELD[fid].hint);
  if (act === 'clear') { delete D().criteria[fid]; renderFilterPop(); return criteriaChanged(); }
  if (act === 'remove') { delete D().criteria[fid]; D().ask = D().ask.filter(x => x !== fid); extra = extra.filter(x => x !== fid); closePop(); return criteriaChanged(); }
  if (applyClick(D().criteria, b)) { renderFilterPop(); criteriaChanged(); }
});
pop.addEventListener('change', e => {
  const t = e.target;
  if (popKind !== 'filter') return;
  if (t.dataset.ask) {
    D().ask = t.checked ? [...new Set([...D().ask, t.dataset.ask])] : D().ask.filter(x => x !== t.dataset.ask);
    markDirty(); renderFilterZone(); return;
  }
  if (applyInput(D().criteria, t)) criteriaChanged();
});
pop.addEventListener('input', e => {
  const t = e.target;
  if (t.hasAttribute('data-pickq')) return pop._draw(t.value.trim().toLowerCase());
  if (popKind === 'filter' && t.tagName === 'INPUT' && t.type !== 'checkbox' && applyInput(D().criteria, t)) criteriaChanged();
});
document.addEventListener('mousedown', e => {
  if (!popKind) return;
  const t = e.target;
  if (pop.contains(t) || t.closest('.select2-container, .swal2-container') || (popAnchor && popAnchor.isConnected && popAnchor.contains(t))) return;
  closePop();
}, true);
document.addEventListener('keydown', e => { if (e.key === 'Escape' && popKind && !document.querySelector('.swal2-container')) closePop(); });
window.addEventListener('resize', closePop);
$('rc-fields').addEventListener('scroll', () => { if (popKind === 'menu') closePop(); });
function addFilterField(fid) {
  if (!shownFields().some(f => f.id === fid)) extra.push(fid);
  renderFilterZone(); renderFields();
  const chip = document.querySelector(`#z-filters [data-filter="${fid}"]`);
  if (chip) openFilter(fid, chip.closest('.rc-chipz') || chip);
}

// ── Filter by another field: every field of the dataset, by group ────────────
function openAddFilter() {
  if (!ds()) return;
  closePop();
  const shown = new Set(shownFields().map(f => f.id));
  const draw = q => {
    const html = CAT.tabs.map(t => {
      const items = CAT.fields.filter(f => f.tab === t.id && inDataset(f)
        && (!q || (f.label + ' ' + (f.hint || '')).toLowerCase().includes(q)));
      if (!items.length) return '';
      return `<div class="addf-group"><h3>${esc(t.label)}</h3>${TAB_NOTES[t.id] ? `<span class="src">${esc(TAB_NOTES[t.id])}</span>` : ''}<div class="addf-list">${
        items.map(f => `<button type="button" class="addf-item" data-addf="${f.id}">
          <span class="t">${esc(f.label)}<span class="ty">${TYPE_WORDS[f.type] || f.type}</span>${shown.has(f.id) ? '<span class="ty">in the bar</span>' : ''}</span>
          ${f.hint ? `<span class="h">${esc(f.hint)}</span>` : ''}</button>`).join('')}</div></div>`;
    }).join('');
    $('addf-list').innerHTML = html || '<div class="empty">No filter matches.</div>';
  };
  $('modal').innerHTML = `<div class="modal-back" id="addf-back"><div class="modal wide addf" role="dialog" aria-modal="true" aria-label="Filter by">
    <div class="panel-head"><h2>Filter by · ${esc(ds().label)}</h2><button class="btn small" id="addf-close" type="button" style="margin-left:auto">Close</button></div>
    <div class="panel-body"><input class="input" id="addf-q" type="search" placeholder="Find a field: radiologist, protocol, turnaround, age…" aria-label="Find a field">
      <div id="addf-list" class="flex flex-col gap-4"></div></div></div></div>`;
  draw('');
  const close = () => { closeModal(); document.removeEventListener('keydown', onKey); };
  const onKey = e => { if (e.key === 'Escape') close(); };
  document.addEventListener('keydown', onKey);
  $('addf-q').addEventListener('input', e => draw(e.target.value.trim().toLowerCase()));
  $('addf-close').onclick = close;
  $('addf-back').addEventListener('click', e => {
    if (e.target.id === 'addf-back') return close();
    const b = e.target.closest('[data-addf]'); if (!b) return;
    close(); addFilterField(b.dataset.addf);
  });
  $('addf-q').focus();
}
function closeModal() { $('modal').innerHTML = ''; }

// ── SQL: in a drawer, on a click ─────────────────────────────────────────────
function openDrawer() { closePop(); $('sql-drawer').hidden = false; renderSQL(); }
function closeDrawer() { $('sql-drawer').hidden = true; }
$('btn-sql').onclick = openDrawer;
$('sql-close').onclick = closeDrawer;
$('sql-back').onclick = closeDrawer;
$('btn-sql-run').onclick = () => { closeDrawer(); runNow(true); };
document.addEventListener('keydown', e => {
  if (e.key === 'Escape' && !$('sql-drawer').hidden && !$('modal').innerHTML && !document.querySelector('.swal2-container')) closeDrawer();
});
function scheduleCompile(ms = 250) { clearTimeout(compileTimer); compileTimer = setTimeout(compile, ms); }
async function compile() {
  if (handMode()) return renderSQL();
  if (!D().dataset) { sqlInfo = {pending: true}; return renderSQL(); }
  try { sqlInfo = await api('/worklists/api/compile', {definition: D()}); }
  catch (e) { sqlInfo = {error: e.message}; }
  renderSQL();
}
function highlight(sql) {
  return esc(sql)
    .replace(/(&#39;[^&]*?&#39;)/g, '<span class="s">$1</span>')
    .replace(/(:[a-z_0-9]+)/g, '<span class="p">$1</span>')
    .replace(/\b(SELECT|FROM|WHERE|AND|OR|NOT|GROUP BY|ORDER BY|LIMIT|AS|COUNT|SUM|AVG|DISTINCT|FILTER|WITHIN GROUP|ANY|ILIKE|ESCAPE|IS|NULL|TRUE|DESC|ASC|NULLS LAST|COALESCE|CAST|LEFT JOIN|JOIN|ON|lower|round|percentile_cont|to_char|date_trunc|lpad|extract)\b/g, '<span class="k">$1</span>');
}
// Two separate things: the SQL built from the builder, only ever SHOWN (read-only), and SQL
// written by hand (settings capability), documented by the SQL reference.
function renderSQL() {
  if (!S) return;
  const hand = handMode(), canSQL = CAT.me.can_write_sql;
  $('sql-seg').hidden = !canSQL;
  [...$('sql-seg').children].forEach(b => b.setAttribute('aria-pressed', String((b.dataset.mode === 'hand') === hand)));
  $('btn-start-gen').hidden = !(hand && canSQL);
  $('sql-view').hidden = hand && canSQL;
  $('sql-edit').hidden = !(hand && canSQL);
  if (hand) {
    if (canSQL) { if (document.activeElement !== $('sql-edit')) $('sql-edit').value = D().sql; }
    else $('sql-view').innerHTML = highlight(D().sql);
    $('params').innerHTML = '<div class="pv">Runs as the read-only reporting account: the rpt views (' + esc(Object.keys(CAT.datasets).join(', ')) + '), nothing else. No parameters: write values in the SQL.</div>';
    $('sql-caption').textContent = canSQL ? 'Your SQL. The SQL reference lists every view, column and rule.' : 'Written in SQL by an administrator.';
    $('sql-note').innerHTML = canSQL ? '' : '<div class="rc-note warn">Read-only: only an administrator can change this SQL.</div>';
    return;
  }
  $('sql-caption').textContent = 'Read-only: built from the builder. Values are bound as parameters, never pasted into the SQL.';
  $('sql-note').innerHTML = '';
  if (!sqlInfo || sqlInfo.pending) { $('sql-view').innerHTML = '<span class="hint">The SQL appears here once “What to count” is chosen.</span>'; $('params').innerHTML = ''; return; }
  if (sqlInfo.error) { $('sql-note').innerHTML = `<div class="rc-note bad">${esc(sqlInfo.error)}</div>`; $('sql-view').innerHTML = ''; $('params').innerHTML = ''; return; }
  $('sql-view').innerHTML = highlight(sqlInfo.sql);
  const p = Object.entries(sqlInfo.params || {});
  $('params').innerHTML = p.length ? p.map(([k, v]) => `<div><span class="pk">:${esc(k)}</span> <span class="pv">= ${esc(Array.isArray(v) ? JSON.stringify(v) : v)}</span></div>`).join('') : '<div class="pv">No parameters.</div>';
}
function renderHandNote() {
  const hand = handMode();
  $('rc-handnote').hidden = !hand;
  $('rc-handnote').innerHTML = hand ? '<div class="rc-note info"><i class="bi bi-code-slash" aria-hidden="true"></i> This report is written in SQL: the builder is not used, the result is the query’s own. <button type="button" class="rc-link" data-act="open-sql">Open the SQL</button></div>' : '';
}
$('rc-handnote').addEventListener('click', e => { if (e.target.closest('[data-act="open-sql"]')) openDrawer(); });
$('btn-copy-sql').onclick = () => {
  const t = D().sql != null ? D().sql : (sqlInfo && sqlInfo.sql) || '';
  if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(t).then(() => toast('SQL copied.'), () => toast('Copy was blocked; select the SQL and copy it by hand.'));
};
async function setMode(hand, sql) {
  if (hand === handMode() && sql === undefined) return;
  if (!hand) {
    if (String(D().sql || '').trim() && !(await askYesNo('Go back to the builder?', 'The SQL you wrote is dropped; the report is built from the builder again.', {yes: 'Use the builder'}))) return;
    D().sql = null;
  } else {
    D().sql = sql !== undefined ? sql : ((sqlInfo && sqlInfo.editable_sql) || '');
  }
  markDirty(); renderAll();
  if (hand) { $('sql-edit').value = D().sql; $('sql-edit').focus(); } else { scheduleCompile(0); scheduleRun(); }
}
$('sql-seg').addEventListener('click', e => { const b = e.target.closest('[data-mode]'); if (b) setMode(b.dataset.mode === 'hand'); });
$('btn-start-gen').onclick = async () => {
  if (!sqlInfo || !sqlInfo.editable_sql) return toast('There is no SQL from the builder yet: switch to “Built from the builder” and choose what to count first.');
  if (String(D().sql || '').trim() && !(await askYesNo('Replace your SQL?', 'The editor gets the SQL built from the builder instead.', {yes: 'Replace'}))) return;
  D().sql = sqlInfo.editable_sql; markDirty(); renderSQL(); $('sql-edit').value = D().sql; $('sql-edit').focus();
};
$('sql-edit').addEventListener('input', e => { D().sql = e.target.value; markDirty(); });

// ── SQL reference ────────────────────────────────────────────────────────────
let REF = null;
async function openReference() {
  if (!REF) { try { REF = await api('/worklists/api/sql-reference'); } catch (e) { return toast(e.message); } }
  let view = REF.views[0].view;
  const rows = (v, q) => v.columns.filter(c => !q || (c.name + ' ' + c.about + ' ' + c.type).toLowerCase().includes(q))
    .map(c => `<tr><td class="col">${esc(c.name)}</td><td class="hint">${esc(c.type)}</td><td>${esc(c.about)}</td></tr>`).join('')
    || '<tr><td colspan="3" class="hint">No column matches.</td></tr>';
  const draw = () => {
    const v = REF.views.find(x => x.view === view);
    $('modal').innerHTML = `<div class="modal-back" id="ref-back"><div class="modal wide ref" role="dialog" aria-modal="true" aria-label="SQL reference">
      <div class="panel-head"><h2>SQL reference · what a report written in SQL may read</h2><button class="btn small" id="ref-close" type="button" style="margin-left:auto">Close</button></div>
      <div class="panel-body">
        <div class="ref-rules">${REF.rules.map(r => `<div class="rule"><b>${esc(r.title)}</b>${esc(r.text)}</div>`).join('')}</div>
        <div class="tabs" role="tablist">${REF.views.map(x => `<button type="button" role="tab" class="tab" data-refview="${esc(x.view)}" aria-selected="${x.view === view}">${esc(x.view)} <span class="hint">${esc(x.label)}</span></button>`).join('')}</div>
        <span class="hint">${esc(v.about)} ${v.columns.length} columns.</span>
        <input class="input" id="ref-q" type="search" placeholder="Find a column, e.g. technologist, TAT, protocol…" aria-label="Find a column">
        <div class="table-wrap" style="max-height:360px"><table><thead><tr><th>Column</th><th>Type</th><th>What it holds</th></tr></thead><tbody id="ref-rows">${rows(v, '')}</tbody></table></div>
        <div class="panel-head" style="padding:0;border:0"><h2>Examples${CAT.me.can_write_sql ? ' · “Use this” puts one in the SQL editor' : ''}</h2></div>
        ${REF.examples.map((x, i) => `<div class="rule"><b>${esc(x.title)}</b><span class="hint">${esc(x.what)}</span><pre class="sql" style="margin-top:6px">${highlight(x.sql)}</pre>${CAT.me.can_write_sql ? `<button class="btn small" type="button" data-useex="${i}" style="margin-top:6px">Use this</button>` : ''}</div>`).join('')}
      </div></div></div>`;
    $('ref-close').onclick = close;
    $('ref-back').addEventListener('click', async e => {
      if (e.target.id === 'ref-back') return close();
      const t = e.target.closest('[data-refview]'); if (t) { view = t.dataset.refview; draw(); return; }
      const u = e.target.closest('[data-useex]');
      if (u) {
        const sql = REF.examples[+u.dataset.useex].sql;
        if (handMode() && String(D().sql || '').trim() && !(await askYesNo('Replace the SQL in the editor?', 'The editor gets this example instead of what is in it now.', {yes: 'Use the example'}))) return;
        close(); openDrawer(); setMode(true, sql); toast('Example placed in the SQL editor. Generate it, then adjust.');
      }
    });
    $('ref-q').addEventListener('input', e => { $('ref-rows').innerHTML = rows(v, e.target.value.trim().toLowerCase()); });
  };
  const close = () => { closeModal(); document.removeEventListener('keydown', onKey, true); };
  const onKey = e => { if (e.key === 'Escape') { e.stopImmediatePropagation(); close(); } };
  document.addEventListener('keydown', onKey, true);
  draw();
}
$('btn-ref').onclick = openReference;

// ── Running: live (as you build) or on Generate ──────────────────────────────
function setMeta(extraHtml = '') {
  const r = lastResult, parts = [];
  if (r) {
    parts.push(`${r.rows.length.toLocaleString()} row${r.rows.length === 1 ? '' : 's'}${r.truncated ? ` (the first ${r.cap.toLocaleString()}: CSV holds up to 100,000)` : ''}`);
    parts.push(r.elapsed_ms < 1000 ? `${r.elapsed_ms} ms` : `${(r.elapsed_ms / 1000).toFixed(1)} s`);
  }
  $('res-meta').innerHTML = esc(parts.join(' · ')) + extraHtml;
}
function setBusy(on) {
  $('results').classList.toggle('busy', on);
  setMeta(on ? ' <span class="busy"><i class="bi bi-arrow-repeat"></i> updating…</span>' : stale ? ' <span class="stale">changed: press Generate</span>' : '');
}
function scheduleRun(ms = 650) {
  if (!S) return;
  if (!LIVE) { stale = !!lastResult; setBusy(false); return; }
  clearTimeout(runTimer);
  runTimer = setTimeout(() => runNow(false), ms);
}
function resetResults() {
  if (chart) { chart.dispose(); chart = null; }
  stale = false; model = null; $('res-meta').innerHTML = ''; $('res-title').textContent = 'Result';
  $('results').innerHTML = `<div class="rc-empty">${!S || !D().dataset ? 'Choose what to count on the left.' : LIVE ? 'The result appears here as you build.' : 'Press <strong>Generate</strong>.'}</div>`;
}
// manual: Generate (asks for the filters marked "ask when run"); live runs use their saved values.
async function runNow(manual) {
  const blockers = runBlockers();
  if (blockers.length) {
    if (manual) { toast('Before it can run: ' + blockers.map(m => m[1]).join(', ') + '.'); focusMissing(blockers[0][0]); }
    else resetResults();
    return;
  }
  const asked = D().ask.filter(id => FIELD[id] && inDataset(FIELD[id]));
  if (!manual || !asked.length || handMode()) return run(D());
  await Promise.all(asked.map(id => FIELD[id]).filter(f => f.type === 'multi' && !Array.isArray(f.options))
    .map(f => cachedOptions(f).catch(() => [])));
  const temp = clone(D());
  const fieldHtml = id => `<span class="label">${esc(FIELD[id].label)}</span>${control(FIELD[id], temp.criteria[id] || {}, 'ask')}`;
  $('modal').innerHTML = `<div class="modal-back"><div class="modal" role="dialog" aria-modal="true" aria-label="Before it runs"><div class="panel-head"><h2>Before it runs</h2></div><div class="panel-body" id="ask-body">${
    asked.map(id => `<div class="field" data-ask-f="${id}">${fieldHtml(id)}</div>`).join('')}
    <div class="inline" style="justify-content:flex-end"><button class="btn" id="ask-cancel" type="button">Cancel</button><button class="btn primary" id="ask-go" type="button">Generate</button></div></div></div></div>`;
  const body = $('ask-body');
  body.addEventListener('click', e => {
    const b = e.target.closest('[data-act]'); if (!b || !applyClick(temp.criteria, b)) return;
    if (b.dataset.act === 'chip') b.setAttribute('aria-pressed', String((temp.criteria[b.dataset.f].values || []).includes(JSON.parse(b.dataset.v))));
    else body.querySelector(`[data-ask-f="${b.dataset.f}"]`).innerHTML = fieldHtml(b.dataset.f);
  });
  body.addEventListener('input', e => applyInput(temp.criteria, e.target));
  body.addEventListener('change', e => applyInput(temp.criteria, e.target));
  $('ask-cancel').onclick = closeModal;
  $('ask-go').onclick = () => { closeModal(); run(temp); };
}
async function run(defn) {
  const seq = ++runSeq;
  clearTimeout(runTimer);
  setBusy(true);
  try {
    const r = await api('/worklists/api/run', {definition: defn});
    if (seq !== runSeq) return;                 // a newer run is under way
    lastResult = r; stale = false;
    renderResults();
  } catch (e) {
    if (seq !== runSeq) return;
    lastResult = null; model = null; if (chart) { chart.dispose(); chart = null; }
    $('results').innerHTML = `<div class="rc-note bad">${esc(e.message)}</div>`;
  } finally {
    if (seq === runSeq) setBusy(false);
  }
}
$('btn-run').onclick = () => runNow(true);
$('rc-live').addEventListener('change', e => {
  LIVE = e.target.checked; storeSet('rc-live', LIVE ? '1' : '0');
  if (LIVE && (stale || !lastResult)) scheduleRun(0); else setBusy(false);
});

// ── The result: list, pivot table, chart ─────────────────────────────────────
const fmt = v => v == null ? '' : (typeof v === 'number' ? (Number.isInteger(v) ? v.toLocaleString() : v.toLocaleString(undefined, {maximumFractionDigits: 2})) : String(v));
const cmp = (a, b) => (a == null) - (b == null) || (typeof a === 'number' && typeof b === 'number' ? a - b : String(a).localeCompare(String(b), undefined, {numeric: true}));
function renderResults() {
  const r = lastResult; if (!r) return;
  $('res-title').textContent = S.name || 'Result';
  setMeta();
  if (chart) { chart.dispose(); chart = null; }
  if (!r.rows.length) { model = null; $('results').innerHTML = '<div class="rc-empty">No rows match these filters.</div>'; return; }
  if (r.kind === 'list') { model = null; $('results').innerHTML = listTable(r, !r.hand_sql); return; }
  model = pivotModel(r);
  if (r.kind === 'table') return renderPivot();
  renderChart(D().output.chart || 'bar');
}
function listTable(r, tools) {
  const o = D().output;
  const cols = r.column_labels || r.columns.map(c => ({id: null, label: c}));
  const head = cols.map(c => {
    if (!tools || !c.id) return `<th class="txt">${esc(c.label)}</th>`;
    const arr = o.sort === c.id ? (o.desc !== false ? '▼' : '▲') : '';
    return `<th class="txt" data-lsort="${c.id}" title="Sort by ${esc(c.label)}">${esc(c.label)}${arr ? `<span class="arr">${arr}</span>` : ''}<span class="tools">
      <button type="button" data-lmove="-1" data-col="${c.id}" title="Move left" aria-label="Move left">‹</button><button type="button" data-lmove="1" data-col="${c.id}" title="Move right" aria-label="Move right">›</button>
      <button type="button" class="danger" data-lhide="${c.id}" title="Hide this column" aria-label="Hide this column">✕</button></span></th>`;
  }).join('');
  return `<div class="rc-table-wrap"><table class="rc-table"><thead><tr>${head}</tr></thead><tbody>${
    r.rows.map(row => `<tr>${row.map(v => `<td class="${typeof v === 'number' ? '' : 'txt'}">${esc(fmt(v))}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
}
// The run's rows as a pivot: one key per combination of the row groupings, one series per
// measure, or per value of the column split.
function pivotModel(r) {
  const rowDims = r.row_dims || r.dims.slice(0, r.dims.length - (r.col_dim ? 1 : 0)), nr = rowDims.length;
  const split = !!r.col_dim, meas = r.measures;
  let keys, series, data;
  if (split) {
    const idx = new Map(), cell = new Map();
    const cols = [...new Set(r.rows.map(x => x[nr]))].sort(cmp);
    keys = [];
    r.rows.forEach(x => {
      const ks = JSON.stringify(x.slice(0, nr));
      if (!idx.has(ks)) { idx.set(ks, keys.length); keys.push(x.slice(0, nr)); }
      cell.set(ks + '\u0001' + x[nr], x[nr + 1]);
    });
    const empty = meas[0].additive ? 0 : null;
    series = cols.map(c => ({label: String(c), additive: meas[0].additive, col: c}));
    data = keys.map(k => cols.map(c => { const v = cell.get(JSON.stringify(k) + '\u0001' + c); return v === undefined ? empty : v; }));
  } else {
    keys = nr ? r.rows.map(x => x.slice(0, nr)) : [[]];
    series = meas.map(m => ({label: m.label, additive: m.additive, measure: m.id}));
    data = nr ? r.rows.map(x => x.slice(nr)) : [r.rows[0]];
  }
  return {nr, split, keys, series, data, rowDims, colDim: r.col_dim, measure: meas[0]};
}
const sum = a => a.reduce((s, v) => s + (+v || 0), 0);
function renderPivot() {
  const m = model, rowTotal = m.split && m.measure.additive;
  const order = m.keys.map((_, i) => i);
  if (pivotSort) {
    const {key, dir} = pivotSort;
    const val = i => key[0] === 'd' ? m.keys[i][+key.slice(1)] : key === 't' ? sum(m.data[i]) : m.data[i][+key.slice(1)];
    order.sort((a, b) => dir * cmp(val(a), val(b)));
  }
  const grouped = !pivotSort && m.nr >= 2, anyAdd = m.series.some(s => s.additive);
  const arrow = key => pivotSort && pivotSort.key === key ? `<span class="arr">${pivotSort.dir > 0 ? '▲' : '▼'}</span>` : '';
  const tool = (attr, i, n, hideLabel) => `<span class="tools">${i > 0 ? `<button type="button" data-${attr}-move="-1" data-i="${i}" title="Move left" aria-label="Move left">‹</button>` : ''}${
    i < n - 1 ? `<button type="button" data-${attr}-move="1" data-i="${i}" title="Move right" aria-label="Move right">›</button>` : ''}<button type="button" class="danger" data-${attr}-rm="${i}" title="${hideLabel}" aria-label="${hideLabel}">✕</button></span>`;
  let head = m.nr ? m.rowDims.map((d, di) => `<th class="txt" data-psort="d${di}">${esc(d.label)}${arrow('d' + di)}${tool('pdim', di, m.nr, 'Take out of Rows')}</th>`).join('') : '<th class="txt"></th>';
  head += m.series.map((s, j) => `<th data-psort="s${j}"${m.split && j === 0 ? ` title="${esc(m.colDim.label)} · ${esc(m.measure.label)}"` : ''}>${esc(s.label)}${arrow('s' + j)}${
    !m.split && m.series.length > 1 ? tool('pser', j, m.series.length, 'Hide this measure') : ''}</th>`).join('');
  if (rowTotal) head += `<th data-psort="t">Total${arrow('t')}</th>`;
  const cells = (vals, attrs) => vals.map((v, j) => `<td class="drill" ${attrs} data-j="${j}">${fmt(v)}</td>`).join('')
    + (rowTotal ? `<td class="drill" ${attrs} data-j="t">${fmt(sum(vals))}</td>` : '');
  const subRow = (g, rows) => {
    const vals = m.series.map((s, j) => s.additive ? sum(rows.map(i => m.data[i][j])) : null);
    return `<tr class="sub"><td class="dim">${esc(g)} · subtotal</td>${'<td></td>'.repeat(m.nr - 1)}${vals.map((v, j) => `<td class="${v == null ? '' : 'drill'}" data-sub="${esc(g)}" data-j="${j}">${fmt(v)}</td>`).join('')}${
      rowTotal ? `<td class="drill" data-sub="${esc(g)}" data-j="t">${fmt(sum(vals))}</td>` : ''}</tr>`;
  };
  let body = '', prev = null, group = [], groupKey = null;
  order.forEach(i => {
    const k = m.keys[i];
    if (grouped && groupKey !== null && k[0] !== groupKey) { if (group.length > 1 && anyAdd) body += subRow(groupKey, group); group = []; }
    const dims = m.nr ? k.map((v, d) => {
      const rep = grouped && prev && k.slice(0, d + 1).every((x, e) => x === prev[e]);
      return `<td class="dim${rep ? ' rep' : ''}">${esc(v)}</td>`;
    }).join('') : '<td class="dim">All</td>';
    body += `<tr>${dims}${cells(m.data[i], `data-i="${i}"`)}</tr>`;
    prev = k; groupKey = k[0]; group.push(i);
  });
  if (grouped && group.length > 1 && anyAdd) body += subRow(groupKey, group);
  if (m.keys.length > 1) {
    const tot = m.series.map((s, j) => s.additive ? sum(m.data.map(row => row[j])) : null);
    body += `<tr class="total"><td class="dim">Total</td>${'<td></td>'.repeat(Math.max(m.nr, 1) - 1)}${tot.map((v, j) => `<td class="${v == null ? '' : 'drill'}" data-total="1" data-j="${j}">${fmt(v)}</td>`).join('')}${
      rowTotal ? `<td class="drill" data-total="1" data-j="t">${fmt(sum(tot))}</td>` : ''}</tr>`;
  }
  $('results').innerHTML = `<div class="rc-table-wrap"><table class="rc-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>
    <p class="rc-hint mt-2">Click a header to sort, a number to see the rows behind it.${m.split ? ` Columns: ${esc(m.colDim.label)} · ${esc(m.measure.label)}.` : ''}</p>`;
}
function chartColors() { const cs = getComputedStyle(document.body); return {fg: cs.getPropertyValue('--rc-muted').trim(), line: cs.getPropertyValue('--rc-line').trim()}; }
const PALETTE = ['#60a5fa', '#34d399', '#fbbf24', '#f472b6', '#a78bfa', '#fb923c', '#22d3ee', '#f87171', '#a3e635', '#e879f9'];
function renderChart(type) {
  const m = model;
  if (type === 'heatmap' && !m.split) { $('results').innerHTML = '<div class="rc-note info">A heatmap needs a field in Columns (e.g. hour) and one in Rows (e.g. weekday).</div>'; return; }
  if (!window.echarts) { $('results').innerHTML = '<div class="rc-note bad">The chart library could not be loaded. Show it as a Table.</div>'; return; }
  $('results').innerHTML = '<div class="rc-chart" id="chart"></div><p class="rc-hint mt-2">Click a bar, a point, a slice or a cell to see the rows behind it.</p>';
  const {fg, line} = chartColors();
  const cats = m.keys.map(k => k.length ? k.join(' · ') : 'All');
  chart = echarts.init($('chart'));
  let option;
  if (type === 'pie') {
    option = {color: PALETTE, tooltip: {trigger: 'item', valueFormatter: fmt}, legend: {type: 'scroll', orient: 'vertical', right: 0, top: 'middle', textStyle: {color: fg}},
      series: [{type: 'pie', radius: ['35%', '70%'], center: ['40%', '50%'], name: m.series[0].label, label: {color: fg},
        data: cats.map((c, i) => ({name: c, value: m.data[i][0]}))}]};
  } else if (type === 'heatmap') {
    const vals = m.data.flat().filter(v => v != null).map(Number);
    const lo = Math.min(...vals, 0), hi = Math.max(...vals, 1), mid = lo + (hi - lo) * 0.55;
    // light ink on the dark (low) cells, dark ink on the light (high) ones
    const pts = [];
    m.data.forEach((row, i) => row.forEach((v, j) => { if (v != null) pts.push({value: [j, i, v], label: {color: +v > mid ? '#0f172a' : '#e2e8f0'}}); }));
    option = {tooltip: {position: 'top', formatter: p => `${esc(cats[p.value[1]])} · ${esc(m.series[p.value[0]].label)}<br><b>${fmt(p.value[2])}</b> ${esc(m.measure.label)}`},
      grid: {left: 10, right: 20, top: 10, bottom: 60, containLabel: true},
      xAxis: {type: 'category', data: m.series.map(s => s.label), axisLabel: {color: fg}, axisLine: {lineStyle: {color: line}}, splitArea: {show: true}},
      yAxis: {type: 'category', data: cats, inverse: true, axisLabel: {color: fg}, axisLine: {lineStyle: {color: line}}, splitArea: {show: true}},
      visualMap: {min: lo, max: hi, calculable: true, orient: 'horizontal', left: 'center', bottom: 0, textStyle: {color: fg},
        inRange: {color: ['#0d366b', '#256abf', '#6da7ec', '#cde2fb']}},
      series: [{type: 'heatmap', data: pts, label: {show: m.keys.length * m.series.length <= 400, formatter: p => fmt(p.value[2])}}]};
  } else {
    option = {color: PALETTE, tooltip: {trigger: 'axis', valueFormatter: fmt}, legend: {textStyle: {color: fg}, top: 0, type: 'scroll'},
      grid: {left: 50, right: 16, top: 36, bottom: 60, containLabel: true},
      xAxis: {type: 'category', data: cats, axisLabel: {color: fg, rotate: cats.length > 8 ? 35 : 0}, axisLine: {lineStyle: {color: line}},
        name: m.rowDims.map(d => d.label).join(' · '), nameLocation: 'middle', nameGap: 44, nameTextStyle: {color: fg}},
      yAxis: {type: 'value', axisLabel: {color: fg}, splitLine: {lineStyle: {color: line}}},
      series: m.series.map((s, j) => ({name: s.label, type: type === 'line' ? 'line' : 'bar', stack: type === 'stacked' ? 'all' : undefined,
        emphasis: {focus: 'series'}, data: m.data.map(row => row[j])}))};
  }
  chart.setOption(option);
  chart.on('click', p => {
    if (type === 'heatmap') return drill(p.value[1], p.value[0]);
    drill(p.dataIndex, type === 'pie' ? 0 : p.seriesIndex);
  });
}
window.addEventListener('resize', () => chart && chart.resize());
window.addEventListener('rc-theme', () => { if (chart && lastResult) renderResults(); });

// Work in the result: sort, move and hide columns, open the rows behind a number.
$('results').addEventListener('click', e => {
  const t = e.target, o = D().output;
  const lm = t.closest('[data-lmove]');
  if (lm) {
    const id = lm.dataset.col, i = o.columns.indexOf(id), to = i + (+lm.dataset.lmove);
    if (i < 0 || to < 0 || to >= o.columns.length) return;
    o.columns.splice(i, 1); o.columns.splice(to, 0, id); return outputChanged();
  }
  const lh = t.closest('[data-lhide]');
  if (lh) { if (o.columns.length === 1) return toast('A list needs at least one column.'); o.columns = o.columns.filter(c => c !== lh.dataset.lhide); return outputChanged(); }
  const ls = t.closest('[data-lsort]');
  if (ls) {
    if (o.sort === ls.dataset.lsort) o.desc = o.desc === false; else { o.sort = ls.dataset.lsort; o.desc = true; }
    markDirty(); renderZones(); scheduleCompile(0); return run(D());
  }
  const pdm = t.closest('[data-pdim-move]');
  if (pdm) { const i = +pdm.dataset.i, to = i + (+pdm.dataset.pdimMove); [o.rows[i], o.rows[to]] = [o.rows[to], o.rows[i]]; return outputChanged(); }
  const pdr = t.closest('[data-pdim-rm]'); if (pdr) { o.rows.splice(+pdr.dataset.pdimRm, 1); return outputChanged(); }
  const psm = t.closest('[data-pser-move]');
  if (psm) { const i = +psm.dataset.i, to = i + (+psm.dataset.pserMove); [o.measures[i], o.measures[to]] = [o.measures[to], o.measures[i]]; return outputChanged(); }
  const psr = t.closest('[data-pser-rm]');
  if (psr) { if (o.measures.length === 1) return toast('Values need at least one measure.'); o.measures.splice(+psr.dataset.pserRm, 1); return outputChanged(); }
  const ps = t.closest('[data-psort]');
  if (ps) {
    const key = ps.dataset.psort, first = key[0] === 'd' ? 1 : -1;
    pivotSort = !pivotSort || pivotSort.key !== key ? {key, dir: first} : pivotSort.dir === first ? {key, dir: -first} : null;
    return renderPivot();
  }
  const cell = t.closest('td.drill');
  if (cell) {
    const j = cell.dataset.j === 't' ? null : +cell.dataset.j;
    if (cell.dataset.sub != null) return drill(null, j, cell.dataset.sub);
    if (cell.dataset.total) return drill(null, j);
    return drill(+cell.dataset.i, j);
  }
});
// The rows behind a cell: row key i (or the first grouping's value for a subtotal, or all for
// the total), and series j when the columns are split.
function drill(i, j, sub) {
  const m = model; if (!m) return;
  const cell = {}, words = [];
  if (i != null) m.rowDims.forEach((d, di) => { cell[d.id] = m.keys[i][di]; words.push(`${d.label}: ${m.keys[i][di]}`); });
  if (sub != null) { cell[m.rowDims[0].id] = sub; words.push(`${m.rowDims[0].label}: ${sub}`); }
  if (m.split && j != null) { cell[m.colDim.id] = m.series[j].col; words.push(`${m.colDim.label}: ${m.series[j].col}`); }
  openDrill(cell, words);
}
async function openDrill(cell, words) {
  $('modal').innerHTML = `<div class="modal-back" id="drill-back"><div class="modal wide" role="dialog" aria-modal="true" aria-label="The rows behind this number">
    <div class="panel-head"><h2>${esc(ds().count_label)} behind · ${esc(words.join(' · ') || 'the total')}</h2><span class="hint" id="drill-meta"></span>
      <button class="btn small" id="drill-close" type="button" style="margin-left:auto">Close</button></div>
    <div class="panel-body" id="drill-body"><div class="empty">Loading…</div></div></div></div>`;
  const close = () => { closeModal(); document.removeEventListener('keydown', onKey); };
  const onKey = e => { if (e.key === 'Escape') close(); };
  document.addEventListener('keydown', onKey);
  $('drill-close').onclick = close;
  $('drill-back').addEventListener('click', e => { if (e.target.id === 'drill-back') close(); });
  try {
    const r = await api('/worklists/api/drill', {definition: D(), cell});
    if (!$('drill-body')) return;
    $('drill-meta').textContent = `${r.rows.length.toLocaleString()} row${r.rows.length === 1 ? '' : 's'}${r.truncated ? ` (the first ${r.cap.toLocaleString()})` : ''}`;
    $('drill-body').innerHTML = r.rows.length ? listTable(r, false) + '<p class="rc-hint">The list columns are the report’s (Show as List to change them).</p>' : '<div class="empty">No rows.</div>';
  } catch (e) { if ($('drill-body')) $('drill-body').innerHTML = `<div class="note bad">${esc(e.message)}</div>`; }
}

$('btn-csv').onclick = async () => {
  const blockers = runBlockers();
  if (blockers.length) { toast('Before it can export: ' + blockers.map(m => m[1]).join(', ') + '.'); focusMissing(blockers[0][0]); return; }
  try {
    const r = await fetch('/worklists/api/csv', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({definition: D(), name: S.name})});
    if (!r.ok) { const d = await r.json().catch(() => ({})); throw new Error(d.error || 'export failed'); }
    const blob = await r.blob(), a = document.createElement('a');
    a.href = URL.createObjectURL(blob); a.download = (S.name || 'report') + '.csv'; a.click(); URL.revokeObjectURL(a.href);
    if (r.headers.get('X-Truncated') === '1') toast('Export capped at 100,000 rows.');
  } catch (e) { toast(e.message); }
};

// ── Saving, the library buttons, the header fields ───────────────────────────
function renderAll() {
  renderLibrary(); renderHeader(); renderFields(); renderZones(); renderHandNote(); renderSQL(); renderStatus();
}
$('btn-new').onclick = async () => {
  if (dirty && !(await askYesNo('Discard your changes?', 'This report has changes that are not saved.', {yes: 'Discard changes'}))) return;
  blank(); toast('New report: choose what to count, then drag fields into the builder.'); $('f-ds').focus();
};
$('btn-dup').onclick = () => {
  const c = clone(S); c.id = null; c.name = (S.name || 'Report') + ' (copy)'; c.visibility = 'personal'; c.can_edit = true;
  c.version = 0; c.versions = []; c.owner_name = null; c.updated_by_name = null; c.updated_at = null;
  load(c); markDirty(); toast('Duplicated into Personal. Save to keep it.');
};
$('btn-del').onclick = async () => {
  if (!S.id || !(await askYesNo(`Delete “${S.name}”?`, 'Its whole version history goes with it. This cannot be undone.', {yes: 'Delete', danger: true, icon: 'error'}))) return;
  try { await api(`/worklists/api/${S.id}/delete`, {}); markDirty(false); toast('Deleted.'); await loadLibrary(); blank(); } catch (e) { toast(e.message); }
};
async function save() {
  if (!S.can_edit) return toast('Read-only for you: duplicate it into Personal to change it.');
  const miss = missingFields();
  if (miss.length) { toast('Fill in first: ' + miss.map(m => m[1]).join(', ') + '.'); focusMissing(miss[0][0]); return; }
  try {
    const r = await api('/worklists/api/save', {id: S.id, name: S.name, description: S.description, folder: S.folder, visibility: S.visibility, definition: D()});
    markDirty(false); toast(`Saved, version ${r.version}.`); await loadLibrary(r.id);
  } catch (e) { toast(e.message); }
}
$('btn-save').onclick = save;
document.addEventListener('keydown', e => {
  if (!(e.ctrlKey || e.metaKey) || !S) return;
  if (e.key.toLowerCase() === 's') { e.preventDefault(); save(); }
  if (e.key === 'Enter' && !e.target.closest('textarea')) { e.preventDefault(); runNow(true); }
});
$('f-history').onchange = async e => {
  const v = e.target.value; if (!v || !S.id) return;
  try {
    const old = await api(`/worklists/api/${S.id}/versions/${v}`);
    S.definition = old.definition; S.definition.ask = S.definition.ask || []; S.definition.criteria = S.definition.criteria || {}; S.name = old.name;
    normalizeOutput(); extra = initialExtra(); markDirty(); renderAll(); scheduleCompile(0); scheduleRun(0);
    toast(`Loaded version ${v}. Save to make it current.`);
  } catch (err) { toast(err.message); }
  e.target.value = '';
};
$('f-name').addEventListener('input', e => { S.name = e.target.value; markDirty(); });
$('f-desc').addEventListener('input', e => { S.description = e.target.value; markDirty(); });
$('f-folder').addEventListener('input', e => { S.folder = e.target.value; markDirty(); });
$('f-vis').addEventListener('click', e => { const b = e.target.closest('button'); if (!b || b.disabled) return; S.visibility = b.dataset.v; markDirty(); renderHeader(); });
$('f-ds').addEventListener('change', e => {
  if (!e.target.value) return;
  closePop();
  const old = D(), keep = extra, ds2 = e.target.value, oldOut = old.output || {};
  S.definition = blankDefinition(ds2, oldOut);
  Object.entries(old.criteria || {}).forEach(([k, v]) => { if (FIELD[k] && FIELD[k].datasets.includes(ds2)) S.definition.criteria[k] = v; });
  S.definition.ask = (old.ask || []).filter(k => FIELD[k] && FIELD[k].datasets.includes(ds2));
  S.definition.sql = old.sql;
  // groupings and measures the new dataset has too stay; its date starts at the last 30 days
  if (old.dataset) {
    const dims = new Set(CAT.datasets[ds2].dimensions.map(x => x.id));
    const rows = (Array.isArray(oldOut.rows) ? oldOut.rows : [oldOut.rows]).filter(x => x && dims.has(x));
    if (rows.length) S.definition.output.rows = rows;
    if (oldOut.cols && dims.has(oldOut.cols)) S.definition.output.cols = oldOut.cols;
    const ms = (oldOut.measures || []).filter(m => CAT.datasets[ds2].measures.some(x => x.id === m));
    if (ms.length) S.definition.output.measures = ms;
  }
  const dd = CAT.datasets[ds2].default_date;
  if (!S.definition.criteria[dd]) S.definition.criteria[dd] = {mode: 'quick', quick: 'last_30_days'};
  normalizeOutput();
  extra = [...new Set([...keep.filter(k => inDataset(FIELD[k])), ...initialExtra()])];
  pivotSort = null; markDirty(); renderAll(); scheduleCompile(0); scheduleRun(0);
});
window.addEventListener('beforeunload', e => { if (dirty) { e.preventDefault(); e.returnValue = ''; } });

// ── Scheduled emails: which saved reports go to whom, and when ───────────────
// One view at a time (list, a schedule's form, its history), so nothing redraws over a form
// someone is filling in. The mail server is set in Administration > Email Server.
const WEEKDAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
const RUN_WORDS = {running: 'Running', sent: 'Sent', partial: 'Sent, incomplete', failed: 'Failed'};
let SC = null;
const scWhen = s => (s.frequency === 'weekly' ? 'Every ' + WEEKDAYS[s.weekday]
  : s.frequency === 'monthly' ? `Day ${s.month_day} of each month` : 'Every day') + ' at ' + s.send_time;
const scRun = r => r ? `<span class="run-${esc(r.status)}">${esc(RUN_WORDS[r.status] || r.status)}</span> <span class="hint">${esc(r.started_at)}</span>` : '<span class="hint">Not sent yet</span>';
function scEntry(e) {
  if (e.error) return `<span class="run-failed">${esc(e.name)}: ${esc(e.error)}</span>`;
  const rows = e.rows === 1 ? '1 row' : `${(e.rows || 0).toLocaleString()} rows`;
  const note = e.too_large ? ', too large to attach' : e.truncated ? ', capped' : e.rows ? '' : ', nothing attached';
  return `${esc(e.name)} <span class="hint">v${esc(e.version)}: ${rows}${note}</span>`;
}
function scListView() {
  const rows = SC.schedules.map(s => `<tr>
    <td><b>${esc(s.name)}</b>${s.enabled ? '' : ' <span class="crit-ask">paused</span>'}<br><span class="hint">${s.worklists.map(w => esc(w.name)).join(' · ')}</span></td>
    <td>${esc(scWhen(s))}${s.enabled ? `<br><span class="hint">Next: ${esc(s.next_run_at)}</span>` : ''}</td>
    <td>${s.recipients.map(esc).join('<br>')}</td>
    <td>${scRun(s.last_run)}</td>
    <td><div class="inline">
      <button class="btn small" type="button" data-act="edit" data-id="${s.id}">Edit</button>
      <button class="btn small" type="button" data-act="send" data-id="${s.id}"${SC.ready ? '' : ' disabled'}>Send now</button>
      <button class="btn small" type="button" data-act="runs" data-id="${s.id}">History</button>
      <button class="btn small danger" type="button" data-act="del" data-id="${s.id}">Delete</button>
    </div></td></tr>`).join('');
  return `${SC.ready ? '' : `<div class="note warn">Nothing can be sent until the mail server is set in <a href="${esc(MAIL_SERVER_URL)}">Administration &gt; Email Server</a>.</div>`}
    <div class="inline">
      <button class="btn small primary" type="button" data-act="new"${SC.ready ? '' : ' disabled'}>New schedule</button>
      <a class="btn small" href="${esc(MAIL_SERVER_URL)}"><i class="bi bi-gear"></i> Mail server</a>
    </div>
    ${rows ? `<div class="table-wrap"><table><thead><tr><th>Schedule</th><th>When (hospital time)</th><th>Recipients</th><th>Last run</th><th></th></tr></thead><tbody>${rows}</tbody></table></div>`
      : '<div class="empty">No scheduled emails yet.</div>'}`;
}
function scFormView() {
  const s = SC.edit, chosen = (s.worklists || []).map(w => w.id);
  const pool = LIB.map(w => ({id: w.id, name: w.name, folder: w.visibility === 'shared' ? w.folder : 'Personal'}))
    .concat((s.worklists || []).filter(w => !LIB.some(x => x.id === w.id)).map(w => ({...w, folder: 'Another admin\'s'})));
  return `<div class="def-grid">
      <div class="field wide"><label for="sc-name">Name</label><input class="input" id="sc-name" value="${esc(s.name || '')}" placeholder="e.g. Monday morning pack"><span class="hint">Becomes the subject of the email.</span></div>
      <div class="field wide"><span class="label">Reports</span><div class="checks pick" id="sc-pick">${pool.map(w => `<label><input type="checkbox" value="${w.id}"${chosen.includes(w.id) ? ' checked' : ''}><span>${esc(w.name)} <span class="hint">${esc(w.folder)}</span></span></label>`).join('') || '<span class="hint">Save a report first.</span>'}</div>
        <span class="hint">Each email runs the last saved version, with its dates worked out on the day it goes. Filters marked “ask when run” use their saved values. A chart arrives as its table.</span></div>
      <div class="field wide"><label for="sc-to">Recipients</label><textarea class="input" id="sc-to" rows="2" placeholder="one@laumc.com, two@laumc.com">${esc((s.recipients || []).join(', '))}</textarea>
        <span class="hint">The attachments can hold patient data, so only addresses at ${esc(SC.mail.smtp_allowed_domains)} are accepted.</span></div>
      <div class="field"><label for="sc-freq">How often</label><select class="input" id="sc-freq">${[['daily', 'Every day'], ['weekly', 'Every week'], ['monthly', 'Every month']].map(([v, l]) => `<option value="${v}"${(s.frequency || 'daily') === v ? ' selected' : ''}>${l}</option>`).join('')}</select></div>
      <div class="field"><label for="sc-time">At (hospital time)</label><input class="input" type="time" id="sc-time" value="${esc(s.send_time || '07:00')}"></div>
      <div class="field" id="sc-weekly"><label for="sc-weekday">On</label><select class="input" id="sc-weekday">${WEEKDAYS.map((d, i) => `<option value="${i}"${(s.weekday ?? 0) === i ? ' selected' : ''}>${d}</option>`).join('')}</select></div>
      <div class="field" id="sc-monthly"><label for="sc-day">On day</label><input class="input" type="number" min="1" max="31" id="sc-day" value="${esc(s.month_day ?? 1)}"><span class="hint">A shorter month sends on its last day.</span></div>
      <label class="field wide" style="flex-direction:row;gap:8px;align-items:center;cursor:pointer"><input type="checkbox" id="sc-on"${s.enabled === false ? '' : ' checked'}> Active (untick to pause it)</label>
    </div>
    <div class="inline" style="justify-content:flex-end"><button class="btn" type="button" data-act="back">Cancel</button><button class="btn primary" type="button" data-act="save">Save schedule</button></div>`;
}
function scRunsView() {
  const rows = SC.runs.list.map(r => `<tr>
    <td>${esc(r.started_at)}<br><span class="hint">${r.started_by ? 'Sent by ' + esc(r.started_by) : 'On schedule'}</span></td>
    <td><span class="run-${esc(r.status)}">${esc(RUN_WORDS[r.status] || r.status)}</span>${r.error ? `<br><span class="hint">${esc(r.error)}</span>` : ''}</td>
    <td>${r.detail.map(scEntry).join('<br>')}</td>
    <td>${r.recipients.map(esc).join('<br>')}</td></tr>`).join('');
  return `<strong>${esc(SC.runs.name)}</strong>
    ${rows ? `<div class="table-wrap"><table><thead><tr><th>When</th><th>Result</th><th>Reports</th><th>Sent to</th></tr></thead><tbody>${rows}</tbody></table></div>` : '<div class="empty">Not sent yet.</div>'}
    <div class="inline" style="justify-content:flex-end"><button class="btn" type="button" data-act="back">Back</button></div>`;
}
function scDraw() {
  const views = {list: scListView, edit: scFormView, runs: scRunsView};
  $('modal').innerHTML = `<div class="modal-back"><div class="modal wide sc" id="sc" role="dialog" aria-modal="true" aria-label="Scheduled emails">
    <div class="panel-head"><h2>Scheduled emails</h2><button class="btn small" type="button" data-act="close" style="margin-left:auto">Close</button></div>
    <div class="panel-body">${views[SC.view]()}</div></div></div>`;
  $('sc').addEventListener('click', scClick);
  if (SC.view === 'edit') { $('sc-freq').addEventListener('change', scFreq); scFreq(); }
}
function scFreq() { $('sc-weekly').hidden = $('sc-freq').value !== 'weekly'; $('sc-monthly').hidden = $('sc-freq').value !== 'monthly'; }
function scClose() { SC = null; closeModal(); document.removeEventListener('keydown', scKey); }
function scKey(e) { if (e.key === 'Escape' && SC && SC.view === 'list' && !document.querySelector('.swal2-container')) scClose(); }
async function scOpen(view = 'list') {
  const was = SC;
  try { SC = await api('/worklists/api/schedules'); } catch (e) { SC = was; return toast(e.message); }
  SC.view = view; scDraw();
}
async function scClick(e) {
  const b = e.target.closest('button[data-act]'); if (!b) return;
  const act = b.dataset.act, s = SC.schedules.find(x => x.id === +b.dataset.id);
  // A slow answer must not be asked for twice: above all not a second email.
  const busy = async (label, work) => { const old = b.innerHTML; b.disabled = true; b.textContent = label; try { await work(); } catch (err) { toast(err.message); } if (b.isConnected) { b.disabled = false; b.innerHTML = old; } };
  if (act === 'close') return scClose();
  if (act === 'back') { SC.view = 'list'; return scDraw(); }
  if (act === 'new') { SC.edit = {}; SC.view = 'edit'; return scDraw(); }
  if (act === 'edit') { SC.edit = s; SC.view = 'edit'; return scDraw(); }
  if (act === 'runs') return busy('Loading…', async () => { SC.runs = {name: s.name, list: await api(`/worklists/api/schedules/${s.id}/runs`)}; SC.view = 'runs'; scDraw(); });
  if (act === 'save') return busy('Saving…', async () => {
    await api('/worklists/api/schedules/save', {id: SC.edit.id, name: $('sc-name').value, recipients: $('sc-to').value,
      worklist_ids: [...document.querySelectorAll('#sc-pick input:checked')].map(i => +i.value),
      frequency: $('sc-freq').value, send_time: $('sc-time').value, weekday: +$('sc-weekday').value, month_day: $('sc-day').value, enabled: $('sc-on').checked});
    toast('Schedule saved.'); await scOpen();
  });
  if (act === 'del') {
    if (!(await askYesNo(`Delete “${s.name}”?`, 'It stops being sent. The record of what it already sent is kept.', {yes: 'Delete', danger: true, icon: 'error'}))) return;
    return busy('Deleting…', async () => { await api(`/worklists/api/schedules/${s.id}/delete`, {}); toast('Deleted.'); await scOpen(); });
  }
  if (act === 'send') {
    if (!(await askYesNo(`Send “${s.name}” now?`, `To ${s.recipients.join(', ')}. Its next regular send stays as it is.`, {yes: 'Send now', icon: 'question'}))) return;
    return busy('Sending…', async () => {
      const r = await api(`/worklists/api/schedules/${s.id}/send`, {});
      toast(r.status === 'sent' ? 'Sent.' : r.status === 'partial' ? 'Sent, but something is missing: see History.' : 'Not sent: ' + (r.error || 'see History.'));
      await scOpen();
    });
  }
}
$('btn-sched').onclick = () => { document.addEventListener('keydown', scKey); scOpen(); };

// ── Start ────────────────────────────────────────────────────────────────────
(async () => {
  LIVE = storeGet('rc-live') !== '0';
  $('rc-live').checked = LIVE;
  try { CAT = await api('/worklists/api/catalog'); } catch (e) { $('results').innerHTML = `<div class="rc-note bad">${esc(e.message)}</div>`; return; }
  CAT.fields.forEach(f => FIELD[f.id] = f);
  $('btn-sched').hidden = !CAT.me.can_schedule;
  await loadLibrary();
  const wanted = +(new URLSearchParams(location.search).get('id') || 0);
  const first = wanted || (LIB[0] && LIB[0].id);
  if (first) { if (!(await open(first))) blank(); } else blank();
})();
})();
