/*
 * static/js/report_composer.js — the Report Composer page (templates/worklists.html,
 * routes/worklists.py). Revamp of 2026-11-20 (operator's spec):
 *
 *   1. the same frame as every report: the left panel (report, what to count, the date
 *      range, the filters, Generate, CSV) and the result in cards;
 *   2. the SQL is not on the page: the SQL button opens it in a drawer, where the
 *      settings capability may also write it by hand;
 *   3. the filters are visible: the dataset's usual ones (catalog 'pinned') as the
 *      reports' searchable dropdowns, each value with how many rows it had in the last
 *      year, the rest one "Add filter" away, and what is applied as tags above the result;
 *   4. one example report left in the library (migration 0181).
 *
 * What a report is (its definition: dataset, criteria, ask, output, sql) did not change,
 * so every saved report, version and scheduled email reads as before.
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
const MAIL_SERVER_URL = $('rc').dataset.mailUrl;
const TYPE_WORDS = {date: 'date', multi: 'choice', text: 'text', yesno: 'yes / no', range: 'number'};
// Where the fields of a group come from, when it is not PACS.
const TAB_NOTES = {
  exam: 'From the RIS (the nightly Oracle ETL): its status history, performed steps and reports, read as Report 35 reads them.',
  nlp: 'From the NLP worker\'s reading of the ORU reports: negations and history removed, benign findings left out.',
  crn: 'From Critical Result Notification (Admin > CRN), matched by accession.',
};
let CAT, FIELD = {}, LIB = [], S = null, extra = [], sqlInfo = null, dirty = false,
    compileTimer = null, chart = null, lastResult = null, quietLib = false;
const optionPromise = {}, optionList = {};

async function api(url, body) {
  const opt = body === undefined ? {} : {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)};
  const r = await fetch(url, opt);
  const d = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(d.error || ('HTTP ' + r.status));
  return d;
}
// Confirmations use SweetAlert2, like the rest of RAYD. The browser's own dialog only if the
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

// ── State ────────────────────────────────────────────────────────────────────
const D = () => S.definition;
const ds = () => CAT.datasets[D().dataset] || null;
const inDataset = f => !!f && f.datasets.includes(D().dataset);
const handMode = () => D().sql != null;
// A new report starts EMPTY (operator, 2026-10-25): no dataset, no criteria, no output kind.
function emptyDefinition() {
  return {dataset: null, criteria: {}, ask: [], output: {kind: null, rows: null, cols: null, measures: [], columns: [], sort: null, desc: true}, sql: null};
}
// What a dataset starts with: a count (gigabytes for Storage) and its usual list columns.
function blankDefinition(dataset, kind = null) {
  const d = CAT.datasets[dataset];
  return {dataset, criteria: {}, ask: [], output: {kind, rows: null, cols: null, measures: [d.default_measure], columns: d.list_default.slice(), sort: d.default_date, desc: true}, sql: null};
}
// The panel: the dataset's date (as the date range), its pinned filters, then those added.
function baseFields() {
  const d = ds(); if (!d) return [];
  return [d.default_date, ...(d.pinned || [])].map(id => FIELD[id]).filter(inDataset);
}
function shownFields() {
  const base = baseFields(), ids = new Set(base.map(f => f.id));
  return base.concat(extra.filter(id => !ids.has(id)).map(id => FIELD[id]).filter(inDataset));
}
// A saved report's criteria on fields outside the base appear as added filters.
function initialExtra() {
  if (!ds()) return [];
  const base = new Set(baseFields().map(f => f.id)), used = new Set([...Object.keys(D().criteria || {}), ...D().ask]);
  return CAT.fields.filter(f => inDataset(f) && !base.has(f.id) && used.has(f.id)).map(f => f.id);
}
function load(w) {
  S = clone(w);
  S.definition.ask = S.definition.ask || [];
  S.definition.criteria = S.definition.criteria || {};
  const d0 = CAT.datasets[S.definition.dataset];
  S.definition.output = Object.assign({kind: S.definition.dataset ? 'list' : null, measures: d0 ? [d0.default_measure] : []}, S.definition.output || {});
  extra = initialExtra();
  markDirty(false); lastResult = null; resetResults();
  renderAll(); scheduleCompile(0); syncUrl();
}
function blank() {
  load({id: null, name: '', description: '', folder: '', visibility: 'personal',
        can_edit: true, version: 0, versions: [], definition: emptyDefinition()});
}
function syncUrl() {
  try { history.replaceState(null, '', S && S.id ? '?id=' + S.id : location.pathname); } catch (e) { /* not needed */ }
}

// ── Required fields and the status line ──────────────────────────────────────
// Building: a name, a dataset and how to show it. Written in SQL: a name and the SQL.
function missingFields() {
  if (!S) return [];
  const out = [];
  if (!String(S.name || '').trim()) out.push(['name', 'Name']);
  if (handMode()) { if (!String(D().sql || '').trim()) out.push(['sql', 'SQL']); return out; }
  if (!D().dataset) out.push(['ds', 'What to count']);
  if (!(D().output || {}).kind) out.push(['kind', 'Show as']);
  return out;
}
function renderStatus() {
  if (!S) return;
  const miss = missingFields(), keys = miss.map(m => m[0]);
  $('f-name').classList.toggle('missing', keys.includes('name'));
  $('f-ds').classList.toggle('missing', keys.includes('ds'));
  const kind = $('fw-kind'); if (kind) kind.classList.toggle('missing', keys.includes('kind'));
  $('sql-wrap').classList.toggle('missing', keys.includes('sql'));
  const st = $('bar-status');
  st.textContent = dirty ? 'Unsaved changes' : (S.id ? `Saved · version ${S.version}` : 'New report · not saved yet');
  st.classList.toggle('dirty', dirty);
  $('bar-missing').textContent = miss.length ? 'Still to fill: ' + miss.map(m => m[1]).join(', ') : '';
}
function focusMissing(key) {
  if (key === 'sql') { openDrawer(); $('sql-edit').focus(); return; }
  const el = {name: $('f-name'), ds: $('f-ds'), kind: document.querySelector('#fw-kind button')}[key];
  if (el) { scrollWithin(el); el.focus({preventScroll: true}); }
}
// What stops a run: everything required except the name.
function runBlockers() { return missingFields().filter(m => m[0] !== 'name'); }

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
  if (!(await open(id))) renderLibrary();  // kept the current one: put the dropdown back
}
async function open(id) {
  if (dirty && !(await askYesNo('Discard your changes?', 'This report has changes that are not saved.', {yes: 'Discard changes'}))) return false;
  try { load(await api('/worklists/api/' + id)); return true; } catch (e) { toast(e.message); return false; }
}

// ── Header: name, description, who sees it, folder, history ──────────────────
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

// ── Criteria: what each one says ─────────────────────────────────────────────
function isActive(f, c) {
  if (!c) return false;
  if (f.type === 'date') return !!c.mode && c.mode !== 'none';
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
    if (c.mode === 'quick') return (CAT.quick_dates.find(q => q.id === c.quick) || {}).label || c.quick;
    if (c.mode === 'relative') return `${c.dir === 'next' ? 'Next' : 'Previous'} ${c.n} ${c.unit}`;
    if (c.mode === 'fixed') return c.from === c.to ? c.from : `${c.from} → ${c.to}`;
  }
  if (f.type === 'multi') {
    const names = c.values.map(v => optLabel(f, v));
    return names.length > 4 ? names.slice(0, 3).join(', ') + ` +${names.length - 3} more` : names.join(', ');
  }
  if (f.type === 'text') return `${c.op || 'contains'} “${c.value}”`;
  if (f.type === 'yesno') return c.value === 'yes' ? 'Yes' : 'No';
  if (f.type === 'range') return [c.min !== '' && c.min != null ? `≥ ${c.min}` : '', c.max !== '' && c.max != null ? `≤ ${c.max}` : ''].filter(Boolean).join(' and ') + (f.unit ? ' ' + f.unit : '');
  return '';
}

// ── The values a dropdown offers: from the data, with their counts ───────────
function cachedOptions(f) {
  const key = D().dataset + '|' + f.id;
  if (!optionPromise[key]) {
    optionPromise[key] = api(`/worklists/api/options?dataset=${enc(D().dataset)}&field=${enc(f.id)}`)
      .then(list => (optionList[key] = list), e => { delete optionPromise[key]; throw e; });
  }
  return optionPromise[key];
}
// What a dropdown shows for what was typed. The first OPTIONS_CAP values (most frequent first)
// are kept; a field with more asks the server for the typed text.
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

// ── The controls, the same in the panel and in the "before it runs" dialog ────
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
    return html;
  }
  if (f.type === 'multi') {
    const sel = c.values || [];
    if (where === 'panel') {
      return `<select multiple class="rc-s2 w-full" id="rc-sel-${f.id}" ${a} aria-label="${esc(f.label)}">${
        sel.map(v => `<option value="${esc(JSON.stringify(v))}" selected>${esc(optLabel(f, v))}</option>`).join('')}</select>`;
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
function dateDefaults(c) {
  if (c.mode === 'relative') { c.dir = c.dir || 'prev'; c.n = c.n || 7; c.unit = c.unit || 'days'; }
  if (c.mode === 'fixed') { const today = isoToday(); c.from = c.from || CAT.report_start_date || today; c.to = c.to || today; }
}
// A control's click or typing, on a set of criteria: the report's, or the dialog's copy.
function applyClick(criteria, b) {
  const fid = b.dataset.f, v = b.dataset.v, act = b.dataset.act;
  if (!fid || !FIELD[fid]) return false;
  const c = criteria[fid] = criteria[fid] || {};
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
  c[k] = k === 'n' ? Math.max(1, parseInt(t.value || '1', 10)) : t.value;
  return true;
}

// ── The left panel ───────────────────────────────────────────────────────────
function blockTools(f) {
  const c = D().criteria[f.id], on = isActive(f, c);
  return (f.hint ? `<button type="button" class="rc-icon" data-act="hint" data-f="${f.id}" title="${esc(f.hint)}" aria-label="About ${esc(f.label)}"><i class="bi bi-question-circle"></i></button>` : '')
    + (on ? `<button type="button" class="rc-icon" data-act="clear" data-f="${f.id}" title="Clear this filter" aria-label="Clear ${esc(f.label)}"><i class="bi bi-eraser"></i></button>` : '')
    + (extra.includes(f.id) ? `<button type="button" class="rc-icon danger" data-act="remove" data-f="${f.id}" title="Take this filter out of the panel" aria-label="Remove ${esc(f.label)}"><i class="bi bi-x-lg"></i></button>` : '');
}
function block(f) {
  const c = D().criteria[f.id] || {}, ask = D().ask.includes(f.id), isRange = f.id === ds().default_date;
  const label = isRange ? `Date range <span class="sub">· ${esc(f.label)}</span>` : esc(f.label);
  return `<div class="rc-f" data-fid="${f.id}">
    <div class="rc-f-head"><label class="rfp-label"${f.type === 'multi' ? ` for="rc-sel-${f.id}"` : ''}>${label}</label><span class="rc-f-tools">${blockTools(f)}</span></div>
    ${control(f, c, 'panel')}
    <label class="rc-ask" title="Ask for this filter each time the report is run"><input type="checkbox" class="rfp-tick" data-ask="${f.id}"${ask ? ' checked' : ''}><span>Ask when run</span></label>
  </div>`;
}
function renderPanel() {
  const box = $('rc-filters');
  destroySelects(box);
  if (handMode()) {
    box.innerHTML = '<div class="rc-sql-note"><b>Written in SQL.</b> The filters are not used: the SQL names its own views and values. <button type="button" class="rc-link" data-act="open-sql">Open the SQL</button></div>';
    return;
  }
  if (!ds()) { box.innerHTML = '<p class="rc-hint">Choose what to count first: the date range and the filters depend on it.</p>'; return; }
  const [date, ...rest] = shownFields();
  box.innerHTML = (date ? block(date) : '')
    + `<div class="rc-sep"><span class="rfp-label" style="margin:0;flex:1">Filters</span><span id="rc-count"></span></div>`
    + rest.map(block).join('')
    + '<button type="button" class="rc-add" data-act="add-filter"><i class="bi bi-plus-lg" aria-hidden="true"></i> Add filter</button>';
  initSelects(box);
  renderCount();
}
function renderCount() {
  const el = $('rc-count'); if (!el || !ds()) return;
  const n = shownFields().filter(f => isActive(f, D().criteria[f.id])).length;
  el.className = n ? 'count' : ''; el.textContent = n ? `${n} applied` : '';
}
function blockEl(fid) { return document.querySelector(`#rc-filters .rc-f[data-fid="${fid}"]`); }
function renderBlock(fid) {
  const el = blockEl(fid);
  if (!el) return renderPanel();
  destroySelects(el);
  el.outerHTML = block(FIELD[fid]);
  initSelects(blockEl(fid));
}
function refreshTools(fid) { const el = blockEl(fid); if (el) el.querySelector('.rc-f-tools').innerHTML = blockTools(FIELD[fid]); }
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
    if (!hasS2()) { sel.size = 6; fillNative(sel, f); return; }
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
function onMultiChange(sel, f) {
  const vals = [...sel.selectedOptions].map(o => { try { return JSON.parse(o.value); } catch (e) { return o.value; } });
  const c = crit(f.id);
  if (JSON.stringify(vals) === JSON.stringify(c.values || [])) return;
  c.values = vals;
  changedSoft(f.id);
}
const crit = id => (D().criteria[id] = D().criteria[id] || {});
function changedSoft(fid) {
  markDirty(); if (fid) refreshTools(fid); renderCount(); renderPills(); scheduleCompile();
}
// Scroll the panel or the main area, never the page: the page is a fixed-height shell, and
// scrollIntoView would also scroll it (hiding the header).
function scrollWithin(el) {
  const box = el && el.closest('aside, main'); if (!box) return;
  const top = el.getBoundingClientRect().top - box.getBoundingClientRect().top;
  box.scrollTo({top: box.scrollTop + top - box.clientHeight / 3, behavior: 'smooth'});
}
function gotoFilter(fid, openIt) {
  const el = blockEl(fid); if (!el) return;
  scrollWithin(el);
  el.classList.remove('flash'); void el.offsetWidth; el.classList.add('flash');
  const sel = el.querySelector('select.rc-s2');
  if (openIt && sel && hasS2()) setTimeout(() => jq(sel).select2('open'), 250);
  else { const inp = el.querySelector('input:not([type=checkbox]), select, button.rc-chip, .rc-seg button'); if (inp) inp.focus({preventScroll: true}); }
}
function unset(fid) {
  delete D().criteria[fid]; D().ask = D().ask.filter(x => x !== fid);
  renderBlock(fid); changedSoft(null);
}

$('rc-filters').addEventListener('click', e => {
  const b = e.target.closest('[data-act]'); if (!b) return;
  const act = b.dataset.act, fid = b.dataset.f;
  if (act === 'open-sql') return openDrawer();
  if (act === 'add-filter') return openAddFilter();
  if (act === 'hint') return explain(FIELD[fid].label, FIELD[fid].hint);
  if (act === 'clear') return unset(fid);
  if (act === 'remove') {
    delete D().criteria[fid]; D().ask = D().ask.filter(x => x !== fid); extra = extra.filter(x => x !== fid);
    markDirty(); renderPanel(); renderPills(); scheduleCompile(); return;
  }
  if (applyClick(D().criteria, b)) { renderBlock(fid); changedSoft(fid); }
});
$('rc-filters').addEventListener('change', e => {
  const t = e.target;
  if (t.dataset.ask) {
    D().ask = t.checked ? [...new Set([...D().ask, t.dataset.ask])] : D().ask.filter(x => x !== t.dataset.ask);
    markDirty(); renderPills(); return;
  }
  if (applyInput(D().criteria, t)) changedSoft(t.dataset.f);
});
$('rc-filters').addEventListener('input', e => {
  const t = e.target;
  if (t.tagName === 'INPUT' && t.type !== 'checkbox' && applyInput(D().criteria, t)) changedSoft(t.dataset.f);
});

// ── Add a filter: every other field of the dataset, by group ─────────────────
function openAddFilter() {
  if (!ds()) return;
  const shown = new Set(shownFields().map(f => f.id));
  const draw = q => {
    const html = CAT.tabs.map(t => {
      const items = CAT.fields.filter(f => f.tab === t.id && inDataset(f)
        && (!q || (f.label + ' ' + (f.hint || '')).toLowerCase().includes(q)));
      if (!items.length) return '';
      return `<div class="addf-group"><h3>${esc(t.label)}</h3>${TAB_NOTES[t.id] ? `<span class="src">${esc(TAB_NOTES[t.id])}</span>` : ''}<div class="addf-list">${
        items.map(f => `<button type="button" class="addf-item" data-addf="${f.id}"${shown.has(f.id) ? ' disabled' : ''}>
          <span class="t">${esc(f.label)}<span class="ty">${TYPE_WORDS[f.type] || f.type}</span>${shown.has(f.id) ? '<span class="ty">in the panel</span>' : ''}</span>
          ${f.hint ? `<span class="h">${esc(f.hint)}</span>` : ''}</button>`).join('')}</div></div>`;
    }).join('');
    $('addf-list').innerHTML = html || '<div class="empty">No filter matches.</div>';
  };
  $('modal').innerHTML = `<div class="modal-back" id="addf-back"><div class="modal wide addf" role="dialog" aria-modal="true" aria-label="Add a filter">
    <div class="panel-head"><h2>Add a filter · ${esc(ds().label)}</h2><button class="btn small" id="addf-close" type="button" style="margin-left:auto">Close</button></div>
    <div class="panel-body"><input class="input" id="addf-q" type="search" placeholder="Find a filter: radiologist, protocol, turnaround, age…" aria-label="Find a filter">
      <div id="addf-list" class="flex flex-col gap-4"></div></div></div></div>`;
  draw('');
  const close = () => { closeModal(); document.removeEventListener('keydown', onKey); };
  const onKey = e => { if (e.key === 'Escape') close(); };
  document.addEventListener('keydown', onKey);
  $('addf-q').addEventListener('input', e => draw(e.target.value.trim().toLowerCase()));
  $('addf-close').onclick = close;
  $('addf-back').addEventListener('click', e => {
    if (e.target.id === 'addf-back') return close();
    const b = e.target.closest('[data-addf]'); if (!b || b.disabled) return;
    const fid = b.dataset.addf;
    if (!extra.includes(fid)) extra.push(fid);
    close(); renderPanel(); gotoFilter(fid, true);
  });
  $('addf-q').focus();
}
function closeModal() { $('modal').innerHTML = ''; }

// ── What is applied: tags above the result ───────────────────────────────────
function renderPills() {
  if (!S) return;
  if (handMode() || !ds()) {
    $('rc-pills').innerHTML = `<span class="rc-hint">${handMode() ? 'Written in SQL: the filters are not used.' : 'Choose what to count on the left.'}</span>`;
    return;
  }
  const pills = shownFields().filter(f => isActive(f, D().criteria[f.id]) || D().ask.includes(f.id)).map(f => {
    const c = D().criteria[f.id] || {}, ask = D().ask.includes(f.id), val = isActive(f, c) ? summary(f, c) : 'any';
    return `<span class="rc-pill${ask ? ' ask' : ''}" role="button" tabindex="0" data-goto="${f.id}" title="${esc(f.label + ': ' + val + (ask ? ' (asked when run)' : ''))}">
      <b>${esc(f.label)}</b><span class="v">${esc(val)}</span>${ask ? '<span class="crit-ask">asked when run</span>' : ''}
      <button type="button" class="x" data-unset="${f.id}" aria-label="Remove the ${esc(f.label)} filter"><i class="bi bi-x"></i></button></span>`;
  });
  $('rc-pills').innerHTML = pills.join('') || '<span class="rc-hint">None yet: the report covers all the data. A date range on the left keeps it fast.</span>';
}
$('rc-pills').addEventListener('click', e => {
  const x = e.target.closest('[data-unset]'); if (x) { e.stopPropagation(); return unset(x.dataset.unset); }
  const p = e.target.closest('[data-goto]'); if (p) gotoFilter(p.dataset.goto, true);
});
$('rc-pills').addEventListener('keydown', e => {
  const p = e.target.closest('[data-goto]');
  if (p && (e.key === 'Enter' || e.key === ' ') && !e.target.closest('[data-unset]')) { e.preventDefault(); gotoFilter(p.dataset.goto, true); }
});

// ── Show as ──────────────────────────────────────────────────────────────────
function renderOutput() {
  if (handMode()) { $('out').innerHTML = '<div class="rc-placeholder wide">Written in SQL: the result is shown as a list, exactly as the query returns it.</div>'; return; }
  const o = D().output, d = ds();
  const kind = `<div id="fw-kind"${o.kind ? '' : ' class="missing"'}><p class="lbl">Show as</p><div class="rc-seg" role="group" aria-label="Show as">${
    [['list', 'List', 'bi-list-ul'], ['table', 'Table', 'bi-table'], ['chart', 'Chart', 'bi-bar-chart']].map(([k, l, i]) =>
      `<button type="button" data-kind="${k}" aria-pressed="${o.kind === k}"><i class="bi ${i}" aria-hidden="true"></i> ${l}</button>`).join('')}</div></div>`;
  if (!d || !o.kind) {
    $('out').innerHTML = kind + `<div class="rc-placeholder" style="grid-column: span 2">${!d ? 'Choose what to count on the left, then how to show it.'
      : 'List: one row per ' + esc(d.count_label.toLowerCase().replace(/s$/, '')) + ', with the columns you pick. Table: counts and measures grouped by what you choose. Chart: the table as bars.'}</div>`;
    return;
  }
  if (o.rows && !d.dimensions.some(x => x.id === o.rows)) o.rows = null;
  if (o.cols && !d.dimensions.some(x => x.id === o.cols)) o.cols = null;
  o.measures = (o.measures || []).filter(m => d.measures.some(x => x.id === m));
  if (!o.measures.length) o.measures = [d.default_measure];
  o.columns = (o.columns || []).filter(c => d.columns.some(x => x.id === c));
  if (!o.columns.length) o.columns = d.list_default.slice();
  if (!d.columns.some(x => x.id === o.sort)) o.sort = d.default_date;
  const toggle = (attr, id, label, on) => `<button type="button" class="rc-chip" data-${attr}="${id}" aria-pressed="${on}">${esc(label)}</button>`;
  const dimOpts = sel => '<option value="">(nothing)</option>' + d.dimensions.map(x => `<option value="${x.id}"${x.id === sel ? ' selected' : ''}>${esc(x.label)}</option>`).join('');
  if (o.kind === 'list') {
    $('out').innerHTML = kind
      + `<div><label class="lbl" for="o-sort">Sort by</label><select class="rc-select" id="o-sort">${d.columns.map(c => `<option value="${c.id}"${c.id === o.sort ? ' selected' : ''}>${esc(c.label)}</option>`).join('')}</select></div>
         <div><label class="lbl" for="o-dir">Order</label><select class="rc-select" id="o-dir"><option value="desc"${o.desc !== false ? ' selected' : ''}>Newest / largest first</option><option value="asc"${o.desc === false ? ' selected' : ''}>Oldest / smallest first</option></select></div>
         <div class="wide"><p class="lbl">Columns <span class="rc-hint" style="text-transform:none;letter-spacing:0">${o.columns.length} of ${d.columns.length}</span>
           <button type="button" class="rc-link" data-cols="all">All</button><button type="button" class="rc-link" data-cols="default">Usual</button></p>
           <div class="rc-chips">${d.columns.map(c => toggle('col', c.id, c.label, o.columns.includes(c.id))).join('')}</div></div>`;
  } else {
    $('out').innerHTML = kind
      + `<div><label class="lbl" for="o-rows">${o.kind === 'chart' ? 'Bars by' : 'Rows by'}</label><select class="rc-select" id="o-rows">${dimOpts(o.rows)}</select></div>
         <div><label class="lbl" for="o-colsplit">${o.kind === 'chart' ? 'Stacked by' : 'Columns by'}</label><select class="rc-select" id="o-colsplit">${dimOpts(o.cols)}</select></div>
         <div class="wide"><p class="lbl">Measures${o.cols ? ' <span class="rc-hint" style="text-transform:none;letter-spacing:0">split by columns shows the first one</span>' : ''}</p>
           <div class="rc-chips">${d.measures.map(m => toggle('m', m.id, m.label, o.measures.includes(m.id))).join('')}</div></div>`;
  }
}
$('out').addEventListener('click', e => {
  const o = D().output, d = ds();
  const k = e.target.closest('[data-kind]');
  if (k) { o.kind = k.dataset.kind; markDirty(); renderOutput(); renderStatus(); scheduleCompile(0); return; }
  const col = e.target.closest('[data-col]'), m = e.target.closest('[data-m]'), all = e.target.closest('[data-cols]');
  if (col) {
    const id = col.dataset.col, order = d.columns.map(c => c.id);
    if (o.columns.includes(id) && o.columns.length === 1) return toast('A list needs at least one column.');
    o.columns = (o.columns.includes(id) ? o.columns.filter(x => x !== id) : [...o.columns, id]).sort((a, b) => order.indexOf(a) - order.indexOf(b));
  } else if (m) {
    const id = m.dataset.m;
    if (o.measures.includes(id) && o.measures.length === 1) return toast('A table needs at least one measure.');
    o.measures = o.measures.includes(id) ? o.measures.filter(x => x !== id) : [...o.measures, id];
  } else if (all) {
    o.columns = all.dataset.cols === 'all' ? d.columns.map(c => c.id) : d.list_default.slice();
  } else return;
  markDirty(); renderOutput(); scheduleCompile(0);
});
$('out').addEventListener('change', e => {
  const o = D().output, t = e.target;
  if (t.id === 'o-rows') o.rows = t.value || null;
  else if (t.id === 'o-colsplit') o.cols = t.value || null;
  else if (t.id === 'o-sort') o.sort = t.value;
  else if (t.id === 'o-dir') o.desc = t.value !== 'asc';
  else return;
  markDirty(); renderOutput(); scheduleCompile(0);
});

// ── SQL: in a drawer, on a click ─────────────────────────────────────────────
function openDrawer() { $('sql-drawer').hidden = false; renderSQL(); }
function closeDrawer() { $('sql-drawer').hidden = true; }
$('btn-sql').onclick = openDrawer;
$('sql-close').onclick = closeDrawer;
$('sql-back').onclick = closeDrawer;
$('btn-sql-run').onclick = () => { closeDrawer(); startRun(); };
document.addEventListener('keydown', e => {
  if (e.key === 'Escape' && !$('sql-drawer').hidden && !$('modal').innerHTML && !document.querySelector('.swal2-container')) closeDrawer();
});
function scheduleCompile(ms = 250) { clearTimeout(compileTimer); compileTimer = setTimeout(compile, ms); }
async function compile() {
  if (handMode()) return renderSQL();
  if (!D().dataset || !(D().output || {}).kind) { sqlInfo = {pending: true}; return renderSQL(); }
  try { sqlInfo = await api('/worklists/api/compile', {definition: D()}); }
  catch (e) { sqlInfo = {error: e.message}; }
  renderSQL();
}
function highlight(sql) {
  return esc(sql)
    .replace(/(&#39;[^&]*?&#39;)/g, '<span class="s">$1</span>')
    .replace(/(:[a-z_]+)/g, '<span class="p">$1</span>')
    .replace(/\b(SELECT|FROM|WHERE|AND|OR|NOT|GROUP BY|ORDER BY|LIMIT|AS|COUNT|SUM|AVG|DISTINCT|FILTER|WITHIN GROUP|ANY|ILIKE|ESCAPE|IS|NULL|TRUE|DESC|ASC|NULLS LAST|COALESCE|CAST|LEFT JOIN|JOIN|ON|lower|round|percentile_cont|to_char|date_trunc|lpad)\b/g, '<span class="k">$1</span>');
}
// Two separate things: the SQL built from the filters, only ever SHOWN (read-only, for
// everyone), and SQL written by hand (settings capability), documented by the SQL reference.
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
  $('sql-caption').textContent = 'Read-only: built from the filters and Show as. Values are bound as parameters, never pasted into the SQL.';
  $('sql-note').innerHTML = '';
  if (!sqlInfo || sqlInfo.pending) { $('sql-view').innerHTML = '<span class="hint">The SQL appears here once “What to count” and “Show as” are chosen.</span>'; $('params').innerHTML = ''; return; }
  if (sqlInfo.error) { $('sql-note').innerHTML = `<div class="rc-note bad">${esc(sqlInfo.error)}</div>`; $('sql-view').innerHTML = ''; $('params').innerHTML = ''; return; }
  $('sql-view').innerHTML = highlight(sqlInfo.sql);
  const p = Object.entries(sqlInfo.params || {});
  $('params').innerHTML = p.length ? p.map(([k, v]) => `<div><span class="pk">:${esc(k)}</span> <span class="pv">= ${esc(Array.isArray(v) ? JSON.stringify(v) : v)}</span></div>`).join('') : '<div class="pv">No parameters.</div>';
}
function renderHandNote() {
  const hand = handMode();
  $('rc-handnote').hidden = !hand;
  $('rc-handnote').innerHTML = hand ? '<div class="rc-note info"><i class="bi bi-code-slash" aria-hidden="true"></i> This report is written in SQL: the filters and “Show as” are not used, the result is the query’s own. <button type="button" class="rc-link" data-act="open-sql">Open the SQL</button></div>' : '';
  $('rc-applied').hidden = hand; $('rc-show').hidden = hand;
}
$('rc-handnote').addEventListener('click', e => { if (e.target.closest('[data-act="open-sql"]')) openDrawer(); });
$('btn-copy-sql').onclick = () => {
  const t = D().sql != null ? D().sql : (sqlInfo && sqlInfo.sql) || '';
  if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(t).then(() => toast('SQL copied.'), () => toast('Copy was blocked; select the SQL and copy it by hand.'));
};
async function setMode(hand, sql) {
  if (hand === handMode() && sql === undefined) return;
  if (!hand) {
    if (String(D().sql || '').trim() && !(await askYesNo('Go back to the filters?', 'The SQL you wrote is dropped; the report is built from its filters again.', {yes: 'Use the filters'}))) return;
    D().sql = null;
  } else {
    D().sql = sql !== undefined ? sql : ((sqlInfo && sqlInfo.editable_sql) || '');
  }
  markDirty(); renderAll();
  // renderSQL leaves a focused editor alone (typing is never overwritten); text put in from
  // outside (the generated SQL, an example) is written here.
  if (hand) { $('sql-edit').value = D().sql; $('sql-edit').focus(); } else scheduleCompile(0);
}
$('sql-seg').addEventListener('click', e => { const b = e.target.closest('[data-mode]'); if (b) setMode(b.dataset.mode === 'hand'); });
$('btn-start-gen').onclick = async () => {
  if (!sqlInfo || !sqlInfo.editable_sql) return toast('There is no SQL from the filters yet: switch to “Built from the filters” and choose what to count first.');
  if (String(D().sql || '').trim() && !(await askYesNo('Replace your SQL?', 'The editor gets the SQL built from the filters instead.', {yes: 'Replace'}))) return;
  D().sql = sqlInfo.editable_sql; markDirty(); renderSQL(); $('sql-edit').value = D().sql; $('sql-edit').focus();
};
$('sql-edit').addEventListener('input', e => { D().sql = e.target.value; markDirty(); });

// ── SQL reference: every view, column and rule SQL written by hand may use ───
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
  const close = () => { closeModal(); document.removeEventListener('keydown', onKey); };
  const onKey = e => { if (e.key === 'Escape') { e.stopImmediatePropagation(); close(); } };
  document.addEventListener('keydown', onKey, true);
  draw();
}
$('btn-ref').onclick = openReference;

// ── Generate and the result ──────────────────────────────────────────────────
function resetResults() {
  if (chart) { chart.dispose(); chart = null; }
  $('rc-kpis').hidden = true; $('res-meta').textContent = ''; $('res-title').textContent = 'Result';
  $('results').innerHTML = '<div class="rc-empty">Choose what to count and the filters on the left, then press <strong>Generate</strong>.</div>';
}
async function run(defn) {
  if (chart) { chart.dispose(); chart = null; }
  $('results').innerHTML = '<div class="rc-empty"><i class="bi bi-hourglass-split" aria-hidden="true"></i> Running…</div>'; $('res-meta').textContent = '';
  try { lastResult = await api('/worklists/api/run', {definition: defn}); }
  catch (e) { lastResult = null; $('rc-kpis').hidden = true; $('results').innerHTML = `<div class="rc-note bad">${esc(e.message)}</div>`; return; }
  renderResults();
}
async function startRun() {
  const blockers = runBlockers();
  if (blockers.length) { toast('Before it can run: ' + blockers.map(m => m[1]).join(', ') + '.'); focusMissing(blockers[0][0]); return; }
  const asked = D().ask.filter(id => FIELD[id] && inDataset(FIELD[id]));
  if (!asked.length || handMode()) return run(D());
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
const fmt = v => v == null ? '' : (typeof v === 'number' ? (Number.isInteger(v) ? v.toLocaleString() : v.toLocaleString(undefined, {maximumFractionDigits: 2})) : String(v));
function renderResults() {
  const r = lastResult; if (!r) return;
  $('res-title').textContent = S.name || 'Result';
  $('res-meta').textContent = r.truncated ? `the first ${r.cap.toLocaleString()} rows: Download CSV holds up to 100,000` : '';
  $('rc-kpis').hidden = false;
  $('kpi-rows').textContent = r.rows.length.toLocaleString();
  $('kpi-rows-sub').textContent = r.truncated ? `capped at ${r.cap.toLocaleString()}` : (r.kind === 'list' ? 'one per line of the list' : 'groups in the table');
  $('kpi-time').textContent = r.elapsed_ms < 1000 ? `${r.elapsed_ms} ms` : `${(r.elapsed_ms / 1000).toFixed(1)} s`;
  if (chart) { chart.dispose(); chart = null; }
  if (!r.rows.length) { $('results').innerHTML = '<div class="rc-empty">No rows match these filters.</div>'; return; }
  if (r.kind === 'list') {
    const labels = r.column_labels ? r.column_labels.map(c => c.label) : r.columns;
    $('results').innerHTML = `<div class="rc-table-wrap"><table class="rc-table"><thead><tr>${labels.map(l => `<th class="txt">${esc(l)}</th>`).join('')}</tr></thead><tbody>${
      r.rows.map(row => `<tr>${row.map(v => `<td class="${typeof v === 'number' ? '' : 'txt'}">${esc(fmt(v))}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
    return;
  }
  const m = matrix(r);
  if (r.kind === 'table') return renderTable(r, m);
  renderChart(r, m);
}
function matrix(r) {
  const nd = r.dims.length, meas = r.measures;
  if (nd === 2) {
    const rows = [...new Set(r.rows.map(x => x[0]))], cols = [...new Set(r.rows.map(x => x[1]))].sort();
    const cell = {}; r.rows.forEach(x => cell[x[0] + '\u0000' + x[1]] = x[2]);
    return {rows, series: cols.map(c => ({label: c, additive: meas[0].additive})), data: rows.map(a => cols.map(b => cell[a + '\u0000' + b] ?? 0)), rowLabel: r.dims[0].label, measureLabel: meas[0].label};
  }
  const rows = nd ? r.rows.map(x => x[0]) : ['All'];
  return {rows, series: meas.map(x => ({label: x.label, additive: x.additive})), data: r.rows.map(x => x.slice(nd)), rowLabel: nd ? r.dims[0].label : '', measureLabel: ''};
}
function renderTable(r, m) {
  const split = r.dims.length === 2, allAdd = m.series.every(s => s.additive);
  const head = `<tr><th class="txt">${esc(m.rowLabel)}${split ? ` · ${esc(m.measureLabel)}` : ''}</th>${m.series.map(s => `<th>${esc(s.label)}</th>`).join('')}${split && allAdd ? '<th>Total</th>' : ''}</tr>`;
  const body = m.rows.map((row, i) => `<tr><td class="txt">${esc(row)}</td>${m.data[i].map(v => `<td>${fmt(v)}</td>`).join('')}${split && allAdd ? `<td>${fmt(m.data[i].reduce((a, b) => a + (+b || 0), 0))}</td>` : ''}</tr>`).join('');
  const total = m.rows.length > 1 ? `<tr class="total"><td class="txt">Total</td>${m.series.map((s, j) => `<td>${s.additive ? fmt(m.data.reduce((a, row) => a + (+row[j] || 0), 0)) : ''}</td>`).join('')}${split && allAdd ? `<td>${fmt(m.data.flat().reduce((a, b) => a + (+b || 0), 0))}</td>` : ''}</tr>` : '';
  $('results').innerHTML = `<div class="rc-table-wrap"><table class="rc-table"><thead>${head}</thead><tbody>${body}${total}</tbody></table></div>`;
}
function renderChart(r, m) {
  $('results').innerHTML = '<div class="rc-chart" id="chart"></div>';
  if (!window.echarts) { $('results').innerHTML = '<div class="rc-note bad">The chart library could not be loaded. Show it as a Table.</div>'; return; }
  const cs = getComputedStyle(document.body);
  const fg = cs.getPropertyValue('--rc-muted').trim(), line = cs.getPropertyValue('--rc-line').trim();
  chart = echarts.init($('chart'));
  chart.setOption({
    color: ['#60a5fa', '#34d399', '#fbbf24', '#f472b6', '#a78bfa', '#fb923c', '#22d3ee', '#f87171'],
    tooltip: {trigger: 'axis'}, legend: {textStyle: {color: fg}, top: 0, type: 'scroll'},
    grid: {left: 50, right: 16, top: 36, bottom: 60, containLabel: true},
    xAxis: {type: 'category', data: m.rows, axisLabel: {color: fg, rotate: m.rows.length > 8 ? 35 : 0}, axisLine: {lineStyle: {color: line}}, name: m.rowLabel, nameLocation: 'middle', nameGap: 44, nameTextStyle: {color: fg}},
    yAxis: {type: 'value', axisLabel: {color: fg}, splitLine: {lineStyle: {color: line}}},
    series: m.series.map((s, j) => ({name: s.label, type: 'bar', data: m.data.map(row => row[j]), stack: r.dims.length === 2 ? 'split' : undefined})),
  });
}
window.addEventListener('resize', () => chart && chart.resize());
window.addEventListener('rc-theme', () => { if (chart && lastResult) renderResults(); });

$('btn-run').onclick = startRun;
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
  renderLibrary(); renderHeader(); renderPanel(); renderPills(); renderOutput(); renderHandNote(); renderSQL(); renderStatus();
}
$('btn-new').onclick = async () => {
  if (dirty && !(await askYesNo('Discard your changes?', 'This report has changes that are not saved.', {yes: 'Discard changes'}))) return;
  blank(); toast('New report: name it, choose what to count and how to show it.'); $('f-name').focus();
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
  if (e.key === 'Enter' && !e.target.closest('textarea')) { e.preventDefault(); startRun(); }
});
$('f-history').onchange = async e => {
  const v = e.target.value; if (!v || !S.id) return;
  try {
    const old = await api(`/worklists/api/${S.id}/versions/${v}`);
    S.definition = old.definition; S.definition.ask = S.definition.ask || []; S.definition.criteria = S.definition.criteria || {}; S.name = old.name;
    extra = initialExtra(); markDirty(); renderAll(); scheduleCompile(0); toast(`Loaded version ${v}. Save to make it current.`);
  } catch (err) { toast(err.message); }
  e.target.value = '';
};
$('f-name').addEventListener('input', e => { S.name = e.target.value; markDirty(); });
$('f-desc').addEventListener('input', e => { S.description = e.target.value; markDirty(); });
$('f-folder').addEventListener('input', e => { S.folder = e.target.value; markDirty(); });
$('f-vis').addEventListener('click', e => { const b = e.target.closest('button'); if (!b || b.disabled) return; S.visibility = b.dataset.v; markDirty(); renderHeader(); });
$('f-ds').addEventListener('change', e => {
  if (!e.target.value) return;
  const old = D(), keep = extra;
  S.definition = blankDefinition(e.target.value, (old.output || {}).kind || null);
  Object.entries(old.criteria || {}).forEach(([k, v]) => { if (FIELD[k] && FIELD[k].datasets.includes(e.target.value)) S.definition.criteria[k] = v; });
  S.definition.ask = (old.ask || []).filter(k => FIELD[k] && FIELD[k].datasets.includes(e.target.value));
  S.definition.sql = old.sql;
  extra = [...new Set([...keep.filter(k => inDataset(FIELD[k])), ...initialExtra()])];
  markDirty(); renderAll(); scheduleCompile(0);
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
  // Your library, plus what the schedule already holds (another admin's personal report).
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
  try { CAT = await api('/worklists/api/catalog'); } catch (e) { $('results').innerHTML = `<div class="rc-note bad">${esc(e.message)}</div>`; return; }
  CAT.fields.forEach(f => FIELD[f.id] = f);
  $('btn-sched').hidden = !CAT.me.can_schedule;
  await loadLibrary();
  const wanted = +(new URLSearchParams(location.search).get('id') || 0);
  const first = wanted || (LIB[0] && LIB[0].id);
  if (first) { if (!(await open(first))) blank(); } else blank();
})();
})();
