/*
 * static/js/report_composer.js — the Report Composer page (templates/worklists.html,
 * routes/worklists.py), written as a sentence since 2026-11-21 (operator: the pivot builder
 * was "still not user friendly": its words were a spreadsheet's — rows, columns, values,
 * measures — and a new report did not say where to start; "even myself i was facing hard
 * time to understand it").
 *
 *   start      "What do you want to know?": one tile per kind of data (exams, studies,
 *              reports ...) with a question or two to start from, then the saved reports
 *   sentence   the report read as one sentence; each blue word is clicked to change it:
 *                Show [number of exams] [+ another number]
 *                for each [Room] and [Month] [+ add]
 *                during [last month], [after hours only], using the [Scheduled for] date
 *                where [Patient type is Emergency] [+ add a condition]
 *              A list reads "List every [exam] with columns [...]".
 *   answer     right below, updated as the sentence changes; shown as a table, bars,
 *              stacked bars, a line, a pie, a heatmap or the list itself; a header click
 *              sorts, any number opens the rows behind it (/worklists/api/drill)
 *   More (...) save a copy, name / folder / sharing, earlier versions, scheduled emails,
 *              the SQL (a drawer; hand-written SQL for the settings capability), delete
 *
 * The definition saved is the one the email and CSV read: {dataset, criteria, ask, output:
 * {kind: list|table|chart, chart, rows: [grouping, ...], cols, measures, columns, sort,
 * desc}, sql}. "for each" is rows (+ cols when the last grouping is spread across the top: a
 * chart's colours, a heatmap's columns, a table when asked). The period is the date
 * criterion of the date used; its days and times are that criterion's windows.
 */
(function () {
'use strict';
const $ = id => document.getElementById(id);
const jq = window.jQuery;
const hasS2 = () => !!(jq && jq.fn && jq.fn.select2);
const clone = o => JSON.parse(JSON.stringify(o));
const esc = s => String(s ?? '').replace(/[&<>"']/g, ch => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'})[ch]);
const enc = encodeURIComponent;
const cap = s => String(s).charAt(0).toUpperCase() + String(s).slice(1);
const BLANK = '(blank)';
const MAX_GROUPS = 4;
const BUCKETS = ['day', 'week', 'month', 'quarter', 'year'];
const MAIL_SERVER_URL = $('rc').dataset.mailUrl;
const VIEWS = [['table', 'Table', 'bi-table'], ['bar', 'Bars', 'bi-bar-chart'], ['stacked', 'Stacked bars', 'bi-bar-chart-steps'],
               ['line', 'Line', 'bi-graph-up'], ['pie', 'Pie', 'bi-pie-chart'], ['heatmap', 'Heatmap', 'bi-grid-3x3'],
               ['list', 'List', 'bi-list-ul']];
const SPLIT_VIEWS = new Set(['bar', 'stacked', 'line', 'heatmap']);

// What each kind of data is called, in the words of the people who read the reports.
const DS_ORDER = ['exams', 'studies', 'reports', 'crn', 'findings', 'series', 'storage'];
const DS_WORDS = {
  exams:    {icon: 'bi-calendar-check', title: 'Exams on the RIS', noun: 'exam', nouns: 'exams',
             about: 'Every exam booked on the RIS: booked, arrived, scanned, done or cancelled; room, technologist, priority, waits.'},
  studies:  {icon: 'bi-images', title: 'Studies in PACS', noun: 'study', nouns: 'studies',
             about: 'Every study stored in PACS: device, procedure, radiologists, report stage and turnaround.'},
  reports:  {icon: 'bi-file-earmark-text', title: 'Radiology reports', noun: 'report', nouns: 'reports',
             about: 'Every report received: radiologist, turnaround, critical findings, CRN.'},
  crn:      {icon: 'bi-bell', title: 'Critical result notifications', noun: 'notification', nouns: 'notifications',
             about: 'Each critical result notification: who was told, when, and how it was acknowledged.'},
  findings: {icon: 'bi-lightbulb', title: 'Findings in reports', noun: 'finding', nouns: 'findings',
             about: 'Each finding the report reader (NLP) found in a report, negations and history removed.'},
  series:   {icon: 'bi-stack', title: 'Series in PACS', noun: 'series', nouns: 'series',
             about: 'Each image series: protocol, body part, station, manufacturer.'},
  storage:  {icon: 'bi-hdd', title: 'PACS storage', noun: 'storage day', nouns: 'storage days',
             about: 'Gigabytes and studies stored per day, device and procedure.'},
};
// A number, as the sentence says it after "Show".
const PHRASE = {
  count: k => 'Number of ' + dsWords(k).nouns, patients: 'Number of patients', studies: 'Number of studies',
  reports: 'Number of reports', images: 'Number of images',
  performed: 'Exams performed', not_performed: 'Exams not performed (not cancelled)', cancelled: 'Exams cancelled',
  approved: 'Exams approved on the RIS', median_wait: 'Median wait, arrived → scan (min)',
  median_exam: 'Median scan duration (min)', median_arrival_to_done: 'Median time, arrived → exam done (min)',
  median_signed_tat: 'Median time, exam done → signed (h)', median_report_tat: 'Median report turnaround (h)',
  avg_report_tat: 'Average report turnaround (h)', median_prelim_tat: 'Median preliminary turnaround (h)',
  median_store_delay: 'Median delay, study → PACS (min)', median_start_delay: 'Median delay, scheduled → started (min)',
  avg_age: 'Average patient age', reported: 'Studies with a report', not_reported: 'Studies without a report',
  final: 'Studies with a final signature', addenda: 'Studies with an addendum',
  critical: k => (k === 'reports' ? 'Reports' : 'Studies') + ' with a critical finding (NLP)',
  crn: k => (k === 'reports' ? 'Reports' : 'Studies') + ' with a CRN sent',
  gb: 'Storage used (GB)', stored_studies: 'Studies stored', acknowledged: 'Notifications acknowledged',
  overdue: 'Notifications that went overdue', median_ack_minutes: 'Median time to acknowledge (min)',
};
// "for each": the groupings by family.
const DIM_GROUP = {
  weekday: 'when', hour: 'when', shift: 'when', week_of_month: 'when',
  modality: 'where', room: 'where', device: 'where', site: 'where', pacs_modality: 'where', study_modality: 'where',
  station: 'where', institution: 'where', department: 'where', manufacturer: 'where',
  technologist: 'who', signing_radiologist: 'who', reading_radiologist: 'who', performing_physician: 'who',
  referring: 'who', signed_by: 'who', approved_by: 'who', radiologist: 'who', doctor: 'who',
  patient_type: 'patient', sex: 'patient', age_band: 'patient', patient_class: 'patient', patient_location: 'patient',
};
const GROUP_ORDER = ['when', 'where', 'who', 'patient', 'what'];
const GROUP_LABEL = {when: 'Time', where: 'Place and device', who: 'People', patient: 'Patient', what: 'Exam, report and status'};
// Days and times, one click each. After hours is the customer's (2026-11-20): Monday to Friday
// 19:00 → 07:00, Saturday 13:00 → Sunday 07:00, Sunday 07:00 → Monday 07:00.
const AT_PRESETS = [
  {id: 'weekdays', label: 'Weekdays only', about: 'Monday to Friday, all day.', wins: [{days: [1, 2, 3, 4, 5], from: '', to: ''}]},
  {id: 'weekend', label: 'Weekends only', about: 'Saturday and Sunday, all day.', wins: [{days: [6, 7], from: '', to: ''}]},
  {id: 'after_hours', times: true, label: 'After hours only', about: 'Mon–Fri 19:00 → 07:00 next day, and Saturday 13:00 → Monday 07:00.',
   wins: [{days: [1, 2, 3, 4, 5], from: '19:00', to: '07:00'}, {days: [6], from: '13:00', to: '07:00'}, {days: [7], from: '07:00', to: '07:00'}]},
  {id: 'day_hours', times: true, label: 'Day hours only', about: 'Mon–Fri 07:00 → 19:00, and Saturday 07:00 → 13:00 (the rest of after hours).',
   wins: [{days: [1, 2, 3, 4, 5], from: '07:00', to: '19:00'}, {days: [6], from: '07:00', to: '13:00'}]},
];
// The questions on the start tiles: a first answer in one click.
const STARTS = {
  exams: [['Exams per room, last month', {rows: ['room'], quick: 'last_month'}],
          ['Exams after hours, by modality', {rows: ['modality'], quick: 'last_month', windows: AT_PRESETS[2].wins}]],
  studies: [['Studies per modality, month by month', {rows: ['modality'], cols: 'study_at:month', quick: 'last_3_months'}],
            ['Report turnaround by radiologist', {rows: ['signing_radiologist'], measures: ['count', 'median_report_tat'], quick: 'last_month'}]],
  reports: [['Critical findings by radiologist', {rows: ['radiologist'], measures: ['count', 'critical'], quick: 'last_month'}]],
  crn: [['Time to acknowledge, by doctor', {rows: ['doctor'], measures: ['count', 'median_ack_minutes'], quick: 'last_3_months'}]],
  findings: [['The most frequent findings', {rows: ['finding'], quick: 'last_month'}]],
  series: [['Series per protocol', {rows: ['protocol'], quick: 'last_month'}]],
  storage: [['Storage per device, month by month', {rows: ['device'], cols: 'study_at:month', quick: 'last_3_months'}]],
};

let CAT, FIELD = {}, LIB = [], S = null, extra = [], sqlInfo = null, dirty = false, compileTimer = null,
    runTimer = null, runSeq = 0, chart = null, lastResult = null, model = null, pivotSort = null,
    WHEN = null, ACROSS = false;
let popKind = null, popFid = null, popAnchor = null, popMode = null, popCustom = false;
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
function markDirty(on = true) { dirty = on; if (S) renderStatus(); }
function isoToday() { const d = new Date(); return new Date(d - d.getTimezoneOffset() * 60000).toISOString().slice(0, 10); }
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
function fmtDay(s) { const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(s || ''); return m ? `${+m[3]} ${MONTHS[+m[2] - 1]} ${m[1]}` : String(s || ''); }

// ── State ────────────────────────────────────────────────────────────────────
const D = () => S.definition;
const ds = () => (S && CAT.datasets[D().dataset]) || null;
const inDataset = f => !!f && f.datasets.includes(D().dataset);
const handMode = () => D().sql != null;
const showValue = () => { const o = D().output || {}; return o.kind === 'chart' ? (o.chart || 'bar') : (o.kind || 'table'); };
const isList = () => showValue() === 'list';
function dsWords(k) {
  k = k || (S && D().dataset);
  const d = CAT.datasets[k];
  return DS_WORDS[k] || {icon: 'bi-table', title: d ? d.label : (k === 'sql' ? 'Written in SQL' : String(k || '')), noun: 'row', nouns: 'rows', about: d ? d.hint : ''};
}
function measurePhrase(id, k) {
  k = k || D().dataset;
  const p = PHRASE[id];
  if (typeof p === 'function') return p(k);
  if (p) return p;
  const m = (CAT.datasets[k] && CAT.datasets[k].measures.find(x => x.id === id)) || {};
  return m.label || id;
}
const dimField = id => FIELD[String(id).split(':')[0]];
const columnLabel = id => ((ds().columns.find(c => c.id === id) || {}).label || id);
// The dataset's dates, its usual one first.
function dateFields() {
  const d = ds(); if (!d) return [];
  const all = CAT.fields.filter(f => f.type === 'date' && inDataset(f));
  return all.filter(f => f.id === d.default_date).concat(all.filter(f => f.id !== d.default_date));
}
// The date the period is on: the first date with a period set, the usual one first.
function computeWhen() {
  const dates = dateFields(), on = dates.find(f => isActive(f, D().criteria[f.id]));
  return on ? on.id : (dates[0] ? dates[0].id : null);
}
// What a kind of data starts with: a count by modality (gigabytes for storage) over the last
// 30 days, or one of the start tiles' questions.
function blankDefinition(dataset, p = {}) {
  const d = CAT.datasets[dataset], dims = new Set(d.dimensions.map(x => x.id));
  const rows = (p.rows || ['modality']).filter(x => dims.has(x));
  const measures = (p.measures || [d.default_measure]).filter(m => d.measures.some(x => x.id === m));
  const criteria = {[d.default_date]: {mode: 'quick', quick: p.quick || 'last_30_days'}};
  if (p.windows) criteria[d.default_date].windows = clone(p.windows);
  return {dataset, criteria, ask: [], sql: null,
          output: {kind: p.kind || 'table', chart: p.chart || 'bar', rows, cols: p.cols && dims.has(p.cols) ? p.cols : null,
                   measures: measures.length ? measures : [d.default_measure], columns: d.list_default.slice(),
                   sort: d.default_date, desc: true}};
}
// Groupings, numbers and list columns the dataset offers (a saved report may name one its
// dataset no longer has); a report saved with one "rows" grouping reads as before.
function normalizeOutput() {
  const o = D().output = D().output || {}, d = ds();
  o.rows = Array.isArray(o.rows) ? o.rows : (o.rows ? [o.rows] : []);
  if (!o.kind) o.kind = 'table';
  if (!o.chart) o.chart = 'bar';
  if (!d) return;
  const dims = new Set(d.dimensions.map(x => x.id));
  o.rows = [...new Set(o.rows.filter(x => dims.has(x)))].slice(0, MAX_GROUPS);
  if (o.cols && (!dims.has(o.cols) || o.rows.includes(o.cols))) o.cols = null;
  o.measures = (o.measures || []).filter(m => d.measures.some(x => x.id === m));
  if (!o.measures.length) o.measures = [d.default_measure];
  o.columns = (o.columns || []).filter(c => d.columns.some(x => x.id === c));
  if (!o.columns.length) o.columns = d.list_default.slice();
  if (!d.columns.some(x => x.id === o.sort)) o.sort = d.default_date;
}
// "for each", in order: the row groupings, then the one spread across the top.
function groupings() { const o = D().output; return [...o.rows, ...(o.cols ? [o.cols] : [])]; }
// The last grouping goes across the top for a chart with colours, a heatmap, or a table when
// asked ("Put ... across the top"); a pie and a list never split.
function applyGroupings(list) {
  const o = D().output, v = showValue();
  list = [...new Set(list)].slice(0, MAX_GROUPS);
  const split = list.length >= 2 && (SPLIT_VIEWS.has(v) || (v === 'table' && ACROSS));
  o.rows = split ? list.slice(0, -1) : list;
  o.cols = split ? list[list.length - 1] : null;
}
function initialExtra() {
  // conditions shown even while empty: those asked when run
  return (D().ask || []).filter(id => FIELD[id] && inDataset(FIELD[id]) && id !== WHEN);
}
function load(w, {askFirst = false} = {}) {
  closePop();
  S = clone(w);
  S.definition.ask = S.definition.ask || [];
  S.definition.criteria = S.definition.criteria || {};
  normalizeOutput();
  WHEN = ds() ? computeWhen() : null;
  ACROSS = !!D().output.cols;
  extra = ds() ? initialExtra() : []; pivotSort = null; popCustom = false;
  markDirty(false); lastResult = null; resetResults();
  showView();
  renderAll(); scheduleCompile(0); syncUrl();
  if (askFirst && D().ask.length) runNow(true); else scheduleRun(0);
}
function startNew(dataset, preset) {
  load({id: null, name: '', description: '', folder: '', visibility: 'personal',
        can_edit: true, version: 0, versions: [], definition: blankDefinition(dataset, preset || {})});
}
function syncUrl() {
  try { history.replaceState(null, '', S && S.id ? '?id=' + S.id : location.pathname); } catch (e) { /* not needed */ }
}

// ── The two screens: start, and a report ─────────────────────────────────────
function showView() { $('rc-start').hidden = true; $('rc-view').hidden = false; }
function showStart() {
  closePop(); closeDrawer();
  S = null; dirty = false; lastResult = null; model = null;
  if (chart) { chart.dispose(); chart = null; }
  $('rc-view').hidden = true; $('rc-start').hidden = false;
  renderStart(); syncUrl();
}
async function goHome() {
  if (dirty && !(await askYesNo('Leave without saving?', 'This report has changes that are not saved.', {yes: 'Leave'}))) return;
  showStart();
}
$('btn-home').onclick = goHome;
function renderStart() {
  $('rc-tiles').innerHTML = DS_ORDER.filter(k => CAT.datasets[k]).map(k => {
    const w = dsWords(k);
    return `<div class="rc-tile"><button type="button" class="rc-tile-main" data-start="${k}" title="Start with ${esc(w.title.toLowerCase())}">
        <i class="bi ${w.icon}" aria-hidden="true"></i><b>${esc(w.title)}</b><span>${esc(w.about)}</span></button>
      <div class="rc-tile-ex">${(STARTS[k] || []).map((s, i) => `<button type="button" data-start="${k}" data-ex="${i}">${esc(s[0])}</button>`).join('')}</div></div>`;
  }).join('');
  renderSaved();
}
function renderSaved() {
  const q = $('rc-saved-q').value.trim().toLowerCase();
  const hit = LIB.filter(w => !q || `${w.name} ${w.description || ''} ${w.folder}`.toLowerCase().includes(q));
  const when = s => fmtDay(String(s || '').slice(0, 10));
  $('rc-saved').innerHTML = [['shared', 'Shared with everyone'], ['personal', 'Only me']].map(([v, l]) => {
    const ws = hit.filter(w => w.visibility === v);
    if (!ws.length) return '';
    return `<div class="rc-saved-group"><h3>${l}</h3><div class="rc-saved-list">${ws.map(w => `<button type="button" class="rc-saved-item" data-open="${w.id}">
        <b>${esc(w.name)}</b>${w.description ? `<span class="d">${esc(w.description)}</span>` : ''}
        <span class="m">${esc([w.folder, dsWords(w.dataset).title, 'saved ' + when(w.updated_at)].filter(Boolean).join(' · '))}</span></button>`).join('')}</div></div>`;
  }).join('') || `<p class="rc-hint">${LIB.length ? 'No saved report matches.' : 'No saved reports yet: start one above, then press Save.'}</p>`;
}
$('rc-saved-q').addEventListener('input', renderSaved);
$('rc-start').addEventListener('click', e => {
  const st = e.target.closest('[data-start]');
  if (st) { const ex = st.dataset.ex != null ? STARTS[st.dataset.start][+st.dataset.ex] : null; return startNew(st.dataset.start, ex && ex[1]); }
  const op = e.target.closest('[data-open]'); if (op) open(+op.dataset.open);
});

// ── Saved reports ────────────────────────────────────────────────────────────
async function loadLibrary() {
  LIB = await api('/worklists/api/list');
  $('folders').innerHTML = [...new Set(LIB.map(w => w.folder))].map(f => `<option value="${esc(f)}">`).join('');
}
async function open(id) {
  if (dirty && !(await askYesNo('Discard your changes?', 'This report has changes that are not saved.', {yes: 'Discard changes'}))) return false;
  try { load(await api('/worklists/api/' + id), {askFirst: true}); return true; } catch (e) { toast(e.message); return false; }
}

// ── Criteria: what each condition says (days and times included) ─────────────
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
const winKey = wins => JSON.stringify(wins.map(w => [[...new Set(w.days || [])].sort(), w.from || '', w.to || '']));
function presetOf(wins) {
  if (!wins.length) return 'any';
  const k = winKey(wins), p = AT_PRESETS.find(x => winKey(x.wins) === k);
  return p ? p.id : 'custom';
}
function atText(wins) {
  const p = AT_PRESETS.find(x => x.id === presetOf(wins));
  return p ? p.label.toLowerCase() : wins.map(windowText).join(' · ');
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
function periodText(c) {
  if (!c || !c.mode || c.mode === 'none') return 'any time';
  if (c.mode === 'quick') { const l = (CAT.quick_dates.find(q => q.id === c.quick) || {}).label || c.quick; return l.charAt(0).toLowerCase() + l.slice(1); }
  if (c.mode === 'relative') return `the ${c.dir === 'next' ? 'next' : 'previous'} ${c.n} ${c.unit}`;
  if (c.mode === 'fixed') return c.from === c.to ? fmtDay(c.from) : `${fmtDay(c.from)} → ${fmtDay(c.to)}`;
  return '';
}
function optionsNow(f) { return Array.isArray(f.options) ? f.options : (optionList[D().dataset + '|' + f.id] || null); }
function optLabel(f, v) {
  const hit = (optionsNow(f) || []).find(x => x.value === v);
  return hit ? hit.label : String(v);
}
// A condition in words (HTML): "Room is CT1 or MR2".
function condHtml(f, c) {
  const k = `<span class="k">${esc(f.label)}</span>`;
  if (!isActive(f, c)) return `${k} is anything`;
  if (f.type === 'date') return `${k} ${esc([c.mode && c.mode !== 'none' ? periodText(c) : '', ...windowsOf(c).map(windowText)].filter(Boolean).join(', '))}`;
  if (f.type === 'multi') {
    const names = c.values.map(v => optLabel(f, v));
    return `${k} is ${esc(names.length > 3 ? names.slice(0, 2).join(', ') + ` or ${names.length - 2} more` : names.join(' or '))}`;
  }
  if (f.type === 'text') return `${k} ${esc(c.op || 'contains')} “${esc(c.value)}”`;
  if (f.type === 'yesno') return `${k}: ${c.value === 'yes' ? 'yes' : 'no'}`;
  if (f.type === 'range') {
    const lo = c.min !== '' && c.min != null, hi = c.max !== '' && c.max != null, u = f.unit ? ' ' + esc(f.unit) : '';
    return `${k} ${lo && hi ? `from ${esc(c.min)} to ${esc(c.max)}` : lo ? `at least ${esc(c.min)}` : `at most ${esc(c.max)}`}${u}`;
  }
  return k;
}

// ── The values a condition offers: from the data, with their counts ──────────
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

// ── A condition's controls, in its editor and in the "before it runs" dialog ─
function control(f, c, where) {
  const a = `data-f="${f.id}"`;
  if (f.type === 'date') {
    const mode = c.mode || 'none';
    const chip = (act, v, label, on) => `<button type="button" class="rc-chip" data-act="${act}" data-v="${v}" ${a} aria-pressed="${on}">${esc(label)}</button>`;
    let html = '<div class="rc-chips">' + chip('quick', 'none', 'Any time', mode === 'none')
      + CAT.quick_dates.map(q => chip('quick', q.id, q.label, mode === 'quick' && c.quick === q.id)).join('')
      + chip('mode', 'fixed', 'From – to', mode === 'fixed') + chip('mode', 'relative', 'Previous / next N', mode === 'relative') + '</div>';
    if (mode === 'fixed') html += `<div class="rc-row"><input type="date" class="rfp-input" data-k="from" ${a} value="${esc(c.from || '')}" aria-label="From">
      <span class="lbl">to</span><input type="date" class="rfp-input" data-k="to" ${a} value="${esc(c.to || '')}" aria-label="To, included"></div><p class="rc-hint mt-1">Both days included.</p>`;
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
        sel.map(v => `<option value="${esc(JSON.stringify(v))}" selected>${esc(optLabel(f, v))}</option>`).join('')}</select>
        <p class="rc-hint mt-2">Pick one or more. Each value shows how many rows it had in the last year; type to find one.</p>`;
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
    ${dayOnly ? '' : `<div class="rc-row"><span class="lbl">from</span><input type="time" class="rfp-input" data-k="wfrom" ${a} value="${esc(w.from || '')}" aria-label="From time">
      <span class="lbl">to</span><input type="time" class="rfp-input" data-k="wto" ${a} value="${esc(w.to || '')}" aria-label="To time"></div>`}
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
  if (!fid || !FIELD[fid] || !act) return false;
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
      width: '100%', placeholder: 'Anything', closeOnSelect: false, templateResult: optTemplate,
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
// A condition or the period changed: the sentence, the SQL, the answer.
function criteriaChanged() {
  markDirty(); renderSentence(); scheduleCompile(); scheduleRun();
}
// The numbers, the groupings or the view changed.
function outputChanged() {
  pivotSort = null;
  markDirty(); normalizeOutput(); renderSentence(); renderViews(); scheduleCompile(0); scheduleRun();
}

// ── The sentence ─────────────────────────────────────────────────────────────
function blank({act, i, f, text, html, x, cls = '', title = ''}) {
  const data = `data-act="${act}"${i != null ? ` data-i="${i}"` : ''}${f ? ` data-f="${f}"` : ''}`;
  const rm = x ? `<button type="button" class="x" data-rm="${x.rm}"${x.i != null ? ` data-i="${x.i}"` : ''}${x.f ? ` data-f="${x.f}"` : ''} aria-label="Remove" title="Remove"><i class="bi bi-x"></i></button>` : '';
  return `<span class="rc-blank ${cls}"><button type="button" class="t" ${data}${title ? ` title="${esc(title)}"` : ''}><span>${html != null ? html : esc(text)}</span><i class="bi bi-chevron-down" aria-hidden="true"></i></button>${rm}</span>`;
}
const onlyFirstNumber = () => !!D().output.cols || ['pie', 'heatmap'].includes(showValue());
function byLabel(id) {
  const [fid, bucket] = String(id).split(':'), f = FIELD[fid];
  if (!bucket) return f ? f.label : id;
  return fid === WHEN ? cap(bucket) : `${f ? f.label : fid}, by ${bucket}`;
}
function suggestName() {
  if (!S || !ds() || handMode()) return 'Untitled report';
  if (isList()) return 'List of ' + dsWords().nouns;
  const g = groupings().map(byLabel);
  return measurePhrase(D().output.measures[0]) + (g.length ? ' by ' + g.join(' and ') : '');
}
function renderSentence() {
  if (!S) return;
  const hand = handMode();
  $('rc-sentence').hidden = hand || !ds();
  $('f-name').placeholder = suggestName();
  if (hand || !ds()) return;
  $('w-what').textContent = isList() ? 'List every' : 'Show';
  $('w-by').textContent = isList() ? 'with columns' : 'for each';
  renderWhat(); renderBy(); renderWhen(); renderWhere();
}
function renderWhat() {
  const o = D().output;
  if (isList()) {
    $('s-what').innerHTML = blank({act: 'what', text: dsWords().noun, title: 'Change what is listed'}) + `<span class="note">${esc(dsWords().title)}, one per line</span>`;
    return;
  }
  const one = onlyFirstNumber();
  $('s-what').innerHTML = o.measures.map((m, i) => (i ? '<span class="and">and</span>' : '') + blank({
    act: 'what', i, text: measurePhrase(m), cls: one && i ? 'dim' : '',
    x: o.measures.length > 1 ? {rm: 'measure', i} : null,
    title: one && i ? 'Not shown in this view: only the first number is' : i ? 'Change this number' : 'Change what is counted'})).join('')
    + '<button type="button" class="rc-add" data-act="add-measure" title="Show another number next to it">+ another number</button>'
    + (one && o.measures.length > 1 ? '<span class="note">this view shows the first number only</span>' : '');
}
function renderBy() {
  if (isList()) {
    const o = D().output;
    $('s-by').innerHTML = o.columns.map((c, i) => `<span class="rc-blank"><span style="padding-right:2px">${esc(columnLabel(c))}</span>${
      o.columns.length > 1 ? `<button type="button" class="x" data-rm="col" data-i="${i}" aria-label="Remove ${esc(columnLabel(c))}" title="Remove"><i class="bi bi-x"></i></button>` : ''}</span>`).join('')
      + '<button type="button" class="rc-add" data-act="add-col">+ add a column</button><span class="note">click a column title in the answer to sort by it</span>';
    return;
  }
  const g = groupings(), v = showValue();
  const chips = g.map((id, i) => byChip(id, i)).join('<span class="and">and</span>');
  const note = !g.length ? '<span class="note">nothing: one total</span>'
    : g.length >= 2 && SPLIT_VIEWS.has(v) ? `<span class="note">the last one gives the ${v === 'heatmap' ? 'columns' : 'colours'}</span>` : '';
  $('s-by').innerHTML = chips + (g.length ? '' : note) + (g.length < MAX_GROUPS ? '<button type="button" class="rc-add" data-act="add-by" title="Break the numbers down by something">+ add</button>' : '') + (g.length ? note : '');
}
function byChip(id, i) {
  const [fid, bucket] = String(id).split(':'), f = FIELD[fid];
  if (!bucket) return blank({act: 'swap-by', i, text: f ? f.label : id, x: {rm: 'by', i}, title: 'Change it'});
  const buckets = ds().dimensions.filter(d => d.id.startsWith(fid + ':')).map(d => d.id.split(':')[1]);
  const pre = fid === WHEN ? '' : `<span class="k">${esc(f ? f.label : fid)}:</span>`;
  return `<span class="rc-blank">${pre}<select data-bucket="${i}" aria-label="Day, week, month, quarter or year">${
    buckets.map(b => `<option value="${b}"${b === bucket ? ' selected' : ''}>${cap(b)}</option>`).join('')}</select>
    <button type="button" class="x" data-rm="by" data-i="${i}" aria-label="Remove" title="Remove"><i class="bi bi-x"></i></button></span>`;
}
function renderWhen() {
  const f = FIELD[WHEN];
  if (!f) { $('s-when').innerHTML = '<span class="note">this data has no date</span>'; return; }
  const c = D().criteria[WHEN] || {}, wins = windowsOf(c), dayOnly = (f.day_only || []).includes(D().dataset);
  const period = periodText(c), dates = dateFields();
  let html = blank({act: 'when', text: period, cls: period === 'any time' ? 'quiet' : '', title: 'Change the period'})
    + '<span class="and">,</span>'
    + blank({act: 'at', text: wins.length ? atText(wins) : (dayOnly ? 'any day' : 'any day or time'), cls: wins.length ? '' : 'quiet',
             title: dayOnly ? 'Only some days of the week' : 'Only some days of the week or hours, e.g. after hours'});
  if (D().ask.includes(WHEN)) html += '<span class="crit-ask" title="Asked each time it runs">asked</span>';
  if (dates.length > 1) {
    html += `<span class="and">using the</span><select class="plain" id="s-datefield" aria-label="Which date the period is on">${
      dates.map(x => `<option value="${x.id}"${x.id === WHEN ? ' selected' : ''}>${esc(x.label)}</option>`).join('')}</select><span class="and">date</span>`;
  }
  $('s-when').innerHTML = html;
}
function conditionFields() {
  const cr = D().criteria;
  return CAT.fields.filter(f => inDataset(f) && f.id !== WHEN && (isActive(f, cr[f.id]) || D().ask.includes(f.id) || extra.includes(f.id)));
}
function renderWhere() {
  const chips = conditionFields().map(f => {
    const c = D().criteria[f.id], ask = D().ask.includes(f.id);
    return blank({act: 'cond', f: f.id, html: condHtml(f, c) + (ask ? ' <span class="crit-ask">asked</span>' : ''),
                  cls: (ask ? 'ask ' : '') + (isActive(f, c) ? '' : 'quiet'), x: {rm: 'cond', f: f.id}, title: 'Change this condition'});
  }).join('<span class="and">and</span>');
  $('s-where').innerHTML = (chips || `<span class="note">no condition: every ${esc(dsWords().noun)} counts</span>`)
    + '<button type="button" class="rc-add" data-act="add-cond" title="Only count some of them: a room, a radiologist, a patient type…">+ add a condition</button>';
}
const sentence = $('rc-sentence');
sentence.addEventListener('click', e => {
  const t = e.target, o = D().output;
  const rm = t.closest('[data-rm]');
  if (rm) {
    const kind = rm.dataset.rm, i = +rm.dataset.i;
    if (kind === 'measure') { o.measures.splice(i, 1); return outputChanged(); }
    if (kind === 'col') { o.columns.splice(i, 1); return outputChanged(); }
    if (kind === 'by') { const g = groupings(); g.splice(i, 1); applyGroupings(g); return outputChanged(); }
    if (kind === 'cond') {
      const fid = rm.dataset.f;
      delete D().criteria[fid]; D().ask = D().ask.filter(x => x !== fid); extra = extra.filter(x => x !== fid);
      if (popFid === fid) closePop();
      return criteriaChanged();
    }
    return;
  }
  const b = t.closest('[data-act]'); if (!b) return;
  const act = b.dataset.act, anchor = b.closest('.rc-blank') || b;
  if (act === 'what') return isList() ? openWhatPicker(anchor, true) : (+b.dataset.i ? openNumberPicker(anchor, +b.dataset.i) : openWhatPicker(anchor, false));
  if (act === 'add-measure') return openNumberPicker(anchor, null);
  if (act === 'add-by') return openByPicker(anchor, null);
  if (act === 'swap-by') return openByPicker(anchor, +b.dataset.i);
  if (act === 'add-col') return openColumnPicker(anchor);
  if (act === 'when') return openFilter(WHEN, anchor, 'when');
  if (act === 'at') return openAt(anchor);
  if (act === 'cond') return openFilter(b.dataset.f, anchor, 'cond');
  if (act === 'add-cond') return openConditionPicker(anchor);
});
sentence.addEventListener('change', e => {
  const t = e.target;
  if (t.dataset.bucket != null) {
    const g = groupings(), i = +t.dataset.bucket;
    g[i] = `${String(g[i]).split(':')[0]}:${t.value}`;
    applyGroupings(g); return outputChanged();
  }
  if (t.id === 's-datefield') return changeWhen(t.value);
});
// The period moves to another date (scheduled, done, signed ...); groupings by the old date
// follow it.
function changeWhen(fid) {
  const old = WHEN, f = FIELD[fid]; if (!f || fid === old) return;
  const c = D().criteria[old];
  delete D().criteria[old];
  if (c) {
    const moved = clone(c);
    if ((f.day_only || []).includes(D().dataset) && moved.windows) moved.windows.forEach(w => { w.from = ''; w.to = ''; });
    D().criteria[fid] = moved;
  }
  if (D().ask.includes(old)) D().ask = [...new Set(D().ask.filter(x => x !== old).concat([fid]))];
  const dims = new Set(ds().dimensions.map(d => d.id));
  applyGroupings(groupings().map(id => { const [a, b] = String(id).split(':'); return b && a === old && dims.has(`${fid}:${b}`) ? `${fid}:${b}` : id; }));
  WHEN = fid; extra = extra.filter(x => x !== fid);
  closePop(); outputChanged();
}

// ── Pickers: what to show, a grouping, a number, a column, a condition ──────
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
  // a condition closed while still empty (and not asked for) leaves the sentence
  const left = popKind === 'filter' && popMode === 'cond' && S && FIELD[popFid] && extra.includes(popFid)
    && !isActive(FIELD[popFid], D().criteria[popFid]) && !D().ask.includes(popFid) ? popFid : null;
  pop.hidden = true; pop.innerHTML = ''; pop.className = 'rc';
  popKind = null; popFid = null; popAnchor = null; popMode = null;
  if (left) { extra = extra.filter(x => x !== left); delete D().criteria[left]; renderWhere(); }
}
// The same blank clicked again closes its popover.
function toggled(kind, anchor) { if (popKind === kind && popAnchor === anchor) { closePop(); return true; } closePop(); return false; }
function openMenu(anchor, title, items, onPick) {
  if (toggled('menu', anchor)) return;
  popKind = 'menu'; popAnchor = anchor; pop.className = 'rc menu';
  pop.innerHTML = (title ? `<div class="rc-menu-head">${esc(title)}</div>` : '') + items.filter(Boolean).map(it => it.sep ? '<div class="rc-menu-sep"></div>'
    : `<button type="button" class="rc-menu-item${it.danger ? ' danger' : ''}" data-menu="${it.act}"${it.disabled ? ' disabled' : ''}><i class="bi ${it.icon}" aria-hidden="true"></i>${esc(it.label)}</button>`).join('');
  pop._pick = onPick;
  placePop(anchor);
  const first = pop.querySelector('.rc-menu-item:not(:disabled)'); if (first) first.focus();
}
function openPicker(anchor, title, items, onPick, placeholder = 'Find…') {
  if (toggled('picker', anchor)) return;
  popKind = 'picker'; popAnchor = anchor; pop.className = 'rc menu';
  pop.innerHTML = `<div class="rc-menu-head">${esc(title)}</div><input type="search" class="rc-pick-q" data-pickq placeholder="${esc(placeholder)}" aria-label="Find">
    <div class="rc-pick-list" style="max-height:340px;overflow-y:auto"></div>`;
  const draw = q => {
    const hit = items.filter(it => !q || `${it.label} ${it.hint || ''} ${it.group || ''}`.toLowerCase().includes(q));
    let last = null;
    pop.querySelector('.rc-pick-list').innerHTML = hit.map(it => {
      const head = it.group && it.group !== last ? `<div class="rc-menu-head">${esc(it.group)}</div>` : '';
      last = it.group;
      return head + `<button type="button" class="rc-menu-item" data-pick="${esc(it.id)}"${it.hint ? ` title="${esc(it.hint)}"` : ''}><span class="tx">${esc(it.label)}${it.hint ? `<span class="h">${esc(it.hint)}</span>` : ''}</span></button>`;
    }).join('') || '<p class="rc-hint" style="padding:8px">Nothing matches.</p>';
  };
  pop._pick = onPick; pop._draw = draw;
  draw('');
  placePop(anchor);
  pop.querySelector('[data-pickq]').focus();
}
// "Show [...]": every kind of data on the left, its numbers on the right; the search finds a
// number in any of them. In a list, picking a kind of data is enough.
function openWhatPicker(anchor, listMode) {
  if (toggled('what', anchor)) return;
  popKind = 'what'; popAnchor = anchor; pop.className = 'rc what';
  let cur = D().dataset;
  pop.innerHTML = `<div class="rc-what-top"><b>${listMode ? 'What do you want to list?' : 'What do you want to show?'}</b>${listMode ? ''
      : '<input type="search" class="rc-pick-q" data-whatq placeholder="Find a number: turnaround, cancelled, critical, storage…" aria-label="Find a number">'}</div>
    <div class="rc-what-body"${listMode ? ' style="grid-template-columns:minmax(0,1fr)"' : ''}><div class="rc-what-ds"></div>${listMode ? '' : '<div class="rc-what-ms"></div>'}</div>`;
  const draw = () => {
    pop.querySelector('.rc-what-ds').innerHTML = DS_ORDER.filter(k => CAT.datasets[k]).map(k => { const w = dsWords(k); return `<button type="button" data-whatds="${k}" aria-pressed="${k === cur}">
      <i class="bi ${w.icon}" aria-hidden="true"></i><span><b>${esc(w.title)}</b><span class="ab">${esc(w.about)}</span></span></button>`; }).join('');
    if (listMode) return;
    const qEl = pop.querySelector('[data-whatq]'), q = qEl ? qEl.value.trim().toLowerCase() : '';
    const items = q
      ? DS_ORDER.filter(k => CAT.datasets[k]).flatMap(k => CAT.datasets[k].measures.map(m => ({k, id: m.id, label: measurePhrase(m.id, k)})))
          .filter(it => `${it.label} ${dsWords(it.k).title}`.toLowerCase().includes(q))
      : CAT.datasets[cur].measures.map(m => ({k: cur, id: m.id, label: measurePhrase(m.id, cur)}));
    pop.querySelector('.rc-what-ms').innerHTML = (q ? '' : `<div class="rc-menu-head">${esc(dsWords(cur).title)}</div>`) + (items.map(it =>
      `<button type="button" class="rc-menu-item" data-whatm="${it.k}|${it.id}"><span class="tx">${esc(it.label)}${q ? `<span class="h">${esc(dsWords(it.k).title)}</span>` : ''}</span></button>`).join('')
      || '<p class="rc-hint" style="padding:10px">Nothing matches.</p>');
  };
  pop._draw = draw; pop._setds = k => { cur = k; const qEl = pop.querySelector('[data-whatq]'); if (qEl) qEl.value = ''; draw(); };
  pop._listMode = listMode;
  draw(); placePop(anchor);
  const qEl = pop.querySelector('[data-whatq]'); if (qEl) qEl.focus();
}
function pickWhat(k, id) {
  if (k !== D().dataset) return switchDataset(k, [id]);
  const o = D().output;
  o.measures = [id, ...o.measures.slice(1).filter(m => m !== id)];
  outputChanged();
}
// Another kind of data: the period, the conditions and the groupings it has too stay.
function switchDataset(k, measures) {
  closePop();
  const old = D(), oldOut = old.output || {}, oldWhen = WHEN, g = groupings();
  const nd = CAT.datasets[k], oldDefault = (CAT.datasets[old.dataset] || {}).default_date;
  const target = oldWhen && oldWhen !== oldDefault && FIELD[oldWhen] && FIELD[oldWhen].datasets.includes(k) ? oldWhen : nd.default_date;
  S.definition = blankDefinition(k, {kind: oldOut.kind, chart: oldOut.chart, rows: [], measures});
  const cr = S.definition.criteria = {};
  Object.entries(old.criteria || {}).forEach(([fid, v]) => { if (fid !== oldWhen && FIELD[fid] && FIELD[fid].datasets.includes(k)) cr[fid] = v; });
  const period = oldWhen && old.criteria[oldWhen] && isActive(FIELD[oldWhen], old.criteria[oldWhen]) ? clone(old.criteria[oldWhen]) : {mode: 'quick', quick: 'last_30_days'};
  if ((FIELD[target].day_only || []).includes(k) && period.windows) period.windows.forEach(w => { w.from = ''; w.to = ''; });
  cr[target] = period;
  S.definition.ask = (old.ask || []).map(x => x === oldWhen ? target : x).filter(x => FIELD[x] && FIELD[x].datasets.includes(k));
  S.definition.sql = old.sql;
  WHEN = target;
  const dims = new Set(nd.dimensions.map(x => x.id));
  const kept = g.map(id => { const [a, b] = String(id).split(':'); return b && a === oldWhen ? `${target}:${b}` : id; }).filter(id => dims.has(id));
  normalizeOutput();
  applyGroupings(kept.length ? kept : (dims.has('modality') ? ['modality'] : []));
  extra = extra.filter(x => FIELD[x] && FIELD[x].datasets.includes(k) && x !== WHEN);
  toast(`Now counting ${dsWords(k).nouns}. What they have in common was kept.`);
  outputChanged();
}
function openNumberPicker(anchor, i) {
  const o = D().output;
  const items = ds().measures.filter(m => !o.measures.includes(m.id) || (i != null && o.measures[i] === m.id))
    .map(m => ({id: m.id, label: measurePhrase(m.id)}));
  openPicker(anchor, i == null ? 'Show another number' : 'Change this number', items, id => {
    if (i == null) o.measures.push(id); else o.measures[i] = id;
    outputChanged();
  }, 'Find a number…');
}
function byItems(swap) {
  const keep = groupings().filter((_, i) => i !== swap);
  const usedF = new Set(keep.map(id => String(id).split(':')[0]));
  const dims = new Set(ds().dimensions.map(d => d.id)), items = [];
  if (WHEN && !usedF.has(WHEN)) BUCKETS.forEach(b => { if (dims.has(`${WHEN}:${b}`)) items.push({id: `${WHEN}:${b}`, label: cap(b), group: GROUP_LABEL.when, hint: `${FIELD[WHEN].label}, ${b} by ${b}`}); });
  const rest = CAT.fields.filter(f => inDataset(f) && f.type !== 'date' && dims.has(f.id) && !usedF.has(f.id));
  GROUP_ORDER.forEach(gk => rest.filter(f => (DIM_GROUP[f.id] || 'what') === gk)
    .forEach(f => items.push({id: f.id, label: f.label, group: GROUP_LABEL[gk], hint: f.hint})));
  dateFields().filter(f => f.id !== WHEN && !usedF.has(f.id) && dims.has(f.id + ':month'))
    .forEach(f => items.push({id: f.id + ':month', label: `${f.label}, by month`, group: 'Other dates', hint: f.hint}));
  return items;
}
function openByPicker(anchor, swap) {
  openPicker(anchor, swap == null ? 'Break it down by' : 'Change it to', byItems(swap), id => {
    const g = groupings();
    if (swap == null) g.push(id); else g[swap] = id;
    applyGroupings(g); outputChanged();
  }, 'Find: room, radiologist, month, weekday…');
}
function openColumnPicker(anchor) {
  const o = D().output;
  openPicker(anchor, 'Add a column', ds().columns.filter(c => !o.columns.includes(c.id)).map(c => ({id: c.id, label: c.label})),
    id => { o.columns.push(id); outputChanged(); }, 'Find a column…');
}
function openConditionPicker(anchor) {
  const shown = new Set(conditionFields().map(f => f.id).concat([WHEN]));
  const avail = CAT.fields.filter(f => inDataset(f) && !shown.has(f.id)), pinned = ds().pinned || [];
  const items = pinned.map(id => avail.find(f => f.id === id)).filter(Boolean).map(f => ({id: f.id, label: f.label, group: 'Most used', hint: f.hint}));
  CAT.tabs.forEach(t => avail.filter(f => f.tab === t.id && !pinned.includes(f.id))
    .forEach(f => items.push({id: f.id, label: f.label, group: t.id === 'primary' ? 'Main' : t.label, hint: f.hint})));
  openPicker(anchor, 'Only count those where…', items, fid => {
    if (!extra.includes(fid)) extra.push(fid);
    renderWhere();
    const chip = document.querySelector(`#s-where [data-act="cond"][data-f="${fid}"]`);
    if (chip) openFilter(fid, chip.closest('.rc-blank') || chip, 'cond');
  }, 'Find: room, radiologist, priority, age…');
}

// ── Editors: the period, a condition, days and times ─────────────────────────
function openFilter(fid, anchor, mode) {
  if (popKind === 'filter' && popFid === fid) return closePop();
  closePop();
  popKind = 'filter'; popFid = fid; popAnchor = anchor; popMode = mode; pop.className = 'rc';
  renderFilterPop();
  placePop(anchor);
}
function renderFilterPop() {
  const f = FIELD[popFid], c = D().criteria[f.id] || {}, ask = D().ask.includes(f.id), when = popMode === 'when';
  destroySelects(pop);
  pop.innerHTML = `<div class="rc-pophead"><b>${when ? 'Which period?' : esc(f.label)}</b>
      ${f.hint ? `<button type="button" class="rc-icon" data-act="hint" data-f="${f.id}" title="${esc(f.hint)}" aria-label="About ${esc(f.label)}"><i class="bi bi-question-circle"></i></button>` : ''}
      <button type="button" class="rc-icon" data-act="pop-close" aria-label="Close"><i class="bi bi-x-lg"></i></button></div>
    ${when ? `<p class="rc-hint" style="margin:-6px 0 10px">On the date: ${esc(f.label)}.</p>` : ''}
    ${control(f, c, 'panel')}
    <div class="rc-popfoot"><label class="rc-ask" title="Each time the report runs, ask for this, its saved value first"><input type="checkbox" class="rfp-tick" data-ask="${f.id}"${ask ? ' checked' : ''}><span>Ask me each time it runs</span></label>
      <span class="flex-1"></span>
      ${!when ? `<button type="button" class="rc-link" style="color:var(--rc-bad)" data-act="remove" data-f="${f.id}">Remove</button>` : ''}
      <button type="button" class="rc-btn small primary" data-act="pop-close">Done</button></div>`;
  initSelects(pop);
}
function openAt(anchor) {
  if (toggled('at', anchor)) return;
  popKind = 'at'; popAnchor = anchor; pop.className = 'rc';
  popCustom = presetOf(windowsOf(D().criteria[WHEN])) === 'custom';
  renderAtPop(); placePop(anchor);
}
function renderAtPop() {
  const f = FIELD[WHEN], wins = windowsOf(D().criteria[WHEN]), dayOnly = (f.day_only || []).includes(D().dataset);
  const cur = popCustom ? 'custom' : presetOf(wins);
  const presets = [{id: 'any', label: dayOnly ? 'Any day' : 'Any day or time', about: 'No limit on the day of the week or the hour.'}]
    .concat(AT_PRESETS.filter(p => !dayOnly || !p.times))
    .concat([{id: 'custom', label: dayOnly ? 'Choose the days' : 'Choose days and times', about: dayOnly ? 'Pick the days of the week yourself.' : 'Your own windows, e.g. Saturday 13:00 → Sunday 07:00.'}]);
  pop.innerHTML = `<div class="rc-pophead"><b>${dayOnly ? 'Which days?' : 'Which days and times?'}</b><button type="button" class="rc-icon" data-act="pop-close" aria-label="Close"><i class="bi bi-x-lg"></i></button></div>
    <div class="rc-presets">${presets.map(p => `<button type="button" class="rc-preset" data-preset="${p.id}" aria-pressed="${cur === p.id}"><b>${esc(p.label)}</b><span>${esc(p.about)}</span></button>`).join('')}</div>
    ${cur === 'custom' ? `<div class="rc-win">${wins.map((w, i) => windowRow(f, w, i, dayOnly)).join('')}
      <button type="button" class="rc-link" data-act="win-add" data-f="${f.id}" style="align-self:flex-start"><i class="bi bi-plus-lg" aria-hidden="true"></i> Another window</button>
      ${dayOnly ? '' : `<p class="rc-hint">A window that ends earlier than it starts runs on to the next morning: Sat 13:00 → 07:00 is Saturday 13:00 to Sunday 07:00. The start is included, the end is not.${
        (f.time_known || []).includes(D().dataset) ? ' A study PACS sent without a time is left out when times are set.' : ''}</p>`}</div>` : ''}
    <div class="rc-popfoot"><span class="rc-hint">On the date: ${esc(f.label)}.</span><span class="flex-1"></span><button type="button" class="rc-btn small primary" data-act="pop-close">Done</button></div>`;
}
function setPreset(id) {
  const c = crit(WHEN), dayOnly = (FIELD[WHEN].day_only || []).includes(D().dataset);
  popCustom = id === 'custom';
  if (id === 'any') delete c.windows;
  else if (id === 'custom') { if (!windowsOf(c).length) c.windows = [{days: [1, 2, 3, 4, 5], from: dayOnly ? '' : '19:00', to: dayOnly ? '' : '07:00'}]; }
  else c.windows = clone(AT_PRESETS.find(p => p.id === id).wins);
  renderAtPop(); placePop(popAnchor && popAnchor.isConnected ? popAnchor : sentence); criteriaChanged();
}
pop.addEventListener('click', e => {
  const t = e.target;
  const m = t.closest('[data-menu]'); if (m && !m.disabled) { const pick = pop._pick; closePop(); return pick(m.dataset.menu); }
  const pk = t.closest('[data-pick]'); if (pk) { const pick = pop._pick; closePop(); return pick(pk.dataset.pick); }
  const wd = t.closest('[data-whatds]');
  if (wd) { if (pop._listMode) { closePop(); return wd.dataset.whatds === D().dataset ? null : switchDataset(wd.dataset.whatds, null); } return pop._setds(wd.dataset.whatds); }
  const wm = t.closest('[data-whatm]'); if (wm) { const [k, id] = wm.dataset.whatm.split('|'); closePop(); return pickWhat(k, id); }
  const pr = t.closest('[data-preset]'); if (pr && popKind === 'at') return setPreset(pr.dataset.preset);
  const b = t.closest('[data-act]'); if (!b) return;
  const act = b.dataset.act;
  if (act === 'pop-close') return closePop();
  if (popKind === 'at') { if (applyClick(D().criteria, b)) { renderAtPop(); criteriaChanged(); } return; }
  if (popKind !== 'filter') return;
  const fid = popFid;
  if (act === 'hint') return explain(FIELD[fid].label, FIELD[fid].hint);
  if (act === 'remove') { delete D().criteria[fid]; D().ask = D().ask.filter(x => x !== fid); extra = extra.filter(x => x !== fid); closePop(); return criteriaChanged(); }
  if (applyClick(D().criteria, b)) { renderFilterPop(); criteriaChanged(); }
});
pop.addEventListener('change', e => {
  const t = e.target;
  if (popKind === 'filter' && t.dataset.ask) {
    D().ask = t.checked ? [...new Set([...D().ask, t.dataset.ask])] : D().ask.filter(x => x !== t.dataset.ask);
    markDirty(); renderSentence(); return;
  }
  if ((popKind === 'filter' || popKind === 'at') && applyInput(D().criteria, t)) criteriaChanged();
});
pop.addEventListener('input', e => {
  const t = e.target;
  if (t.hasAttribute('data-pickq')) return pop._draw(t.value.trim().toLowerCase());
  if (t.hasAttribute('data-whatq')) return pop._draw();
  if ((popKind === 'filter' || popKind === 'at') && t.tagName === 'INPUT' && t.type !== 'checkbox' && applyInput(D().criteria, t)) criteriaChanged();
});
document.addEventListener('mousedown', e => {
  if (!popKind) return;
  const t = e.target;
  if (pop.contains(t) || t.closest('.select2-container, .swal2-container') || (popAnchor && popAnchor.isConnected && popAnchor.contains(t))) return;
  closePop();
}, true);
document.addEventListener('keydown', e => { if (e.key === 'Escape' && popKind && !document.querySelector('.swal2-container')) closePop(); });
window.addEventListener('resize', closePop);
$('rc-view').closest('main').addEventListener('scroll', () => { if (popKind && popKind !== 'filter' && popKind !== 'at') closePop(); });
function closeModal() { $('modal').innerHTML = ''; }

// ── The title bar: name, status, save, download, more ────────────────────────
function renderTop() {
  if (document.activeElement !== $('f-name')) $('f-name').value = S.name || '';
  $('save-label').textContent = S.can_edit ? 'Save' : 'Save a copy';
  $('btn-save').title = S.can_edit ? 'Save (Ctrl+S)' : 'This shared report is read-only for you: save a copy to change it';
  renderStatus();
}
function renderStatus() {
  const st = $('bar-status');
  st.textContent = dirty ? 'Unsaved changes' : S.id ? 'Saved' : 'Not saved yet';
  st.classList.toggle('dirty', dirty);
  st.title = S.id ? `Version ${S.version}${S.owner_name ? ' · owner ' + S.owner_name : ' · shared example'}${S.updated_by_name ? ' · last saved by ' + S.updated_by_name : ''}${S.updated_at ? ' · ' + S.updated_at : ''}` : '';
}
$('f-name').addEventListener('input', e => { S.name = e.target.value; markDirty(); });
$('btn-more').onclick = () => {
  openMenu($('btn-more'), '', [
    {act: 'dup', icon: 'bi-files', label: 'Save a copy'},
    {act: 'details', icon: 'bi-pencil', label: 'Name, folder and sharing…', disabled: !S.can_edit},
    {act: 'history', icon: 'bi-clock-history', label: 'Earlier versions…', disabled: !(S.versions || []).length},
    CAT.me.can_schedule && {act: 'sched', icon: 'bi-envelope', label: 'Scheduled emails…'},
    {act: 'sql', icon: 'bi-code-slash', label: 'Show the SQL (advanced)'},
    {sep: true},
    {act: 'del', icon: 'bi-trash', label: 'Delete this report', disabled: !S.id || !S.can_edit, danger: true},
  ], act => ({dup: duplicate, details: () => openSaveDialog(false), history: openHistory, sched: schedOpen, sql: openDrawer, del: deleteReport})[act]());
};
function duplicate() {
  const c = clone(S); c.id = null; c.name = (S.name || suggestName()) + ' (copy)'; c.visibility = 'personal'; c.can_edit = true;
  c.version = 0; c.versions = []; c.owner_name = null; c.updated_by_name = null; c.updated_at = null;
  load(c); markDirty(); toast('Copied into your own reports. Save to keep it.');
}
async function deleteReport() {
  if (!S.id || !(await askYesNo(`Delete “${S.name}”?`, 'Its whole version history goes with it. This cannot be undone.', {yes: 'Delete', danger: true, icon: 'error'}))) return;
  try { await api(`/worklists/api/${S.id}/delete`, {}); markDirty(false); toast('Deleted.'); await loadLibrary(); showStart(); } catch (e) { toast(e.message); }
}
// Name, what it is for, who can open it, folder: asked at the first save, or from More.
function openSaveDialog(thenSave) {
  const canShare = !!CAT.me.can_edit_shared;
  $('modal').innerHTML = `<div class="modal-back" id="sv-back"><div class="modal" role="dialog" aria-modal="true" aria-label="Save the report">
    <div class="panel-head"><h2>${thenSave ? 'Save this report' : 'Name, folder and sharing'}</h2></div>
    <div class="panel-body">
      <div class="field"><label for="sv-name">Name</label><input class="input" id="sv-name" maxlength="200" value="${esc(S.name || suggestName())}"></div>
      <div class="field"><label for="sv-desc">What it is for (optional)</label><input class="input" id="sv-desc" value="${esc(S.description || '')}" placeholder="e.g. the monthly volume for the department meeting"></div>
      <div class="field"><span class="label">Who can open it</span><div class="rc-radio">
        <label><input type="radio" name="sv-vis" value="personal"${S.visibility !== 'shared' ? ' checked' : ''}><span>Only me<span class="h">It shows in your saved reports only.</span></span></label>
        <label><input type="radio" name="sv-vis" value="shared"${S.visibility === 'shared' ? ' checked' : ''}${canShare || S.visibility === 'shared' ? '' : ' disabled'}><span>Everyone who can open the Report Composer<span class="h">${
          canShare ? 'They can open it and save a copy of it.' : 'Only an administrator can share a report with everyone.'}</span></span></label></div></div>
      <div class="field"><label for="sv-folder">Folder (optional)</label><input class="input" id="sv-folder" list="folders" value="${esc(S.folder || '')}" placeholder="General"></div>
      <div class="rc-inl" style="justify-content:flex-end"><button class="btn" id="sv-cancel" type="button">Cancel</button><button class="btn primary" id="sv-ok" type="button">${thenSave ? 'Save' : 'Done'}</button></div>
    </div></div></div>`;
  const close = () => { closeModal(); document.removeEventListener('keydown', onKey); };
  const onKey = e => { if (e.key === 'Escape') close(); if (e.key === 'Enter' && e.target.id !== 'sv-desc') ok(); };
  const ok = () => {
    const name = $('sv-name').value.trim();
    if (!name) { $('sv-name').classList.add('missing'); $('sv-name').focus(); return; }
    S.name = name; S.description = $('sv-desc').value.trim(); S.folder = $('sv-folder').value.trim();
    S.visibility = (document.querySelector('input[name="sv-vis"]:checked') || {}).value || 'personal';
    close(); markDirty(); renderTop();
    if (thenSave) doSave();
  };
  document.addEventListener('keydown', onKey);
  $('sv-cancel').onclick = close;
  $('sv-ok').onclick = ok;
  $('sv-back').addEventListener('click', e => { if (e.target.id === 'sv-back') close(); });
  $('sv-name').focus(); $('sv-name').select();
}
function save() {
  if (!S) return;
  if (!S.can_edit) { duplicate(); return openSaveDialog(true); }
  if (handMode() && !String(D().sql || '').trim()) { toast('Write the SQL first.'); return openDrawer(); }
  if (!S.id || !String(S.name || '').trim()) return openSaveDialog(true);
  doSave();
}
async function doSave() {
  try {
    const r = await api('/worklists/api/save', {id: S.id, name: S.name, description: S.description, folder: S.folder, visibility: S.visibility, definition: D()});
    await loadLibrary();
    const fresh = await api('/worklists/api/' + r.id);
    S.id = fresh.id; S.version = fresh.version; S.versions = fresh.versions; S.can_edit = fresh.can_edit;
    S.owner_name = fresh.owner_name; S.updated_by_name = fresh.updated_by_name; S.updated_at = fresh.updated_at;
    markDirty(false); renderTop(); syncUrl(); toast(`Saved, version ${r.version}.`);
  } catch (e) { toast(e.message); }
}
$('btn-save').onclick = save;
function openHistory() {
  const vs = S.versions || [];
  $('modal').innerHTML = `<div class="modal-back" id="hi-back"><div class="modal" role="dialog" aria-modal="true" aria-label="Earlier versions">
    <div class="panel-head"><h2>Earlier versions</h2><button class="btn small" id="hi-close" type="button" style="margin-left:auto">Close</button></div>
    <div class="panel-body"><span class="hint">Open one to see it; save to make it the current version again.</span>
      <div class="table-wrap"><table><thead><tr><th>Version</th><th>Saved</th><th>By</th><th></th></tr></thead><tbody>${vs.map(v =>
        `<tr><td>v${v.version}</td><td>${esc(v.saved_at)}</td><td>${esc(v.saved_by || '')}</td><td><button class="btn small" type="button" data-ver="${v.version}">Open</button></td></tr>`).join('')}</tbody></table></div></div></div></div>`;
  $('hi-close').onclick = closeModal;
  $('hi-back').addEventListener('click', async e => {
    if (e.target.id === 'hi-back') return closeModal();
    const b = e.target.closest('[data-ver]'); if (!b) return;
    try {
      const old = await api(`/worklists/api/${S.id}/versions/${b.dataset.ver}`);
      closeModal();
      S.definition = old.definition; S.definition.ask = S.definition.ask || []; S.definition.criteria = S.definition.criteria || {}; S.name = old.name;
      normalizeOutput(); WHEN = computeWhen(); ACROSS = !!D().output.cols; extra = initialExtra();
      markDirty(); renderAll(); scheduleCompile(0); scheduleRun(0);
      toast(`Version ${b.dataset.ver} opened. Save to make it the current one.`);
    } catch (err) { toast(err.message); }
  });
}

// ── SQL: in a drawer, from More ──────────────────────────────────────────────
function openDrawer() { closePop(); $('sql-drawer').hidden = false; renderSQL(); }
function closeDrawer() { $('sql-drawer').hidden = true; }
$('sql-close').onclick = closeDrawer;
$('sql-back').onclick = closeDrawer;
$('btn-sql-run').onclick = () => { closeDrawer(); runNow(true); };
document.addEventListener('keydown', e => {
  if (e.key === 'Escape' && !$('sql-drawer').hidden && !$('modal').innerHTML && !document.querySelector('.swal2-container')) closeDrawer();
});
function scheduleCompile(ms = 250) { clearTimeout(compileTimer); compileTimer = setTimeout(compile, ms); }
async function compile() {
  if (!S) return;
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
// Two separate things: the SQL built from the sentence, only ever SHOWN (read-only), and SQL
// written by hand (settings capability), documented by the SQL reference.
function renderSQL() {
  if (!S) return;
  const hand = handMode(), canSQL = CAT.me.can_write_sql;
  $('sql-seg').hidden = !canSQL;
  [...$('sql-seg').children].forEach(b => b.setAttribute('aria-pressed', String((b.dataset.mode === 'hand') === hand)));
  $('btn-start-gen').hidden = !(hand && canSQL);
  $('sql-view').hidden = hand && canSQL;
  $('sql-edit').hidden = !(hand && canSQL);
  $('sql-wrap').classList.toggle('missing', hand && !String(D().sql || '').trim());
  if (hand) {
    if (canSQL) { if (document.activeElement !== $('sql-edit')) $('sql-edit').value = D().sql; }
    else $('sql-view').innerHTML = highlight(D().sql);
    $('params').innerHTML = '<div class="pv">Runs as the read-only reporting account: the rpt views (' + esc(Object.keys(CAT.datasets).join(', ')) + '), nothing else. No parameters: write values in the SQL.</div>';
    $('sql-caption').textContent = canSQL ? 'Your SQL. The SQL reference lists every view, column and rule.' : 'Written in SQL by an administrator.';
    $('sql-note').innerHTML = canSQL ? '' : '<div class="rc-note warn">Read-only: only an administrator can change this SQL.</div>';
    return;
  }
  $('sql-caption').textContent = 'Read-only: built from the sentence. Values are bound as parameters, never pasted into the SQL.';
  $('sql-note').innerHTML = '';
  if (!sqlInfo || sqlInfo.pending) { $('sql-view').innerHTML = '<span class="hint">The SQL appears here once there is something to count.</span>'; $('params').innerHTML = ''; return; }
  if (sqlInfo.error) { $('sql-note').innerHTML = `<div class="rc-note bad">${esc(sqlInfo.error)}</div>`; $('sql-view').innerHTML = ''; $('params').innerHTML = ''; return; }
  $('sql-view').innerHTML = highlight(sqlInfo.sql);
  const p = Object.entries(sqlInfo.params || {});
  $('params').innerHTML = p.length ? p.map(([k, v]) => `<div><span class="pk">:${esc(k)}</span> <span class="pv">= ${esc(Array.isArray(v) ? JSON.stringify(v) : v)}</span></div>`).join('') : '<div class="pv">No parameters.</div>';
}
function renderHandNote() {
  const hand = handMode();
  $('rc-handnote').hidden = !hand;
  $('rc-handnote').innerHTML = hand ? '<div class="rc-note info"><i class="bi bi-code-slash" aria-hidden="true"></i> This report is written in SQL: the answer is the query’s own. <button type="button" class="rc-link" data-act="open-sql">Open the SQL</button></div>' : '';
}
$('rc-handnote').addEventListener('click', e => { if (e.target.closest('[data-act="open-sql"]')) openDrawer(); });
$('btn-copy-sql').onclick = () => {
  const t = D().sql != null ? D().sql : (sqlInfo && sqlInfo.sql) || '';
  if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(t).then(() => toast('SQL copied.'), () => toast('Copy was blocked; select the SQL and copy it by hand.'));
};
async function setMode(hand, sql) {
  if (hand === handMode() && sql === undefined) return;
  if (!hand) {
    if (String(D().sql || '').trim() && !(await askYesNo('Go back to the sentence?', 'The SQL you wrote is dropped; the report is built from the sentence again.', {yes: 'Use the sentence'}))) return;
    D().sql = null;
  } else {
    D().sql = sql !== undefined ? sql : ((sqlInfo && sqlInfo.editable_sql) || '');
  }
  markDirty(); renderAll();
  if (hand) { $('sql-edit').value = D().sql; $('sql-edit').focus(); } else { scheduleCompile(0); scheduleRun(); }
}
$('sql-seg').addEventListener('click', e => { const b = e.target.closest('[data-mode]'); if (b) setMode(b.dataset.mode === 'hand'); });
$('btn-start-gen').onclick = async () => {
  if (!sqlInfo || !sqlInfo.editable_sql) return toast('There is no SQL from the sentence yet: switch to “Built from the sentence” first.');
  if (String(D().sql || '').trim() && !(await askYesNo('Replace your SQL?', 'The editor gets the SQL built from the sentence instead.', {yes: 'Replace'}))) return;
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
        close(); openDrawer(); setMode(true, sql); toast('Example placed in the SQL editor. Run it, then adjust.');
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

// ── Running: as the sentence changes ─────────────────────────────────────────
function setMeta(extraHtml = '') {
  const r = lastResult, parts = [];
  if (r) {
    parts.push(`${r.rows.length.toLocaleString()} line${r.rows.length === 1 ? '' : 's'}${r.truncated ? ` (the first ${r.cap.toLocaleString()}: Download holds up to 100,000)` : ''}`);
    parts.push(r.elapsed_ms < 1000 ? `${r.elapsed_ms} ms` : `${(r.elapsed_ms / 1000).toFixed(1)} s`);
  }
  $('res-meta').innerHTML = esc(parts.join(' · ')) + extraHtml;
}
function setBusy(on) {
  $('results').classList.toggle('busy', on);
  setMeta(on ? ' <span class="busy"><i class="bi bi-arrow-repeat"></i> updating…</span>' : '');
}
function scheduleRun(ms = 650) {
  if (!S) return;
  clearTimeout(runTimer);
  runTimer = setTimeout(() => runNow(false), ms);
}
function resetResults() {
  if (chart) { chart.dispose(); chart = null; }
  model = null; $('res-meta').innerHTML = '';
  $('results').innerHTML = '<div class="rc-empty">The answer appears here.</div>';
}
const runBlockers = () => !S ? ['nothing'] : handMode() ? (String(D().sql || '').trim() ? [] : ['the SQL']) : (D().dataset ? [] : ['what to count']);
// asked: the conditions marked "ask me each time it runs" are asked for (on opening a saved
// report, on Run again, Ctrl+Enter); runs as the sentence changes use their saved values.
async function runNow(asked) {
  const blockers = runBlockers();
  if (blockers.length) { if (asked) toast('Before it can run: ' + blockers.join(', ') + '.'); else resetResults(); return; }
  const ask = D().ask.filter(id => FIELD[id] && inDataset(FIELD[id]));
  if (!asked || !ask.length || handMode()) return run(D());
  await Promise.all(ask.map(id => FIELD[id]).filter(f => f.type === 'multi' && !Array.isArray(f.options))
    .map(f => cachedOptions(f).catch(() => [])));
  const temp = clone(D());
  const fieldHtml = id => `<span class="label">${esc(id === WHEN ? 'Period (' + FIELD[id].label + ')' : FIELD[id].label)}</span>${control(FIELD[id], temp.criteria[id] || {}, 'ask')}`;
  $('modal').innerHTML = `<div class="modal-back"><div class="modal" role="dialog" aria-modal="true" aria-label="Before it runs"><div class="panel-head"><h2>Before it runs</h2></div><div class="panel-body" id="ask-body">${
    ask.map(id => `<div class="field" data-ask-f="${id}">${fieldHtml(id)}</div>`).join('')}
    <div class="rc-inl" style="justify-content:flex-end"><button class="btn" id="ask-cancel" type="button">Use the saved values</button><button class="btn primary" id="ask-go" type="button">Run</button></div></div></div></div>`;
  const body = $('ask-body');
  body.addEventListener('click', e => {
    const b = e.target.closest('[data-act]'); if (!b || !applyClick(temp.criteria, b)) return;
    if (b.dataset.act === 'chip') b.setAttribute('aria-pressed', String((temp.criteria[b.dataset.f].values || []).includes(JSON.parse(b.dataset.v))));
    else body.querySelector(`[data-ask-f="${b.dataset.f}"]`).innerHTML = fieldHtml(b.dataset.f);
  });
  body.addEventListener('input', e => applyInput(temp.criteria, e.target));
  body.addEventListener('change', e => applyInput(temp.criteria, e.target));
  $('ask-cancel').onclick = () => { closeModal(); run(D()); };
  $('ask-go').onclick = () => { closeModal(); run(temp); };
}
async function run(defn) {
  const seq = ++runSeq;
  clearTimeout(runTimer);
  setBusy(true);
  try {
    const r = await api('/worklists/api/run', {definition: defn});
    if (seq !== runSeq || !S) return;                 // a newer run is under way, or the report closed
    lastResult = r;
    renderResults();
  } catch (e) {
    if (seq !== runSeq || !S) return;
    lastResult = null; model = null; if (chart) { chart.dispose(); chart = null; }
    $('results').innerHTML = `<div class="rc-note bad">${esc(e.message)}</div>`;
  } finally {
    if (seq === runSeq && S) setBusy(false);
  }
}
$('btn-run').onclick = () => runNow(true);

// ── The answer: views, pivot table, chart, list ──────────────────────────────
function renderViews() {
  const hand = handMode(), has = !!ds(), v = has ? showValue() : null;
  $('rc-views').hidden = hand || !has;
  if (!hand && has) {
    $('rc-views').innerHTML = VIEWS.map(([k, l, i]) => `<button type="button" data-view="${k}" aria-pressed="${v === k}"><i class="bi ${i}" aria-hidden="true"></i> ${
      k === 'list' ? 'List of ' + esc(dsWords().nouns) : l}</button>`).join('');
  }
  const g = has && !hand ? groupings() : [], show = !hand && has && v === 'table' && g.length >= 2;
  $('rc-across-box').hidden = !show;
  if (show) { $('rc-across').checked = !!D().output.cols; $('rc-across-label').textContent = `Put “${byLabel(g[g.length - 1])}” across the top`; }
}
$('rc-views').addEventListener('click', e => {
  const b = e.target.closest('[data-view]'); if (!b) return;
  const v = b.dataset.view, o = D().output, g = groupings();
  if (v === 'table' || v === 'list') o.kind = v; else { o.kind = 'chart'; o.chart = v; }
  if (v !== 'list') applyGroupings(g);
  outputChanged();
});
$('rc-across').addEventListener('change', e => { ACROSS = e.target.checked; applyGroupings(groupings()); outputChanged(); });
const fmt = v => v == null ? '' : (typeof v === 'number' ? (Number.isInteger(v) ? v.toLocaleString() : v.toLocaleString(undefined, {maximumFractionDigits: 2})) : String(v));
const cmp = (a, b) => (a == null) - (b == null) || (typeof a === 'number' && typeof b === 'number' ? a - b : String(a).localeCompare(String(b), undefined, {numeric: true}));
function renderResults() {
  const r = lastResult; if (!r) return;
  setMeta();
  if (chart) { chart.dispose(); chart = null; }
  if (!r.rows.length) {
    model = null;
    const f = FIELD[WHEN], hours = f && !handMode() && windowsOf(D().criteria[WHEN]).some(w => w.from || w.to);
    const timeless = hours && (f.time_known || []).includes(D().dataset);
    $('results').innerHTML = `<div class="rc-empty">Nothing matches. Try a longer period or fewer conditions.${timeless
      ? ` With hours set (${esc(atText(windowsOf(D().criteria[WHEN])))}), ${esc(dsWords().nouns)} that PACS sent without a time are left out.` : ''}</div>`;
    return;
  }
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
      <button type="button" class="danger" data-lhide="${c.id}" title="Remove this column" aria-label="Remove this column">✕</button></span></th>`;
  }).join('');
  return `<div class="rc-table-wrap"><table class="rc-table"><thead><tr>${head}</tr></thead><tbody>${
    r.rows.map(row => `<tr>${row.map(v => `<td class="${typeof v === 'number' ? '' : 'txt'}">${esc(fmt(v))}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
}
// The run's rows as a pivot: one key per combination of the row groupings, one series per
// number, or per value of the grouping across the top.
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
  let head = m.nr ? m.rowDims.map((d, di) => `<th class="txt" data-psort="d${di}">${esc(d.label)}${arrow('d' + di)}${tool('pdim', di, m.nr, 'Remove this breakdown')}</th>`).join('') : '<th class="txt"></th>';
  head += m.series.map((s, j) => `<th data-psort="s${j}"${m.split && j === 0 ? ` title="${esc(m.colDim.label)} · ${esc(m.measure.label)}"` : ''}>${esc(s.label)}${arrow('s' + j)}${
    !m.split && m.series.length > 1 ? tool('pser', j, m.series.length, 'Remove this number') : ''}</th>`).join('');
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
    <p class="rc-hint mt-2">Click a column title to sort, any number to see the ${esc(dsWords().nouns)} behind it.${m.split ? ` Across the top: ${esc(m.colDim.label)} · ${esc(m.measure.label)}.` : ''}</p>`;
}
function chartColors() { const cs = getComputedStyle(document.body); return {fg: cs.getPropertyValue('--rc-muted').trim(), line: cs.getPropertyValue('--rc-line').trim()}; }
const PALETTE = ['#60a5fa', '#34d399', '#fbbf24', '#f472b6', '#a78bfa', '#fb923c', '#22d3ee', '#f87171', '#a3e635', '#e879f9'];
function renderChart(type) {
  const m = model;
  if (type === 'heatmap' && !m.split) { $('results').innerHTML = '<div class="rc-note info">A heatmap needs two things after “for each”, e.g. Weekday and Hour.</div>'; return; }
  if (!window.echarts) { $('results').innerHTML = '<div class="rc-note bad">The chart library could not be loaded. Show it as a Table.</div>'; return; }
  $('results').innerHTML = `<div class="rc-chart" id="chart"></div><p class="rc-hint mt-2">Click a bar, a point, a slice or a cell to see the ${esc(dsWords().nouns)} behind it.</p>`;
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
      // slanted labels (many bars) say what they are; the axis title would sit on them
      xAxis: {type: 'category', data: cats, axisLabel: {color: fg, rotate: cats.length > 8 ? 35 : 0}, axisLine: {lineStyle: {color: line}},
        name: cats.length > 8 ? '' : m.rowDims.map(d => d.label).join(' · '), nameLocation: 'middle', nameGap: 44, nameTextStyle: {color: fg}},
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

// Work in the answer: sort, move and remove columns, open the rows behind a number.
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
    markDirty(); scheduleCompile(0); return run(D());
  }
  const pdm = t.closest('[data-pdim-move]');
  if (pdm) { const i = +pdm.dataset.i, to = i + (+pdm.dataset.pdimMove); [o.rows[i], o.rows[to]] = [o.rows[to], o.rows[i]]; return outputChanged(); }
  const pdr = t.closest('[data-pdim-rm]'); if (pdr) { o.rows.splice(+pdr.dataset.pdimRm, 1); return outputChanged(); }
  const psm = t.closest('[data-pser-move]');
  if (psm) { const i = +psm.dataset.i, to = i + (+psm.dataset.pserMove); [o.measures[i], o.measures[to]] = [o.measures[to], o.measures[i]]; return outputChanged(); }
  const psr = t.closest('[data-pser-rm]');
  if (psr) { if (o.measures.length === 1) return; o.measures.splice(+psr.dataset.pserRm, 1); return outputChanged(); }
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
// the total), and series j when the last grouping is across the top.
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
    $('drill-meta').textContent = `${r.rows.length.toLocaleString()} line${r.rows.length === 1 ? '' : 's'}${r.truncated ? ` (the first ${r.cap.toLocaleString()})` : ''}`;
    $('drill-body').innerHTML = r.rows.length ? listTable(r, false) + `<p class="rc-hint">The columns are those of the list: show the answer as “List of ${esc(dsWords().nouns)}” to change them.</p>` : '<div class="empty">No rows.</div>';
  } catch (e) { if ($('drill-body')) $('drill-body').innerHTML = `<div class="note bad">${esc(e.message)}</div>`; }
}

$('btn-csv').onclick = async () => {
  const blockers = runBlockers();
  if (blockers.length) { toast('Before it can be downloaded: ' + blockers.join(', ') + '.'); return; }
  try {
    const name = S.name || suggestName();
    const r = await fetch('/worklists/api/csv', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({definition: D(), name})});
    if (!r.ok) { const d = await r.json().catch(() => ({})); throw new Error(d.error || 'the download failed'); }
    const blob = await r.blob(), a = document.createElement('a');
    a.href = URL.createObjectURL(blob); a.download = name + '.csv'; a.click(); URL.revokeObjectURL(a.href);
    if (r.headers.get('X-Truncated') === '1') toast('The download stops at 100,000 lines.');
  } catch (e) { toast(e.message); }
};

// ── Everything on the report screen ──────────────────────────────────────────
function renderAll() {
  renderTop(); renderSentence(); renderViews(); renderHandNote(); renderSQL();
}
document.addEventListener('keydown', e => {
  if (!(e.ctrlKey || e.metaKey) || !S || $('rc-view').hidden) return;
  if (e.key.toLowerCase() === 's') { e.preventDefault(); save(); }
  if (e.key === 'Enter' && !e.target.closest('textarea')) { e.preventDefault(); runNow(true); }
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
    <td><div class="rc-inl">
      <button class="btn small" type="button" data-act="edit" data-id="${s.id}">Edit</button>
      <button class="btn small" type="button" data-act="send" data-id="${s.id}"${SC.ready ? '' : ' disabled'}>Send now</button>
      <button class="btn small" type="button" data-act="runs" data-id="${s.id}">History</button>
      <button class="btn small danger" type="button" data-act="del" data-id="${s.id}">Delete</button>
    </div></td></tr>`).join('');
  return `${SC.ready ? '' : `<div class="note warn">Nothing can be sent until the mail server is set in <a href="${esc(MAIL_SERVER_URL)}">Administration &gt; Email Server</a>.</div>`}
    <div class="rc-inl">
      <button class="btn small primary" type="button" data-act="new"${SC.ready ? '' : ' disabled'}>New schedule</button>
      <a class="btn small" href="${esc(MAIL_SERVER_URL)}"><i class="bi bi-gear"></i> Mail server</a>
    </div>
    ${rows ? `<div class="table-wrap"><table><thead><tr><th>Schedule</th><th>When (hospital time)</th><th>Recipients</th><th>Last run</th><th></th></tr></thead><tbody>${rows}</tbody></table></div>`
      : '<div class="empty">No scheduled emails yet.</div>'}`;
}
function scFormView() {
  const s = SC.edit, chosen = (s.worklists || []).map(w => w.id);
  const pool = LIB.map(w => ({id: w.id, name: w.name, folder: w.visibility === 'shared' ? w.folder : 'Only me'}))
    .concat((s.worklists || []).filter(w => !LIB.some(x => x.id === w.id)).map(w => ({...w, folder: 'Another admin\'s'})));
  return `<div class="def-grid">
      <div class="field wide"><label for="sc-name">Name</label><input class="input" id="sc-name" value="${esc(s.name || '')}" placeholder="e.g. Monday morning pack"><span class="hint">Becomes the subject of the email.</span></div>
      <div class="field wide"><span class="label">Reports</span><div class="checks pick" id="sc-pick">${pool.map(w => `<label><input type="checkbox" value="${w.id}"${chosen.includes(w.id) ? ' checked' : ''}><span>${esc(w.name)} <span class="hint">${esc(w.folder)}</span></span></label>`).join('') || '<span class="hint">Save a report first.</span>'}</div>
        <span class="hint">Each email runs the last saved version, with its dates worked out on the day it goes. Conditions marked “ask me each time it runs” use their saved values. A chart arrives as its table.</span></div>
      <div class="field wide"><label for="sc-to">Recipients</label><textarea class="input" id="sc-to" rows="2" placeholder="one@laumc.com, two@laumc.com">${esc((s.recipients || []).join(', '))}</textarea>
        <span class="hint">The attachments can hold patient data, so only addresses at ${esc(SC.mail.smtp_allowed_domains)} are accepted.</span></div>
      <div class="field"><label for="sc-freq">How often</label><select class="input" id="sc-freq">${[['daily', 'Every day'], ['weekly', 'Every week'], ['monthly', 'Every month']].map(([v, l]) => `<option value="${v}"${(s.frequency || 'daily') === v ? ' selected' : ''}>${l}</option>`).join('')}</select></div>
      <div class="field"><label for="sc-time">At (hospital time)</label><input class="input" type="time" id="sc-time" value="${esc(s.send_time || '07:00')}"></div>
      <div class="field" id="sc-weekly"><label for="sc-weekday">On</label><select class="input" id="sc-weekday">${WEEKDAYS.map((d, i) => `<option value="${i}"${(s.weekday ?? 0) === i ? ' selected' : ''}>${d}</option>`).join('')}</select></div>
      <div class="field" id="sc-monthly"><label for="sc-day">On day</label><input class="input" type="number" min="1" max="31" id="sc-day" value="${esc(s.month_day ?? 1)}"><span class="hint">A shorter month sends on its last day.</span></div>
      <label class="field wide" style="flex-direction:row;gap:8px;align-items:center;cursor:pointer"><input type="checkbox" id="sc-on"${s.enabled === false ? '' : ' checked'}> Active (untick to pause it)</label>
    </div>
    <div class="rc-inl" style="justify-content:flex-end"><button class="btn" type="button" data-act="back">Cancel</button><button class="btn primary" type="button" data-act="save">Save schedule</button></div>`;
}
function scRunsView() {
  const rows = SC.runs.list.map(r => `<tr>
    <td>${esc(r.started_at)}<br><span class="hint">${r.started_by ? 'Sent by ' + esc(r.started_by) : 'On schedule'}</span></td>
    <td><span class="run-${esc(r.status)}">${esc(RUN_WORDS[r.status] || r.status)}</span>${r.error ? `<br><span class="hint">${esc(r.error)}</span>` : ''}</td>
    <td>${r.detail.map(scEntry).join('<br>')}</td>
    <td>${r.recipients.map(esc).join('<br>')}</td></tr>`).join('');
  return `<strong>${esc(SC.runs.name)}</strong>
    ${rows ? `<div class="table-wrap"><table><thead><tr><th>When</th><th>Result</th><th>Reports</th><th>Sent to</th></tr></thead><tbody>${rows}</tbody></table></div>` : '<div class="empty">Not sent yet.</div>'}
    <div class="rc-inl" style="justify-content:flex-end"><button class="btn" type="button" data-act="back">Back</button></div>`;
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
function schedOpen() { document.addEventListener('keydown', scKey); scOpen(); }
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

// ── Start ────────────────────────────────────────────────────────────────────
(async () => {
  try { CAT = await api('/worklists/api/catalog'); } catch (e) { $('rc-start').hidden = false; $('rc-tiles').innerHTML = `<div class="rc-note bad">${esc(e.message)}</div>`; return; }
  CAT.fields.forEach(f => FIELD[f.id] = f);
  try { await loadLibrary(); } catch (e) { toast(e.message); }
  const wanted = +(new URLSearchParams(location.search).get('id') || 0);
  if (!wanted || !(await open(wanted))) showStart();
})();
})();
