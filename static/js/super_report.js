// static/js/super_report.js — the Super Report page (templates/super_report.html), moved out of
// the template (review of 2026-11-20). Loaded with defer, after the shared panel's jQuery and
// select2 (filter_panel_head() in the head). Functions stay global: the page's onclick
// handlers call them.

// ── The shared left panel (_report_filter_panel.html) ────────────────
// One multi-select dropdown per filter, named as /viewer/super-report reads them,
// each with its "Apply filter" tick (<name>_on). Their options come from
// /viewer/super-report/filters. Generate runs the report here instead of reloading.
const $start = () => document.getElementById('rfp-start');
const $end   = () => document.getElementById('rfp-end');
function panelSelects() { return [...document.querySelectorAll('#mainForm select[multiple]')]; }
function tickOf(sel)    { return document.querySelector(`#mainForm input[name="${sel.name}_on"]`); }
function labelOf(sel)   { const l = document.querySelector(`label[for="${sel.id}"]`); return l ? l.textContent.trim() : sel.name; }
function picked(sel)    { return [...sel.selectedOptions].map(o => o.value); }
function applied(sel)   { const t = tickOf(sel); return (!t || t.checked) ? picked(sel) : []; }
function setPicked(sel, vals) {
    // values a saved preset holds that the list does not offer (yet) still show
    vals.forEach(v => {
        if (![...sel.options].some(o => o.value === v)) sel.add(new Option(v, v));
    });
    $(sel).val(vals).trigger('change');
}

document.getElementById('mainForm').addEventListener('submit', e => {
    e.preventDefault();
    runSuperReport();
});

// ── Dates ─────────────────────────────────────────────────────────
// Opens on the report start date set on the ETL Control Room; the last 30 days
// only when none is set.
if (!$start().value) {
    const prior = new Date(); prior.setDate(prior.getDate() - 30);
    $start().value = prior.toISOString().split('T')[0];
}
if (!$end().value) $end().value = new Date().toISOString().split('T')[0];
updateCmpDates();
[$start(), $end()].forEach(el => el.addEventListener('change', updateCmpDates));

// The single source of the AUTOMATIC comparison window: the same number of days, ending
// the day before start. The server validates whatever is sent (dates, order).
function updateCmpDates() {
    const s = $start().value;
    const e = $end().value;
    if (s) {                                   // the end date cannot go before the start
        $end().min = s;
        if ($end()._flatpickr) $end()._flatpickr.set('minDate', s);
    }
    if (!s || !e) return;
    const ds = new Date(s), de = new Date(e);
    const delta = Math.round((de - ds) / 86400000) + 1;
    if (delta <= 0) {                          // end before start: no comparison window
        document.getElementById('cmp-start').value = '';
        document.getElementById('cmp-end').value   = '';
        document.getElementById('cmp-auto-label').textContent = 'end is before start';
        return;
    }
    const ce = new Date(ds); ce.setDate(ce.getDate() - 1);
    const cs = new Date(ce); cs.setDate(cs.getDate() - delta + 1);
    document.getElementById('cmp-end').value   = ce.toISOString().split('T')[0];
    document.getElementById('cmp-start').value = cs.toISOString().split('T')[0];
    document.getElementById('cmp-auto-label').textContent = `auto · ${delta}d prior`;
}

// ── Load the dropdowns' options ───────────────────────────────────
// A failure is said on the page, at the top of the saved presets (it was in the console only),
// and "Clear all" then tries again.
let _filtersFailed = false;
function showFiltersNotice() {
    const old = document.getElementById('sr-filters-error');
    if (old) old.remove();
    const list = document.getElementById('presets-list');
    if (_filtersFailed && list) list.insertAdjacentHTML('afterbegin',
        '<p id="sr-filters-error" role="alert" class="text-[10px] font-bold mb-1" style="color:#f87171;">' +
        'Could not load filter options — click Clear all to retry</p>');
}

async function loadFilters() {
    try {
        const res  = await fetch('/viewer/super-report/filters');
        const data = await res.json();
        if (!res.ok || data.error) throw new Error(data.error || `HTTP ${res.status}`);
        const aeMap      = data.ae_modality_map    || {};
        const stationMap = data.ae_station_map     || {};
        const procMap    = data.procedure_name_map || {};
        panelSelects().forEach(sel => {
            const chosen = new Set(picked(sel));
            sel.innerHTML = (data[sel.name] || []).map(v => {
                let label = v;
                if (sel.name === 'storing_ae') {
                    label = stationMap[v] ? `${stationMap[v]} (${v})` : aeMap[v] ? `${v} (${aeMap[v]})` : v;
                } else if (sel.name === 'procedure_code' && procMap[v]) {
                    label = `${procMap[v]} (${v})`;
                }
                return `<option value="${escHtml(v)}"${chosen.has(v) ? ' selected' : ''}>${escHtml(label)}</option>`;
            }).join('');
            if ($(sel).data('select2')) $(sel).select2('destroy');
            $(sel).select2({ width: '100%' });
        });
        _filtersFailed = false;
    } catch(e) {
        _filtersFailed = true;
        console.error('Filter load failed', e);
    }
    showFiltersNotice();
}

function clearFilters() {
    panelSelects().forEach(sel => $(sel).val(null).trigger('change'));
    document.getElementById('f-has_report').value = '';
    document.getElementById('age-min').value = '';
    document.getElementById('age-max').value = '';
    if (_filtersFailed) loadFilters();                       // the notice's "retry"
}

// ── Build query string ────────────────────────────────────────────
function buildQS() {
    const p = new URLSearchParams();
    p.set('start',     $start().value);
    p.set('end',       $end().value);
    p.set('cmp_start', document.getElementById('cmp-start').value);
    p.set('cmp_end',   document.getElementById('cmp-end').value);

    // a dropdown counts only while its "Apply filter" tick is on
    panelSelects().forEach(sel => applied(sel).forEach(v => p.append(sel.name, v)));

    const hr   = document.getElementById('f-has_report').value;
    const amin = document.getElementById('age-min').value;
    const amax = document.getElementById('age-max').value;
    if (hr)   p.set('has_report', hr);
    if (amin) p.set('age_min', amin);
    if (amax) p.set('age_max', amax);
    return p.toString();
}

// ── Filter Banner ─────────────────────────────────────────────────
function renderFilterBanner() {
    const el = document.getElementById('filter-banner');
    if (!el) return;

    const start   = $start().value;
    const end     = $end().value;
    const cmpS    = document.getElementById('cmp-start').value;
    const cmpE    = document.getElementById('cmp-end').value;
    const ageMin  = document.getElementById('age-min').value;
    const ageMax  = document.getElementById('age-max').value;
    const hasRep  = document.getElementById('f-has_report').value;

    // Collect active filter chips
    const chips = [];

    panelSelects().forEach(sel => {
        applied(sel).forEach(v => chips.push({ label: labelOf(sel), value: v }));
    });

    if (hasRep)   chips.push({ label: 'Report Status', value: hasRep === 'Yes' ? 'Has Report' : 'No Report' });
    if (ageMin)   chips.push({ label: 'Age ≥', value: ageMin });
    if (ageMax)   chips.push({ label: 'Age ≤', value: ageMax });

    const isFiltered = chips.length > 0;
    el.className = 'filter-banner' + (isFiltered ? ' filtered' : '');

    // Date row (every value escaped: the chips carry values from the database)
    let html = `<div class="fb-row">
        <span class="fb-filter-label">Period</span>
        <span class="fb-date">${escHtml(start)}</span>
        <span class="fb-sep">→</span>
        <span class="fb-date">${escHtml(end)}</span>`;
    if (cmpS && cmpE) {
        html += `<span class="fb-sep" style="margin-left:6px;">vs</span>
        <span class="fb-cmp">${escHtml(cmpS)} → ${escHtml(cmpE)}</span>`;
    }
    html += `</div>`;

    // Filter chips row
    if (isFiltered) {
        html += `<div class="fb-row">
            <span class="fb-filter-label">Filters</span>`;
        chips.forEach(c => {
            const chip = escHtml(`${c.label}: ${c.value}`);
            html += `<span class="fb-chip" title="${chip}">${chip}</span>`;
        });
        html += `</div>`;
        html += `<div class="fb-note warn">&#9888; Analysis scoped to the filters above — totals exclude data outside this selection.</div>`;
    } else {
        html += `<div class="fb-note">No filters applied — showing all data for the selected period.</div>`;
    }

    el.innerHTML = html;
    el.style.display = '';
}

// ── Run ───────────────────────────────────────────────────────────
async function runSuperReport() {
    const start = $start().value;
    const end   = $end().value;
    if (!start || !end) { alert('Select a date range'); return; }
    if (end < start)    { alert('The end date is before the start date'); return; }

    document.getElementById('super-empty').classList.add('hidden');
    document.getElementById('super-results').classList.add('hidden');
    document.getElementById('super-loading').classList.remove('hidden');
    document.querySelector('#mainForm .rfp-generate').disabled = true;

    const steps = ['Querying database','Joining tables','Aggregating metrics','Writing summary'];
    let si = 0;
    const iv = setInterval(() => {
        document.getElementById('loading-step').textContent = steps[si++ % steps.length];
    }, 1600);

    try {
        const res  = await fetch(`/viewer/super-report?${buildQS()}`);
        const data = await res.json();
        clearInterval(iv);
        document.getElementById('super-loading').classList.add('hidden');
        if (data.error) {
            alert('Error: ' + data.error);
            document.getElementById('super-empty').classList.remove('hidden');
        } else {
            renderFilterBanner();
            render(data);
            document.getElementById('super-results').classList.remove('hidden');
            loadSites();
        }
    } catch(e) {
        clearInterval(iv);
        document.getElementById('super-loading').classList.add('hidden');
        document.getElementById('super-empty').classList.remove('hidden');
        alert('Network error: ' + e.message);
    }
    document.querySelector('#mainForm .rfp-generate').disabled = false;
}

// ── Sites side by side, per modality (item 13b) ───────────────────
async function loadSites() {
    const card = document.getElementById('super-sites-card');
    if (!card) return;                       // the account holds one site
    try {
        const d = await fetch(`/viewer/super-report/sites?${buildQS()}`).then(r => r.json());
        if (d.error || !d.sites || d.sites.length < 2) { card.style.display = 'none'; return; }
        renderSites(d);
        card.style.display = '';
    } catch (e) { card.style.display = 'none'; }
}

function renderSites(d) {
    const fmt = v => (v == null ? '—' : Number(v).toLocaleString('en-US'));
    const head1 = d.sites.map(s => `<th colspan="4" style="text-align:center;padding:6px 8px;border-bottom:1px solid #28332e;color:#e2e8f0">${escHtml(s.code)}<span style="color:#64748b;font-weight:600"> · ${escHtml(s.name)}</span></th>`).join('');
    const head2 = d.sites.map(() => ['Studies', 'Patients', 'Median TAT (min)', 'GB']
        .map(h => `<th style="text-align:right;padding:4px 8px;color:#64748b;font-size:9px;text-transform:uppercase">${h}</th>`).join('')).join('');
    const cell = v => `<td style="text-align:right;padding:4px 8px;font-variant-numeric:tabular-nums">${v}</td>`;
    const rows = d.modalities.map(m => `<tr style="border-top:1px solid #1d2522"><td style="padding:4px 8px;font-weight:800;color:#e2e8f0">${escHtml(m.modality)}</td>` +
        d.sites.map(s => {
            const r = m.by_site[s.code] || {};
            return cell(fmt(r.studies || 0)) + cell(fmt(r.patients || 0)) + cell(fmt(r.median_tat_min)) + cell(fmt(r.gb || 0));
        }).join('') + '</tr>').join('');
    const total = `<tr style="border-top:2px solid #28332e;font-weight:900"><td style="padding:6px 8px">Total</td>` +
        d.sites.map(s => {
            const t = d.totals[s.code] || {};
            return cell(fmt(t.studies || 0)) + cell(fmt(t.patients || 0)) + cell('') + cell(fmt(t.gb || 0));
        }).join('') + '</tr>';
    document.getElementById('super-sites').innerHTML =
        `<table style="width:100%;border-collapse:collapse;font-size:11px;min-width:520px">` +
        `<thead><tr><th></th>${head1}</tr><tr><th style="text-align:left;padding:4px 8px;color:#64748b;font-size:9px;text-transform:uppercase">Modality</th>${head2}</tr></thead>` +
        `<tbody>${rows || ''}${total}</tbody></table>` +
        (d.modalities.length ? '' : '<p class="sr-muted" style="font-size:10px;padding:8px">No study on either site for these dates and filters.</p>');
}

// ── Render ────────────────────────────────────────────────────────
function render(d) {
    const cur = d.current, prev = d.previous;

    document.getElementById('period-label').textContent =
        `${d.delta_days} days · comparing to ${d.cmp_start} → ${d.cmp_end}`;

    // Split Clinical Insights out of narrative before typewriter renders it
    const insightsSection = (d.narrative || []).find(s => s.title === 'Clinical Insights');
    const narrativeOnly   = (d.narrative || []).filter(s => s.title !== 'Clinical Insights');
    renderNarrative(narrativeOnly);
    renderInsights(insightsSection);

    // KPIs
    const ae  = (cur.ae_ops  || {});
    const pae = (prev.ae_ops || {});
    const kpis = [
        {label:'Studies',    cur:cur.kpis.total_studies,  prev:prev.kpis.total_studies,  color:'#60a5fa', icon:'bi-journal-medical'},
        {label:'Patients',   cur:cur.kpis.total_patients, prev:prev.kpis.total_patients, color:'#34d399', icon:'bi-people'},
        {label:'Images',     cur:cur.kpis.total_images,   prev:prev.kpis.total_images,   color:'#a78bfa', icon:'bi-images'},
        {label:'Storage GB', cur:cur.storage.total_gb,    prev:prev.storage.total_gb,    color:'#2dd4bf', icon:'bi-hdd-stack'},
        {label:'Orders',     cur:cur.orders.total,        prev:prev.orders.total,        color:'#f97316', icon:'bi-clipboard2-pulse'},
        {label:'Busiest AE', cur:ae.busiest_ae,           prev:null,                     color:'#818cf8', icon:'bi-broadcast',    isText:true, sub: ae.busiest_cnt ? fmt(ae.busiest_cnt)+' studies' : ''},
        {label:'Idle AE',    cur:ae.idle_ae,              prev:null,                     color:'#f59e0b', icon:'bi-pause-circle', isText:true, sub: (ae.idle_cnt !== undefined && ae.idle_cnt !== null) ? fmt(ae.idle_cnt)+' studies' : ''},
        {label:'ER Delayed', cur:ae.er_delayed,           prev:pae.er_delayed,           color:'#f87171', icon:'bi-activity',
         title:'Non-ER studies with TAT above P75 on days with concurrent ER volume'},
    ];
    document.getElementById('super-kpis').innerHTML = kpis.map(k => {
        let valHtml, badgeHtml;
        if (k.isText) {
            valHtml   = `<div class="text-sm font-black text-white truncate mt-1" title="${escHtml(k.cur||'')}">${escHtml(k.cur||'—')}</div>`;
            badgeHtml = k.sub ? `<span style="font-size:9px;color:#64748b">${k.sub}</span>` : '';
        } else {
            const p = pct(k.cur, k.prev);
            valHtml   = `<div class="text-xl font-black text-white font-mono">${fmt(k.cur)}</div>`;
            badgeHtml = p !== null
                ? `<span style="font-size:9px;font-weight:700;color:${p>=0?'#34d399':'#f87171'}">${p>=0?'+':''}${p}%</span>`
                : '';
        }
        const titleAttr = k.title ? ` title="${escHtml(k.title)}"` : '';
        return `<div class="card kpi-card p-4"${titleAttr} style="border-top-color:${k.color}">
            <div class="flex items-center gap-2 mb-1">
                <i class="bi ${k.icon}" style="color:${k.color};font-size:12px;"></i>
                <span class="text-[9px] font-black uppercase tracking-widest text-slate-500">${k.label}</span>
            </div>
            ${valHtml}
            <div class="mt-1">${badgeHtml}</div>
        </div>`;
    }).join('');

    // Volume
    const v = cur.volume, pv = prev.volume;
    document.getElementById('super-volume').innerHTML =
        row('Studies',   fmt(cur.kpis.total_studies), pct(cur.kpis.total_studies, prev.kpis.total_studies)) +
        row('Avg/day',   fmtD(v.avg_per_day), pct(v.avg_per_day, pv.avg_per_day)) +
        row('Peak day',  v.peak_day||'—', null) +
        row('Peak count',fmt(v.peak_count), null) +
        subSection('Top Modalities', (v.top_modalities||[]).map(m=>row(m.modality, fmt(m.cnt), null)).join(''));

    // Storage
    const s = cur.storage, ps = prev.storage;
    document.getElementById('super-storage').innerHTML =
        row('Total GB',     fmtD(s.total_gb), pct(s.total_gb, ps.total_gb)) +
        row('Avg GB/day',   fmtD(s.avg_gb_per_day), pct(s.avg_gb_per_day, ps.avg_gb_per_day)) +
        row('Total images', fmt(s.total_study_count), null) +
        subSection('By Modality', (s.by_modality||[]).map(m=>row(m.modality, fmtD(m.gb)+' GB', null)).join(''));

    // Physicians
    document.getElementById('super-physicians').innerHTML =
        (cur.physicians||[]).map(p => row(p.physician, fmt(p.cnt), null)).join('') ||
        '<p class="text-slate-600 text-xs">No data</p>';

    // Orders
    const o = cur.orders, po = prev.orders;
    document.getElementById('super-orders').innerHTML =
        row('Total', fmt(o.total), pct(o.total, po.total)) +
        row('Fulfilled', fmt(o.fulfilled), null) +
        rowWarn('Fulfillment %', fmtD(o.fulfillment_pct)+'%', parseFloat(o.fulfillment_pct||0) < 85) +
        subSection('By Modality', (o.by_modality||[]).map(m=>row(m.modality, fmt(m.cnt), null)).join(''));

    // Turnaround Time
    const t = cur.tat, pt_ = prev.tat;
    document.getElementById('super-tat').innerHTML =
        row('Overall median', fmtD(t.median_tat_min)+' min', pct(t.median_tat_min, pt_.median_tat_min)) +
        row('Reported', fmt(t.reported_count), null) +
        row('ER median', t.er_reported_count ? fmtD(t.er_median_tat_min)+' min' : '—',
            (t.er_reported_count && pt_.er_reported_count) ? pct(t.er_median_tat_min, pt_.er_median_tat_min) : null) +
        row('Non-ER median', t.non_er_reported_count ? fmtD(t.non_er_median_tat_min)+' min' : '—',
            (t.non_er_reported_count && pt_.non_er_reported_count) ? pct(t.non_er_median_tat_min, pt_.non_er_median_tat_min) : null) +
        subSection('Slowest Modalities', (t.by_modality||[]).map(m=>row(m.modality, fmtD(m.median_tat_min)+' min', null)).join('')
            || '<p class="text-slate-600 text-xs">No data</p>');

    // Critical Findings (CRN)
    const c = cur.crn, pc_ = prev.crn;
    document.getElementById('super-crn').innerHTML =
        (!d.crn_enabled ? '<p style="font-size:9px;color:#f59e0b;font-weight:700;margin-bottom:6px;">CRN IS OFF — Admin &gt; CRN &gt; Settings</p>'
          : d.crn_dry_run ? '<p style="font-size:9px;color:#f59e0b;font-weight:700;margin-bottom:6px;">DRY-RUN MODE — no real SMS sent yet</p>' : '') +
        row('Critical findings', fmt(c.critical_count), pct(c.critical_count, pc_.critical_count)) +
        row('CRNs', fmt(c.total_notifications), null) +
        row('Acknowledged', fmt(c.acknowledged), null) +
        rowWarn('Went overdue', fmt(c.overdue), c.overdue > 0) +
        rowWarn('Still open', fmt(c.open), c.open > 0) +
        row('Median time to ack', c.median_ack_min !== null ? fmtD(c.median_ack_min)+' min' : '—', null);

    // Device Utilization
    const u = cur.utilization, pu = prev.utilization;
    document.getElementById('super-utilization').innerHTML = (u && u.available)
        ? row('Avg utilization', fmtD(u.avg_utilization)+'%', (pu && pu.available) ? pct(u.avg_utilization, pu.avg_utilization) : null) +
          rowWarn('Over 85% (stressed)', fmt(u.high_stress_count), u.high_stress_count > 0) +
          row('Under 30% (idle)', fmt(u.low_util_count), null) +
          row('No schedule data', fmt(u.no_schedule_count), null) +
          subSection('Busiest Devices', (u.busiest||[]).map(b=>row(b.ae, fmtD(b.avg)+'%', null)).join('')
              || '<p class="text-slate-600 text-xs">No data</p>')
        : '<p class="text-slate-600 text-xs">No utilization data for this period.</p>';

    // Demographics
    const dm = cur.demographics, pdm = prev.demographics;
    document.getElementById('super-demographics').innerHTML = [
        {label:'Male',       val:fmt(dm.male),       chg:pct(dm.male,pdm.male),       color:'#60a5fa'},
        {label:'Female',     val:fmt(dm.female),     chg:pct(dm.female,pdm.female),   color:'#f472b6'},
        {label:'Inpatient',  val:fmt(dm.inpatient),  chg:pct(dm.inpatient,pdm.inpatient),  color:'#f97316'},
        {label:'Outpatient', val:fmt(dm.outpatient), chg:pct(dm.outpatient,pdm.outpatient),color:'#34d399'},
    ].map(c=>`<div class="bg-[#141a17] rounded-xl p-4 border border-[#28332e]">
        <p class="text-[10px] font-black uppercase tracking-widest mb-1" style="color:${c.color}">${c.label}</p>
        <p class="text-2xl font-black text-white font-mono">${c.val}</p>
        ${c.chg!==null?`<p style="font-size:9px;font-weight:700;color:${c.chg>=0?'#34d399':'#f87171'}">${c.chg>=0?'+':''}${c.chg}%</p>`:''}`
    +`</div>`).join('');

    // Patient class breakdown
    const pcb = dm.pc_breakdown || [];
    const pcTotal = pcb.reduce((s, r) => s + (r.cnt||0), 0);
    document.getElementById('super-pc-breakdown').innerHTML = pcb.length
      ? pcb.map(r => {
          const pct_ = pcTotal ? Math.round(r.cnt / pcTotal * 100) : 0;
          return `<div class="flex items-center gap-2 text-xs">
            <span class="font-mono font-black text-white w-20 shrink-0">${escHtml(r.class)}</span>
            <div class="flex-1 bg-[#28332e] rounded-full h-1.5">
              <div class="h-1.5 rounded-full bg-[#34d399]" style="width:${pct_}%"></div>
            </div>
            <span class="text-slate-400 w-16 text-right shrink-0">${fmt(r.cnt)} <span class="text-slate-600">(${pct_}%)</span></span>
          </div>`;
        }).join('')
      : '<p class="text-slate-600 text-xs">No patient class data</p>';

    // Radiologist matrix
    srRadMatrixInit(cur.rad_matrix || {});
    // Residents by their wet reads (PACS audit trail, item 8)
    srWetReads(cur.resident_wetreads || {});
    // Patient schedule lead time (Report 40's figures)
    srLeadTime(cur.lead_time, prev.lead_time, d);
}

// ── Patient schedule lead time (Report 40's definitions) ──────────
function srDays(v) {
    if (v === null || v === undefined) return '—';
    if (Number(v) === 0) return 'Same day';
    return `${_round(Number(v), 1)} day${Number(v) === 1 ? '' : 's'}`;
}
// A change badge. worseUp: a rise is bad news (longer waits, more cancellations) and reads red;
// neutral: no colour judgement (a share of same-day bookings is neither good nor bad).
function srChange(text, delta, {worseUp = false, neutral = false} = {}) {
    if (delta === null || delta === undefined || !Number.isFinite(delta)) return '';
    if (delta === 0) text = 'no change';
    const color = neutral || delta === 0 ? '#64748b' : ((delta > 0) === worseUp ? '#f87171' : '#34d399');
    return `<span style="font-size:9px;font-weight:700;color:${color}">${text}</span>`;
}
function srPts(a, b) {
    return (a === null || a === undefined || b === null || b === undefined) ? null : _round(Number(a) - Number(b), 1);
}
function srLeadTime(lt, plt, d) {
    const card = document.getElementById('super-leadtime-card');
    if (!lt || !lt.kpi) { card.style.display = 'none'; return; }
    card.style.display = '';
    const k = lt.kpi, pk = (plt && plt.kpi) || {}, ahead = lt.ahead_days || 7;
    const pctTxt = v => (v === null || v === undefined) ? '—' : `${_round(Number(v), 1)}%`;
    const ptsBadge = (a, b, opt) => { const p = srPts(a, b); return srChange(`${p > 0 ? '+' : ''}${p} pts`, p, opt); };
    const medDelta = srPts(k.median_days, pk.median_days);
    const tiles = [
        {label: 'Appointments', val: fmt(k.exams), badge: (() => { const p = pct(k.exams, pk.exams); return srChange(`${p > 0 ? '+' : ''}${p}%`, p, {neutral: true}); })(), color: '#38bdf8'},
        {label: 'Median lead time', val: srDays(k.median_days), badge: srChange(`${medDelta > 0 ? '+' : ''}${medDelta} day${Math.abs(medDelta) === 1 ? '' : 's'}`, medDelta, {worseUp: true}), color: '#60a5fa'},
        {label: 'Booked the same day', val: pctTxt(k.same_day_pct), badge: ptsBadge(k.same_day_pct, pk.same_day_pct, {neutral: true}), color: '#a78bfa'},
        {label: `Booked ${ahead}+ days ahead`, val: pctTxt(k.ahead_pct), badge: ptsBadge(k.ahead_pct, pk.ahead_pct, {neutral: true}), color: '#2dd4bf'},
        {label: 'Cancelled', val: pctTxt(k.cancelled_pct), badge: ptsBadge(k.cancelled_pct, pk.cancelled_pct, {worseUp: true}), color: '#f87171'},
        {label: 'Scheduled more than once', val: pctTxt(k.rescheduled_pct), badge: ptsBadge(k.rescheduled_pct, pk.rescheduled_pct, {worseUp: true}), color: '#f59e0b'},
    ];
    document.getElementById('super-leadtime-tiles').innerHTML = tiles.map(t => `<div class="bg-[#141a17] rounded-xl p-4 border border-[#28332e]">
        <p class="text-[10px] font-black uppercase tracking-widest mb-1" style="color:${t.color}">${t.label}</p>
        <p class="text-2xl font-black text-white font-mono">${t.val}</p>
        <p style="min-height:12px">${t.badge}</p></div>`).join('');

    // "booked after the appointment" only when there is one
    const buckets = (lt.buckets || []).filter(b => b.key !== 'after' || b.exams), total = buckets.reduce((s, b) => s + (b.exams || 0), 0);
    document.getElementById('super-leadtime-buckets').innerHTML = total ? buckets.map(b => {
        const share = Math.round(b.exams / total * 100), late = b.key === 'after';
        return `<div class="flex items-center gap-2 text-xs">
            <span class="w-44 shrink-0 ${late ? 'text-amber-400' : 'text-slate-300'} font-bold">${escHtml(b.label)}</span>
            <div class="flex-1 bg-[#28332e] rounded-full h-1.5"><div class="h-1.5 rounded-full" style="width:${share}%;background:${late ? '#f59e0b' : '#38bdf8'}"></div></div>
            <span class="text-slate-400 w-24 text-right shrink-0">${fmt(b.exams)} <span class="text-slate-600">(${share}%)</span></span></div>`;
    }).join('') : '<p class="text-slate-600 text-xs">No RIS appointment in this period.</p>';

    const rows = lt.by_modality || [];
    const th = (s, left) => `<th style="padding:6px 8px;text-align:${left ? 'left' : 'right'};color:#64748b;font-size:9px;text-transform:uppercase;letter-spacing:.05em;white-space:nowrap;">${s}</th>`;
    const td = (s, left, strong) => `<td style="padding:5px 8px;text-align:${left ? 'left' : 'right'};color:${strong ? '#e2e8f0' : '#cbd5e1'};${strong ? 'font-weight:700;' : ''}white-space:nowrap;">${s}</td>`;
    document.getElementById('super-leadtime-mod').innerHTML = rows.length
        ? `<table style="width:100%;border-collapse:collapse;font-size:11px;"><thead style="background:#1a2420;"><tr>${th('Modality', true)}${th('Appointments')}${th('Median')}${th('9 in 10 within')}${th('Same day')}${th(ahead + '+ days')}${th('Cancelled')}</tr></thead><tbody>${
            rows.map(r => `<tr style="border-top:1px solid #1e2a26;">${td(escHtml(r.value), true, true)}${td(fmt(r.exams))}${td(srDays(r.median_days))}${td(srDays(r.p90_days))}${td(pctTxt(r.same_day_pct))}${td(pctTxt(r.ahead_pct))}${td(pctTxt(r.cancelled_pct))}</tr>`).join('')}</tbody></table>`
        : '<p class="text-slate-600 text-xs" style="padding:8px">No data</p>';

    const notes = [`RIS appointments from ${escHtml($start().value || '')} to ${escHtml($end().value || '')}: from the order's creation on the RIS to the appointment, in days. Changes against ${escHtml(d.cmp_start)} → ${escHtml(d.cmp_end)}.`];
    if (k.after) notes.push(`${fmt(k.after)} booked after the appointment (registered afterwards) are counted aside, not in the lead times.`);
    notes.push(lt.modality_filter ? 'The Modality filter applies here; the other filters on the left are the PACS study\'s and do not.'
                                  : 'Of the filters on the left, only Modality applies here (the others are the PACS study\'s).');
    document.getElementById('super-leadtime-note').innerHTML = notes.join(' ');
}

// ── Wet reads per resident ────────────────────────────────────────
let _srWet = {};
function srWetReads(w) {
    _srWet = w || {};
    const card = document.getElementById('super-wetread-card');
    const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c]);
    if (!w || !w.coverage) {                 // the audit trail is not copied on this install
        card.style.display = 'none';
        return;
    }
    card.style.display = '';
    const mods = w.modalities || [], rows = w.residents || [];
    document.getElementById('sr-wet-note').textContent = rows.length
        ? `${w.total.toLocaleString('en-US')} studies wet-read by a resident, each counted once for the first resident's wet read. From the PACS audit trail: studies that reached PACS from ${w.coverage.from}.`
        : `No wet read by a resident in this range (the PACS audit trail covers studies that reached PACS from ${w.coverage.from}).`;
    const th = s => `<th style="padding:6px 8px;text-align:right;color:#64748b;font-size:9px;text-transform:uppercase;letter-spacing:.05em;">${s}</th>`;
    document.getElementById('sr-wet-thead').innerHTML = rows.length
        ? `<tr><th style="padding:6px 8px;text-align:left;color:#64748b;font-size:9px;text-transform:uppercase;">Resident</th>${mods.map(m => th(esc(m))).join('')}${th('Total')}</tr>` : '';
    document.getElementById('sr-wet-tbody').innerHTML = rows.map(r =>
        `<tr style="border-top:1px solid #1e2a26;"><td style="padding:5px 8px;color:#e2e8f0;font-weight:700;">${esc(r.name)}</td>` +
        mods.map(m => `<td style="padding:5px 8px;text-align:right;color:#cbd5e1;">${r.by_modality[m] || ''}</td>`).join('') +
        `<td style="padding:5px 8px;text-align:right;color:#34d399;font-weight:800;">${r.total}</td></tr>`).join('');
}
function srExportWetCsv() {
    const w = _srWet, mods = w.modalities || [], rows = w.residents || [];
    if (!rows.length) return;
    const q = v => '"' + String(v == null ? '' : v).replace(/"/g, '""') + '"';
    const lines = [['Resident', ...mods, 'Total'].map(q).join(','),
                   ...rows.map(r => [r.name, ...mods.map(m => r.by_modality[m] || 0), r.total].map(q).join(','))];
    _srDownloadCsv(lines.join('\n'), `resident_wet_reads_${new Date().toISOString().slice(0, 10)}.csv`);
}

// ── Radiologist Matrix ────────────────────────────────────────────
let _srRadData = {}, _srRadMode = 'mod';

function srRadMatrixInit(radData) {
    _srRadData = radData;
    const card = document.getElementById('super-rad-matrix-card');
    const hasMod  = (radData.by_modality  || []).length > 0;
    const hasAe   = (radData.by_aetitle   || []).length > 0;
    const hasProc = (radData.by_procedure || []).length > 0;
    if (!hasMod && !hasAe && !hasProc) { card.style.display = 'none'; return; }
    card.style.display = '';
    srRadToggle('mod');
    srBuildProcTable();
}

window.srRadToggle = function(mode) {
    _srRadMode = mode;
    const btnMod   = document.getElementById('sr-rad-toggle-mod');
    const btnAe    = document.getElementById('sr-rad-toggle-ae');
    const btnMonth = document.getElementById('sr-rad-toggle-month');
    const active   = 'background:rgba(96,165,250,0.15);color:#60a5fa;';
    const inactive = 'background:transparent;color:#64748b;';
    btnMod.style.cssText   = (mode === 'mod'   ? active : inactive) + 'padding:4px 12px;border:none;cursor:pointer;';
    btnAe.style.cssText    = (mode === 'ae'    ? active : inactive) + 'padding:4px 12px;border:none;cursor:pointer;';
    if (btnMonth) btnMonth.style.cssText = (mode === 'month' ? active : inactive) + 'padding:4px 12px;border:none;cursor:pointer;';
    const rows = mode === 'mod'   ? (_srRadData.by_modality || [])
               : mode === 'ae'    ? (_srRadData.by_aetitle  || [])
               :                    (_srRadData.by_month     || []);
    srDrawCrossTab(rows, mode === 'month');
};

function _srFmtMonth(ym) {
    const [y, m] = ym.split('-');
    return ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][parseInt(m,10)-1] + ' ' + y;
}

function srDrawCrossTab(rows, isMonth) {
    const el = document.getElementById('sr-rad-xtab');
    if (!rows.length) {
        el.innerHTML = '<p class="sr-muted" style="font-size:10px;padding:8px 0;">No data for this period.</p>';
        return;
    }
    const allRads = [...new Set(rows.map(r => r.radiologist))].sort();
    const allDims = [...new Set(rows.map(r => r.dim))];
    const dimTotals = {}, lookup = {};
    rows.forEach(r => {
        dimTotals[r.dim] = (dimTotals[r.dim] || 0) + r.cnt;
        lookup[r.dim + '|' + r.radiologist] = r.cnt;
    });
    if (isMonth) { allDims.sort(); } else { allDims.sort((a, b) => (dimTotals[b] || 0) - (dimTotals[a] || 0)); }
    const radMax = {};
    allRads.forEach(rad => {
        radMax[rad] = Math.max(...allDims.map(d => lookup[d + '|' + rad] || 0));
    });
    const headerCells = allRads.map(r =>
        `<th style="padding:6px 8px;text-align:right;white-space:nowrap;font-size:9px;font-weight:900;text-transform:uppercase;letter-spacing:0.06em;color:#64748b;">${escHtml(r)}</th>`
    ).join('');
    const bodyRows = allDims.map(dim => {
        const total = dimTotals[dim] || 0;
        const label = isMonth ? _srFmtMonth(dim) : dim;
        const cells = allRads.map(rad => {
            const cnt = lookup[dim + '|' + rad] || 0;
            const intensity = radMax[rad] > 0 ? cnt / radMax[rad] : 0;
            const bg = cnt > 0 ? `rgba(96,165,250,${0.08 + intensity * 0.45})` : 'transparent';
            const fg = intensity > 0.6 ? '#e2e8f0' : (cnt > 0 ? '#93c5fd' : '#334155');
            return `<td style="padding:5px 8px;text-align:right;font-family:monospace;font-weight:700;background:${bg};color:${fg};">${cnt || ''}</td>`;
        }).join('');
        return `<tr style="border-top:1px solid #1e2a26;">
            <td style="padding:5px 8px;font-size:10px;color:#94a3b8;position:sticky;left:0;background:#181f1c;font-weight:600;min-width:100px;">${escHtml(label)}</td>
            ${cells}
            <td style="padding:5px 8px;text-align:right;font-family:monospace;font-weight:900;color:#60a5fa;">${total}</td>
        </tr>`;
    }).join('');
    const footCells = allRads.map(rad => {
        const t = allDims.reduce((s, d) => s + (lookup[d + '|' + rad] || 0), 0);
        return `<td style="padding:5px 8px;text-align:right;font-family:monospace;font-weight:900;color:#f59e0b;border-top:2px solid #28332e;">${t}</td>`;
    }).join('');
    const grandTotal = allDims.reduce((s, d) => s + (dimTotals[d] || 0), 0);
    el.innerHTML = `<div style="overflow-x:auto;max-height:280px;overflow-y:auto;border:1px solid #1e2a26;border-radius:8px;">
        <table style="width:100%;border-collapse:collapse;font-size:10px;min-width:300px;">
            <thead style="position:sticky;top:0;z-index:2;background:#1a2420;">
                <tr>
                    <th style="padding:6px 8px;text-align:left;position:sticky;left:0;background:#1a2420;min-width:100px;font-size:9px;font-weight:900;text-transform:uppercase;letter-spacing:0.06em;color:#64748b;"></th>
                    ${headerCells}
                    <th style="padding:6px 8px;text-align:right;color:#94a3b8;font-size:9px;font-weight:900;text-transform:uppercase;">Total</th>
                </tr>
            </thead>
            <tbody>
                ${bodyRows}
                <tr style="background:rgba(96,165,250,0.04);">
                    <td style="padding:5px 8px;font-size:9px;font-weight:900;text-transform:uppercase;letter-spacing:0.06em;color:#64748b;position:sticky;left:0;background:rgba(24,31,28,0.95);border-top:2px solid #28332e;">TOTAL</td>
                    ${footCells}
                    <td style="padding:5px 8px;text-align:right;font-family:monospace;font-weight:900;color:#60a5fa;border-top:2px solid #28332e;">${grandTotal}</td>
                </tr>
            </tbody>
        </table>
    </div>`;
}

function srBuildProcTable() {
    const procRows = _srRadData.by_procedure || [];
    const empty = document.getElementById('sr-proc-empty');
    const tbl   = document.getElementById('sr-proc-table');
    if (!procRows.length) { empty.style.display = ''; tbl.style.display = 'none'; return; }
    empty.style.display = 'none'; tbl.style.display = '';

    // Build pivot: procedures × radiologists
    const allRads = [...new Set(procRows.map(r => r.radiologist))].sort();
    const allProcs = [...new Set(procRows.map(r => r.proc))];
    const procTotals = {};
    const lookup = {};
    procRows.forEach(r => {
        procTotals[r.proc] = (procTotals[r.proc] || 0) + r.cnt;
        lookup[r.proc + '|' + r.radiologist] = r.cnt;
    });
    allProcs.sort((a, b) => (procTotals[b] || 0) - (procTotals[a] || 0));

    // Per-radiologist max for color scaling
    const radMax = {};
    allRads.forEach(rad => {
        radMax[rad] = Math.max(...allProcs.map(p => lookup[p + '|' + rad] || 0));
    });

    // Store for filtering
    window._srProcAllProcs = allProcs;
    window._srProcAllRads  = allRads;
    window._srProcTotals   = procTotals;
    window._srProcLookup   = lookup;
    window._srProcRadMax   = radMax;

    // Build header
    const thead = document.getElementById('sr-proc-thead');
    thead.innerHTML = `<tr style="font-size:9px;font-weight:900;text-transform:uppercase;letter-spacing:0.06em;color:#64748b;">
        <th style="padding:6px 8px;text-align:left;position:sticky;left:0;background:#1a2420;min-width:160px;">Procedure</th>
        ${allRads.map(r => `<th style="padding:6px 8px;text-align:right;white-space:nowrap;">${escHtml(r)}</th>`).join('')}
        <th style="padding:6px 8px;text-align:right;color:#94a3b8;">Total</th>
    </tr>`;

    srProcFilter();
}

window.srProcFilter = function() {
    const search = (document.getElementById('sr-proc-search')?.value || '').toLowerCase();
    const topN   = parseInt(document.getElementById('sr-proc-topn')?.value || '20');
    const allProcs = window._srProcAllProcs || [];
    const allRads  = window._srProcAllRads  || [];
    const totals   = window._srProcTotals   || {};
    const lookup   = window._srProcLookup   || {};
    const radMax   = window._srProcRadMax   || {};

    const filtered = allProcs
        .filter(p => !search || p.toLowerCase().includes(search))
        .slice(0, topN);

    const tbody = document.getElementById('sr-proc-tbody');
    if (!tbody) return;
    tbody.innerHTML = filtered.map(proc => {
        const total = totals[proc] || 0;
        const cells = allRads.map(rad => {
            const cnt = lookup[proc + '|' + rad] || 0;
            const intensity = radMax[rad] > 0 ? cnt / radMax[rad] : 0;
            const bg = cnt > 0 ? `rgba(96,165,250,${0.08 + intensity * 0.45})` : 'transparent';
            const fg = intensity > 0.6 ? '#e2e8f0' : (cnt > 0 ? '#93c5fd' : '#334155');
            return `<td style="padding:5px 8px;text-align:right;font-family:monospace;font-weight:700;background:${bg};color:${fg};">${cnt || ''}</td>`;
        }).join('');
        return `<tr style="border-top:1px solid #1e2a26;" data-proc="${escHtml(proc)}">
            <td style="padding:5px 8px;font-size:10px;color:#94a3b8;position:sticky;left:0;background:#181f1c;font-weight:600;">${escHtml(proc)}</td>
            ${cells}
            <td style="padding:5px 8px;text-align:right;font-family:monospace;font-weight:900;color:#60a5fa;">${total}</td>
        </tr>`;
    }).join('');

    // Footer totals row
    const footCells = allRads.map(rad => {
        const t = (window._srProcAllProcs || []).reduce((s, p) => s + (lookup[p + '|' + rad] || 0), 0);
        return `<td style="padding:5px 8px;text-align:right;font-family:monospace;font-weight:900;color:#f59e0b;border-top:2px solid #28332e;">${t}</td>`;
    }).join('');
    const grandTotal = Object.values(window._srProcTotals || {}).reduce((s, v) => s + v, 0);
    tbody.innerHTML += `<tr style="background:rgba(96,165,250,0.04);">
        <td style="padding:5px 8px;font-size:9px;font-weight:900;text-transform:uppercase;letter-spacing:0.06em;color:#64748b;position:sticky;left:0;background:rgba(24,31,28,0.95);border-top:2px solid #28332e;">TOTAL</td>
        ${footCells}
        <td style="padding:5px 8px;text-align:right;font-family:monospace;font-weight:900;color:#60a5fa;border-top:2px solid #28332e;">${grandTotal}</td>
    </tr>`;
};

// ── PDF export ───────────────────────────────────────────────────
function _srExportElemPdf(el, title, filename) {
    if (!el) return;
    const btn = event?.currentTarget;
    if (btn) { btn.textContent = '…'; btn.disabled = true; }
    // Strip scroll/clip constraints so html2canvas captures the full table
    const clipped = [...el.querySelectorAll('*')].filter(n => n.style && (n.style.maxHeight || n.style.overflowY === 'auto' || n.style.overflowY === 'scroll'));
    const saved = clipped.map(n => ({ n, mh: n.style.maxHeight, oy: n.style.overflowY, ox: n.style.overflowX }));
    saved.forEach(s => { s.n.style.maxHeight = 'none'; s.n.style.overflowY = 'visible'; s.n.style.overflowX = 'visible'; });
    html2canvas(el, { backgroundColor: '#181f1c', scale: 2, useCORS: true }).then(canvas => {
        saved.forEach(s => { s.n.style.maxHeight = s.mh; s.n.style.overflowY = s.oy; s.n.style.overflowX = s.ox; });
        const { jsPDF } = window.jspdf;
        const doc = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' });
        const pw = doc.internal.pageSize.getWidth(), ph = doc.internal.pageSize.getHeight(), mg = 12;
        const usableW = pw - mg * 2;
        const mmPerPx = usableW / canvas.width;
        const fillBg = () => { doc.setFillColor(24,31,28); doc.rect(0,0,pw,ph,'F'); };
        fillBg();
        doc.setFont('helvetica','bold'); doc.setFontSize(11); doc.setTextColor(226,232,240);
        doc.text(title, mg, mg+5);
        doc.setFont('helvetica','normal'); doc.setFontSize(8); doc.setTextColor(100,116,139);
        doc.text(`${$start().value} – ${$end().value}`, mg, mg+10);   // the period shown
        let srcY = 0, destY = mg + 14;
        while (srcY < canvas.height) {
            const availH = ph - destY - mg;
            const slicePx = Math.min(Math.floor(availH / mmPerPx), canvas.height - srcY);
            if (slicePx <= 0) break;
            const tmp = document.createElement('canvas');
            tmp.width = canvas.width; tmp.height = slicePx;
            tmp.getContext('2d').drawImage(canvas, 0, srcY, canvas.width, slicePx, 0, 0, canvas.width, slicePx);
            doc.addImage(tmp.toDataURL('image/png'), 'PNG', mg, destY, usableW, slicePx * mmPerPx);
            srcY += slicePx;
            if (srcY < canvas.height) { doc.addPage(); fillBg(); destY = mg; }
        }
        doc.save(`${filename}_${new Date().toISOString().slice(0,10)}.pdf`);
        if (btn) { btn.innerHTML = '<i class="bi bi-file-earmark-pdf"></i> PDF'; btn.disabled = false; }
    }).catch(() => {
        saved.forEach(s => { s.n.style.maxHeight = s.mh; s.n.style.overflowY = s.oy; s.n.style.overflowX = s.ox; });
        if (btn) { btn.innerHTML = '<i class="bi bi-file-earmark-pdf"></i> PDF'; btn.disabled = false; }
    });
}

// ── Radiologist CSV export ────────────────────────────────────────
function _srDownloadCsv(csv, filename) {
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href = url; a.download = filename;
    document.body.appendChild(a); a.click();
    document.body.removeChild(a); URL.revokeObjectURL(url);
}

function _srCsvSection(rows, colHeader, isMonth) {
    if (!rows.length) return [];
    const q = v => '"' + String(v||'').replace(/"/g,'""') + '"';
    const allRads = [...new Set(rows.map(r => r.radiologist))].sort();
    const allDims = [...new Set(rows.map(r => r.dim))];
    const dimTotals = {}, lookup = {};
    rows.forEach(r => {
        dimTotals[r.dim] = (dimTotals[r.dim]||0) + r.cnt;
        lookup[r.dim+'|'+r.radiologist] = r.cnt;
    });
    if (isMonth) { allDims.sort(); } else { allDims.sort((a,b) => (dimTotals[b]||0)-(dimTotals[a]||0)); }
    return [
        [colHeader, ...allRads, 'Total'].map(q).join(','),
        ...allDims.map(dim => {
            const label = isMonth ? _srFmtMonth(dim) : dim;
            return [label, ...allRads.map(rad => lookup[dim+'|'+rad]||0), dimTotals[dim]||0].map(q).join(',');
        }),
        ['TOTAL', ...allRads.map(rad => allDims.reduce((s,d)=>s+(lookup[d+'|'+rad]||0),0)),
            allDims.reduce((s,d)=>s+(dimTotals[d]||0),0)].map(q).join(','),
    ];
}

window.srExportCsv = function() {
    const sections = [
        ['=== By Modality ===', ..._srCsvSection(_srRadData.by_modality||[], 'Modality', false)],
        [],
        ['=== By AE Title ===', ..._srCsvSection(_srRadData.by_aetitle||[],  'AE Title', false)],
        [],
        ['=== By Month ===',    ..._srCsvSection(_srRadData.by_month||[],     'Month',    true)],
    ];
    const lines = sections.map(s => Array.isArray(s) ? s.join('\n') : s);
    _srDownloadCsv(lines.join('\n'), `rad_matrix_${new Date().toISOString().slice(0,10)}.csv`);
};

window.srExportProcCsv = function() {
    const allProcs = window._srProcAllProcs || [];
    const allRads  = window._srProcAllRads  || [];
    const totals   = window._srProcTotals   || {};
    const lookup   = window._srProcLookup   || {};
    if (!allProcs.length) return;
    const topN  = parseInt(document.getElementById('sr-proc-topn')?.value || '20');
    const search = (document.getElementById('sr-proc-search')?.value || '').toLowerCase();
    const procs = allProcs.filter(p => !search || p.toLowerCase().includes(search)).slice(0, topN);
    const q = v => '"' + String(v||'').replace(/"/g,'""') + '"';
    const lines = [
        ['Procedure', ...allRads, 'Total'].map(q).join(','),
        ...procs.map(proc =>
            [proc, ...allRads.map(rad => lookup[proc+'|'+rad]||0), totals[proc]||0].map(q).join(',')
        ),
        ['TOTAL', ...allRads.map(rad => allProcs.reduce((s,p)=>s+(lookup[p+'|'+rad]||0),0)),
            Object.values(totals).reduce((s,v)=>s+v,0)].map(q).join(','),
    ];
    _srDownloadCsv(lines.join('\n'), `rad_by_procedure_${new Date().toISOString().slice(0,10)}.csv`);
};

// ── Narrative typewriter ──────────────────────────────────────────
function renderNarrative(sections) {
    const box = document.getElementById('super-narrative');
    box.innerHTML = '';
    if (!Array.isArray(sections) || !sections.length) return;

    // Build DOM skeleton — all sections empty to start
    const sectionEls = sections.map(sec => {
        const div = document.createElement('div');
        div.className = 'narrative-section';
        div.innerHTML = `
            <p class="text-[9px] font-black uppercase tracking-widest mb-2 flex items-center gap-1"
               style="color:${escHtml(sec.color)}">
                <i class="bi ${escHtml(sec.icon)}"></i> ${escHtml(sec.title)}
            </p>
            <ul></ul>`;
        box.appendChild(div);
        return { ul: div.querySelector('ul'), bullets: sec.bullets || [] };
    });

    // Flatten all bullets into a queue: { ulEl, text }
    const queue = [];
    sectionEls.forEach(({ ul, bullets }) => {
        bullets.forEach(text => queue.push({ ul, text }));
    });

    let qi = 0;
    function typeNextBullet() {
        if (qi >= queue.length) return;
        const { ul, text } = queue[qi++];

        const li  = document.createElement('li');
        li.style.color = '#94a3b8';
        const cursor = document.createElement('span');
        cursor.className = 'narrative-cursor';
        li.appendChild(cursor);
        ul.appendChild(li);

        let ci = 0;
        const charDelay = 18;  // ms per character
        const bulletDelay = 120; // pause between bullets

        function typeChar() {
            if (ci < text.length) {
                li.insertBefore(document.createTextNode(text[ci++]), cursor);
                setTimeout(typeChar, charDelay);
            } else {
                cursor.remove();
                setTimeout(typeNextBullet, bulletDelay);
            }
        }
        typeChar();
    }
    typeNextBullet();
}

// ── Clinical Insights renderer ────────────────────────────────────
function renderInsights(sec) {
    const card = document.getElementById('super-insights-card');
    const box  = document.getElementById('super-insights');
    if (!sec || !sec.signals) { card.style.display = 'none'; return; }

    const signals = sec.signals || [];
    card.style.display = '';

    if (!signals.length) {
        box.innerHTML = `<div class="flex items-center gap-2 px-3 py-2 rounded-lg bg-emerald-500/10 border border-emerald-500/20">
            <i class="bi bi-check-circle-fill text-emerald-400 shrink-0"></i>
            <p class="text-xs text-emerald-300">All department metrics within normal thresholds — no anomalies detected.</p>
        </div>`;
        return;
    }

    const sevMap = {
        critical: { bg: 'bg-rose-500/10',   border: 'border-rose-500/25',   icon: 'bi-exclamation-triangle-fill text-rose-400',   label: 'text-rose-300' },
        warning:  { bg: 'bg-amber-500/10',  border: 'border-amber-500/25',  icon: 'bi-exclamation-circle-fill text-amber-400',    label: 'text-amber-300' },
        info:     { bg: 'bg-blue-500/10',   border: 'border-blue-500/20',   icon: 'bi-info-circle-fill text-blue-400',            label: 'text-blue-300' },
    };

    box.innerHTML = signals.map(s => {
        const st = sevMap[s.severity] || sevMap.info;
        return `<div class="flex items-start gap-2 px-3 py-2 rounded-lg ${st.bg} border ${st.border}">
            <i class="bi ${st.icon} mt-0.5 shrink-0"></i>
            <div>
                <p class="text-[10px] font-black ${st.label} uppercase">${escHtml(s.entity||'')}</p>
                <p class="text-xs text-zinc-300 leading-relaxed">${escHtml(s.message||'')}</p>
            </div>
        </div>`;
    }).join('');
}

// ── Helpers ───────────────────────────────────────────────────────
// The same text as the narrative's Python helpers (utils/stats.py _fmt and _pct, f"{x:.1f}"), so a
// number reads the same in the narrative and on a card (review of 2026-11-20): '—' for no value
// (parseInt(null) printed "NaN"), en-US separators whatever the browser's language, and an exact
// half rounded to even as Python's round() does (toFixed rounds it up: 36.25 -> 36.3, Python 36.2).
function _round(x, d) {
    const m = Math.pow(10, d), v = x * m;
    if (!Number.isInteger(v) && Number.isInteger(v * 2)) {      // an exact half
        const f = Math.floor(v);
        return (f % 2 === 0 ? f : f + 1) / m;
    }
    return parseFloat(x.toFixed(d));                           // exact value, like round()
}
function pct(a, b) {
    a = parseFloat(a||0); b = parseFloat(b||0);
    return (b && Number.isFinite(a) && Number.isFinite(b)) ? _round((a-b)/b*100, 1) : null;
}
function fmt(n)  { const v = parseInt(n, 10); return Number.isFinite(v) ? v.toLocaleString('en-US') : '—'; }
function fmtD(n) { const v = parseFloat(n);   return Number.isFinite(v) ? _round(v, 1).toFixed(1) : '—'; }

function row(label, val, pctVal) {
    const badge = pctVal !== null && pctVal !== undefined
        ? ` <span style="font-size:9px;font-weight:700;color:${pctVal>=0?'#34d399':'#f87171'}">${pctVal>=0?'+':''}${pctVal}%</span>`
        : '';
    return `<div class="row-stat"><span class="lbl">${label}</span><span class="val">${val}${badge}</span></div>`;
}
function rowWarn(label, val, warn) {
    return `<div class="row-stat"><span class="lbl">${label}</span><span class="val ${warn?'warn':''}">${val}</span></div>`;
}
function subSection(title, html) {
    return `<div class="pt-2 mt-1 border-t border-[#1a2420]">
        <p class="text-[9px] text-slate-600 font-bold uppercase tracking-widest mb-1">${title}</p>
        ${html}
    </div>`;
}
function escHtml(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
function escJs(s)   { return String(s).replace(/'/g,"\\'"); }

// ── Saved Presets ─────────────────────────────────────────────────

async function loadPresets() {
    try {
        const res  = await fetch('/viewer/super-report/saved');
        const data = await res.json();
        renderPresets(data);
    } catch(e) {
        document.getElementById('presets-list').innerHTML =
            '<p class="text-[10px] text-slate-600 italic">Could not load</p>';
        showFiltersNotice();
    }
}

function renderPresets(presets) {
    const el = document.getElementById('presets-list');
    if (!presets.length) {
        el.innerHTML = '<p class="text-[10px] text-slate-600 italic">No saved presets</p>';
        showFiltersNotice();
        return;
    }
    el.innerHTML = presets.map(p => `
        <div class="flex items-center gap-1 group">
            <button onclick="loadPreset(${p.id})"
                    class="flex-1 text-left text-[10px] font-bold text-slate-400 hover:text-white truncate transition-colors py-1 px-2 rounded hover:bg-[#1d2522]"
                    title="${escHtml(p.name)} · ${p.updated_at}">
                <i class="bi bi-bookmark-fill text-[#60a5fa] mr-1" style="font-size:8px;"></i>
                ${escHtml(p.name)}
            </button>
            <button onclick="deletePreset(${p.id}, this)"
                    class="opacity-0 group-hover:opacity-100 text-slate-600 hover:text-red-400 transition-all px-1 text-xs">
                <i class="bi bi-trash3"></i>
            </button>
        </div>
    `).join('');
    showFiltersNotice();
}

async function saveCurrentPreset() {
    const name = prompt('Name this preset:');
    if (!name || !name.trim()) return;

    const filters = collectFilters();
    try {
        const res  = await fetch('/viewer/super-report/save', {
            method:  'POST',
            headers: {'Content-Type': 'application/json'},
            body:    JSON.stringify({ name: name.trim(), filters })
        });
        const data = await res.json();
        if (data.success) {
            showToast(`Saved "${name.trim()}"`);
            loadPresets();
        } else {
            alert('Save failed: ' + data.error);
        }
    } catch(e) {
        alert('Network error: ' + e.message);
    }
}

async function loadPreset(id) {
    try {
        const res  = await fetch('/viewer/super-report/saved');
        const list = await res.json();
        const preset = list.find(p => p.id === id);
        if (!preset) return;
        applyFilters(preset.filters);
        showToast(`Loaded "${preset.name}"`);
    } catch(e) {
        alert('Could not load preset');
    }
}

async function deletePreset(id, btn) {
    if (!confirm('Delete this preset?')) return;
    try {
        const res = await fetch(`/viewer/super-report/saved/${id}`, { method: 'DELETE' });
        const data = await res.json();
        if (data.success) { loadPresets(); showToast('Deleted'); }
        else alert('Delete failed: ' + data.error);
    } catch(e) {
        alert('Network error');
    }
}

// Collect all current filter values into a plain object for saving
function collectFilters() {
    const f = {
        start:      $start().value,
        end:        $end().value,
        cmp_start:  document.getElementById('cmp-start').value,
        cmp_end:    document.getElementById('cmp-end').value,
        age_min:    document.getElementById('age-min').value,
        age_max:    document.getElementById('age-max').value,
        has_report: document.getElementById('f-has_report').value,
    };
    panelSelects().forEach(sel => {
        f[sel.name] = picked(sel);
        const t = tickOf(sel);
        if (t) f[`${sel.name}_on`] = t.checked;
    });
    return f;
}

// Apply a saved filter object back to the panel. Presets saved before the shared
// panel hold a single value as a string for some filters ('' = all).
function applyFilters(f) {
    if (f.start)      $start().value = f.start;
    if (f.end)        $end().value   = f.end;
    if (f.age_min)    document.getElementById('age-min').value = f.age_min;
    if (f.age_max)    document.getElementById('age-max').value = f.age_max;
    if (f.has_report !== undefined)
        document.getElementById('f-has_report').value = f.has_report;

    panelSelects().forEach(sel => {
        if (f[sel.name] === undefined) return;
        const v = f[sel.name];
        setPicked(sel, Array.isArray(v) ? v : (v ? [v] : []));
        const t = tickOf(sel);
        if (t && f[`${sel.name}_on`] !== undefined) t.checked = !!f[`${sel.name}_on`];
    });

    updateCmpDates();
    // the comparison dates the preset was saved with, when it has them
    if (f.cmp_start) document.getElementById('cmp-start').value = f.cmp_start;
    if (f.cmp_end)   document.getElementById('cmp-end').value   = f.cmp_end;
}

// Simple toast
function showToast(msg) {
    let t = document.getElementById('_toast');
    if (!t) {
        t = document.createElement('div');
        t.id = '_toast';
        t.style.cssText = 'position:fixed;bottom:24px;right:24px;background:#064e3b;color:#6ee7b7;border:1px solid #065f46;padding:10px 18px;border-radius:10px;font-size:12px;font-weight:700;z-index:9999;transition:opacity 0.3s;';
        document.body.appendChild(t);
    }
    t.textContent = msg;
    t.style.opacity = '1';
    clearTimeout(t._to);
    t._to = setTimeout(() => t.style.opacity = '0', 2500);
}

// ── Daily Briefing ────────────────────────────────────────────────
window._briefings = {};

async function loadBriefings() {
    try {
        const rows = await fetch('/viewer/super-report/snapshots').then(r => r.json());
        if (rows.error || !Array.isArray(rows) || !rows.length) {
            document.getElementById('briefing-body').innerHTML =
                '<p class="sr-muted" style="font-size:11px;font-style:italic;">No briefing available yet — runs nightly at 05:15.</p>';
            return;
        }
        rows.forEach(r => { window._briefings[r.period_label] = r; });

        // Show first available tab
        const order = ['last_30d','last_90d','ytd'];
        const first = order.find(k => window._briefings[k]) || rows[0].period_label;
        showBriefing(first);
    } catch(e) {
        document.getElementById('briefing-body').innerHTML =
            '<p style="color:#f87171;font-size:11px;">Could not load briefing.</p>';
    }
}

function showBriefing(label) {
    // Update tab styles
    ['last_30d','last_90d','ytd'].forEach(k => {
        const t = document.getElementById(`tab-${k}`);
        if (t) t.classList.toggle('active', k === label);
    });

    const snap = window._briefings[label];
    const body = document.getElementById('briefing-body');
    const upd  = document.getElementById('briefing-updated');

    if (!snap) {
        body.innerHTML = '<p class="sr-muted" style="font-size:11px;font-style:italic;">No snapshot for this period yet.</p>';
        upd.textContent = '';
        return;
    }

    upd.textContent = snap.computed_at ? `Updated ${snap.computed_at}` : '';

    // Render narrative as styled pre block, highlight ALERTS section
    const lines = (snap.narrative || '').split('\n');
    let html = '';
    let inAlerts = false;
    lines.forEach(line => {
        if (line.startsWith('ALERTS')) {
            inAlerts = true;
            html += `<div class="briefing-section-head" style="margin-top:10px;">${escHtml(line)}</div>`;
        } else if (line.startsWith('DAILY BRIEFING')) {
            html += `<div style="font-size:11px;font-weight:900;color:#e2e8f0;margin-bottom:8px;">${escHtml(line)}</div>`;
            inAlerts = false;
        } else if (inAlerts && line.startsWith('—')) {
            html += `<div class="briefing-alert" style="font-size:12px;padding:1px 0;">${escHtml(line)}</div>`;
        } else if (line === '') {
            html += '<div style="height:6px;"></div>';
        } else {
            html += `<div class="briefing-pre">${escHtml(line)}</div>`;
        }
    });
    body.innerHTML = html || '<p class="sr-muted" style="font-size:11px;font-style:italic;">Empty briefing.</p>';
}

// ── Init ──────────────────────────────────────────────────────────
loadFilters();
loadPresets();
loadBriefings();

// ── Theme toggle ───────────────────────────────────────────────────
function toggleTheme() {
    document.body.classList.toggle('light');
    const icon = document.getElementById('theme-icon');
    if (icon) icon.className = document.body.classList.contains('light') ? 'bi bi-sun-fill' : 'bi bi-moon-fill';
    fetch('/user/theme', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({theme: document.body.classList.contains('light') ? 'light' : 'dark'}) });
}
// Apply saved theme on load
fetch('/user/theme').then(r => r.json()).then(d => {
    if (d.theme === 'light') { document.body.classList.add('light'); const i = document.getElementById('theme-icon'); if(i) i.className = 'bi bi-sun-fill'; }
}).catch(() => {});

// Auto-load conflict patients card
(async function() {
    const badge = document.getElementById('sr-conflict-badge');
    const errEl = document.getElementById('sr-conflict-error');
    try {
        const d = await fetch('/patients/conflicts').then(r => r.json());
        if (d.error) throw new Error(d.error);
        badge.textContent = d.count.toLocaleString('en-US') + ' conflict' + (d.count !== 1 ? 's' : '');
        badge.style.background  = d.count > 0 ? 'rgba(244,63,94,0.15)'  : 'rgba(34,197,94,0.15)';
        badge.style.color       = d.count > 0 ? '#f43f5e'               : '#22c55e';
        badge.style.borderColor = d.count > 0 ? 'rgba(244,63,94,0.3)'  : 'rgba(34,197,94,0.3)';
    } catch(e) {
        badge.textContent = 'Error';
        errEl.textContent = 'Could not load conflict data.';
        errEl.style.display = 'block';
    }
})();
