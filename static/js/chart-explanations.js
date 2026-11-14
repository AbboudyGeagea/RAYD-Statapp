/**
 * Chart explanations (LAUMC) — meeting of 2026-11-13, item 12.
 *
 * An info icon next to a chart or card title opens a short note: what it shows, how to
 * read it, what to look for. Ported from the Bekaa and HL7 branches, but every text below
 * is written against LAUMC's own queries (routes/report_NN.py), not copied: the other
 * branches count differently (RH only, devices and RIS exams, PACS sign events).
 *
 * The note opens in the browser's own <dialog>, so nothing is loaded from a CDN (the
 * hospital network may block one). Icons are wired by a data attribute, so icons in HTML
 * injected later work too:
 *
 *   <i class="bi bi-info-circle chart-info-icon" data-explain="r22-tree"
 *      role="button" tabindex="0" aria-label="About this chart" title="About this chart"></i>
 *
 * Keys are per page: r22- (Report 22). tests/test_chart_explanations.py fails when a
 * template names a key that has no text here, or a page with icons does not load this file.
 */

const ChartExplanations = {
  explanations: {

    // ── Report 22 — Flow Analytics ──────────────────────────────────────────────
    'r22-orphans': {
      title: 'HL7 Orphan Orders',
      purpose: 'Orders received over HL7 in the selected period (by scheduled date, else the date received) that are not cancelled, have no PACS study with the same accession number, and were not linked to a study by hand.',
      valueMeaning: 'One number. Each orphan is an order the hospital placed that never became a study RAYD can see.',
      interpretation: 'It stays at 0 until the HIS order feed reaches RAYD (forwarded through Mirth). After that, a small steady number is normal: an exam ordered in several parts is often stored under one accession. A sudden rise means accession numbers are not reaching the PACS, or exams are done without their order.'
    },
    'r22-volume': {
      title: 'Exam Volume — Modality × Month',
      purpose: 'Studies per modality per month for the dates and sidebar filters chosen, Rizk Hospital only. Structured reports, other objects, bone densitometry and cardiology are always left out; reports without images (LAUMCWFM1AR) are counted aside under the table.',
      valueMeaning: 'Modality comes from the device the study was stored on. With "Count linked exams separately" ticked, a PACS study that covers several RIS exams (a CT abdomen and pelvis is one study, two exams) counts once per exam that reached exam done on the RIS; a study with no finished RIS exam still counts once; two PACS copies of one accession count once. Unticked, each PACS study counts once.',
      interpretation: 'Compare the unticked numbers with the PACS and the ticked ones with RIS finished exams. The current month is incomplete until it ends. A modality that jumps from one month to the next usually means a device was added or remapped on the Mapping page.'
    },
    'r22-early-churn': {
      title: 'Early Churn Warning — Physicians at Risk',
      purpose: 'Referring physicians still sending patients whose monthly referrals fell during the selected period.',
      valueMeaning: 'Each physician’s active months in the period are split into a first and a second half. 1st / 2nd Half Avg = average referrals per month in each half. A physician is listed with at least 3 active months, at least 15 studies, a second half at least 25% lower, and a referral in the last 90 days of the period. High Risk = a drop of 40% or more. At most 3 are shown. Days Silent = days since their last referral.',
      interpretation: 'Use a range of 6 months or more; with 2 or 3 months the halves are too short to mean much. Studies with no referring name are left out.'
    },
    'r22-confirmed-churn': {
      title: 'Confirmed Churn — Month-over-Month Drop',
      purpose: 'Referring physicians whose referrals in their latest active month were lower than in the month before.',
      valueMeaning: 'Prev = referrals in the earlier month, the large number = the latest month, the badge = the change in %. Only physicians with at least 5 referrals in the earlier month are shown, biggest drop first, 10 at most.',
      interpretation: 'If the range ends in the middle of a month, that month is incomplete and many physicians will look as if they dropped. End the range on the last day of a month for a fair comparison.'
    },
    'r22-tree': {
      title: 'Modality → Room → Top 5 Procedures',
      purpose: 'The period’s studies broken down by modality, then by the room (device) that stored them, then the 5 most common study descriptions in each room.',
      valueMeaning: 'Each node shows its study count; click a node to open or close it. Rooms carry their RIS room name, else the AE title. UNMAPPED = a device with no modality on the Mapping page; No Description = a study with no DICOM description.',
      interpretation: 'A room under the wrong modality, or a large UNMAPPED branch, means the device mapping needs fixing.'
    },
    'r22-top-referrers': {
      title: 'Top Referring Physicians (Study Volume)',
      purpose: 'The 10 referring physicians with the most studies in the period, as named on the PACS study.',
      valueMeaning: 'Bar length = number of studies, not patients. Unknown = studies that carry no referring name.',
      interpretation: 'Compare with Physician Loyalty: a physician high here but lower there sends the same patients back often. A large Unknown bar means referring names are not reaching the PACS.'
    },
    'r22-loyalty': {
      title: 'Physician Loyalty (Unique Patient Count)',
      purpose: 'The 10 referring physicians who sent the most different patients in the period.',
      valueMeaning: 'Bar = number of distinct patients; a patient referred three times counts once. Unknown = studies with no referring name.',
      interpretation: 'Physicians high on both referral charts bring a broad patient base. A big gap between a physician’s studies and patients means many repeat exams for the same people.'
    },
    'r22-phys-modality': {
      title: 'Physician → Modality Preference (Top 10)',
      purpose: 'For the 10 referring physicians with the most studies, how their studies split across modalities.',
      valueMeaning: 'One stacked bar per physician; each colour is a modality and its length is the number of studies. Modality comes from the device mapping; UNMAPPED = device not mapped. Studies with no referring name are left out.',
      interpretation: 'Shows each physician’s specialty pattern, and which modality would feel it if that physician’s referrals changed.'
    },
    'r22-phys-age': {
      title: 'Average Patient Age per Physician',
      purpose: 'Average patient age at the time of the exam for the 15 referring physicians with the most studies (at least 5 each).',
      valueMeaning: 'Bar = mean age in years. Ages outside 0–110 are left out; the badge shows how many studies that removed.',
      interpretation: 'Low averages point to paediatric practice, high ones to geriatric care. A physician whose average changes a lot between periods may have changed practice, or has data-entry problems.'
    },
    'r22-proc-age': {
      title: 'Age Distribution per Procedure',
      purpose: 'How patient ages spread for the 20 most frequent procedures (each with at least 5 studies), named from the procedure catalogue, else by code.',
      valueMeaning: 'Whiskers = youngest and oldest patient, box = the middle 50% of patients, green line = median age, ◆ = mean age. The table underneath lists every value. Ages outside 0–110 are left out.',
      interpretation: 'A wide box means the procedure is used across all ages. A patient far outside the usual range for a procedure is worth checking for a data-entry error.'
    },
    'r22-phys-status': {
      title: 'Study Status per Top Physician',
      purpose: 'For the 10 referring physicians with the most studies, how their studies split by current PACS study status.',
      valueMeaning: 'Stacked bars; each colour is a PACS study status and its length is the number of studies.',
      interpretation: 'A physician with a much larger share in one status (unread, for example) than the others points to a workflow problem for that service.'
    },
    'r22-proc-modality': {
      title: 'Procedure Breakdown by Modality',
      purpose: 'The procedures done on each modality in the period, most frequent first.',
      valueMeaning: 'Each procedure is the study’s DICOM description, else its catalogue name or code. Count = studies; the bar = its share of the modality. The 20 most frequent per modality are listed.',
      interpretation: 'Use it to check that each modality’s procedure mix is what you expect, and to spot descriptions typed several ways that split one procedure into many rows.'
    },
    'r22-status': {
      title: 'Case Status Distribution',
      purpose: 'Studies in the period grouped by their current PACS study status.',
      valueMeaning: 'Bar = number of studies in that status. Click a bar to list the studies (up to 500), with an export.',
      interpretation: 'A growing share of unread or unapproved studies is a backlog; open the bar to see which rooms and dates it comes from.'
    },
    'r22-gender': {
      title: 'Gender',
      purpose: 'Studies in the period by patient sex.',
      valueMeaning: 'Counts studies, not patients, with sex M or F and an age from 0 to 110; the badge shows how many studies were left out.',
      interpretation: 'A large number left out means sex is missing on many patient records.'
    },
    'r22-age': {
      title: 'Age Group',
      purpose: 'Studies in the period by patient age group.',
      valueMeaning: 'Counts studies, not patients, with an age from 0 to 110 and sex M or F; the badge shows how many studies were left out for their age.',
      interpretation: 'Shows which age groups the department serves most, for planning paediatric or geriatric capacity.'
    }
  },

  _dialog: null,

  _build() {
    const d = document.createElement('dialog');
    d.id = 'chart-explain-dialog';
    d.setAttribute('aria-labelledby', 'chart-explain-title');
    d.innerHTML = `
      <h2 id="chart-explain-title" class="cx-title"></h2>
      <div class="cx-block"><p class="cx-label">What it shows</p><p class="cx-text" data-part="purpose"></p></div>
      <div class="cx-block"><p class="cx-label">How to read it</p><p class="cx-text" data-part="valueMeaning"></p></div>
      <div class="cx-block"><p class="cx-label">What to look for</p><p class="cx-text" data-part="interpretation"></p></div>
      <form method="dialog" class="cx-actions"><button class="cx-close" value="close">Close</button></form>`;
    d.addEventListener('click', e => { if (e.target === d) d.close(); });   // click outside
    document.body.appendChild(d);
    return d;
  },

  show(key) {
    const exp = this.explanations[key];
    if (!exp) {
      console.warn(`No chart explanation for: ${key}`);
      return;
    }
    if (!this._dialog) this._dialog = this._build();
    const d = this._dialog;
    d.querySelector('.cx-title').textContent = exp.title;
    d.querySelectorAll('[data-part]').forEach(p => { p.textContent = exp[p.dataset.part]; });
    if (typeof d.showModal === 'function') d.showModal();
    else alert(`${exp.title}\n\nWhat it shows: ${exp.purpose}\n\nHow to read it: ${exp.valueMeaning}\n\nWhat to look for: ${exp.interpretation}`);
  }
};

// Capture phase, so an icon inside a clickable header (tab, collapsible) opens its
// explanation without also triggering the header.
document.addEventListener('click', function (e) {
  const icon = e.target.closest && e.target.closest('[data-explain]');
  if (!icon) return;
  e.preventDefault();
  e.stopPropagation();
  ChartExplanations.show(icon.dataset.explain);
}, true);

document.addEventListener('keydown', function (e) {
  if (e.key !== 'Enter' && e.key !== ' ') return;
  const icon = e.target.closest && e.target.closest('[data-explain]');
  if (!icon) return;
  e.preventDefault();
  ChartExplanations.show(icon.dataset.explain);
});

(function addStyles() {
  if (document.getElementById('chart-explanation-styles')) return;
  const s = document.createElement('style');
  s.id = 'chart-explanation-styles';
  s.textContent = `
    #chart-explain-dialog { width: min(560px, calc(100vw - 32px)); background: #232b28; color: #e2e8f0;
      border: 1px solid #28332e; border-radius: 12px; padding: 24px 26px 20px;
      box-shadow: 0 24px 60px rgba(0,0,0,0.6); font-size: 13px; line-height: 1.55; }
    #chart-explain-dialog::backdrop { background: rgba(4, 8, 6, 0.6); }
    #chart-explain-dialog .cx-title { margin: 0 0 16px; color: #60a5fa; font-size: 17px; font-weight: 900; }
    #chart-explain-dialog .cx-block { margin: 0 0 14px; }
    #chart-explain-dialog .cx-label { margin: 0 0 4px; color: #60a5fa; font-weight: 700; font-size: 11px;
      text-transform: uppercase; letter-spacing: 0.06em; }
    #chart-explain-dialog .cx-text { margin: 0; color: #cbd5e1; }
    #chart-explain-dialog .cx-actions { display: flex; justify-content: flex-end; margin: 6px 0 0; }
    #chart-explain-dialog .cx-close { background: #60a5fa; color: #0a1628; font-weight: 800; border: none;
      border-radius: 8px; padding: 8px 20px; text-transform: uppercase; letter-spacing: 0.05em;
      font-size: 11px; cursor: pointer; }
    .chart-info-icon { display: inline-flex; align-items: center; justify-content: center; width: 18px;
      height: 18px; border-radius: 50%; background: rgba(96, 165, 250, 0.15); color: #60a5fa;
      font-size: 12px; font-style: normal; text-transform: none; letter-spacing: 0; vertical-align: middle;
      cursor: pointer; transition: background 0.2s ease; margin-left: 6px; flex-shrink: 0; }
    .chart-info-icon:hover, .chart-info-icon:focus-visible { background: rgba(96, 165, 250, 0.3); outline: none; }
    body.light #chart-explain-dialog { background: #ffffff; color: #1a202c; border-color: #e2e8f0; }
    body.light #chart-explain-dialog .cx-title, body.light #chart-explain-dialog .cx-label { color: #2b6cb0; }
    body.light #chart-explain-dialog .cx-text { color: #475569; }
    body.light #chart-explain-dialog .cx-close { background: #0ea5e9; color: #ffffff; }
    body.light .chart-info-icon { background: rgba(14, 165, 233, 0.15); color: #0284c7; }`;
  document.head.appendChild(s);
})();
