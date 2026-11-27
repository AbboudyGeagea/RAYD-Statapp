/**
 * Chart Explanations
 * An info icon next to a chart or card title opens a short note: what it shows,
 * how to read it, what to look for (and, where it matters, what to keep in mind).
 * Shown in a SweetAlert2 modal.
 *
 * Mechanism ported from the Mazloum branch (178e573d). Every text below was checked
 * against CHN's own queries: CHN is a modality-only site (the "AE" columns hold the
 * modality group, migration 0063), its HL7 feeds start on 6 Oct 2026, Report 25 still
 * counts TAT from midnight of the study date, and Report 36 counts TAT from the first
 * open (utils/tat_first_open.py). Keys per page: r22-, r25-, r27-, sr-, ri-, yd-, oru-, r36-.
 * Icons are wired by a data attribute, so icons inside HTML injected later work too:
 *
 *   <i class="bi bi-info-circle chart-info-icon" data-explain="r22-tree"
 *      role="button" tabindex="0" aria-label="About this chart" title="About this chart"></i>
 *
 * An entry may add `caveats` ("Good to know") after the three standard blocks.
 */

const ChartExplanations = {
  explanations: {

    // ── Report 22 — Operations (Flow Analytics & Churn) ─────────────────────
    'r22-orphans': {
      title: 'HL7 Orphan Orders',
      purpose: 'Non-cancelled HL7 orders in the selected period (by scheduled date, or received date when there is none) that have no PACS study with the same accession number and were not linked by hand.',
      valueMeaning: 'One number. Each orphan is an order the hospital placed that never turned into a study RAYD can see.',
      interpretation: 'A small steady number is normal: an exam ordered in several parts is often stored under one accession. A sudden rise usually means accession numbers are not reaching PACS, or exams are being done without their order.',
      caveats: 'CHN’s HL7 order feed starts on 6 October 2026, so a period before that date shows no orphans.'
    },
    'r22-early-churn': {
      title: 'Early Churn Warning',
      purpose: 'Referring physicians who are still sending patients but whose monthly referrals fell during the selected period.',
      valueMeaning: 'Each doctor’s active months in the period are split into two halves. 1st / 2nd Half Avg = average referrals per month in each half. A doctor is listed when the second half is at least 25% lower, with at least 3 active months, at least 15 studies, and a referral in the last 90 days of the period. High Risk = a drop of 40% or more. At most 3 doctors are shown. Days Silent = days since their last referral.',
      interpretation: 'Use a range of 6 months or more; with 2–3 months the halves are too short to mean much. Studies with no referring doctor name (Unknown) are left out.'
    },
    'r22-confirmed-churn': {
      title: 'Confirmed Churn — Month-over-Month Drop',
      purpose: 'Doctors whose referrals in their latest active month were lower than in the month before.',
      valueMeaning: 'Prev = referrals in the earlier month, the large number = the latest month, the badge = % change. Only doctors with at least 5 referrals in the earlier month are shown, biggest drop first, top 10.',
      interpretation: 'If the selected range ends in the middle of a month, that month is incomplete and many doctors will look like they dropped. End the range on the last day of a month for a fair comparison.'
    },
    'r22-tree': {
      title: 'Modality → Top 5 Procedures',
      purpose: 'Breaks the period’s studies down by modality, then shows the 5 most common study descriptions in each modality.',
      valueMeaning: 'Each node shows its study count. Click a node to open or close it. There is no device level: CHN’s PACS overwrites the device AE title, so RAYD groups studies by modality only. UNMAPPED = a modality with no row on the Modality mapping page (rare: the ETL adds new modalities by itself).',
      interpretation: 'A modality much bigger or smaller than usual, or study descriptions under the wrong modality, are worth a look.'
    },
    'r22-top-referrers': {
      title: 'Top Referring Physicians (Study Volume)',
      purpose: 'The 10 doctors who referred the most studies in the period.',
      valueMeaning: 'Bar length = number of studies, not patients. Studies with no referring doctor name are grouped as Unknown and can appear in the list.',
      interpretation: 'Compare with Physician Loyalty: a doctor who ranks high here but lower there sends the same patients back often.'
    },
    'r22-loyalty': {
      title: 'Physician Loyalty (Unique Patient Count)',
      purpose: 'The 10 doctors who referred the most different patients in the period.',
      valueMeaning: 'Bar = number of distinct patients. A patient referred three times counts once. Studies with no referring doctor name are grouped as Unknown.',
      interpretation: 'Doctors high on both referral charts bring a broad patient base. A big gap between a doctor’s studies and patients means many repeat exams for the same people.'
    },
    'r22-phys-modality': {
      title: 'Physician → Modality Preference (Top 10)',
      purpose: 'For the 10 highest-volume referring doctors, how their studies split across modalities.',
      valueMeaning: 'One stacked bar per doctor; each colour is a modality and its length is the number of studies. Unknown doctors are left out.',
      interpretation: 'Shows each doctor’s specialty pattern, and which modality would feel it if that doctor’s referrals changed.'
    },
    'r22-phys-age': {
      title: 'Average Patient Age per Physician',
      purpose: 'Average patient age for the 15 highest-volume referring doctors with at least 5 studies.',
      valueMeaning: 'Bar = mean age at the time of the exam. Ages outside 0–110 are left out (the badge shows how many).',
      interpretation: 'Low averages point to paediatric practice, high ones to geriatric care. A doctor whose average changes a lot between periods may have changed practice, or has data-entry problems.'
    },
    'r22-proc-age': {
      title: 'Age Distribution per Procedure Code',
      purpose: 'How patient ages spread for the 20 most frequent procedure codes (each with at least 5 studies).',
      valueMeaning: 'Whiskers = youngest and oldest patient, box = the middle 50% of patients, line = median age, ◆ = mean age. The table underneath lists every value.',
      interpretation: 'A wide box means the procedure is used across all ages. A patient far outside the usual range for a procedure is worth checking for a data-entry error.'
    },
    'r22-phys-status': {
      title: 'Study Status per Top Physician',
      purpose: 'For the 10 highest-volume referring doctors, how their studies split by current PACS study status.',
      valueMeaning: 'Stacked bars; each colour is a status and its length is the number of studies.',
      interpretation: 'A doctor with a much larger share in one status (for example unread) than other doctors points to a workflow problem for that service.'
    },
    'r22-status': {
      title: 'Case Status Distribution',
      purpose: 'Studies in the period grouped by their current PACS study status.',
      valueMeaning: 'Bar height = number of studies in that status. Click a bar to list those studies, with CSV export.',
      interpretation: 'A growing unread bar means a reporting backlog. Status names come straight from PACS.'
    },
    'r22-gender': {
      title: 'Gender (Rose)',
      purpose: 'Male / female split of the period’s studies.',
      valueMeaning: 'Petal size = number of studies (not patients). Studies whose sex is not M or F, or whose age is outside 0–110, are left out; the badge shows how many had no usable sex.',
      interpretation: 'A strong skew usually reflects a dedicated service, such as mammography.'
    },
    'r22-age': {
      title: 'Age Group (Wave)',
      purpose: 'How the period’s studies spread across patient age groups.',
      valueMeaning: 'Each point = number of studies in that age group, from the age group on the patient record. Studies with sex other than M/F or age outside 0–110 are left out.',
      interpretation: 'Peaks show which age groups drive demand.'
    },

    // ── Report 25 — Executive ───────────────────────────────────────────────
    'r25-tat-percentiles': {
      title: 'TAT Percentiles',
      purpose: 'How report turnaround time (TAT) is spread across all reported studies in the period.',
      valueMeaning: 'On this report, TAT = minutes from midnight (00:00) of the study date to the final signed report: PACS keeps the study date without a time. P25: a quarter of studies were faster. Median: half were faster. P75: three quarters were faster.',
      interpretation: 'Compare periods by the median, not the average: a few very late reports pull the average up. Because the clock starts at midnight, every TAT here also includes the hours between midnight and the scan.',
      caveats: 'Report 36 measures radiologist TAT from the first open of the study to the final report, so its figures are lower and are the ones to use for reading speed.'
    },
    'r25-tat-class': {
      title: 'TAT per Patient Class',
      purpose: 'Average report TAT for each patient class on the PACS study.',
      valueMeaning: 'Bar = average TAT in minutes, from midnight of the study date to the final signed report. Studies with no final report are left out.',
      interpretation: 'Emergency should be fastest. Averages are pulled up by a few late reports, so read this next to the TAT Percentiles strip.'
    },
    'r25-hourly': {
      title: 'Hourly Arrival Pattern',
      purpose: 'How many orders are scheduled in each hour of the day, added up over the whole period.',
      valueMeaning: 'X = hour of the day (0–23), Y = number of orders scheduled in that hour. Only orders that produced a PACS study count.',
      interpretation: 'Peaks are the busiest hours for staffing; long quiet stretches are good times for maintenance.'
    },
    'r25-modality-split': {
      title: 'Modality Volume Split',
      purpose: 'Share of the period’s studies by modality.',
      valueMeaning: 'Each slice is one modality; hover for the count and %. At CHN the modality is the PACS study’s modality group (the PACS does not keep the device identity).',
      interpretation: 'Shows where the workload sits.'
    },
    'r25-duration-tat': {
      title: 'Duration vs. TAT Correlation',
      purpose: 'Whether longer exams also take longer to report.',
      valueMeaning: 'Each dot is one study. X = expected exam duration from the procedure map (minutes), Y = TAT (minutes, from midnight of the study date). r is the correlation: near 1 = strongly linked, near 0 = unrelated. Extreme values are removed first (the badge shows how many).',
      interpretation: 'A low r is normal: report delays come from reading and sign-off, not from how long the scan takes. Studies whose procedure has no duration are not shown.'
    },
    'r25-util-matrix': {
      title: 'Utilization & Revenue Matrix',
      purpose: 'How much of each modality’s opening time was used, per weekday.',
      valueMeaning: 'One row per modality: CHN’s PACS does not keep device identity, so capacity is set per modality (the sum of the site’s devices of that type). Cell = exam minutes on that weekday ÷ (opening minutes for that weekday × number of those weekdays in the period). Exam minutes come from the procedure durations; opening minutes from the modality capacity or weekly schedule. Red > 85%, green 30–85%, amber < 30%, grey = 0% (no exams, or no opening hours set for that day). RVU = technical RVU total.',
      interpretation: 'Red means the modality is overbooked on that day; amber means spare capacity.',
      caveats: 'New procedure codes start at 15 minutes and 1 RVU until real values are entered on Mapping → Procedures, and opening hours come from the Modality page. Check those first when a value looks off.'
    },
    'r25-ae-tat': {
      title: 'Best vs Worst AE by Average TAT',
      purpose: 'Average report TAT for each AE, fastest to slowest.',
      valueMeaning: 'At CHN the AE is the modality group (the PACS overwrites device AE titles), so each bar is a modality. Bar = average TAT in minutes, from midnight of the study date. AEs with fewer than 5 reported studies are left out.',
      interpretation: 'Same grouping as Avg TAT by Modality, average only. Modalities differ in reading time, so compare a modality with itself over time.'
    },
    'r25-mod-tat': {
      title: 'Avg TAT by Modality',
      purpose: 'Average and median report TAT per modality.',
      valueMeaning: 'Minutes from midnight of the study date to the final report, fastest to slowest. Modalities with fewer than 5 reported studies are left out.',
      interpretation: 'When the average is far above the median, a small number of very late reports is pulling it up.'
    },
    'r25-eff-score': {
      title: 'Modality Efficiency Score',
      purpose: 'How much technical RVU each modality produced for each 1% of its capacity used.',
      valueMeaning: 'One bar per modality (CHN’s AE = modality group). Score = RVU ÷ average utilization %. Green = within 75% of the best, blue = 40–75%, amber = below 40%. Modalities with 0% utilization are left out.',
      interpretation: 'A low score means the modality is busy compared with the RVU it brings, often because of long, low-RVU exams. RVU defaults to 1 per procedure, so the score only means something once RVUs are set on the Procedures page.'
    },
    'r25-stress-output': {
      title: 'Stress vs Output',
      purpose: 'Each modality’s utilization against its RVU output.',
      valueMeaning: 'Each dot is a modality (CHN’s AE = modality group). X = average utilization %, Y = total technical RVU, bigger dot = higher utilization. Red = above 85% (the dashed stress line), amber = below 30%, blue = in between.',
      interpretation: 'Low utilization with high RVU is efficient; high utilization with low RVU means the modality is busy with low-value work.'
    },
    'r25-tat-dist': {
      title: 'TAT Distribution',
      purpose: 'How many studies fall in each turnaround range.',
      valueMeaning: 'Bars for 0–30, 31–60, 61–90, 91–120, 121–180, 181–240, 241–360 and over 360 minutes, counted from midnight of the study date. Green = fast, blue = moderate, amber = slow, red = over 4 hours.',
      interpretation: 'Because the clock starts at midnight, a study scanned in the afternoon already has hundreds of minutes before anyone reads it, so the “360m+” bar is always tall here. Report 36 shows reading TAT from the first open.'
    },
    'r25-rvu-tat': {
      title: 'RVU vs TAT',
      purpose: 'Whether higher-value studies are reported faster or slower than low-value ones.',
      valueMeaning: 'Each dot is a study. X = clinical (radiologist) RVU from the Procedures page, Y = TAT in minutes from midnight of the study date. Extreme values are removed first (the badge shows how many).',
      interpretation: 'High-RVU studies should not wait longer than low-RVU ones.'
    },
    'r25-outliers': {
      title: 'TAT Outlier Detection',
      purpose: 'Studies with an extremely long TAT.',
      valueMeaning: 'A study is listed when its TAT is above the upper quartile plus 1.5 times the spread of the middle 50% (the standard outlier rule). Up to 50 studies, longest first, with modality, radiologist and patient class.',
      interpretation: 'Look for repeats: the same radiologist, modality or patient class showing up again and again usually points to one cause.'
    },
    'r25-unread-aging': {
      title: 'Unread Study Aging',
      purpose: 'Studies in the period that are still unread, grouped by age.',
      valueMeaning: 'Age = time since midnight of the study date (PACS keeps the date only), in four buckets: 0–24h, 24–48h, 48–72h, 72h+. The chart splits each bucket by modality. Only studies whose PACS status contains “unread” count.',
      interpretation: 'Anything in 72h+ is a backlog to chase. Report 36 lists the same backlog by the radiologist each study is assigned to.'
    },
    'r25-shift': {
      title: 'Studies per Shift',
      purpose: 'How the period’s orders spread across the Morning, Afternoon and Night shifts.',
      valueMeaning: 'Each order with a PACS study is placed in a shift by its scheduled hour. Any hour outside the Morning and Afternoon hours counts as Night. Set the hours with Configure Shifts.',
      interpretation: 'Compare the volume of each shift with the staff on that shift.'
    },
    'r25-addendum': {
      title: 'Addendum Rate by Radiologist',
      purpose: 'How often each radiologist’s final reports later received an addendum.',
      valueMeaning: '% = reports with an addendum ÷ final reports signed, for radiologists with at least 5 signed reports. Reports are counted on the day they were finalized and radiologist names are resolved as on Report 36, so each radiologist’s report total matches Report 36.',
      interpretation: 'An addendum can be a correction or just added information, so a high rate is a reason to review, not a verdict.'
    },
    'r25-tech-by-tech': {
      title: 'By Technician',
      purpose: 'Exam speed and issue rate for each technician, from the HL7 orders.',
      valueMeaning: 'Done TAT = minutes from the order’s scheduled time to when the exam was marked done (or, if it was never marked, when the scanner finished in PACS). Scanner TAT = scheduled time to scanner completion. vs Team = difference from the department average. Issues % = exams with at least one flag. Exams over 24 hours are kept out of the averages and counted under Outliers.',
      interpretation: 'Compare technicians who work on the same modality (Main Type). A high Issues % deserves a look at that technician’s flagged exams below.',
      caveats: 'CHN’s HL7 order feed starts on 6 October 2026: earlier periods have no technician data.'
    },
    'r25-tech-by-mod': {
      title: 'By Modality',
      purpose: 'Average and median Done TAT per modality.',
      valueMeaning: 'Exams = completed HL7 orders (marked done, or completed by the scanner in PACS). Done TAT = minutes from the scheduled time to done.',
      interpretation: 'When the average is far above the median, a few long exams are pulling it up.'
    },
    'r25-tech-exams': {
      title: 'Flagged Exams',
      purpose: 'Completed exams whose timing looks unusual.',
      valueMeaning: 'Expected = the procedure’s duration (30 minutes if it has none). Flags: Before scheduled = done before its scheduled time. Too early = under half the expected time. Too late = over twice the expected time. Overlap = on the same modality, still not done when the next exam was due to start. The date box narrows the list to one day.',
      interpretation: 'A flag is a prompt, not proof: an early “done” often means the order was rescheduled. Acknowledge a flag once it is explained.'
    },
    'r25-tech-ack': {
      title: 'Acknowledgements Log',
      purpose: 'Flagged exams and whether someone has reviewed them.',
      valueMeaning: 'One row per flagged exam. Ack’d by and Ack’d at fill in once someone acknowledges the flag, with their note.',
      interpretation: 'Rows with no acknowledgement are flags nobody has reviewed yet.'
    },
    'r25-tech-never-done': {
      title: 'Never Marked Done',
      purpose: 'Orders past their expected end time that were never marked done and have no scanner completion time.',
      valueMeaning: 'Overdue By = time since the scheduled time plus the expected duration.',
      interpretation: 'Usually exams that were done but not closed, or orders that should have been cancelled.'
    },

    // ── Report 27 — Audit & Comparison ──────────────────────────────────────
    'r27-multi-order': {
      title: 'Multiple Orders Detected',
      purpose: 'Orders that look like duplicates: same patient, same procedure code, same scheduled day.',
      valueMeaning: 'Flagged Orders = all orders in such groups, Order Groups = number of groups, Affected Patients = distinct patients.',
      interpretation: 'Some are legitimate, such as repeat exams. Many groups point to duplicate order entry.'
    },
    'r27-hourly': {
      title: 'Hourly Load (Volume Trend)',
      purpose: 'How many orders are scheduled in each hour of the day across the period.',
      valueMeaning: 'X = hour (0–23), Y = orders by scheduled time, including orders that never produced a study.',
      interpretation: 'R25’s Hourly Arrival Pattern counts only orders with a study. A big difference at some hour means many orders at that time are not performed.'
    },
    'r27-match': {
      title: 'Proc-ID Reconciliation',
      purpose: 'Whether the procedure code on the order matches the procedure code on its PACS study.',
      valueMeaning: 'Match = identical codes. Mismatch = different codes, or no study linked to the order (so orphan orders count as mismatches).',
      interpretation: 'A high mismatch share means exams change after ordering, or the two systems use different codes. Subtract the Orphan Orders figure above to see true code changes.'
    },
    'r27-demo': {
      title: 'Demographics (Age & Gender)',
      purpose: 'Orders by patient age group and sex.',
      valueMeaning: 'Age groups 0–8, 9–18, 19–64 and 65+, stacked by sex. Age is calculated from the date of birth as of today, not at the time of the order. Unknown = no date of birth.',
      interpretation: 'Shows which groups drive demand. For periods far in the past, ages read slightly older than they were at the exam.'
    },
    'r27-status': {
      title: 'Order Status Mix',
      purpose: 'Orders by their current status.',
      valueMeaning: 'Bar = number of orders with each status code (for example CA = cancelled).',
      interpretation: 'A high cancelled share, or many orders stuck in an early status, points to a workflow problem.'
    },
    'r27-ae': {
      title: 'Modality Distribution',
      purpose: 'Orders by the modality group of their linked PACS study.',
      valueMeaning: 'Bar = orders whose linked study belongs to that modality group (CT, MR, US…). Unknown = orders with no study.',
      interpretation: 'Shows how orders spread across modalities. A large Unknown bar means many orphan orders.'
    },

    // ── Super Report ────────────────────────────────────────────────────────
    'sr-briefing': {
      title: 'Daily Briefing',
      purpose: 'A ready-made summary of the last 30 days, last 90 days and year to date.',
      valueMeaning: 'Computed every morning by the analytics job; the time shown is when it ran. The 30- and 90-day views are compared with the period of the same length just before; year to date is compared with the same dates last year.',
      interpretation: 'A quick read before running a full report. It ignores the filters on the left.'
    },
    'sr-conflicts': {
      title: 'Conflict Patients',
      purpose: 'Patients whose PACS ID contains “$$$”, which marks an unresolved identity or merge conflict.',
      valueMeaning: 'The badge shows how many; Export CSV downloads the list.',
      interpretation: 'These patients’ studies may be split across records. Ask the PACS administrators to merge or correct them.'
    },
    'sr-summary': {
      title: 'Executive Summary',
      purpose: 'A plain-language summary of the report for the selected period and filters.',
      valueMeaning: 'Written from the same numbers as the cards below: volume, orders, referrers, demographics, TAT, storage, the comparison with the earlier period, and alerts. If no comparison period is set, it is the same number of days just before the selected period.',
      interpretation: 'Alerts follow fixed rules: order fulfillment under 85%, storage above 2 GB per day or up more than 30%, study volume down more than 20%, or a gender split beyond 70/30.'
    },
    'sr-volume': {
      title: 'Volume & Trends',
      purpose: 'Study volume for the period.',
      valueMeaning: 'Studies, average per day, busiest day and its count, and the top 5 modalities. % = change from the comparison period.',
      interpretation: 'The daily average counts only days that had studies, so closed days do not drag it down.'
    },
    'sr-storage': {
      title: 'Storage & Images',
      purpose: 'PACS storage used by the period’s studies.',
      valueMeaning: 'Total GB, average GB per day and the 5 modalities using the most GB come from the nightly storage summary. Total images = images in the period’s studies, as counted by PACS (the same figure as the Images card), with its change against the comparison period.',
      interpretation: 'GB per day rising while study volume stays flat means bigger studies, for example new protocols or thinner slices.'
    },
    'sr-physicians': {
      title: 'Top Referring Physicians',
      purpose: 'The 10 doctors who referred the most studies in the period.',
      valueMeaning: 'Count = studies. Only studies with a referring doctor’s last name count.',
      interpretation: 'Open Referring Intel for one doctor’s full history.'
    },
    'sr-orders': {
      title: 'Orders & Fulfillment',
      purpose: 'Orders scheduled in the period and how many produced a PACS study.',
      valueMeaning: 'Fulfilled = orders linked to a study. Fulfillment % = fulfilled ÷ total. Only the order modality and order control filters apply here.',
      interpretation: 'Below 85% is flagged. Unfulfilled orders are cancelled exams, no-shows, or exams whose study never linked to the order.'
    },
    'sr-demographics': {
      title: 'Patient Demographics',
      purpose: 'Sex and patient-class split of the period’s studies.',
      valueMeaning: 'Counts are studies, not distinct patients. Inpatient and Outpatient group the patient-class codes listed in the pc_inpatient / pc_outpatient settings; the bars below show every class code as stored.',
      interpretation: 'Codes in neither group, such as emergency, appear only in the bar list.'
    },
    'sr-insights': {
      title: 'Clinical Insights',
      purpose: 'Automatic signals from comparing this period with the comparison period.',
      valueMeaning: '⚠ = critical, ▲ = warning, ℹ = information, most serious first.',
      interpretation: 'These are rule-based checks on the numbers in this report, not clinical judgements.'
    },
    'reports-per-radiologist': {
      title: 'Reports per Radiologist',
      purpose: 'How many reported studies each radiologist signed, split by modality, AE or month.',
      valueMeaning: 'Rows = the chosen split, columns = radiologists, cells = studies dated in the period that have a final report signed by that radiologist. At CHN the AE is the modality group, so By AE Title mirrors By Modality. The Procedure Breakdown table below does the same for the 60 most common procedure codes. Name variants are merged using the physician alias mapping.',
      interpretation: 'This table follows the study date, like the rest of the Super Report. Report 36 counts the same reports on the day they were signed, so its totals for a period can differ.'
    },

    // ── Referring Intel ─────────────────────────────────────────────────────
    'ri-loyalty-map': {
      title: 'Loyalty & Turnaround Correlation',
      purpose: 'For all referring doctors at once: does slower reporting go with fewer returning patients?',
      valueMeaning: 'Each bubble is a doctor with at least 10 studies in the History Window. X = median TAT (PACS arrival to final signed report), Y = % of their patients who had another study from them within 90 days, size = study volume. Click a bubble to open that doctor.',
      interpretation: 'Slow reports with few returns is the group to look at first. Return rates also depend on specialty, so compare similar doctors.'
    },
    'ri-summary': {
      title: 'Physician Summary',
      purpose: 'Headline numbers for the selected doctor.',
      valueMeaning: 'Every number covers the History Window (6M / 12M / 24M / All), like the rest of the page. Median TAT = PACS arrival to final signed report, compared with the department median over the last 90 days. Activity Trend compares the share of the window’s studies that came in the last 30 days with an even pace over the window (not shown for All).',
      interpretation: 'A red Median TAT means this doctor’s patients wait longer for reports than the department average. A falling Activity Trend is an early sign of fewer referrals.'
    },
    'ri-volume': {
      title: 'Monthly Study Volume',
      purpose: 'Studies this doctor referred each month in the History Window.',
      valueMeaning: 'One value per month.',
      interpretation: 'A decline over several months is a stronger signal than one low month. The current month is still incomplete.'
    },
    'ri-tat': {
      title: 'Monthly Median TAT',
      purpose: 'How quickly this doctor’s patients got their reports, month by month.',
      valueMeaning: 'Median minutes from PACS arrival to final signed report, per month in the History Window.',
      interpretation: 'A rising line is worth raising with the reading team before it becomes a complaint.'
    },
    'ri-modality': {
      title: 'Modality Mix',
      purpose: 'Which modalities this doctor’s studies used, in the History Window.',
      valueMeaning: 'Top 12 modalities by number of studies.',
      interpretation: 'Shows which department depends most on this doctor’s referrals.'
    },
    'ri-body': {
      title: 'Body Parts',
      purpose: 'The body parts examined in this doctor’s studies, in the History Window.',
      valueMeaning: 'Top 10 body parts as recorded in PACS; Unspecified = left blank.',
      interpretation: 'Reflects the doctor’s specialty. A large Unspecified share is a PACS data-quality issue.'
    },
    'ri-class': {
      title: 'Patient Class',
      purpose: 'Split of this doctor’s studies by patient class, in the History Window.',
      valueMeaning: 'Studies per patient-class code on the PACS study; Unknown = empty.',
      interpretation: 'Tells you whether this is mainly a hospital, clinic or emergency referrer.'
    },
    'ri-age': {
      title: 'Patient Age Distribution',
      purpose: 'Ages of this doctor’s patients at the time of the exam, in the History Window.',
      valueMeaning: 'Studies per 10-year age band.',
      interpretation: 'Shows the patient population this doctor serves.'
    },
    'ri-findings': {
      title: 'Top NLP-Extracted Findings',
      purpose: 'The most common findings in reports for this doctor’s patients.',
      valueMeaning: 'From HL7 report messages analysed by the NLP worker, matched to studies by accession number. Only affirmed findings count: a finding written as negative (e.g. “no fracture”) is not counted. Top 20, in the History Window.',
      interpretation: 'Shows the case mix behind the referrals; useful when discussing protocols with the doctor.',
      caveats: 'CHN’s HL7 report feed starts on 6 October 2026: older studies have no findings.'
    },
    'ri-critical': {
      title: 'Critical Findings',
      purpose: 'How many of this doctor’s analysed reports contained a critical finding.',
      valueMeaning: '% = reports the NLP flagged as critical ÷ reports analysed, in the History Window.',
      interpretation: 'Compare with doctors of the same specialty: emergency and oncology referrers naturally run higher.'
    },
    'ri-return': {
      title: 'Patient Return Rate',
      purpose: 'How many of this doctor’s patients came back for another study they referred.',
      valueMeaning: 'Patients with a study within 30, 90 and 365 days of a previous study from the same doctor, in the History Window. Each patient counts once.',
      interpretation: 'A high rate can mean planned follow-up imaging or repeat exams; read it together with the case mix.'
    },
    'ri-pt-trend': {
      title: 'Volume × Patients per Month',
      purpose: 'Studies and distinct patients per month for this doctor.',
      valueMeaning: 'Two series per month in the History Window: studies and distinct patients.',
      interpretation: 'A widening gap between the two means more exams per patient.'
    },
    'ri-recent': {
      title: 'Last 20 Studies',
      purpose: 'This doctor’s most recent studies in the History Window.',
      valueMeaning: 'TAT = minutes from PACS arrival to final signed report; empty = not reported yet.',
      interpretation: 'A quick check of what this doctor sent most recently, and whether it has been reported.'
    },

    // ── Dashboard — Yesterday tab ───────────────────────────────────────────
    'yd-glance': {
      title: 'Yesterday at a Glance',
      purpose: 'Yesterday’s activity (Beirut time) in numbers.',
      valueMeaning: 'Orders = orders scheduled for yesterday; Cancelled = status CA. Studies = PACS studies dated yesterday, compared with the daily average of the 7 days before. Unique Patients: new = no other study in the past year. ER Patients = studies whose PACS patient location is ER. Peak Hour = the hour with the most scheduled orders.',
      interpretation: 'The figures come from the data sync, which runs every 5 hours, so yesterday is complete only after the first sync of the morning.'
    },
    'yd-physicians': {
      title: 'Top Referring Physicians',
      purpose: 'The 5 doctors who referred the most studies dated yesterday.',
      valueMeaning: 'Bar = studies. Studies with no referring doctor name are not counted.',
      interpretation: 'Open Referring Intel for a doctor’s longer-term pattern.'
    },
    'yd-ae': {
      title: 'AE Performance',
      purpose: 'How each modality was used yesterday.',
      valueMeaning: 'At CHN the AE is the modality group (the PACS overwrites device AE titles). Procedures = studies per modality. Utilization = exam minutes ÷ capacity. Exam minutes come from the procedure durations (15 minutes when a procedure has none); capacity comes from the Modality page (the sum of that modality’s devices), then the weekly schedule, else 480 minutes. On Utilization, amber = 85% or more, red = 100% or more.',
      interpretation: 'A modality over 100% usually means its durations or capacity are set wrong, not that it really ran beyond its hours.'
    },
    'yd-reconciliation': {
      title: 'Orders ↔ PACS Reconciliation',
      purpose: 'Whether yesterday’s orders turned into the expected number of PACS studies.',
      valueMeaning: 'Orders Placed = yesterday’s non-cancelled orders with a numeric accession. Orders for the same patient and modality with consecutive accession numbers form one Linked Group, because a protocol such as a full-body CT produces one study from several orders. Expected Studies = linked groups + single orders. Discrepancy = expected − PACS studies.',
      interpretation: 'A positive discrepancy means fewer studies than expected (missed or unlinked exams); a negative one means studies without an order for that day.'
    },

    // ── ORU Analytics ───────────────────────────────────────────────────────
    'oru-spotlight': {
      title: 'Critical Findings — Needs Attention',
      purpose: 'The latest reports in which a critical finding was detected.',
      valueMeaning: 'Same detection as the Critical Findings Log further down; View All jumps there.',
      interpretation: 'Check that each one was communicated to the referring doctor.'
    },
    'oru-normal-abnormal': {
      title: 'Normal vs Abnormal Rate',
      purpose: 'Share of reports in the period that read as normal.',
      valueMeaning: 'Normal = the report’s impression (or the whole report, when the message has no impression) contains one of these English phrases: no acute, unremarkable, within normal, normal study, no significant, no abnormality, no evidence of acute, no pathological, no active disease, normal limits. Everything else counts as abnormal.',
      interpretation: 'This is a phrase search, not a reading of the report, so treat it as a rough trend. A normal report worded differently, or written in French, counts as abnormal, so the true normal rate is higher. A sudden change usually comes from a change in report wording or templates.'
    },
    'oru-modality': {
      title: 'Reports by Modality',
      purpose: 'Number of reports per modality in the period.',
      valueMeaning: 'Modality comes from the report message; if missing, from the HL7 order, then the PACS study with the same accession, then the procedure mapping.',
      interpretation: 'Should follow study volume. A modality with many studies but few reports means reports are not reaching RAYD over HL7.'
    },
    'oru-procs': {
      title: 'Top Procedures by Volume',
      purpose: 'The 10 procedures with the most reports in the period.',
      valueMeaning: 'Count of reports per procedure code, with the procedure name from the message.',
      interpretation: 'Shows where the reading workload is concentrated.'
    },
    'oru-physicians': {
      title: 'Reporting Physician Activity',
      purpose: 'The 10 reporting physicians with the most reports in the period.',
      valueMeaning: 'Physicians are shown by the ID sent in the HL7 message, not by name.',
      interpretation: 'Use it to see the reading workload by radiologist. Report 36 shows the same workload by name, from PACS.'
    },
    'oru-gaps': {
      title: 'Section Gap Alerts',
      purpose: 'Reports where a standard section is missing or empty.',
      valueMeaning: 'Counts for Technique, Findings (Résultats) and Conclusion; English and French section headers are both recognised (for example Impression counts as Conclusion). Click a card for the radiologists concerned (by physician ID), with CSV export.',
      interpretation: 'A radiologist with many gaps may be using a different template, or skipping a section.'
    },
    'oru-treemap': {
      title: 'Report Term Treemap',
      purpose: 'The diagnoses that appear most often in the period’s reports.',
      valueMeaning: 'Box size = number of reports containing the diagnosis. Only affirmed findings count (negated ones like “no fracture” are not), and diagnoses marked benign are left out. The search field filters the terms shown.',
      interpretation: 'Gives the disease burden and case mix at a glance.'
    },
    'oru-sections': {
      title: 'Section Content Analysis',
      purpose: 'The most common words in each section of the reports.',
      valueMeaning: 'One treemap per section (Technique, Findings, Conclusion); box size = how often the word appears.',
      interpretation: 'Useful for checking that each section is used for what it should contain.'
    },
    'oru-critical-log': {
      title: 'Critical Findings Log',
      purpose: 'Reports flagged as containing a critical finding.',
      valueMeaning: 'A report is flagged when the NLP finds an affirmed (not negated) diagnosis that is not marked benign, or one of the critical keywords (built-in, or added under Manage Keywords). Every report in the period is checked; the 20 most recent flagged ones are listed, and the Critical Findings count at the top counts all of them.',
      interpretation: 'Use it for follow-up and audit. If a real critical finding is missing, add its keyword with Manage Keywords.'
    },
    'oru-nlp': {
      title: 'NLP Analysis Results',
      purpose: 'A second, statistical analysis of the reports. Every report is analysed automatically in the background; the clusters are rebuilt on the 1st of each month, or with Rebuild Clusters.',
      valueMeaning: 'Before scoring, medspaCy removes every negated or historical mention (“no fracture”, “pas de fracture”, “history of stroke”), the same way as for the Critical Findings Log. Classification = Normal, Borderline or Critical, with a severity score from 1 to 5; a report with nothing affirmed is Normal. Diagnostic Clusters group reports with similar wording (TF-IDF and K-means) and are named from their top words; “No affirmed findings” holds the reports with nothing left once negations are removed. Extracted Medical Terms = the affirmed keywords this analysis found.',
      interpretation: 'Use it for trends and case mix, not to judge an individual patient: it is a word-statistics model, so read the report itself for any single case.'
    },

    // ── Report 36 — Radiology Reporting Analytics ───────────────────────────
    'r36-about': {
      title: 'Report 36 — How It Counts',
      purpose: 'The radiologists’ reporting work: how many final reports each radiologist signed, how fast they read (TAT), what they read, when they sign during the day, and what is still waiting for them.',
      valueMeaning: 'One rule for every count: a report is counted on the day it was FINALIZED, not the study day. A study done on 1 March and signed on 3 March counts once, on 3 March. The radiologist is the one who signed the final report: the signing name in PACS, or the PACS signer login when PACS has no name (for example DBUSTROS). Name variants are merged through the physician alias mapping, and reports with no known signer are left out. Structured reports (SR) generated by PACS are not studies and are never counted. The left-panel filters (patient class, modality, AE, patient location) apply to every panel; at CHN the AE is the modality group.',
      interpretation: 'Use this page for radiologist workload and speed. Report 25 and the Super Report count by study date, so for the same period their totals can differ from this page: a study done on 31 March and signed on 1 April is in March there and in April here.',
      caveats: 'The data comes from the PACS sync, which runs every 5 hours, so the latest few hours may be missing. The sync re-reads studies from the last 10 days only: a report signed more than 10 days after its study date would not reach RAYD (none seen so far; the longest delay measured was 7.5 days). The Unread Backlog card is the one panel that uses the study date instead of the final date (an unread study has no final date).'
    },
    'r36-insights': {
      title: 'Statistical Insights',
      purpose: 'Automatic checks on each radiologist’s numbers for the selected period, shown only when something stands out.',
      valueMeaning: 'Two checks run. TAT skew: a radiologist’s average TAT divided by their median TAT (both from first open to final). 1.8× or more = warning (a few slow cases pull the average up), 2.5× or more = critical (very delayed cases dominate the average). Batch signing: 5 or more final reports signed by the same radiologist within 3 minutes; click the signal to list the accessions and times. Red = critical, amber = warning.',
      interpretation: 'A skew signal sends you to that radiologist’s slowest studies: open their row in the Peer Ranking and compare the average with the median. A batch signal shows reports that were probably read earlier and signed together.',
      caveats: 'These are statistical prompts, not judgements. Batch signing is often legitimate, for example validating reports dictated earlier, or signing a resident’s reports at the end of a list. “No statistical anomalies” means neither check fired, not that every number is good.'
    },
    'r36-unread-assign': {
      title: 'Unread Backlog by Assigned Radiologist',
      purpose: 'Studies dated in the selected period that are still unread in PACS, grouped by the radiologist each study is assigned to in PACS (“assign to radiologist”).',
      valueMeaning: 'Unread = the PACS study status contains “unread”, the same rule as the home page and Report 25’s Unread Study Aging. Unread = number of such studies for that radiologist; By Modality splits them; Oldest Study = the earliest study date among them, with how many days ago that was (red at 3 days or more). Not assigned = unread studies nobody was assigned to. The PACS assignment is a login (DBUSTROS@DN); it is shown as the radiologist’s name when the alias mapping or the reports that login signed give one, otherwise as the login. The Unread and Assigned counters at the top add up the rows.',
      interpretation: 'A long row, or an oldest study several days back, is the backlog to chase with that radiologist. A large Not assigned row means studies are waiting without an owner.',
      caveats: 'This card follows the study date: unread studies have no final date. Status and assignee are as of the last sync (shown in the subtitle). The sync re-reads only the last 10 days of studies, so a study read more than 10 days after its study date can still show as unread here; for long periods, treat the oldest rows with that in mind. Scanned documents (OT) with no report are counted as unread too.'
    },
    'r36-tat': {
      title: 'TAT — First Open to Final Report',
      purpose: 'How radiologist turnaround time (TAT) is measured on this page, at the hospital’s request: from the first time a radiologist opened the study to the final report.',
      valueMeaning: 'TAT = final report time − first open. First open = the first time a radiologist displayed the images (Vue PACS workstation or Vue Motion) or locked the study for reading, to the second, taken from the PACS audit trail. A radiologist here is any PACS login that has signed at least one final report, so a technologist checking images or a clinician viewing them does not start the clock. Only opens before the final signature count: reopening a signed study does not move its TAT. The line under the heatmap tells you how many of the period’s final reports were timed.',
      interpretation: 'This is reading time from first contact, not waiting time from the scan. It replaces the old clock (final − midnight of the study date), which added every hour between midnight and the scan to each study: a 1-hour read on a 14:00 scan used to show as 15 hours.',
      caveats: 'The first open can be by a colleague: if one radiologist opens a study and another signs it, the clock starts at the first one’s open. A study is NOT MEASURED (counted in volumes, left out of every TAT figure, never given a guessed start) when no radiologist opened it before the final, or when it falls outside the audit copy: it reached PACS before the copy’s first day, or was signed after the last copy. The copy is refreshed with each sync (every 5 hours), so the most recent reports show as not measured until the next sync.'
    },
    'r36-tat-heatmap': {
      title: 'Radiologist TAT Heatmap',
      purpose: 'Average TAT for each radiologist on each modality.',
      valueMeaning: 'Row = radiologist who signed the final report, column = modality, number and colour = average TAT in minutes from first open to final (dark blue = fast, through yellow, to red = slow; the scale under the chart runs from 0 to the slowest cell). Only timed studies count; a missing cell means the radiologist had no timed study on that modality. Hover a cell for the TAT, the number of final reports and how many of them were timed, and the clinical RVU; click a cell to jump to that radiologist in the Peer Ranking.',
      interpretation: 'Compare down a column (the same modality), not across a row: an MR naturally takes longer to read than an X-ray. A cell built on a handful of timed studies can be extreme by chance, so check the timed count on hover before drawing conclusions.'
    },
    'r36-peer': {
      title: 'Peer Ranking',
      purpose: 'Each radiologist’s reading speed and volume side by side.',
      valueMeaning: 'Avg TAT and Median = minutes from first open to final, over the radiologist’s timed studies. Studies = all final reports they signed in the period. Timed = how many of those have a TAT (see the TAT note). RVU = clinical RVU from Mapping → Procedures (1 per study while a procedure has no value set). Avg TAT is coloured by rank among colleagues: green = fastest third, blue = middle third, red = slowest third; the bar shows the average against the slowest colleague. Sort with the buttons (click again to reverse; radiologists with no timed study always go last). The arrow on a row opens the split by modality.',
      interpretation: 'Rank by the median: a few delayed reports inflate the average. Read speed together with volume and case mix: a radiologist reading many complex studies will be slower. A ranking built on few timed studies is fragile.'
    },
    'r36-volume': {
      title: 'Reports per Radiologist',
      purpose: 'How many final reports each radiologist signed in the period, split by modality, AE or month.',
      valueMeaning: 'Rows = the chosen split, columns = radiologists, cells = final reports signed in the period (counted on the day they were finalized). By Month uses the month of the final signature. At CHN the AE is the modality group, so By AE Title mirrors By Modality. Shading is relative to each radiologist’s own busiest row, so it shows where each person’s work goes, not who did more. TOTAL row and column add up the table. CSV exports all three splits; PDF prints what is on screen.',
      interpretation: 'Shows who carries which part of the workload. Use By Month to spot absences, holidays or a shift of work between radiologists. The grand total equals the Final Reports counter at the top of the page.'
    },
    'r36-procedures': {
      title: 'Procedure Breakdown',
      purpose: 'The 60 procedures with the most final reports in the period, and who reported them.',
      valueMeaning: 'Each procedure is shown by its name from the order table (the most common order text for its code); a code with no order text keeps its code. Codes that share one name are counted as one row. Cells = final reports signed by that radiologist; shading is relative to each radiologist’s own busiest procedure. Search filters by name; Show top limits the rows; the CSV and PDF follow your search and limit.',
      interpretation: 'Shows sub-specialisation: who reads the mammograms, the cardiac CTs, the MSK MRs. A procedure read by only one person is a cover risk when that person is away.'
    },
    'r36-cadence': {
      title: 'Reporting Cadence Analysis',
      purpose: 'When each radiologist signs final reports across the day and the week, built only from the final-signature timestamps.',
      valueMeaning: 'Heatmap: rows = weekday, columns = hour, colour = how many reports the radiologist signed in that hour, added up over the period; click a box to list the accessions signed in it, with a Copy button. First / Last Signature = the average time of each working day’s first and last signature. Inactive Intervals / Day = average number of gaps of 20 minutes or more between two consecutive signatures on the same day; Avg Interval Duration = their average length. Days = days with at least one signature. Daily Log = the last 60 working days: first and last signature, reports signed, span in hours, and the intervals. Enhanced View relabels the same numbers (Arrival, Departure, Breaks) and marks intervals by length: ☕ up to 35 min, 🚬 36–70 min, 🏃 over 70 min; a first signature at 10:00 or later is shown in amber. PNG exports a radiologist’s card with its daily log.',
      interpretation: 'Shows working patterns: when reading starts and ends, the busiest hours, and how evenly reports are signed. Compare the heatmap with when studies arrive to see whether reading keeps up.',
      caveats: 'A signature is not presence. Between two signatures a radiologist may be reading a long case, dictating, doing procedures, teaching or in a meeting; signing in batches makes the day look shorter and the gaps longer; a night shift is split at midnight into two days. The Enhanced View labels are informal shorthand, not attendance records, and should not be used for staffing or HR decisions on their own.'
    }
  },

  show: function (key) {
    const exp = this.explanations[key];
    if (!exp) {
      console.warn(`No chart explanation for: ${key}`);
      return;
    }
    const esc = s => String(s).replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));
    if (typeof Swal === 'undefined') {
      // SweetAlert2 comes from a CDN; if it is blocked, still show the text.
      alert(`${exp.title}\n\nWhat it shows: ${exp.purpose}\n\nHow to read it: ${exp.valueMeaning}\n\nWhat to look for: ${exp.interpretation}`
            + (exp.caveats ? `\n\nGood to know: ${exp.caveats}` : ''));
      return;
    }
    const block = (label, text, last) => `
      <div${last ? '' : ' style="margin-bottom: 16px;"'}>
        <p class="swal-info-label">${label}</p>
        <p class="swal-info-text">${esc(text)}</p>
      </div>`;
    Swal.fire({
      title: esc(exp.title),
      html: `<div style="text-align: left; font-size: 14px;">
        ${block('What it shows', exp.purpose)}
        ${block('How to read it', exp.valueMeaning)}
        ${block('What to look for', exp.interpretation, !exp.caveats)}
        ${exp.caveats ? block('Good to know', exp.caveats, true) : ''}
      </div>`,
      icon: 'info',
      confirmButtonText: 'Close',
      confirmButtonColor: '#60a5fa',
      background: '#232b28',
      color: '#e2e8f0',
      customClass: {
        container: 'swal-info-container',
        popup: 'swal-info-popup',
        title: 'swal-info-title',
        htmlContainer: 'swal-info-html',
        confirmButton: 'swal-info-button'
      }
    });
  }
};

// Capture phase, so an icon inside a clickable header (tab, collapsible) opens
// its explanation without also triggering the header.
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

function initChartExplanationStyles() {
  if (document.getElementById('chart-explanation-styles')) return;

  const styleEl = document.createElement('style');
  styleEl.id = 'chart-explanation-styles';
  styleEl.textContent = `
    .swal-info-container {
      --swal-modal-width: 560px !important;
    }
    .swal-info-popup {
      background: #232b28 !important;
      border: 1px solid #28332e !important;
      border-radius: 12px !important;
      box-shadow: 0 24px 60px rgba(0,0,0,0.6) !important;
    }
    .swal-info-title {
      color: #60a5fa !important;
      font-size: 18px !important;
      font-weight: 900 !important;
      margin-bottom: 16px !important;
    }
    .swal-info-html {
      color: #e2e8f0 !important;
      font-size: 13px !important;
      max-height: 65vh;
      overflow-y: auto !important;
    }
    .swal-info-label {
      margin: 0 0 6px 0;
      font-weight: 600;
      color: #60a5fa;
    }
    .swal-info-text {
      margin: 0;
      color: #cbd5e1;
      line-height: 1.55;
    }
    .swal-info-button {
      background-color: #60a5fa !important;
      color: #0a1628 !important;
      font-weight: 800 !important;
      border-radius: 8px !important;
      padding: 10px 24px !important;
      text-transform: uppercase !important;
      letter-spacing: 0.05em !important;
      font-size: 11px !important;
    }
    .swal-info-button:hover {
      background-color: #3b82f6 !important;
      filter: brightness(1.1);
    }

    /* Info icon */
    .chart-info-icon {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 18px;
      height: 18px;
      border-radius: 50%;
      background: rgba(96, 165, 250, 0.15);
      color: #60a5fa;
      font-size: 12px;
      font-style: normal;
      text-transform: none;
      letter-spacing: 0;
      vertical-align: middle;
      cursor: pointer;
      transition: all 0.2s ease;
      margin-left: 6px;
      flex-shrink: 0;
    }
    .chart-info-icon:hover,
    .chart-info-icon:focus-visible {
      background: rgba(96, 165, 250, 0.3);
      transform: scale(1.1);
      outline: none;
    }
    .chart-info-icon:active {
      transform: scale(0.95);
    }

    /* Light mode */
    body.light .swal-info-popup {
      background: #ffffff !important;
      border-color: #e2e8f0 !important;
    }
    body.light .swal-info-title {
      color: #2b6cb0 !important;
    }
    body.light .swal-info-html {
      color: #1a202c !important;
    }
    body.light .swal-info-label {
      color: #2b6cb0;
    }
    body.light .swal-info-text {
      color: #475569;
    }
    body.light .swal-info-button {
      background-color: #0ea5e9 !important;
      color: #ffffff !important;
    }
    body.light .swal-info-button:hover {
      background-color: #0284c7 !important;
    }
    body.light .chart-info-icon {
      background: rgba(14, 165, 233, 0.15);
      color: #0284c7;
    }
    body.light .chart-info-icon:hover {
      background: rgba(14, 165, 233, 0.3);
    }
  `;
  document.head.appendChild(styleEl);
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initChartExplanationStyles);
} else {
  initChartExplanationStyles();
}
