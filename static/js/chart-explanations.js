/**
 * Chart Explanations
 * An info icon next to a chart or card title opens a short note: what it shows,
 * how to read it, what to look for. Shown in a SweetAlert2 modal.
 *
 * Copied by hand from the HL7 branch (34eb2d37) and adapted: the texts below are
 * written against Mazloum's own queries, keyed per page (r22-, r25-, r27-, sr-,
 * ri-, yd-, oru-). Icons are wired by a data attribute instead of inline onclick,
 * so icons inside HTML injected later (R25 Technicians tab) work too:
 *
 *   <i class="bi bi-info-circle chart-info-icon" data-explain="r22-tree"
 *      role="button" tabindex="0" aria-label="About this chart" title="About this chart"></i>
 */

const ChartExplanations = {
  explanations: {

    // ── Report 22 — Operations (Flow Analytics & Churn) ─────────────────────
    'r22-orphans': {
      title: 'HL7 Orphan Orders',
      purpose: 'Non-cancelled HL7 orders in the selected period (by scheduled date, or received date when there is none) that have no PACS study with the same accession number and were not linked by hand.',
      valueMeaning: 'One number. Each orphan is an order the hospital placed that never turned into a study RAYD can see.',
      interpretation: 'A small steady number is normal: an exam ordered in several parts is often stored under one accession. A sudden rise usually means accession numbers are not reaching PACS, or exams are being done without their order.'
    },
    'r22-early-churn': {
      title: 'Early Churn Warning',
      purpose: 'Referring physicians who are still sending patients but whose monthly referrals fell during the selected period.',
      valueMeaning: 'Each doctor’s active months in the period are split into two halves. 1st / 2nd Half Avg = average referrals per month in each half. A doctor is listed when the second half is at least 25% lower, with at least 3 active months, at least 15 studies, and a referral in the last 90 days of the period. High Risk = a drop of 40% or more. At most 3 doctors are shown. Days Silent = days since their last referral.',
      interpretation: 'Use a range of 6 months or more; with 2–3 months the halves are too short to mean much. External referrals and unknown names are left out.'
    },
    'r22-confirmed-churn': {
      title: 'Confirmed Churn — Month-over-Month Drop',
      purpose: 'Doctors whose referrals in their latest active month were lower than in the month before.',
      valueMeaning: 'Prev = referrals in the earlier month, the large number = the latest month, the badge = % change. Only doctors with at least 5 referrals in the earlier month are shown, biggest drop first, top 10.',
      interpretation: 'If the selected range ends in the middle of a month, that month is incomplete and many doctors will look like they dropped. End the range on the last day of a month for a fair comparison.'
    },
    'r22-tree': {
      title: 'Modality → AE Source → Top 5 Procedures',
      purpose: 'Breaks the period’s studies down by modality, then by the device (AE title) that stored them in PACS, then the 5 most common study descriptions on each device.',
      valueMeaning: 'Each node shows its study count. Click a node to open or close it. UNMAPPED = a device with no entry on the Modality mapping page.',
      interpretation: 'A device under the wrong modality, or a large UNMAPPED branch, means the device mapping needs fixing.'
    },
    'r22-top-referrers': {
      title: 'Top Referring Physicians (Study Volume)',
      purpose: 'The 10 doctors who referred the most studies in the period.',
      valueMeaning: 'Bar length = number of studies, not patients. EXTERNAL DOC is a placeholder for outside referrers, so it is not ranked; its total is shown under the chart.',
      interpretation: 'Compare with Physician Loyalty: a doctor who ranks high here but lower there sends the same patients back often.'
    },
    'r22-loyalty': {
      title: 'Physician Loyalty (Unique Patient Count)',
      purpose: 'The 10 doctors who referred the most different patients in the period.',
      valueMeaning: 'Bar = number of distinct patients. A patient referred three times counts once.',
      interpretation: 'Doctors high on both referral charts bring a broad patient base. A big gap between a doctor’s studies and patients means many repeat exams for the same people.'
    },
    'r22-phys-modality': {
      title: 'Physician → Modality Preference (Top 10)',
      purpose: 'For the 10 highest-volume referring doctors, how their studies split across modalities.',
      valueMeaning: 'One stacked bar per doctor; each colour is a modality and its length is the number of studies. Modality comes from the device mapping; UNMAPPED = device not mapped.',
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
      valueMeaning: 'TAT = minutes from the study’s arrival in PACS to the final signed report. P25: a quarter of studies were faster. Median: half were faster. P75: three quarters. P90: the slowest 10% took longer than this.',
      interpretation: 'Compare periods by the median, not the average: a few very late reports pull the average up. A P90 far above the median means a long tail of late reports; the Efficiency tab lists them under “TAT — Above P90”.'
    },
    'r25-wait': {
      title: 'Patient Wait Time',
      purpose: 'Time from the HL7 order message to the study being stored in PACS.',
      valueMeaning: 'Average, median and P90 in minutes, and the number of studies measured. Only studies whose accession matches an HL7 order count. Gaps that are negative or longer than 14 days are treated as bad matches and left out.',
      interpretation: 'This is not the time a patient sat in the waiting room: it also includes orders placed in advance and scheduling delays. Use it for trends, and check the slowest cases under “Patient Wait Time — Above P90”.'
    },
    'r25-tat-class': {
      title: 'TAT per Patient Class',
      purpose: 'Average report TAT for each patient class on the PACS study.',
      valueMeaning: 'Bar = average TAT in minutes, from the study’s arrival in PACS to the final signed report. Studies with no final report are left out.',
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
      valueMeaning: 'Each slice is one modality; hover for the count and %. Modality comes from the device (AE title) mapping; N/A = device not mapped.',
      interpretation: 'Shows where the workload sits. A growing N/A slice means a new device needs mapping on the Modality page.'
    },
    'r25-duration-tat': {
      title: 'Duration vs. TAT Correlation',
      purpose: 'Whether longer exams also take longer to report.',
      valueMeaning: 'Each dot is one study. X = expected exam duration from the procedure map (minutes), Y = TAT (minutes). r is the correlation: near 1 = strongly linked, near 0 = unrelated. Extreme values are removed first (the badge shows how many).',
      interpretation: 'A low r is normal: report delays come from reading and sign-off, not from how long the scan takes. Studies whose procedure has no duration are not shown.'
    },
    'r25-util-matrix': {
      title: 'Device Utilization & Revenue Matrix',
      purpose: 'How much of each device’s opening time was used, per weekday.',
      valueMeaning: 'Cell = exam minutes on that weekday ÷ (opening minutes for that weekday × number of those weekdays in the period). Exam minutes come from the procedure durations; opening minutes from the device capacity or weekly schedule. Red > 85%, green 30–85%, amber < 30%, grey = 0% (no exams, or no opening hours set for that day). Device RVU = technical RVU total.',
      interpretation: 'Red means the device is overbooked on that day; amber means spare capacity. Wrong durations or opening hours on the Modality and Procedures pages make these numbers wrong, so check those first when a value looks off.'
    },
    'r25-ae-tat': {
      title: 'Best vs Worst AE by Average TAT',
      purpose: 'Average report TAT for each device (AE title), fastest to slowest.',
      valueMeaning: 'Bar = average TAT in minutes for studies stored by that device. Devices with fewer than 5 reported studies are left out.',
      interpretation: 'A device that is always slow often reflects the kind of exams it does; compare devices of the same modality.'
    },
    'r25-mod-tat': {
      title: 'Avg TAT by Modality',
      purpose: 'Average and median report TAT per modality.',
      valueMeaning: 'Minutes, fastest to slowest. Modalities with fewer than 5 reported studies are left out.',
      interpretation: 'When the average is far above the median, a small number of very late reports is pulling it up.'
    },
    'r25-rad-heatmap': {
      title: 'Radiologist TAT Heatmap',
      purpose: 'Average report TAT for each radiologist on each modality.',
      valueMeaning: 'Row = radiologist who signed the final report, column = modality, colour = average TAT (darker = slower). Hover a cell for the TAT, study count and RVU. Only studies with a final report count.',
      interpretation: 'Compare down a column (same modality), not across columns: modalities differ in reading time.'
    },
    'r25-peer': {
      title: 'Peer Ranking',
      purpose: 'Each radiologist’s reporting speed and volume side by side.',
      valueMeaning: 'Avg TAT and Median in minutes, Studies = final reports signed, RVU = clinical RVU. Sort with the buttons; the arrow on a row opens the split by patient location and modality. Avg TAT is coloured by rank among colleagues.',
      interpretation: 'Rank by the median: a few delayed reports inflate the average. Read speed together with volume, since a radiologist reading many complex studies will be slower.'
    },
    'reports-per-radiologist': {
      title: 'Reports per Radiologist',
      purpose: 'How many final reports each radiologist signed, split by modality, device (AE title) or month.',
      valueMeaning: 'Rows = radiologists, columns = the chosen split, cells = studies they signed. The Procedure Breakdown table below does the same for the 60 most common procedure codes. Name variants are merged using the physician alias mapping.',
      interpretation: 'Shows who carries which part of the workload. Use By Month to spot absences or a shift in workload.'
    },
    'r25-eff-score': {
      title: 'Modality Efficiency Score',
      purpose: 'How much technical RVU each device produced for each 1% of its capacity used.',
      valueMeaning: 'One bar per device (AE title). Score = device RVU ÷ average utilization %. Green = within 75% of the best device, blue = 40–75%, amber = below 40%. Devices with 0% utilization are left out.',
      interpretation: 'A low score means the device is busy compared with the RVU it brings, often because of long, low-RVU exams. RVU defaults to 1 per procedure, so the score only means something once RVUs are set on the Procedures page.'
    },
    'r25-stress-output': {
      title: 'Stress vs Output',
      purpose: 'Each device’s utilization against its RVU output.',
      valueMeaning: 'Each dot is a device. X = average utilization %, Y = total technical RVU, bigger dot = higher utilization. Red = above 85% (the dashed stress line), amber = below 30%, blue = in between.',
      interpretation: 'Low utilization with high RVU is efficient; high utilization with low RVU means the device is busy with low-value work.'
    },
    'r25-tat-dist': {
      title: 'TAT Distribution',
      purpose: 'How many studies fall in each turnaround range.',
      valueMeaning: 'Bars for 0–30, 31–60, 61–90, 91–120, 121–180, 181–240, 241–360 and over 360 minutes. Green = fast, blue = moderate, amber = slow, red = over 4 hours.',
      interpretation: 'Most studies should sit on the left. A tall “360m+” bar usually means studies done late in the day were reported the next morning.'
    },
    'r25-rvu-tat': {
      title: 'RVU vs TAT',
      purpose: 'Whether higher-value studies are reported faster or slower than low-value ones.',
      valueMeaning: 'Each dot is a study. X = RVU, Y = TAT in minutes. Extreme values are removed first (the badge shows how many).',
      interpretation: 'High-RVU studies should not wait longer than low-RVU ones.'
    },
    'r25-outliers': {
      title: 'TAT Outlier Detection',
      purpose: 'Studies with an extremely long TAT.',
      valueMeaning: 'A study is listed when its TAT is above the upper quartile plus 1.5 times the spread of the middle 50% (the standard outlier rule). Up to 50 studies, longest first, with device, radiologist and patient class.',
      interpretation: 'Look for repeats: the same radiologist, device or patient class showing up again and again usually points to one cause.'
    },
    'r25-p90': {
      title: 'TAT — Above P90',
      purpose: 'The slowest 10% of studies by TAT, for root-cause review.',
      valueMeaning: 'Every study above the period’s P90, longest first, up to 50.',
      interpretation: 'Unlike the outlier list, this always shows the worst 10%, even in a period where nothing is extreme.'
    },
    'r25-wait-p90': {
      title: 'Patient Wait Time — Above P90',
      purpose: 'The studies with the longest gap between the HL7 order message and PACS storage.',
      valueMeaning: 'Studies above the wait-time P90, longest first, up to 50.',
      interpretation: 'Long gaps are often orders entered days in advance rather than real waiting; check the scheduled date before acting.'
    },
    'r25-unread-aging': {
      title: 'Unread Study Aging',
      purpose: 'Studies in the period that are still unread, grouped by age.',
      valueMeaning: 'Age = time since midnight of the study date (PACS keeps the date only), in four buckets: 0–24h, 24–48h, 48–72h, 72h+. The chart splits each bucket by modality. Only studies whose PACS status contains “unread” count.',
      interpretation: 'Anything in 72h+ is a backlog to chase.'
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
      valueMeaning: '% = reports with an addendum ÷ final reports signed, for radiologists with at least 5 signed reports. The overall rate covers those same radiologists.',
      interpretation: 'An addendum can be a correction or just added information, so a high rate is a reason to review, not a verdict.'
    },
    'r25-tech-by-tech': {
      title: 'By Technician',
      purpose: 'Exam speed and issue rate for each technician.',
      valueMeaning: 'Done TAT = minutes from the order’s scheduled time to when the exam was marked done (or, if it was never marked, when the scanner finished in PACS). Scanner TAT = scheduled time to scanner completion. vs Team = difference from the department average. Issues % = exams with at least one flag. Exams over 24 hours are kept out of the averages and counted under Outliers.',
      interpretation: 'Compare technicians who work on the same modality (Main Type). A high Issues % deserves a look at that technician’s flagged exams below.'
    },
    'r25-tech-by-mod': {
      title: 'By Modality',
      purpose: 'Average and median Done TAT per modality.',
      valueMeaning: 'Exams = completed exams that have a PACS study. Done TAT = minutes from the scheduled time to done.',
      interpretation: 'When the average is far above the median, a few long exams are pulling it up.'
    },
    'r25-tech-exams': {
      title: 'Technician Exams',
      purpose: 'Every completed exam in the period, with flags for unusual timing.',
      valueMeaning: 'Expected = the procedure’s duration (30 minutes if it has none). Flags: Before scheduled = done before its scheduled time. Too early = under half the expected time. Too late = over twice the expected time. Overlap = on the same modality, still not done when the next exam was due to start. “Flagged only” shows just the flagged exams; the CSV follows your filters.',
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
    'r27-busiest': {
      title: 'Busiest Day & Time (Weekly Pattern)',
      purpose: 'Which weekday and time of day get the most orders.',
      valueMeaning: 'Rows = weekdays, columns = 2-hour blocks covering the hours seen in the data. Shade = total orders in that slot over the period (darker = busier); the line in each cell shows that slot’s day-by-day count. Click a cell to split it into CT, MR, DX, CR and Other.',
      interpretation: 'Use it to place staff, and to plan maintenance in the quiet slots.'
    },
    'r27-dom': {
      title: 'Busiest Day of Month',
      purpose: 'Orders per calendar day (1st to 31st), added up over every month in the period.',
      valueMeaning: 'Bar = total orders scheduled on that day of the month.',
      interpretation: 'Days 29–31 do not occur in every month, so they naturally look lower. Recurring peaks often follow administrative cycles rather than clinical need.'
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
      title: 'Modality Distribution (AE)',
      purpose: 'Orders by the device (AE title) that stored their PACS study.',
      valueMeaning: 'Bar = orders whose linked study was stored by that device. Unknown = orders with no study.',
      interpretation: 'Shows how orders spread across machines. A large Unknown bar means many orphan orders.'
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
      valueMeaning: 'Written from the same numbers as the cards below: volume, orders, referrers, demographics, TAT, storage, the comparison with the earlier period, and alerts.',
      interpretation: 'Alerts follow fixed rules: order fulfillment under 85%, storage above 2 GB per day or up more than 30%, study volume down more than 20%, or a gender split beyond 70/30.'
    },
    'sr-compare': {
      title: 'Period Comparison',
      purpose: 'The selected period and the comparison period side by side.',
      valueMeaning: 'If no comparison period is set, it is the same number of days just before the selected period. Change = current − comparison; green = better, red = worse, grey = no better or worse direction. Fulfillment % changes are in points. ER-delayed studies = non-emergency studies whose report took longer than the period’s 75th-percentile TAT (PACS arrival to final report) on days that also had emergency studies.',
      interpretation: 'In short periods one unusual day can swing the %, so compare like with like, such as full months.'
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
      valueMeaning: 'From the nightly storage summary: total GB, average GB per day, and the 5 modalities using the most GB.',
      interpretation: 'GB per day rising while study volume stays flat means bigger studies, for example new protocols or thinner slices.'
    },
    'sr-physicians': {
      title: 'Top Referring Physicians',
      purpose: 'The 10 doctors who referred the most studies in the period.',
      valueMeaning: 'Count = studies. EXTERNAL DOC (outside referrers) is not ranked and is shown separately as External referrals.',
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
      valueMeaning: 'Total Studies, Unique Patients, Median TAT, Critical Rate and Last Study cover the doctor’s full history; 90d Return Rate uses the History Window. Median TAT = PACS arrival to final signed report, compared with the department median over the last 90 days. Activity Trend compares the share of the doctor’s studies from the last 30 days with an even pace over the History Window.',
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
      purpose: 'Which modalities this doctor’s studies used, over their full history.',
      valueMeaning: 'Top 12 modalities by number of studies.',
      interpretation: 'Shows which department depends most on this doctor’s referrals.'
    },
    'ri-body': {
      title: 'Body Parts',
      purpose: 'The body parts examined in this doctor’s studies, over their full history.',
      valueMeaning: 'Top 10 body parts as recorded in PACS; Unspecified = left blank.',
      interpretation: 'Reflects the doctor’s specialty. A large Unspecified share is a PACS data-quality issue.'
    },
    'ri-class': {
      title: 'Patient Class',
      purpose: 'Split of this doctor’s studies by patient class, over their full history.',
      valueMeaning: 'Studies per patient-class code on the PACS study; Unknown = empty.',
      interpretation: 'Tells you whether this is mainly a hospital, clinic or emergency referrer.'
    },
    'ri-age': {
      title: 'Patient Age Distribution',
      purpose: 'Ages of this doctor’s patients at the time of the exam, over their full history.',
      valueMeaning: 'Studies per 10-year age band.',
      interpretation: 'Shows the patient population this doctor serves.'
    },
    'ri-findings': {
      title: 'Top NLP-Extracted Findings',
      purpose: 'The most common findings in reports for this doctor’s patients.',
      valueMeaning: 'From HL7 report messages analysed by the NLP worker, matched to studies by accession number. Only affirmed findings count: a finding written as negative (e.g. “no fracture”) is not counted. Top 20, full history.',
      interpretation: 'Shows the case mix behind the referrals; useful when discussing protocols with the doctor.'
    },
    'ri-critical': {
      title: 'Critical Findings',
      purpose: 'How many of this doctor’s analysed reports contained a critical finding.',
      valueMeaning: '% = reports the NLP flagged as critical ÷ reports analysed, over the full history.',
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
      purpose: 'This doctor’s most recent studies.',
      valueMeaning: 'TAT = minutes from PACS arrival to final signed report; empty = not reported yet.',
      interpretation: 'A quick check of what this doctor sent most recently, and whether it has been reported.'
    },

    // ── Dashboard — Yesterday tab ───────────────────────────────────────────
    'yd-glance': {
      title: 'Yesterday at a Glance',
      purpose: 'Yesterday’s activity (Beirut time) in numbers.',
      valueMeaning: 'Orders = orders scheduled for yesterday; Cancelled = status CA. Studies = PACS studies dated yesterday, compared with the daily average of the 7 days before. Unique Patients: new = no other study in the past year. ER Patients = studies whose PACS patient location is ER. Peak Hour = the hour with the most scheduled orders.',
      interpretation: 'The figures come from the nightly data sync, so yesterday is complete only after that morning’s sync has run.'
    },
    'yd-physicians': {
      title: 'Top Referring Physicians',
      purpose: 'The 5 doctors who referred the most studies dated yesterday.',
      valueMeaning: 'Bar = studies. External referrals (outside doctors) are counted separately and not ranked.',
      interpretation: 'Open Referring Intel for a doctor’s longer-term pattern.'
    },
    'yd-ae': {
      title: 'AE Performance',
      purpose: 'How each device (AE title) was used yesterday.',
      valueMeaning: 'Procedures = studies per device. Utilization = exam minutes ÷ device capacity. Exam minutes come from the procedure durations (15 minutes when a procedure has none); capacity comes from the Modality page, then the weekly schedule, else 480 minutes. On Utilization, amber = 85% or more, red = 100% or more.',
      interpretation: 'A device over 100% usually means its durations or capacity are set wrong, not that it really ran beyond its hours.'
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
      valueMeaning: 'Normal = the conclusion (or the whole report when there is no conclusion) contains a normal phrase such as “unremarkable”, “no acute” or “within normal”. Everything else counts as abnormal.',
      interpretation: 'This is a phrase match, not a reading of the report, so treat it as a trend. A sudden change usually comes from a change in report wording or templates.'
    },
    'oru-modality': {
      title: 'Reports by Modality',
      purpose: 'Number of reports per modality in the period.',
      valueMeaning: 'Modality comes from the order; if missing, from the PACS study, then from the procedure mapping.',
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
      interpretation: 'Use it to see the reading workload by radiologist.'
    },
    'oru-gaps': {
      title: 'Section Gap Alerts',
      purpose: 'Reports where a standard section is missing or empty.',
      valueMeaning: 'Counts for Technique, Résultats and Conclusion. Click a card for the radiologists concerned (by physician ID), with CSV export.',
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
      valueMeaning: 'One treemap per section (Technique, Résultats, Conclusion); box size = how often the word appears.',
      interpretation: 'Useful for checking that each section is used for what it should contain.'
    },
    'oru-critical-log': {
      title: 'Critical Findings Log',
      purpose: 'Reports flagged as containing a critical finding.',
      valueMeaning: 'A report is flagged when the NLP finds an affirmed (not negated) diagnosis that is not marked benign, or one of the custom critical keywords added under Manage Keywords. The most recent reports in the period are checked, and up to 20 flagged reports are listed.',
      interpretation: 'Use it for follow-up and audit. If a real critical finding is missing, add its keyword with Manage Keywords.'
    },
    'oru-nlp': {
      title: 'NLP Analysis Results',
      purpose: 'A second, statistical analysis of the reports, run on demand.',
      valueMeaning: 'Classification = Normal, Borderline or Critical, with a severity score from 1 to 5. Diagnostic Clusters group reports with similar wording (TF-IDF and K-means) and are named from their top words. Extracted Medical Terms = the keywords this analysis found.',
      interpretation: 'This analysis does not handle negation (“no fracture” can still count “fracture”), so use it for trends, not for individual patients.'
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
      alert(`${exp.title}\n\nWhat it shows: ${exp.purpose}\n\nHow to read it: ${exp.valueMeaning}\n\nWhat to look for: ${exp.interpretation}`);
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
        ${block('What to look for', exp.interpretation, true)}
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
      --swal-modal-width: 520px !important;
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
