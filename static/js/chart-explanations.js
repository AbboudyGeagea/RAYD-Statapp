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
 * Keys are per page: r22- (Report 22), r25- (Report 25), and so on. tests/test_chart_explanations.py fails when a
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
    },

    // ── Report 25 — Shift & Device Utilization ──────────────────────────────────
    // Rows: Rizk Hospital studies in the dates and filters chosen that have a report time
    // and a reading radiologist. TAT = report time (last composed, else PACS final, else
    // the ORU result) minus the PACS insert time (else midnight of the study date).
    'r25-hourly-arrivals': {
      title: 'Hourly Arrival Pattern',
      purpose: 'RIS orders linked to the period’s studies, by the hour they were scheduled for.',
      valueMeaning: 'Bar = number of orders scheduled in that hour, added up over the whole range (not an average per day). It is the booked time, not the time the patient arrived or was scanned.',
      interpretation: 'Peaks show the hours the schedule fills. Compare with Hourly Utilization by Modality (actual scan start) to see whether the work follows the schedule or slips later.'
    },
    'r25-modality-split': {
      title: 'Modality Volume Split',
      purpose: 'The page’s studies (reported, Rizk Hospital) split by modality.',
      valueMeaning: 'Slice = number of studies on that modality and its share of the total. Modality comes from the device on the Mapping page; N/A = a device with no mapping.',
      interpretation: 'Only reported studies are counted here, so the totals are lower than Report 22’s exam volume for a period whose reports are not all written yet.'
    },
    'r25-duration-tat': {
      title: 'Duration vs. TAT Correlation',
      purpose: 'Each reported study as one point: how long the procedure takes against how long its report took.',
      valueMeaning: 'X = the procedure’s duration from the procedure catalogue (15 min when the procedure has none), Y = TAT in minutes. Extreme values on either axis are removed (outside 1.5 × the interquartile range); the badge says how many. r = the correlation, from −1 to 1: near 0 means scan length does not explain report time.',
      interpretation: 'The duration is a catalogue value, so points line up in vertical stripes, one per duration. A low r is the usual result and means delays come from reporting, not from the scan.'
    },
    'r25-peak-heatmap': {
      title: 'Hourly Utilization by Modality',
      purpose: 'Exams per modality per hour of the day, by the time the scan started on the RIS (performed procedure step).',
      valueMeaning: 'Darker cell = more exams started in that hour on that modality, added up over the whole range. Exams with no RIS start time are not shown.',
      interpretation: 'Quiet hours are candidates for maintenance or extra elective slots. A modality with no cells at all has no RIS start times in the period.'
    },
    'r25-device-matrix': {
      title: 'Device Utilization & Revenue Matrix',
      purpose: 'For each Rizk Hospital device with reported studies, how much of its opening time was used on each weekday of the period, and the RVU its studies carry.',
      valueMeaning: 'Cell = minutes used ÷ (daily opening minutes × number of that weekday in the range). Minutes used = the RIS start-to-end time of its exams when the device has any for that weekday, else the catalogue duration of its reported studies. Opening minutes = the daily capacity on the Mapping page, else the device’s weekly schedule; no schedule gives 0%. Red above 85%, amber below 30%. Clinical and technical RVU come from the procedure catalogue (1.0 when a procedure has none) and are never added together.',
      interpretation: 'A device at 0% every day usually has no opening hours defined, not no work. A device above 100% is working past its defined hours, or its capacity is set too low on the Mapping page.'
    },
    'r25-hours-split': {
      title: 'Opening Hours vs After Hours',
      purpose: 'Per modality, the scanning minutes that fell inside the devices’ RIS opening windows and outside them.',
      valueMeaning: 'An exam counts by the time it started (a 16:50–18:10 exam on a device closing at 18:00 counts as in-hours). In-hours % = inside minutes ÷ the opening time. After-hours % = outside minutes ÷ the closed time, which is mostly night, so small numbers are normal. Only exams with a RIS start and end time are counted; devices without a schedule are left out (the badge counts them).',
      interpretation: 'A modality with a few percent after hours is already doing real night volume. Use the exam count beside it, not the percentage alone.'
    },
    'r25-usage-heatmap': {
      title: 'When Is This Modality Actually Used',
      purpose: 'For the modality picked, scanning minutes by weekday and hour, against the hours its devices are scheduled open.',
      valueMeaning: 'Cell colour = minutes of exams started in that hour (RIS start to end), added up over the range. An hour counts as open when any device of that modality has an open window in it. Cells are marked unknown, not closed, when the modality has no schedule.',
      interpretation: 'Busy cells outside the open hours mean work done off schedule; open hours that stay empty are booked time not used.'
    },
    'r25-conflicts': {
      title: 'Assignment Discrepancies',
      purpose: 'Exams whose patient class or timing contradicts what the RIS schedule reserved that slot for.',
      valueMeaning: 'The exam’s RIS start time is matched to its device’s schedule window. Flags: an outpatient or ER patient in inpatient-reserved time, a non-ER patient in ER-reserved time, an exam during maintenance, on a holiday slot or while the device was closed. The count is the full total; each row opens to the most recent examples.',
      interpretation: 'A few are normal (an urgent case slotted where there was room). Many in one row mean the schedule no longer matches how the device is really used.'
    },
    'r25-ae-tat': {
      title: 'Best vs Worst AE by Average TAT',
      purpose: 'Average turnaround per device (room), for devices with at least 5 reported studies in the period.',
      valueMeaning: 'Bar = average TAT in minutes from PACS arrival to report time, fastest first. Rooms carry their RIS room name, else the AE title.',
      interpretation: 'TAT is mostly reporting time, so a slow room usually reflects what it scans (complex CT and MR take longer to report), not the technologists.'
    },
    'r25-modality-tat': {
      title: 'Avg TAT by Modality',
      purpose: 'Average and median turnaround per modality, for modalities with at least 5 reported studies.',
      valueMeaning: 'TAT in minutes from PACS arrival to report time. The median is the middle study; it is not pulled up by a few very late reports, so it is the fairer benchmark.',
      interpretation: 'A large gap between average and median means a few studies waited a very long time; find them in TAT Outlier Detection.'
    },
    'r25-eff-score': {
      title: 'Modality Efficiency Score',
      purpose: 'Per device, the technical RVU it produced for each percent of its opening time it used.',
      valueMeaning: 'Score = the device’s technical RVU ÷ its average utilization % (from the matrix). Green = within 75% of the best device, amber = below 40% of it. Devices at 0% utilization are left out.',
      interpretation: 'High scores come from devices doing high-value procedures in little time. A device with a high score and low utilization has room for more of the same work. Procedures missing an RVU in the catalogue count 1.0 and lower the score.'
    },
    'r25-stress-output': {
      title: 'Stress vs Output',
      purpose: 'Each device placed by its utilization % against the technical RVU its studies carry.',
      valueMeaning: 'X = average utilization % from the matrix, Y = technical RVU; larger dots = higher utilization.',
      interpretation: 'Bottom right (busy, low RVU) = time spent on low-value work; top left (quiet, high RVU) = capacity that pays off if filled.'
    },
    'r25-rvu-tat': {
      title: 'RVU vs TAT',
      purpose: 'Each reported study placed by its RVU against its turnaround, in two series: clinical and technical RVU.',
      valueMeaning: 'X = the procedure’s RVU from the catalogue, Y = TAT in minutes. Extreme values are removed for each series on its own (outside 1.5 × the interquartile range); the badge says how many.',
      interpretation: 'If high-RVU studies sit higher, complex studies wait longer for their report. Procedures with no RVU in the catalogue all sit at 1.0.'
    },
    'r25-outliers': {
      title: 'TAT Outlier Detection',
      purpose: 'The reported studies whose turnaround is far above the rest of the period.',
      valueMeaning: 'A study is listed when its TAT is above the third quartile plus 1.5 × the interquartile range; the 50 longest are shown, longest first, with room, radiologist and procedure.',
      interpretation: 'Look for one room, radiologist or procedure repeating in the list. Studies reported days later are often addenda or reports reopened on the PACS.'
    },
    'r25-unread-aging': {
      title: 'Unread Study Aging',
      purpose: 'Studies in the period whose PACS status is still unread, by how long ago their study date was.',
      valueMeaning: 'Age is counted from midnight of the study date to now, so a study done this afternoon is already a few hours old. Bands 0–24 h, 24–48 h, 48–72 h and 72 h+; the chart splits each band by modality. Every Rizk Hospital study counts, with or without a report.',
      interpretation: 'The 72 h+ band is the backlog to chase. Studies that will never be read on the PACS (outside reads, scanned documents) also sit there for ever; clear their status on the PACS or leave their device out with the filters.'
    },
    'r25-shifts': {
      title: 'Studies per Shift',
      purpose: 'RIS orders linked to the period’s studies, split into morning, afternoon and night shifts by their scheduled hour.',
      valueMeaning: 'Shift hours are the ones set with Configure Shift Hours; anything outside morning and afternoon counts as night. The chart splits each shift by modality.',
      interpretation: 'It is the booked time, not the scan time, so walk-ins and ER cases scanned at night but scheduled earlier count in the earlier shift.'
    },
    'r25-addendum': {
      title: 'Addendum Rate by Radiologist',
      purpose: 'For each radiologist who signed at least 5 final reports in the period, the share of studies that carry an addendum.',
      valueMeaning: 'Rate = studies flagged with an addendum ÷ studies with a PACS final signer and time. Names are merged through the physician aliases. The overall rate covers the listed radiologists only.',
      interpretation: 'An addendum is a correction or an addition after signing. A rate well above the others is worth a quality review; a rate of 0 for everyone usually means the PACS does not set the addendum flag.'
    },

    // ── Report 23 — Population Analytics ────────────────────────────────────────
    // Rows: Rizk Hospital studies in the dates and filters chosen, one row per RIS order
    // linked to the study (a study with no order counts once). SR, OT, bone densitometry
    // and reports without images are left out.
    'r23-conflicts': {
      title: 'Conflict Patients',
      purpose: 'Patients whose PACS fallback ID contains $$$, the PACS mark for an identity it could not resolve (a merge conflict), among patients with at least one Rizk Hospital study.',
      valueMeaning: 'The badge is the number of such patients. It ignores the dates and filters: it is the state of the patient list today. Export CSV lists them with their IDs, birth date, sex and number of studies.',
      interpretation: 'Each one may have studies split across two records, which undercounts returning patients and can hide prior exams. The list should shrink as the PACS team resolves them.'
    },
    'r23-class': {
      title: 'Patient Class Volume',
      purpose: 'The period’s studies by patient class code, as stamped on the PACS study.',
      valueMeaning: 'Bar = number of rows in that class. A study linked to several RIS orders counts once per order, as in the total above.',
      interpretation: 'Shows the inpatient, outpatient and emergency mix. A blank or unexpected code means the class did not reach the PACS for those studies.'
    },
    'r23-mod-age': {
      title: 'Average Age by Modality',
      purpose: 'Average patient age at the exam per modality.',
      valueMeaning: 'Bar = mean age in years. Studies with no age, or an age outside 0–110, are left out; the badge counts the ones outside the range.',
      interpretation: 'Mammography usually sits high and modalities with many children low. A modality whose average moves a lot between periods has a changed patient mix.'
    },
    'r23-age-proc': {
      title: 'Study Distribution by Age Group & Procedure',
      purpose: 'Studies per age group, split into the 5 most frequent procedures and Others.',
      valueMeaning: 'Stacked bar per age group; each colour is a procedure, named from the procedure catalogue, else by its RIS code. Studies with no age at all fall into 65+.',
      interpretation: 'Shows which exams each age group needs. A procedure growing in one group can justify dedicated slots, for example paediatric ultrasound.'
    },
    'r23-gender': {
      title: 'Gender Ratio',
      purpose: 'The period’s studies by patient sex.',
      valueMeaning: 'Slice = number of rows per sex code as on the PACS patient record (M, F, and any other code such as O or U). Every age counts.',
      interpretation: 'A visible slice for O or U means sex is missing or unknown on those patient records.'
    },
    'r23-monthly': {
      title: 'Monthly Volume Trend (Seasonal)',
      purpose: 'Studies per month across the selected range.',
      valueMeaning: 'Point = number of rows whose study date falls in that month, after the filters.',
      interpretation: 'Use a year or more to see seasons. The first and last months are partial when the range starts or ends mid-month.'
    },
    'r23-heatmap': {
      title: 'Modality × Patient Class Heatmap',
      purpose: 'Studies for each modality and patient class pair.',
      valueMeaning: 'Darker cell = more studies; hover for the count. Studies with no class are left out.',
      interpretation: 'Shows which modalities depend on emergency or inpatient work, and which are mainly outpatient, for staffing each shift.'
    },
    'r23-age-mod': {
      title: 'Age Distribution per Modality',
      purpose: 'For each modality, how its studies split across age groups.',
      valueMeaning: 'Stacked bar per modality; each colour is an age group, with the same cut-offs as the Age Group & Procedure chart. Studies with no age count in 0–1 month here.',
      interpretation: 'A large 0–1 month share on an adult modality usually means missing ages, not newborns.'
    },
    'r23-cube': {
      title: 'Age × Gender × Modality Cube',
      purpose: 'The 5 busiest modalities, each split by age group and sex.',
      valueMeaning: 'Age groups: under 19, 19–35, 36–64, 65+ (studies with no age count in 65+). U = sex not recorded.',
      interpretation: 'Use it to compare who each modality serves, for example women 36–64 on mammography against the whole population.'
    },
    'r23-return': {
      title: 'Patient Return Rate',
      purpose: 'How many patients came back for another study within 30, 60 and 90 days of their first study in the range.',
      valueMeaning: 'Only patients whose first study is at least 30 (60, 90) days before the end of the range count, so each has had the full window to return. Rate = those with another study on a later day within the window ÷ those patients. Visits before the start date are not seen.',
      interpretation: 'High rates point to follow-up programmes or repeat imaging. Patients split across two records (see Conflict Patients) lower the rate.'
    },

    // ── Report 27 — Order Audit ─────────────────────────────────────────────────
    // Rows: RIS orders (one per scheduled procedure step) scheduled in the dates chosen.
    'r27-agreement': {
      title: 'Do the RIS and PACS agree?',
      purpose: 'Month by month, the exams approved on the RIS against the Rizk Hospital studies approved on the PACS.',
      valueMeaning: 'RIS = orders in status Approved by scheduled month, with the parts of one exam ordered together (same RIS linked group) counted once, so a CT abdomen and pelvis is one. PACS = studies with status APPROVED by study month; structured reports, other objects and bone densitometry are left out. Difference = PACS − RIS. Months ending less than 14 days ago are greyed: their reports are still being written. The verdict uses the worst finished month: under 3% agree, under 10% drifting, above that broken.',
      interpretation: 'A small steady gap is normal: walk-ins and imported outside studies have no RIS order. A month that suddenly drifts means one side stopped receiving data; check the ETL log for that month.'
    },
    'r27-multi-order': {
      title: 'Multiple Orders Detected',
      purpose: 'Groups of RIS orders for the same patient, same procedure code and same scheduled date.',
      valueMeaning: 'Flagged Orders = all orders in those groups, Order Groups = number of groups, Affected Patients = distinct patients.',
      interpretation: 'Some are real (both sides, two parts of one exam); many are duplicates entered twice. Duplicates usually have no study of their own and show up again in Orphan Orders.'
    },
    'r27-hourly': {
      title: 'Hourly Load',
      purpose: 'For each hour of the day, how many exams are scheduled and how many really start, as an average per day over the dates chosen.',
      valueMeaning: 'Blue = orders by their scheduled hour. Red = exams by the hour the RIS status became Started (the first time). Both are divided by the number of days in the range. The line under the chart ranks Monday to Friday by exams started per day; Saturday and Sunday are shown apart.',
      interpretation: 'Red to the right of blue means exams start later than booked. If red stays at 0, the RIS status history (Started times) has not reached RAYD.'
    },
    'r27-proc-match': {
      title: 'Proc-ID Reconciliation',
      purpose: 'Orders whose RIS procedure code is exactly the procedure code on their PACS study.',
      valueMeaning: 'Match = the two codes are identical; Mismatch = they differ, or the order has no PACS study yet (cancelled, not done, or not linked).',
      interpretation: 'Read it with Orphan Orders: most mismatches are orders with no study. Among linked orders, a mismatch means the exam done was not the exam ordered, or the codes differ between the RIS and the PACS catalogue.'
    },
    'r27-busiest-matrix': {
      title: 'Busiest Day & Time (Weekly Pattern)',
      purpose: 'Orders by weekday and 2-hour block of their scheduled time.',
      valueMeaning: 'Cell shade = total orders in that slot over the whole range (darker = busier); the small line inside shows that slot day by day. Blocks cover only the hours that occur in the data. This grid counts scheduled orders, not exams started.',
      interpretation: 'Use it to see which slots are booked hardest. Compare with the busiest-day line under Hourly Load, which counts exams actually started.'
    },
    'r27-day-of-month': {
      title: 'Busiest Day of Month',
      purpose: 'Orders by calendar day of the month (1 to 31) of their scheduled date, added up over every month in the range.',
      valueMeaning: 'Bar = orders scheduled on that day number. Days 29 to 31 occur in fewer months, so they are naturally lower.',
      interpretation: 'Recurring peaks on the same day numbers usually follow insurance or billing cycles, or recurring clinics.'
    },
    'r27-peak-hours': {
      title: 'Peak Hours per Modality and Day',
      purpose: 'For each modality and day of the week, the hour when the most exams started.',
      valueMeaning: 'An exam counts at the hour it started on the RIS (the performed step’s start, else the RIS status Started), the clock of the red line in Hourly Load. The cell shows that hour (12:00 = 12:00 to 13:00), the average number started in it on such a day (its count divided by the number of those weekdays in the range) and the next busiest hour. Modality: the device’s when the exam has a PACS study, else the RIS order’s. Darker = busier for that modality; — = no exam of that modality started on that weekday.',
      interpretation: 'Use it to staff each modality for its own rush: the busiest hour often differs between modalities and between weekdays and Saturdays.'
    },
    'r27-status-mix': {
      title: 'Order Status Mix',
      purpose: 'Orders by their current RIS status, grouped into lifecycle stages.',
      valueMeaning: 'Each bar groups several RIS statuses; hover to see which and how many. Completed / Reported covers exam done, dictated, signed and approved. Cancelled includes discontinued. Unknown = a RIS status RAYD has no mapping for.',
      interpretation: 'Old dates should be almost all Completed or Cancelled; orders still Scheduled or Arrived weeks later were never closed on the RIS. The Cancelled Exams CSV in the sidebar lists the cancelled ones.'
    },

    // ── Report 32 — Radiologist Intelligence ────────────────────────────────────
    'r32-sla': {
      title: 'TAT Target Compliance by Patient Class',
      purpose: 'How many reported Rizk Hospital studies met their reporting target, per patient class.',
      valueMeaning: 'TAT runs from the PACS arrival to the report time (last composed, else PACS final, else the HL7 result). Targets: inpatient 24 h, urgent (ER) 24 h, outpatient 48 h unless changed in settings. % = studies at or under target ÷ studies scored; studies whose class has no target are not scored.',
      interpretation: 'Below 90% is shown in red. Open the radiologist cards below to see who the late reports belong to.'
    },
    'r32-rad-perf': {
      title: 'Radiologist Performance',
      purpose: 'One card per reading radiologist for the reported Rizk Hospital studies in the period.',
      valueMeaning: 'Reader = who last composed the report on the PACS, else the PACS final signer, else the HL7 report’s physician; names are merged through the physician aliases. Studies, average and median TAT (PACS arrival to report time, minutes), clinical RVU, RVU per hour (clinical RVU ÷ the catalogue duration of studies that have one), % within target. Percentile = rank among peers by average TAT; lower is faster.',
      interpretation: 'Compare medians, not averages: one study left open for days lifts an average a lot. A reader with few studies can rank anywhere by chance.'
    },
    'r32-rad-volume': {
      title: 'Reports per Radiologist',
      purpose: 'Reported Rizk Hospital studies per reader, broken down by modality, room, procedure or month.',
      valueMeaning: 'Cell = distinct studies with a report time. Reader = the PACS signing physician’s name, else who last composed the report, else the PACS final signer, merged through the physician aliases. The procedure view covers the 60 most frequent procedures.',
      interpretation: 'Shows who carries which modality and how the load moves month to month. Unknown readers are left out, so the total can be below the page total.'
    },
    'r32-per-day': {
      title: 'Reports per Radiologist per Day',
      purpose: 'How many reports each radiologist approved each day, for every modality or one, and how many wet reads each resident signed.',
      valueMeaning: 'Each study once, on the day of its first approval (or first wet read) in the PACS audit trail. Days = days with at least one; Per day = the total divided by those days. Over 62 days the columns become weeks. The CSV has every person, day and modality.',
      interpretation: 'Shows who carries the daily load, and on which days. A radiologist with few days but a high per-day figure reports in bursts. Weekend days are greyed in the header.'
    },
    'r36-sign-exam': {
      title: 'RIS exam done → signature',
      purpose: 'How long after the technologist marks the exam done on the RIS the radiologist approves the report, per signing radiologist.',
      valueMeaning: 'Start = the first "exam done" step of the study\'s RIS order. End = the first approval in the PACS audit trail (status PENDING APPROVED or ApprovedSign Complete), with its login, to the second. Median hours per radiologist; the tooltip adds the 90th percentile and the study count. Studies with no exam done, or exam done after the approval, are left out of this chart.',
      interpretation: 'The RIS clock: it starts when the patient has been scanned. A long bar means reports wait for that radiologist; compare with the PACS clock next to it, which starts when the images arrive.'
    },
    'r36-sign-arrival': {
      title: 'Images reach PACS → signature',
      purpose: 'How long after the images arrive on the PACS the radiologist approves the report, per signing radiologist.',
      valueMeaning: 'Start = the time the PACS stored the study (insert time). End = the first approval in the PACS audit trail. Median hours per radiologist, 90th percentile and count in the tooltip. Only studies that arrived while the audit copy covers them (shown above the tiles).',
      interpretation: 'The PACS clock: it does not depend on the RIS. When it differs a lot from the RIS clock, the exam-done step is entered late or early on the RIS.'
    },
    'r36-sign-open': {
      title: 'Reporting time: first open → signature',
      purpose: 'How long the report takes once someone starts reading: from the first open of the images to the approval.',
      valueMeaning: 'Green: from the first open by any resident or radiologist (the PACS reading groups). Amber: from the signing radiologist\'s own first open. Opens are the audit trail\'s image displays and reading locks, the same as Report 37\'s first open. Median hours per signing radiologist.',
      interpretation: 'Green much longer than amber: the study waits between the resident\'s read and the radiologist\'s. Amber is the radiologist\'s own reading-to-signing time; it includes time the study stayed open or was reopened.'
    },
    'r39-staff': {
      title: 'Staff status',
      purpose: 'Everyone on the RIS and PACS, grouped by PACS group and RIS role, with whether they count as having left. Built every night from the RIS staff list and the PACS accounts.',
      valueMeaning: 'Left = no active row on the RIS (and no active person shares the login), or every PACS account set to a leaver status (suspended, disabled; the list is shown above the table). A lock after wrong passwords does not count, nor does having no PACS account. A RIS person and a PACS login are linked by the login in the RIS resource ID, else by e-mail. Signed = studies whose report the login last composed, in the period; performed = RIS exams the person is on, in the period.',
      interpretation: 'Left but signed or performed in the period: the RIS or PACS still has them inactive by mistake, or a shared account; fix the RIS or PACS before switching on. Once on, they read "Former staff" on every report, and every total keeps their studies.'
    },
    'r27-his-report': {
      title: 'HIS orders vs RIS',
      purpose: 'Follows every exam a doctor ordered on the HIS into radiology: was it registered on the RIS, done, or lost on the way.',
      valueMeaning: 'One line per billed HIS order: an order the HIS sent as a new order (NW). Orders seen only as changes are not counted (shown under the figures). The HIS sends each order, change and cancellation to RAYD as it happens; the RIS orders arrive with the nightly copy, each carrying the HIS order number it came from, which is how the two are matched. Orders of the other hospital are never shown here.',
      interpretation: 'Start with "Not on the RIS": orders the HIS sent that radiology never registered. Then the times: how long an order waits before radiology registers it, and before the exam is done.'
    },
    'r27-his-kpi-orders': {
      title: 'Billed HIS orders',
      purpose: 'How many billed exam orders doctors placed on the HIS in the date range.',
      valueMeaning: 'Billed = the HIS sent it as a new order (NW). Counted once per HIS order number, whatever the number of messages (new, changed, cancelled); a billed order cancelled later still counts. Orders seen only as changes are left out. Only since the HIS started sending orders to RAYD (18 Nov 2026).',
      interpretation: 'The demand radiology received from the wards in the period.'
    },
    'r27-his-kpi-on-ris': {
      title: 'On the RIS',
      purpose: 'The share of HIS orders that radiology registered on the RIS.',
      valueMeaning: 'Orders with at least one RIS order carrying their HIS number, out of the orders the nightly RIS copy could already see. Orders placed after the last copy are left out until the next one.',
      interpretation: 'Close to 100 % is expected, less the orders the wards cancelled. A lower share means orders reach radiology another way or not at all.'
    },
    'r27-his-kpi-not-on-ris': {
      title: 'Not on the RIS',
      purpose: 'HIS orders that radiology never registered.',
      valueMeaning: 'Not cancelled on the HIS, placed before the last nightly RIS copy, and no RIS order carries their HIS number. The list at the bottom shows them; the Excel download adds the patient ID.',
      interpretation: 'Each one is an exam a doctor asked for that may never have been done. Check them on the RIS.'
    },
    'r27-his-kpi-cancelled': {
      title: 'Cancelled on the HIS',
      purpose: 'Orders the ward cancelled on the HIS.',
      valueMeaning: 'The latest message the HIS sent for the order is a cancellation (CA), whether or not the RIS had registered it.',
      interpretation: 'Normal in small numbers. Many cancellations from one ward are worth a word with that ward.'
    },
    'r27-his-kpi-pending': {
      title: 'Not copied yet',
      purpose: 'Orders too recent to judge.',
      valueMeaning: 'Placed after the last nightly RIS copy began and not yet on it. RAYD copies the RIS once a night, so today\'s orders show their outcome tomorrow morning.',
      interpretation: 'Not a problem: these move to Done, On the RIS or Not on the RIS after the next nightly copy.'
    },
    'r27-his-kpi-to-ris': {
      title: 'HIS → RIS registration',
      purpose: 'How long after the doctor ordered on the HIS radiology registered the exam on the RIS.',
      valueMeaning: 'Median minutes from the HIS order (its first message) to the creation of its RIS order. Orders registered on the RIS before the HIS message came are counted aside, as shown under the figure.',
      interpretation: 'Short is good: the order reaches radiology quickly. A long time means orders wait at the desk before anyone registers them.'
    },
    'r27-his-kpi-to-done': {
      title: 'HIS → exam done',
      purpose: 'How long after the doctor ordered on the HIS the exam was done.',
      valueMeaning: 'Median hours from the HIS order to the exam done on the RIS, for the orders already done.',
      interpretation: 'The whole wait a patient goes through from order to exam. Compare wards and procedures in the breakdown.'
    },
    'r27-his-kpi-ris-only': {
      title: 'RIS orders, no HIS order',
      purpose: 'Exams radiology registered on the RIS that never came from the HIS.',
      valueMeaning: 'RIS orders created in the period (since the HIS feed began) with no HIS order number, or with a number the HIS never sent to RAYD.',
      interpretation: 'Walk-ins and orders typed directly on the RIS. Many of them mean orders bypass the HIS.'
    },
    'r27-his-outcomes': {
      title: 'What became of the HIS orders',
      purpose: 'Every order the HIS sent in the period (one per HIS order number, its latest message), and what the RIS did with it.',
      valueMeaning: 'Done = a RIS order created from it was completed or its exam done. On the RIS, not done yet = registered and still scheduled, arrived or in progress. Cancelled on the RIS = every RIS order of it cancelled. Cancelled on the HIS = its latest HIS message cancels it. Not on the RIS = no RIS order carries its HIS number although the nightly RIS copy came after it. Not copied yet = placed after the last nightly RIS copy, so it cannot be judged yet.',
      interpretation: 'Not on the RIS is the one to chase: an order the doctor placed that radiology never registered. The table below lists them; the Excel download has every order.'
    },
    'r27-his-breakdown': {
      title: 'HIS orders breakdown',
      purpose: 'The same outcomes per room (it opens on it), HIS location (the ward that ordered), procedure, patient class, or the last HIS message (NW new, RQ changed, CA cancelled).',
      valueMeaning: 'Room, one per order: the room the RIS performed the exam in. An order not performed yet takes the room the RIS booked it on (the booked room is where the exam ends up in more than 99 % of cases). One not booked yet takes the room this hospital performed that procedure in most over the last 90 days, else the procedure\'s only bookable room in the RIS booking rules (MODALITY_SCHEDULE); several bookable rooms and no recent exam = "Room not decided"; "No room in the RIS lookup" = the RIS does not know the code. The line above the table says how many came each way; each room\'s row downloads its orders as Excel. On the RIS = share of the orders already copied that have a RIS order. HIS → RIS = minutes from the order on the HIS to its registration on the RIS (median). HIS → done = hours to the exam done (median).',
      interpretation: 'A ward with many orders not on the RIS, or a long HIS → RIS time, points at orders that reach radiology late or not at all.'
    },
    'r27-his-missing': {
      title: 'HIS orders not on the RIS',
      purpose: 'The orders placed on the HIS that no RIS order carries, although the nightly RIS copy came after them.',
      valueMeaning: 'HIS order number, when it was placed, its latest HIS message, procedure, the HIS location and class. Patient ID is in the Excel download; no names.',
      interpretation: 'Check them on the RIS: never registered, registered under another order, or cancelled on paper only.'
    },
    'live-board': {
      title: 'Live Department',
      purpose: 'What each imaging room of the chosen hospital is doing right now, from the RIS messages as they arrive: one card per room, by its RIS room name, all on one screen for a wall TV (Full screen hides the browser; patients show as initials unless Names is pressed on that screen).',
      valueMeaning: 'Scheduled = exams booked on the room for today (cancelled ones left out); "to come" = not arrived yet. Arrived = patients who arrived and have not started, with the longest wait; a wait turns red once it passes the limit set for that modality. The running exam shows when it should end: its start plus the minutes the RIS booked for it (else the procedure\'s usual duration). Orange = still running past that time. Yellow = the room\'s last exam was marked done in less than the set share of its expected time (50 % unless changed), until the next exam starts. Utilization = busy time ÷ opening time so far today. A room shows when it has anything booked, waiting, running or done today, or when it is open today and the RIS booked something on it in the last 30 days.',
      interpretation: 'Red waits point to a room that cannot keep up; orange to an exam running long; yellow to an exam closed on the RIS sooner than it could really have been done (a status entered late or early). Unlike the reports, the board is not a day late: it moves with each message.'
    },
    'live-board-kpis': {
      title: 'Live Department, the figures at the top',
      purpose: 'The whole hospital at a glance, for the rooms on the board.',
      valueMeaning: 'Busy = rooms with an exam running / rooms shown. Late = exams running past their expected end. Arrived = patients waiting, with how many are over their wait limit. Longest wait = the patient who has waited longest. Scheduled = today\'s bookings on these rooms, with how many have not arrived. Done today = exams completed since midnight. Utilization = busy time ÷ opening time so far today, all rooms together.',
      interpretation: 'Compare Scheduled with Done today through the day; a growing "over the limit" count means patients are waiting longer than the department allows.'
    },
    'r40-report': {
      title: 'Patient Schedule Lead Time',
      purpose: 'How long before their appointment patients were booked, for the exams whose appointment falls in the date range.',
      valueMeaning: 'Booked = when the order was created on the RIS. Appointment = the date and time the exam was booked for. Lead time = appointment day minus booking day, in days (0 = the same day). The other hospital\'s exams, the HIS\'s duplicate orders and test patients are left out.',
      interpretation: 'A long lead time for a kind of exam means patients wait days or weeks for a slot: a capacity question for that room or modality. Use the filters and the breakdown to find where.'
    },
    'r40-kpi-exams': {
      title: 'Exams',
      purpose: 'How many exams have their appointment in the date range.',
      valueMeaning: 'RIS exams with both a booking time and an appointment, appointment in the range. Cancelled exams are included (and counted under Cancelled).',
      interpretation: 'The volume every other figure on the page is a share of.'
    },
    'r40-kpi-median': {
      title: 'Median lead time',
      purpose: 'The typical time between booking and appointment.',
      valueMeaning: 'Half of the exams were booked fewer days ahead than this, half more. Shown in days, and below in hours. Exams booked after their appointment time are left out.',
      interpretation: '0 days means most exams are booked and done the same day (walk-ins, inpatients, ER). A higher median means patients mostly wait for a slot.'
    },
    'r40-kpi-p90': {
      title: '90th percentile',
      purpose: 'How long the longest waits are.',
      valueMeaning: '9 in 10 exams were booked within this many days of their appointment; the remaining 1 in 10 waited longer.',
      interpretation: 'Watch it more than the median: it shows the patients who wait the longest.'
    },
    'r40-kpi-same-day': {
      title: 'Same day',
      purpose: 'The share of exams booked on the day of the appointment.',
      valueMeaning: 'Booking day = appointment day, out of the exams not booked after their appointment.',
      interpretation: 'High for ER, inpatients and walk-ins; low for planned outpatient exams.'
    },
    'r40-kpi-ahead': {
      title: 'A week or more ahead',
      purpose: 'The share of exams booked 7 days or more before the appointment.',
      valueMeaning: 'Appointment day minus booking day of 7 or more, out of the exams not booked after their appointment.',
      interpretation: 'A rising share means appointments are getting further away: the room is filling up.'
    },
    'r40-kpi-cancelled': {
      title: 'Cancelled',
      purpose: 'The share of exams later cancelled on the RIS.',
      valueMeaning: 'Exams whose RIS status is cancelled or discontinued, out of all exams with an appointment in the range.',
      interpretation: 'Compare with the lead time in the breakdown: long waits often go with more cancellations.'
    },
    'r40-kpi-rescheduled': {
      title: 'Scheduled more than once',
      purpose: 'The share of exams booked again for another time.',
      valueMeaning: 'Exams that got the Scheduled status more than once on the RIS (from the RIS status history).',
      interpretation: 'Frequent rescheduling in one room or modality points at a scheduling or capacity problem there.'
    },
    'r40-kpi-after': {
      title: 'Booked after the appointment',
      purpose: 'Exams whose order was created after the appointment time.',
      valueMeaning: 'Usually walk-ins registered on the RIS after they were seen. They are counted here and left out of every lead time figure.',
      interpretation: 'Many of them means exams are often registered afterwards rather than booked.'
    },
    'r40-buckets': {
      title: 'How far ahead',
      purpose: 'How many exams were booked how long before their appointment, for the appointments in the period.',
      valueMeaning: 'Lead time = the appointment day minus the day the order was created on the RIS, in calendar days; 0 = booked the same day. "Booked after the appointment": the order was created after the appointment time (a walk-in registered afterwards); those are left out of every lead time figure.',
      interpretation: 'A tall same-day bar means mostly walk-ins and urgent work; a long tail past 30 days points at a waiting list for that kind of exam. Use the breakdown below to see which modality or room it comes from.'
    },
    'r40-heat': {
      title: 'When bookings are made',
      purpose: 'The weekday and hour the orders were created on the RIS, for the appointments in the period.',
      valueMeaning: 'Each cell counts the bookings made in that hour of that weekday (hospital time). Darker = more.',
      interpretation: 'Shows when the booking desk and the wards order most, for staffing the desk and the phones.'
    },
    'r40-breakdown': {
      title: 'Lead time breakdown',
      purpose: 'The lead time figures for each value of the dimension picked: modality, procedure, room, patient class, ordering department, referring physician, priority, age band, sex, or when the booking was made.',
      valueMeaning: 'Median and 90th percentile in days (and the median in hours), the share booked the same day and a week or more ahead, the share later cancelled on the RIS, and the share that got the Scheduled status more than once (booked again for another time). "Not recorded" = the RIS has no value.',
      interpretation: 'Compare a room or procedure with the others: a high median with many reschedules points at a capacity or scheduling problem there. Long lists show their largest rows; the Excel download has every exam.'
    },
    // ── Report Composer (/report-composer) ──────────────────────────────────────
    'rc-start': {
      title: 'Report Composer',
      purpose: 'Make your own report on the RIS and PACS data, without writing SQL. Pick what to count (exams on the RIS, studies in PACS, reports, critical result notifications …) or one of the questions under a tile: an answer comes straight away.',
      valueMeaning: 'Every report counts as the numbered reports do: structured reports (SR) are left out, the modality comes from the device mapping, times are hospital time. Below the tiles: the reports saved, those shared with everyone first, then your own.',
      interpretation: 'Start from the closest question, then change it: every blue word in the sentence is clicked to change it. Save keeps it; More (…) shares it, copies it or sends it by email on a schedule.'
    },
    'rc-report': {
      title: 'This report',
      purpose: 'Its name (click to change it), whether it is saved, and what you can do with it: Save, Download the answer as a file Excel opens, and More (…): save a copy, name, folder and who can open it, earlier versions, scheduled emails, the SQL it runs, delete.',
      valueMeaning: '“Only me”: in your saved reports only. “Everyone”: everyone who can open the Report Composer can open it and save a copy; only an administrator can share or change a shared report. Every save is kept as a version.',
      interpretation: 'All reports (top left) goes back to the start. A shared report you cannot change says “Save a copy”: your copy is yours to change.'
    },
    'rc-sentence': {
      title: 'The sentence',
      purpose: 'The report, read as one sentence. Click any blue word to change it. Show: what is counted or timed (+ another number puts a second one next to it). For each: what the numbers are broken down by, up to 4 (room, then month …). During: the period, and the days and times (weekdays, weekends, after hours, or your own). Where: the conditions (only CT and MR, only emergency patients …).',
      valueMeaning: 'Several values in one condition count any of them (CT or MR); several conditions all apply together (CT or MR, and in room CT1). (blank) = the value is missing. A window of days and times keeps what falls inside it: “Sat 13:00 → 07:00 next day” is Saturday 13:00 to Sunday 07:00, the start included, the end not. After hours = Monday to Friday 19:00 → 07:00, and Saturday 13:00 → Monday 07:00. “Using the … date” picks which date the period is on (booked, scanned, done, signed …).',
      interpretation: 'The × takes a word out. A condition marked “asked” is asked for each time the report is opened or run again, its saved value first. Without a period the report reads all the data, which is slow.'
    },
    'rc-result': {
      title: 'The answer',
      purpose: 'The sentence above, answered from the data RAYD holds (the nightly copy from the RIS and PACS, the HL7 reports, CRN), updated each time the sentence changes. Show it as a table, bars, stacked bars, a line, a pie, a heatmap, or the list itself (one line per exam, study …).',
      valueMeaning: 'A count counts the kind of data chosen: an exam is one RIS worklist entry, a study one PACS study. Turnarounds and waits are in the unit their name says (minutes or hours). The table adds a subtotal and a Total where the number adds up (counts, gigabytes; not medians or averages). Bars and lines colour by the last “for each”; a heatmap puts it across the top. The page shows the first 2,000 lines; Download holds up to 100,000.',
      interpretation: 'Click a column title to sort; click any number, bar, slice or cell to list the exams or studies behind it. Data reach RAYD overnight, so today is incomplete for anything read from the RIS or PACS.'
    },
    'r32-addendum': {
      title: 'Addendum Rate by Radiologist',
      purpose: 'For each reader with at least 5 reported studies in the period, the share of studies that carry an addendum.',
      valueMeaning: 'Reader = who last composed the report, else the PACS final signer, merged through the aliases. Rate = studies flagged with an addendum on the PACS ÷ the reader’s reported studies. The overall rate covers the listed readers only.',
      interpretation: 'An addendum is a correction or an addition after signing. A rate well above the others is worth a quality review; 0 for everyone usually means the PACS does not set the flag.'
    },
    'r32-cadence': {
      title: 'Reporting Cadence Analysis',
      purpose: 'For each reader, when their reports are written: a weekday × hour heatmap and a day-by-day log.',
      valueMeaning: 'Each report time (last composed, else PACS final) is one event. First and last event of a day = start and end of reporting. A gap of 20 minutes or more between two reports is a break: up to 35 min coffee, up to 70 min long break, longer = away.',
      interpretation: 'It sees report times only: reading without signing, procedures, teaching or meetings all look like breaks. Use it to understand workload patterns, not as an attendance record.'
    },

    // ── Report 33 — KPI Detailed Reading ────────────────────────────────────────
    'r33-kpi': {
      title: 'KPI Detailed Reading',
      purpose: 'Turnaround counts per modality and patient class, in the hospital’s KPI spreadsheet format: how many studies fell in each time band, per stage and per reader.',
      valueMeaning: 'Stages: Exam done → read (first signature), Signed 1 → approved, Exam done → approved. Exam done comes from the HL7 order feed (exam done, else the PACS completion message); approved = last composed, else PACS final, else the HL7 result. Urgent = ER location. TAVI and coronary CT are left out. Only Exam done → approved is scored against the target: green 90% or more, amber 75–90%, red below.',
      interpretation: 'Studies with no exam-done message drop out of the first and third stages, so a block with few studies usually means the HL7 order feed did not cover them, not that few studies were done.'
    },

    // ── Report 34 — Device Utilization ──────────────────────────────────────────
    'r34-matrix': {
      title: 'Device Utilization & Revenue Matrix',
      purpose: 'For each Rizk Hospital device with reported studies, how much of its opening time was used on each weekday of the period, and the RVU its studies carry.',
      valueMeaning: 'Cell = minutes used ÷ (daily opening minutes × number of that weekday in the range). Minutes used = the RIS start-to-end time of its exams when the device has any for that weekday, else the catalogue duration of its reported studies. Opening minutes = the daily capacity on the Mapping page for days in the device’s weekly schedule. A day with no schedule shows “no schedule”, not 0%. Red above 85%, amber below 30%.',
      interpretation: 'Devices counted under No Schedule Configured need opening hours before their utilization means anything. Above 100% = work past the defined hours, or a capacity set too low.'
    },

    // ── Report 35 — Technician TAT ──────────────────────────────────────────────
    // Rows: RIS exams that reached status Arrived in the dates chosen.
    'r35-daily': {
      title: 'Daily TAT Trend',
      purpose: 'Per day of arrival, the average time from RIS Arrived to RIS Exam Done of the exams completed.',
      valueMeaning: 'Day = the day the patient was marked Arrived. Line = average minutes; bars = exams completed and how many of them carry a flag.',
      interpretation: 'For inpatients, Arrived is set early to send the order to the device worklist, so their times include waiting on the ward. Compare like with like using the patient class filter.'
    },
    'r35-by-tech': {
      title: 'By Technician',
      purpose: 'Arrived-to-Exam-Done time per technologist, from the technologist recorded on the RIS performed procedure step.',
      valueMeaning: 'Average and median minutes (exams over 24 h are left out of both and counted as outliers); PACS = arrived to the scanner’s end time. Flag rate = flagged exams ÷ exams done. Δ = the technologist’s average minus the department average.',
      interpretation: 'A high flag rate with a short average usually means exams closed on the RIS before the scan ended (Too Early); a long average with overlaps means exams left open while the next patient was scanned.'
    },
    'r35-by-modality': {
      title: 'By Modality',
      purpose: 'Arrived-to-Exam-Done time per modality, all technologists together.',
      valueMeaning: 'Average and median minutes over the exams completed. The note appears when the average is 1.5× the median or more: a few very long exams are lifting it.',
      interpretation: 'Use the median to compare modalities; MR and CT naturally take longer than X-ray.'
    },
    'r35-jci': {
      title: 'JCI Compliance — Order to Acquisition',
      purpose: 'Time from the scheduled time of the order to the moment the scan started on the RIS, per order priority, against the targets set by an administrator.',
      valueMeaning: 'Interval = RIS scan start − scheduled time, in minutes; negative intervals (scanned before the booked time) are left out. Compliance = exams within their priority’s target ÷ exams of priorities that have a target. Priorities without a target show a median only.',
      interpretation: 'Set a target for every priority that matters for JCI, otherwise it is not scored. Long medians on routine priorities usually mean patients booked far ahead, not delays.'
    },
    'r35-flagged': {
      title: 'Flagged Exams',
      purpose: 'Completed exams whose times do not fit the procedure.',
      valueMeaning: 'Negative TAT = Exam Done before Arrived. Too Early = done in under half the procedure’s catalogue duration (30 min when it has none). Too Late = over twice the duration. Overlap = still open when the next patient on the same modality arrived. PACS/RIS mismatch = PACS arrival and RIS exam done more than the modality’s threshold apart (30 min unless set). An exam can carry several flags. Acknowledge records that a flag was reviewed.',
      interpretation: 'Flags point at status clicks on the RIS, not necessarily at slow work. Many Too Early flags on one modality often mean its catalogue durations are too long.'
    },
    'r35-never-done': {
      title: 'Never Marked Done',
      purpose: 'Exams marked Arrived that never reached Exam Done on the RIS and are past their expected end.',
      valueMeaning: 'Expected end = arrival time + the procedure’s catalogue duration (30 min when it has none). Overdue = minutes since then.',
      interpretation: 'Usually exams done but not closed on the RIS, or patients who left. They stay open on the RIS worklist until someone closes them.'
    },
    'r35-ack-log': {
      title: 'Acknowledgements Log',
      purpose: 'Flags in the period that someone reviewed and acknowledged, with who, when and the note.',
      valueMeaning: 'One row per acknowledged exam. Acknowledging does not remove the flag from the counts; it records the review.',
      interpretation: 'Use it to follow up: flags acknowledged with the same note again and again point at a process to fix.'
    },

    // ── Report 36 — Radiology Reporting Analytics ───────────────────────────────
    // The Exam Done Anchor toggle picks where the clock starts: the RIS exam done, or
    // the PACS arrival. The clock stops at a signature.
    'r36-residents': {
      title: 'Residents — Median TAT',
      purpose: 'For each resident, the median time to their signatures, per modality and patient class.',
      valueMeaning: 'Every signature counts for whoever made it: a first (preliminary) signature and the last composition of the report. The clock starts at the RIS exam done or the PACS arrival (the toggle). Role comes from the PACS reading-permission groups, corrected by an administrator where needed. Hours.',
      interpretation: 'Residents read first, so their medians should be lower than the radiologists’. Signers with no known role are counted aside, not guessed.'
    },
    'r36-radiologists': {
      title: 'Radiologists — Median TAT',
      purpose: 'For each radiologist, the median time to their signatures, per modality and patient class.',
      valueMeaning: 'Same rules as the resident table: every signature counts for whoever made it, the clock starts at the RIS exam done or the PACS arrival (the toggle), role from the PACS groups and the administrator’s corrections. Hours.',
      interpretation: 'A radiologist whose median is close to the residents’ is often approving straight after the resident; a much higher one may be reading cases without a resident first.'
    },
    'r36-modality': {
      title: 'Modality TAT — Median TAT',
      purpose: 'Department-wide median time to signature per modality and patient class, all signers together.',
      valueMeaning: 'Same signatures and clock as the two tables above, without the split by person. Class: ER = patient location ER, then inpatient and outpatient by class code, Other for the rest. Hours.',
      interpretation: 'Each study can count twice (first signature and final composition); compare rows with each other, not with Report 25’s per-study TAT.'
    },
    'r36-wait': {
      title: 'Patient Wait Time',
      purpose: 'Time from RIS Scheduled to RIS Arrived, per modality and patient class.',
      valueMeaning: 'Both times are RIS status changes of the exam; only exams with arrival after the scheduled time count. Average and median minutes; 0–30, 30–60 and 60+ = how many patients fell in each band.',
      interpretation: 'Scheduled is when the booking was made in the RIS status history, so for exams booked days ahead this measures lead time, not time in the waiting room. Read it per class: ER should be short.'
    },
    'r36-tech-tat': {
      title: 'Technician TAT by AE Station',
      purpose: 'One bubble per room: average against median turnaround of its reported studies.',
      valueMeaning: 'Turnaround here runs from the PACS arrival to the report time (studies over 24 h left out), the same as Report 25. X = average, Y = median, bubble size = studies. Colour bands 0–30, 30–60, 60+ minutes.',
      interpretation: 'Despite the name this is report time per room, not technologist time; technologist times are on Report 35. Bubbles far below the dashed line have a few very late reports lifting their average.'
    },
    'r36-reader-volume': {
      title: 'Reports per Reader',
      purpose: 'Reported Rizk Hospital studies per reader, by modality, room or month, with a role badge.',
      valueMeaning: 'Reader = who last composed the report on the PACS, merged through the physician aliases. Cell = distinct studies. Badges come from the PACS reading-permission groups and the administrator’s corrections.',
      interpretation: 'Use it with the TAT tables: a reader with a high volume and a high median may need cases spread differently.'
    },
    'r36-cadence': {
      title: 'Reporting Cadence Analysis',
      purpose: 'For each signer of final reports, when they sign: a weekday × hour heatmap and a day-by-day log.',
      valueMeaning: 'Each PACS final signature time is one event. First and last of a day = start and end of signing. A gap of 20 minutes or more is a break: up to 35 min coffee, up to 70 min long break, longer = away. Readers who only sign preliminary reports do not appear.',
      interpretation: 'It sees signatures only, so reading, procedures and meetings look like breaks. Use it for workload patterns, not as an attendance record.'
    },

    'sr-resident-wetreads': {
      title: 'Wet Reads per Resident',
      purpose: 'How many studies each resident wet-read (signed first, before the radiologist), by modality.',
      valueMeaning: 'Each study counts once, for the resident whose wet read came first, from the PACS audit trail (every wet read with its login and time). A resident is a member of the PACS "residents" group. Only studies that reached PACS since the audit copy began (1 October 2026) can count, within the dates and filters chosen. Staff who left show as "Former staff" while that setting is on.',
      interpretation: 'This is residents\' reading work, which the radiologist table above does not show (it credits the radiologist who approved). Compare residents over the same period and modalities.'
    },
    'sr-leadtime': {
      title: 'Patient Schedule Lead Time',
      purpose: 'How long ahead patients get their appointment: from the moment the exam is booked on the RIS to the appointment itself, for the RIS appointments in the period. The same figures as Report 40, which has every breakdown (procedure, room, department, referrer, booking hours) and the exams in Excel.',
      valueMeaning: 'Lead time = appointment day minus booking day, in calendar days (0 = booked the same day). The median is the middle exam: half were booked closer to the appointment, half further ahead. Cancelled = cancelled on the RIS; scheduled more than once = booked again for another time. An exam registered after its appointment (a walk-in entered afterwards) is counted aside and left out of the lead times. The small figures under each tile compare with the comparison period: days for the median, points for the percentages; red when waits or cancellations grow.',
      interpretation: 'A longer median means patients wait longer for a slot; read it with the modality table, because one busy modality (often MR) sets most of the wait. Of the filters on the left only Modality applies here: the others describe the PACS study, which the RIS records differently.'
    },
    'sr-leadtime-buckets': {
      title: 'How far ahead the exams were booked',
      purpose: 'The appointments of the period by how far ahead they were booked: the same day, the next day, 2–3 days, up to over 60 days.',
      valueMeaning: 'Each bar is the share of the appointments in that band, with the count beside it. "Booked after the appointment" (amber) are exams registered on the RIS after their appointment time, usually walk-ins.',
      interpretation: 'Most same-day bookings are walk-ins, inpatients and emergencies; a long tail past 30 days points at a modality or room with too few slots.'
    },
    'sr-leadtime-table': {
      title: 'Lead time by modality',
      purpose: 'The lead time figures for each modality, the busiest first.',
      valueMeaning: 'Appointments in the period; the median lead time; "9 in 10 within" = 90 % of the exams were booked no further ahead than this; the share booked the same day, booked a week or more ahead, and cancelled.',
      interpretation: 'Compare modalities with a similar number of appointments. Report 40 breaks the same figures down by procedure, room, ordering department and referring physician.'
    },

    // Report 36: the radiologists' reading TAT (the KPI) and the three-clock comparison
    'r36-reading-kpi': {
      title: 'Radiologist Reading TAT (KPI)',
      purpose: 'How long a radiologist takes to approve a study once they start on it, the department\'s KPI for radiologists.',
      valueMeaning: 'From the approving radiologist\'s first open of the study after the resident\'s wet read (Signed 1), or their first open when there was no wet read, to their approval in PACS. Every time comes from the PACS audit trail, to the second, for studies that reached PACS since the audit copy began.',
      interpretation: 'It leaves out the time a study waits before the radiologist opens it, so it measures reading and approving, not the queue. Use the comparison tab for the full turnaround.'
    },
    'r36-reading-median': {
      title: 'Median reading TAT',
      purpose: 'The typical time from the radiologist\'s first open after the wet read to the approval.',
      valueMeaning: 'Half of the measured studies were approved faster than this, half slower, in hours.',
      interpretation: 'The everyday pace; not pulled up by a few studies left open for hours, which the 90th percentile shows.'
    },
    'r36-reading-p90': {
      title: '90th percentile reading TAT',
      purpose: 'The slow end: 9 studies out of 10 were approved within this time.',
      valueMeaning: 'Hours from the radiologist\'s first open after the wet read to the approval, for the measured studies.',
      interpretation: 'Far above the median means some studies stay open a long time before approval: look for them in the radiologist table.'
    },
    'r36-reading-measured': {
      title: 'Measured studies',
      purpose: 'How many approved studies the reading TAT could be measured on.',
      valueMeaning: 'Studies approved in PACS in the date range whose approving radiologist opened them after the wet read (or at all, with no wet read), out of every approved study.',
      interpretation: 'The closer to the approved total, the more complete the figure.'
    },
    'r36-reading-not-measured': {
      title: 'Not measured',
      purpose: 'Approved studies with no start for the reading clock.',
      valueMeaning: 'The approving radiologist has no open of the study in the audit trail after the wet read: they opened it only before the resident\'s wet read, or approved without an open being recorded.',
      interpretation: 'A high count can mean studies approved from a list without being reopened; these are left out rather than given a made-up start.'
    },
    'r36-reading-bands': {
      title: 'Reading TAT bands',
      purpose: 'How the measured studies spread over time windows, for all studies and for those with and without a wet read.',
      valueMeaning: 'Share of measured studies approved within 1 hour, 1 to 3 hours, 3 to 5 hours and over 5 hours of the radiologist\'s first open after the wet read. Approved = every approved study of the group; Measured = those with a start.',
      interpretation: 'A growing red band (over 5 hours) means studies are left open long before approval. With and without a wet read show whether studies read by a resident first are approved faster.'
    },
    'r36-bands-clock': {
      title: 'Where the clock starts',
      purpose: 'Chooses the moment the turnaround starts: when the technologist marks the exam done on the RIS, or when the images reach PACS.',
      valueMeaning: 'RIS exam done is the study\'s first Exam Done on the RIS; Images reach PACS is the time PACS received the study. A study without that moment, or whose moment comes after the wet read or approval, is left out of that clock only.',
      interpretation: 'The two clocks usually differ by a few minutes. A large gap means images reach PACS long after the exam is marked done, or the reverse. Use RIS exam done for the hospital\'s targets.'
    },
    'r36-bands-wet': {
      title: 'Wet read: turnaround in time bands',
      purpose: 'How long after the exam the first wet read (the resident\'s first reading) was signed in PACS, counted in time bands, per resident, modality and patient type.',
      valueMeaning: 'Each study once, for its first wet read in the PACS audit trail and who signed it. Studies = studies measured with the chosen clock; Median and 90th pct = half, and 9 in 10, were wet-read within that time; each band = the share of studies (and how many) whose wet read fell in that window.',
      interpretation: 'Read the first bands: the larger their share, the faster the wet reads. Compare residents on the same modality and patient type; ER studies should sit in the first band.'
    },
    'r36-bands-approve': {
      title: 'Approval: turnaround in time bands',
      purpose: 'How long after the exam the radiologist approved the report (the final signature at LAUMC), counted in time bands, per radiologist, modality and patient type.',
      valueMeaning: 'Each study once, for its first approval in the PACS audit trail and the radiologist who approved it. Studies = studies measured with the chosen clock; Median and 90th pct = half, and 9 in 10, were approved within that time; each band = the share (and number) of studies approved in that window.',
      interpretation: 'The last band holds the studies approved later than the longest limit. Set the limits to the hospital\'s targets (24 h inpatient and ER, 48 h outpatient) to see who, and which modality, misses them.'
    },
    'r36-bands-edit': {
      title: 'Time bands',
      purpose: 'Sets the windows the studies are counted in, as upper limits in hours.',
      valueMeaning: 'Limits 3, 5, 12, 24 give five bands: up to 3 hours, 3 to 5, 5 to 12, 12 to 24 and over 24 hours. Apply changes the table at once, for you only; Everyone\'s bands goes back to the saved ones; Save for everyone (administrators) makes yours the bands everyone starts with.',
      interpretation: 'Use the hospital\'s own targets as limits, so the first bands mean on time and the last band means late.'
    },
    'r36-reading-by-rad': {
      title: 'Reading TAT by approving radiologist',
      purpose: 'Each radiologist\'s own reading time, from their first open after the wet read to their approval.',
      valueMeaning: 'Measured / approved studies, median and 90th percentile in hours, and the share in each band, per radiologist who approved.',
      interpretation: 'Compare radiologists on the same kind of work: a radiologist approving mostly complex studies can be slower for good reason.'
    },
    'r36-reading-by-mod': {
      title: 'Reading TAT by modality',
      purpose: 'Reading time from the first open after the wet read to the approval, per modality.',
      valueMeaning: 'Measured / approved studies, median and 90th percentile in hours, and the share in each band, per modality.',
      interpretation: 'MRI and CT take longer to read than X-ray by nature; compare a modality with itself over time.'
    },
    'r36-cmp-exam': {
      title: 'RIS exam done → approval',
      purpose: 'The full turnaround seen from the RIS: from the technologist marking the exam done to the radiologist\'s approval.',
      valueMeaning: 'Median, 90th percentile (hours) and number of studies whose RIS exam done comes before the approval in the PACS audit trail.',
      interpretation: 'Includes the wait for the images, the wait in the reading queue and the reading itself. A late "exam done" on the RIS makes it look shorter.'
    },
    'r36-cmp-arrival': {
      title: 'PACS arrival → approval',
      purpose: 'The turnaround seen from PACS: from the images being stored to the radiologist\'s approval.',
      valueMeaning: 'Median, 90th percentile (hours) and number of studies, from the time PACS stored the study to the approval in the audit trail.',
      interpretation: 'Starts when the images can actually be read, so it does not depend on when the RIS status was entered.'
    },
    'r36-cmp-reading': {
      title: 'First open after the wet read → approval',
      purpose: 'The radiologist\'s own part: from the approving radiologist\'s first open after the wet read to their approval.',
      valueMeaning: 'Median, 90th percentile (hours) and number of studies, the same figure as the Reading TAT KPI card.',
      interpretation: 'The gap between this and the other two clocks is the time a study waits before the radiologist starts on it.'
    },
    'r36-cmp-common': {
      title: 'The three clocks on the same studies',
      purpose: 'A like-for-like comparison: the medians of the three clocks on the studies that have all three.',
      valueMeaning: 'Studies with a RIS exam done, a PACS arrival and a radiologist open after the wet read, all before the approval; the median of each clock on them.',
      interpretation: 'Each tile above counts its own studies; these medians compare the very same studies, so the differences are the waiting steps.'
    },
    'r36-cmp-modality': {
      title: 'The three clocks by modality',
      purpose: 'Where the time goes for each modality: full turnaround from the RIS and from PACS beside the radiologist\'s reading time.',
      valueMeaning: 'Median hours of each clock per modality, each on the studies that have that clock.',
      interpretation: 'A tall green or blue bar over a short orange one means studies wait long before the radiologist opens them.'
    },
    'r36-cmp-class': {
      title: 'The three clocks by patient class',
      purpose: 'The same comparison for ER, inpatients and outpatients.',
      valueMeaning: 'Median hours of each clock per patient class (ER from the patient location, then the PACS patient class), each on the studies that have that clock.',
      interpretation: 'ER studies should show short waits before the radiologist opens them; a long one is worth looking into.'
    },
    'r36-cmp-rad': {
      title: 'The three clocks by approving radiologist',
      purpose: 'Each radiologist\'s turnaround from the RIS and from PACS beside their own reading time.',
      valueMeaning: 'Approved studies and, for each clock, the median in hours with the number of studies it counts.',
      interpretation: 'A radiologist with a short reading time but a long turnaround receives studies late or picks them up late; the reverse means slow reading.'
    },

    // ── Super Report ────────────────────────────────────────────────────────────
    // Rows: Rizk Hospital studies in the dates and filters chosen, compared with the
    // previous period of the same length (or the comparison dates chosen).
    'sr-briefing': {
      title: 'Daily Briefing',
      purpose: 'A short written summary computed every night for three fixed periods: the last 30 days, the last 90 days and the year to date, each ending yesterday.',
      valueMeaning: 'Rizk Hospital only; structured reports, other objects and bone densitometry are left out. Each period is compared with the one before it of the same length (the year to date with the same dates last year). It covers volume and studies per day, median TAT (PACS arrival to the report time: last composed, else PACS final), orders linked to a study, storage, the share of studies with a report, top referring physicians and the modality mix. Alerts: median TAT over 8 h, reports on under 90% of studies, fewer than 85% of orders linked to a study, storage up more than 30%, volume down more than 20%.',
      interpretation: 'It does not follow the dates or filters on this page; it is the same text for everyone that day. “Updated” beside the title is when it was computed; an older date means the nightly job did not run.'
    },
    'sr-summary': {
      title: 'Executive Summary',
      purpose: 'A written summary of the figures below for the dates and filters chosen.',
      valueMeaning: 'Each sentence compares the period with the comparison period (by default the same number of days just before). Arrows and percentages are the change between the two. The sentences come from fixed rules, not from AI.',
      interpretation: 'Compare periods of the same length and kind: a month with holidays against one without will show a drop that is not a trend.'
    },
    'sr-sites': {
      title: 'Sites side by side',
      purpose: 'Rizk Hospital (RH) and Saint John (SJH) per modality for the same dates and filters, each read from its own database. Shown only to accounts that may open both sites.',
      valueMeaning: 'Studies = distinct PACS studies on the site\'s own devices (modality from the device on that site\'s Mapping page). Patients = distinct patients. Median TAT = PACS arrival to report (last composed, else PACS final), for reported studies only. GB = image storage from that site\'s nightly storage summary.',
      interpretation: 'Both sites are counted the same way, so a gap between them is real, not a difference of method. An AE title filter belongs to one site and empties the other.'
    },
    'sr-volume': {
      title: 'Volume & Trends',
      purpose: 'How many Rizk Hospital studies were done in the period and how they spread over the days.',
      valueMeaning: 'Studies = distinct PACS studies. Avg/day = studies ÷ days that had at least one study. Peak day = the date with the most studies. Top modalities = the 5 busiest, modality from the device on the Mapping page.',
      interpretation: 'Days with no study at all (holidays) do not lower the average, so it is the average of working days.'
    },
    'sr-storage': {
      title: 'Storage & Images',
      purpose: 'Storage used by the images of the period, from the nightly storage summary.',
      valueMeaning: 'Total GB = image size added up over the study dates in the period. Avg GB/day = total ÷ days with data. The row named Total images is the number of studies in the storage summary. By Modality = the 5 largest. Only the dates and the modality filter apply.',
      interpretation: 'Storage grows with CT and MR volume; a jump without a volume change usually means a new protocol with more images.'
    },
    'sr-physicians': {
      title: 'Top Referring Physicians',
      purpose: 'The 10 referring physicians with the most studies in the period, as named on the PACS study.',
      valueMeaning: 'Count = studies, not patients. Studies without a referring last name are left out.',
      interpretation: 'Report 22 has the full referral analysis, including physicians whose referrals are falling.'
    },
    'sr-orders': {
      title: 'Orders & Fulfillment',
      purpose: 'RIS orders scheduled in the period and how many became a PACS study.',
      valueMeaning: 'Total = orders (one per scheduled procedure step) by scheduled date. Fulfilled = orders linked to a PACS study. Fulfillment % turns red under 85%. The modality filter for orders is separate from the study filters.',
      interpretation: 'Orders scheduled in the last days of the period, or cancelled, are not linked yet, so the rate is always lower for recent dates. Report 27 shows the order statuses.'
    },
    'sr-tat': {
      title: 'Turnaround Time',
      purpose: 'Median time from the PACS arrival of a study to its report, for the period.',
      valueMeaning: 'Report time = last composed, else PACS final. Reported = studies with a report time. ER and non-ER are split by the patient class codes in the setting pc_emergency, not by the ER location Report 25 uses, so the two can differ. Slowest modalities = the 5 with the highest median.',
      interpretation: 'The median is the middle study, so a few very late reports do not move it. If ER median shows —, no study in the period carries one of the ER class codes.'
    },
    'sr-crn': {
      title: 'Critical Findings (CRN)',
      purpose: 'Critical results found in the period and the critical result notifications sent for them.',
      valueMeaning: 'Critical findings = HL7 reports the language engine marked critical, by result date. CRNs, acknowledged, went overdue, still open and median time to acknowledge are the figures of Administration → CRN → Report. Only the dates apply, not the other filters. A banner shows when CRN is off or in dry-run.',
      interpretation: 'While CRN is off, only the first row has meaning. Once live, anything still open or overdue needs a call.'
    },
    'sr-util': {
      title: 'Device Utilization',
      purpose: 'A summary of Report 34’s device utilization for the dates chosen.',
      valueMeaning: 'Avg utilization = the average of the devices’ weekly averages, devices with an opening schedule only. Over 85% and under 30% count devices; No schedule data = devices without opening hours. Busiest = the 3 highest. Only the dates apply.',
      interpretation: 'Open Report 34 for the day-by-day matrix. Devices without a schedule cannot be judged until opening hours are entered.'
    },
    'sr-demo': {
      title: 'Patient Demographics',
      purpose: 'The period’s studies by patient sex and by patient class.',
      valueMeaning: 'Counts studies, not patients. Male and female come from the PACS patient record. Inpatient and outpatient group the class codes listed in the settings pc_inpatient and pc_outpatient. All Patient Classes lists every raw class code with its share.',
      interpretation: 'A class code in the list that is in neither setting is counted in neither tile; add it to the right setting.'
    },
    'sr-insights': {
      title: 'Clinical Insights',
      purpose: 'Automatic warnings when a figure on this page crosses a fixed threshold.',
      valueMeaning: 'Rules: volume down 25% or more against the comparison period (critical) or up 30% (info); median TAT over 24 h (critical) or 8 h (warning); a modality with median TAT over 24 h; reports on under 60% (critical) or 80% (warning) of studies; storage up 40% or more.',
      interpretation: 'A recent period always has fewer reports written, so coverage warnings for the last few days are expected.'
    },
    'sr-rad-matrix': {
      title: 'Reports per Radiologist',
      purpose: 'Reported Rizk Hospital studies per reader, by modality, room or month, with a procedure breakdown.',
      valueMeaning: 'Reader = the PACS signing physician’s name, else who last composed the report, else the PACS final signer, merged through the physician aliases. Cell = distinct studies with a report time. The procedure table covers the 60 most frequent procedures.',
      interpretation: 'Readers with no name are left out, so totals can be below the study count.'
    },

    // ── Home page (daily briefing and Yesterday tab) ────────────────────────────
    'home-briefing': {
      title: 'Daily Briefing',
      purpose: 'A summary of the last complete day in RAYD (the latest study date before today), Rizk Hospital only.',
      valueMeaning: 'Orders = RIS orders scheduled that day, not cancelled, against the average per day of the 30 days before and the same weekday last week. Signed = that day’s studies with a report time; the reporting rate divides them by the orders. Unread = studies whose PACS status is still unread. Avg TAT and ER TAT = minutes from midnight of the study date to the report time (under 48 h only); ER = location ER or a class naming emergency. Active rads = people who composed reports.',
      interpretation: 'TAT here starts at midnight, so it is longer than Report 25’s, which starts when the study reached the PACS; use it to compare days, not against targets.'
    },
    'home-referrers': {
      title: 'Top Referring Physicians (yesterday)',
      purpose: 'The 5 referring physicians with the most Rizk Hospital studies yesterday.',
      valueMeaning: 'Count = studies, named as on the PACS study. Studies without a referring first name are left out.',
      interpretation: 'One day is a small sample; Report 22 has the trend.'
    },
    'home-ae': {
      title: 'AE Performance (yesterday)',
      purpose: 'Yesterday’s work per room.',
      valueMeaning: 'Procedures = studies stored by the room, named by its RIS room name, else the AE title. Utilization = the catalogue duration of those studies (15 min when a procedure has none) ÷ the room’s daily capacity on the Mapping page, else its weekly schedule, else 8 h.',
      interpretation: 'Utilization here uses catalogue durations, not real scan times, so it is a rough guide; Report 34 uses the RIS start and end times where they exist.'
    },
    'home-reconciliation': {
      title: 'Orders ↔ PACS Reconciliation (yesterday)',
      purpose: 'Yesterday’s exams done on the RIS against the Rizk Hospital studies stored on the PACS for the same day.',
      valueMeaning: 'RIS Exams Done = scheduled procedure steps scheduled yesterday that reached Exam Done or later. The parts of one exam ordered together share a RIS linked group (a CT abdomen and pelvis is two steps, one study), so each group is one expected study; Linked Groups counts the groups with more than one part. PACS Studies = yesterday’s studies, structured reports and other objects left out. Discrepancy = expected − PACS; positive means fewer studies than expected.',
      interpretation: 'A small gap is normal: walk-ins and imported outside studies have no RIS order (negative), and an exam closed on the RIS before its images were sent shows as positive until they arrive. A large positive gap means images did not reach the PACS or were stored under another accession; Report 27 shows the same comparison month by month.'
    },

    // ── ER Analytics Dashboard ──────────────────────────────────────────────────
    // Rows: Rizk Hospital studies with patient location ER in the dates chosen.
    // TAT = report time (last composed, else PACS final, else the HL7 result) minus the
    // PACS arrival.
    'er-unread': {
      title: 'Currently Unread ER Studies — Today',
      purpose: 'ER studies dated today that have no report yet.',
      valueMeaning: 'Waiting = minutes since the study reached the PACS. Up to 50, longest first; marked SLA BREACH once past the target chosen on the page, else Pending.',
      interpretation: 'RAYD sees today’s studies only after they are copied from the PACS, so this list can lag behind the PACS worklist.'
    },
    'er-trend': {
      title: 'Daily Average TAT',
      purpose: 'Average ER turnaround per study date, in hours.',
      valueMeaning: 'TAT = from the PACS arrival to the report time (last composed, else PACS final, else the HL7 report). Studies with no report yet are left out.',
      interpretation: 'One very late report lifts a day’s average a lot when the ER volume is small; check the distribution below before acting on a single spike.'
    },
    'er-histogram': {
      title: 'TAT Distribution',
      purpose: 'How the period’s reported ER studies spread by turnaround time.',
      valueMeaning: 'Studies are grouped into up to 10 natural groups of similar TAT (k-means), so the bars have unequal widths; each label is the group’s range. Green = within the target chosen on the page (60 min by default), amber = up to 1.5× the target, red = beyond.',
      interpretation: 'Most of the volume should sit in the green groups; a separate red group far to the right is reports written the next day.'
    },
    'er-modality': {
      title: 'Average TAT by Modality',
      purpose: 'Average ER turnaround per modality, for modalities with at least 3 ER studies in the period.',
      valueMeaning: 'Bar = average minutes from the PACS arrival to the report time, fastest first.',
      interpretation: 'CT and X-ray carry most ER work; a slow modality with few studies may be a handful of complex cases.'
    },
    'er-hour': {
      title: 'ER Volume by Hour of Day',
      purpose: 'ER studies by the hour they reached the PACS, added up over the range.',
      valueMeaning: 'Bar = number of ER studies arriving in that hour.',
      interpretation: 'Use it to match reading cover to the hours ER imaging peaks.'
    },
    'er-radiologist': {
      title: 'Average TAT by Radiologist',
      purpose: 'Average ER turnaround per reader, for readers with at least 3 ER studies; the 15 fastest.',
      valueMeaning: 'Reader = who last composed the report, else the PACS final signer, merged through the physician aliases. Hover for the study count and how many were within the target.',
      interpretation: 'Readers covering nights get fewer, often harder, ER cases; compare them with care.'
    },

    // ── ORU Analytics (HL7 radiology reports) ───────────────────────────────────
    // Rows: report messages received over HL7 (and RIS reports copied by the ETL), by
    // the report's own date, for the days and procedure chosen.
    'oru-normal': {
      title: 'Normal vs Abnormal Rate',
      purpose: 'The share of reports whose conclusion reads as normal.',
      valueMeaning: 'Normal = the conclusion (else the whole report) contains a normal phrase such as “unremarkable”, “no acute”, “within normal limits”. Everything else counts as abnormal, which includes reports simply worded differently.',
      interpretation: 'It is a word match, not a clinical judgement; use it to follow trends, and the critical findings log for cases that matter.'
    },
    'oru-modality': {
      title: 'Reports by Modality',
      purpose: 'Reports per modality.',
      valueMeaning: 'Modality as carried by the report message. UNK = the message has none.',
      interpretation: 'A large UNK bar means the report feed is not filling the modality; the critical findings log fills it from the PACS study instead.'
    },
    'oru-procedures': {
      title: 'Top Procedures by Volume',
      purpose: 'The 10 procedures with the most reports.',
      valueMeaning: 'Count = reports per procedure code, named as in the report message.',
      interpretation: 'Compare with Report 22’s procedure breakdown: a big difference means reports for some procedures are not reaching RAYD.'
    },
    'oru-physicians': {
      title: 'Reporting Physician Activity',
      purpose: 'The 10 physicians who signed the most reports.',
      valueMeaning: 'The signer’s ID in the message is matched to the RIS staff list to show a name; an unmatched ID is shown as it is.',
      interpretation: 'An ID shown instead of a name means that person is missing from the RIS staff list RAYD copies.'
    },
    'oru-gaps': {
      title: 'Section Gap Alerts',
      purpose: 'Reports in which a section is empty: technique, results (Résultats) or conclusion.',
      valueMeaning: 'Each report is split by its section headings; a section present but empty, or missing, counts as a gap. A report with no headings at all counts as results only.',
      interpretation: 'Empty conclusions matter most: the referring physician reads that part first.'
    },
    'oru-treemap': {
      title: 'Report Term Treemap',
      purpose: 'The diagnoses found most often in the period’s reports.',
      valueMeaning: 'Box size = number of reports with that diagnosis affirmed. Negated or historical mentions (“no fracture”, “previous fracture”) are not counted, and benign findings are left out. Reports not yet analysed by the language worker are matched with the same rules on the fly.',
      interpretation: 'Use the filter box to find a term. A diagnosis growing from one period to the next may reflect case mix or a new referral stream.'
    },
    'oru-sections': {
      title: 'Section Content Analysis',
      purpose: 'The most common terms in each report section: technique, results and conclusion.',
      valueMeaning: 'Box size = how often the term appears in that section across the period’s reports; common words are removed.',
      interpretation: 'Technique terms show protocol habits; results and conclusion terms show what is being found and how it is worded.'
    },
    'oru-critical': {
      title: 'Critical Findings Log',
      purpose: 'The 20 most recent reports in the period with a critical finding.',
      valueMeaning: 'A report is listed when the language rules find an affirmed critical diagnosis, or, failing that, a critical keyword from the dictionary that is not negated. Modality comes from the report, else its PACS study; a modality guessed from the procedure name is marked as inferred.',
      interpretation: 'This is a screening list for review, not the CRN notification record; CRN has its own log under Administration.'
    },
    'oru-nlp-class': {
      title: 'NLP Classification',
      purpose: 'Reports classified by RAYD’s secondary text model as normal, borderline or critical.',
      valueMeaning: 'Critical = enough weighted critical terms affirmed (negated mentions removed). Normal = two or more normal phrases, or nothing affirmed. Everything else is borderline. Only reports scored by the current model count.',
      interpretation: 'Borderline is a large catch-all; read it as “not clearly normal”, not as a clinical category.'
    },
    'oru-severity': {
      title: 'Severity Distribution (1–5 scale)',
      purpose: 'Reports by the severity score of the secondary text model.',
      valueMeaning: '1 = normal, around 2–3 = borderline, rising to 5 with the weight of affirmed critical terms. Bars group scores by whole number.',
      interpretation: 'A shift of the bars to the right over time means more reports with serious findings, or wording that uses more critical terms.'
    },
    'oru-clusters': {
      title: 'Diagnostic Clusters',
      purpose: 'Groups of reports with similar wording, found automatically (k-means on term weights).',
      valueMeaning: 'Each cluster shows its label (its most typical terms), the number of reports and their average severity. Only reports scored by the current cluster model count.',
      interpretation: 'Clusters are statistical groupings, not diagnoses; use them to spot themes, such as a cluster of chest CT follow-ups.'
    },
    'oru-terms': {
      title: 'Extracted Medical Terms',
      purpose: 'The terms that best characterise the period’s reports, from the secondary text model.',
      valueMeaning: 'Each report contributes its most distinctive terms (weighted against all reports); the list counts how many reports carry each term.',
      interpretation: 'Generic words rank low by design; terms at the top are what sets these reports apart.'
    },

    // ── Report 29 — Infrastructure & Storage Audit ──────────────────────────────
    // Rows: the nightly storage summary (image sizes copied from the PACS) for the
    // study dates chosen, per procedure, modality and device.
    'r29-modality': {
      title: 'Storage by Modality (GB)',
      purpose: 'Gigabytes of images stored per modality for the studies of the period.',
      valueMeaning: 'Bar = image size added up per modality, from the nightly storage summary built from the PACS image sizes.',
      interpretation: 'CT and MR usually dominate. Use it to forecast archive growth; the Storage Forecast on the home page projects it forward.'
    },
    'r29-ae-density': {
      title: 'AE Storage Density (Avg MB/Study)',
      purpose: 'Average image size per study for each room, the 15 largest.',
      valueMeaning: 'Bar = the room’s total MB ÷ its studies. Rooms carry their RIS room name, else the AE title. Rooms far above the rest are removed first (above Q3 + 1.5 × the interquartile range); the badge says how many. Red above 500 MB.',
      interpretation: 'A room much heavier than others of the same modality may store extra series, thin slices or uncompressed images; worth a look with the modality vendor.'
    },
    'r29-protocols': {
      title: 'Protocol Density Inventory',
      purpose: 'Storage per procedure, modality and room, largest first.',
      valueMeaning: 'Studies, total GB and average MB per study for each combination. Critical above 500 MB per study, warning 250–500 MB, OK below 250 MB. The Storage Alert above lists up to 5 procedures averaging over 500 MB (after removing extreme values).',
      interpretation: 'Start with the rows that combine a high study count and a high average: changing one protocol there saves the most space.'
    },

    // ── Report AI — forecasts (utils/forecasting.py, since 2026-11-18) ────────────
    'ai-report': {
      title: 'AI Intelligence',
      purpose: 'Forecasts of the archive’s storage, the daily study volume and each device’s workload, and the referring physicians whose referrals changed clearly. Rizk Hospital only.',
      valueMeaning: 'History comes from the dates picked on the left. Forecasts are trained every night at 06:30 on everything since January 2022 and start the day after the latest data, so they show when the end date is today or yesterday.',
      interpretation: 'Use it to plan staffing around holidays, to see which devices will run hot in the next month, and to know how long the archive lasts. The forecast range matters as much as the line: plan for its upper edge when capacity is tight.'
    },
    'ai-model': {
      title: 'How the forecasts are made',
      purpose: 'A Prophet model learns, from the hospital’s own history, its weekly pattern, its yearly season, its growth, and what each kind of holiday does. A second step learns from the model’s own errors of the last 14 days and pulls the next days the same way (if the last days ran above the forecast, the next ones are raised), fading over the following days.',
      valueMeaning: 'Chosen on a test over the last 15 months: the straight line used before missed the daily volume by 67 % on average, this model by about 15 %. Neural networks did no better at 90 days. Periods marked unusual in the calendar (COVID, the 2024 war) are left out of training.',
      interpretation: 'About 14 % of the day-to-day variation (walk-ins, ER arrivals) cannot be predicted by any model, so read single days with their range. The next-day error under each chart says how the model has done over the last 30 days.'
    },
    'ai-calendar': {
      title: 'Holidays & closures',
      purpose: 'The list of holidays and closures the forecasts use. Seeded with Lebanon’s official holidays from 2019 to 2028, corrected from the hospital’s own data for past years (the days each Eid really closed, holidays moved to a Monday, Easter Mondays).',
      valueMeaning: 'Closed = runs like a Sunday (about 30 % of a weekday). Half day = Christmas Eve and New Year’s Eve. No effect = listed but worked (Ashura, the Prophet’s Birthday, the Annunciation since 2025). Unusual period = left out of training (lockdown, war, strike).',
      interpretation: 'Eid dates after 2026 are estimates: correct them when they are announced, and add one-off closures (a strike, a state funeral) as they happen. Changes count from the next training, tonight or with Retrain now. One list serves both hospitals.'
    },
    'ai-storage-current': {
      title: 'Current Storage',
      purpose: 'How much the archive holds now: every GB of images stored since RAYD started counting, whatever dates are picked.',
      valueMeaning: 'GB, against the capacity set in the sidebar (administrators can change it).',
      interpretation: 'If the capacity is not the archive’s real size, the days to capacity are wrong: check the capacity first.'
    },
    'ai-storage-daily': {
      title: 'Expected per Day',
      purpose: 'The GB a day the model expects over the next 90 days, on average.',
      valueMeaning: 'Weekdays store more, weekends and holidays much less; this is the average over all of them. Large one-off loads (a few studies of hundreds of GB, as on 30 July 2026) are kept out of the daily pattern and added back as their yearly average.',
      interpretation: 'A rise from one month to the next usually means more CT or MR studies, or a protocol that stores thinner slices.'
    },
    'ai-storage-full': {
      title: 'Days to Capacity',
      purpose: 'How many days until the archive is full at the forecast pace, and the date.',
      valueMeaning: 'The forecast GB per day are added up from today until they fill the free space; past the 2-year forecast, the pace of its last 90 days is continued. The range below uses the forecast’s high and low edges.',
      interpretation: 'Plan new storage well before the soonest date of the range. A storage forecast 90 days ahead has been within about ±16 % of what came; years ahead it is a guide, not a date.'
    },
    'ai-storage-chart': {
      title: 'GB Stored per Day + 90-Day Forecast',
      purpose: 'The GB of images stored each day in the dates picked, followed by the next 90 days of forecast.',
      valueMeaning: 'Solid line = what was stored. Dashed line = the forecast; shaded = its likely range. Hover a day for its holiday, if any.',
      interpretation: 'Single tall spikes are one-off loads, not a trend. The weekly saw-tooth is weekends.'
    },
    'ai-volume-total': {
      title: 'Total Studies',
      purpose: 'Studies in the dates picked, Rizk Hospital only.',
      valueMeaning: 'One study per PACS study. Structured reports and bone densitometry are left out; the modality comes from the device first.',
      interpretation: 'Compare with the same dates of last year to see growth.'
    },
    'ai-volume-avg': {
      title: 'Per Day',
      purpose: 'Average studies a day in the dates picked, and the average Monday to Friday, Saturday and Sunday.',
      valueMeaning: 'The weekday split leaves holidays out, so a Christmas week does not lower the Monday-to-Friday figure.',
      interpretation: 'Saturdays run at about a third of a weekday and Sundays at about a quarter at LAUMC: staff the weekend from these, not from the overall average.'
    },
    'ai-volume-next': {
      title: 'Next 90 Days',
      purpose: 'How many studies the model expects in the next 90 days, with the likely range.',
      valueMeaning: 'The sum of the daily forecast, holidays included. The range is the sum of the daily low and high edges, so it is wide on purpose.',
      interpretation: 'Use it for staffing and consumables. A month of actuals running near the high edge means demand is rising faster than the trend.'
    },
    'ai-volume-growth': {
      title: 'Growth',
      purpose: 'The underlying growth of daily volume over the last year, once the weekly pattern, season and holidays are taken out.',
      valueMeaning: 'In % a year, from the model’s trend. About +8 % a year since 2022 at LAUMC.',
      interpretation: 'A trend that flattens or turns negative is worth discussing with the referring departments before it shows in monthly totals.'
    },
    'ai-volume-chart': {
      title: 'Daily Study Volume + 90-Day Forecast',
      purpose: 'Studies each day in the dates picked, followed by the next 90 days of forecast.',
      valueMeaning: 'Blue bars = studies; purple bars = holidays. Red dots = unusual days, far from that weekday’s normal (holidays are not counted). Dashed line = forecast, shaded = its likely range, where about 8 days in 10 should fall.',
      interpretation: 'The forecast dips on weekends and holidays because the hospital does: that is the point of the model. Several unusual days in a row point at something real: a device down, a strike, an ETL gap.'
    },
    'ai-volume-upcoming': {
      title: 'Coming Holidays',
      purpose: 'The holidays and half days in the next 90 days, from the calendar, with the studies the model expects on each.',
      valueMeaning: 'Expected = the forecast for that day, which already includes how such a day has run in past years.',
      interpretation: 'If a date is wrong (an Eid announced a day later, a holiday moved to the Monday), correct it in Holidays & closures.'
    },
    'ai-volume-mod': {
      title: 'Volume Split by Modality',
      purpose: 'Share of the studies of the dates picked, per modality.',
      valueMeaning: 'Modality from the procedure first, then the device.',
      interpretation: 'A share that moves over months shows where growth comes from.'
    },
    'ai-util-anomalies': {
      title: 'Unusual Days',
      purpose: 'Device-days far from that device’s normal for the same weekday, in the dates picked.',
      valueMeaning: 'More than two standard deviations from the device’s average for that weekday. Holidays are not counted: the calendar explains them.',
      interpretation: 'A run of unusual days on one device usually means it was down or overbooked.'
    },
    'ai-util-high': {
      title: 'Above the high limit',
      purpose: 'Devices whose average utilization in the dates picked is above the high limit: 85 % unless an administrator has changed it (setting ai_util_high_warn_pct).',
      valueMeaning: 'Utilization = minutes of exams (measured on the RIS where available, else estimated from the procedure durations) ÷ the device’s opening minutes that weekday.',
      interpretation: 'These devices have no slack: delays and overtime follow. Check their 30-day forecast.'
    },
    'ai-util-low': {
      title: 'Below the low limit',
      purpose: 'Devices whose average utilization in the dates picked is between 0 and the low limit: 30 % unless an administrator has changed it (setting ai_util_low_warn_pct).',
      valueMeaning: 'Same calculation as the high limit. Devices with no opening hours on the RIS are not counted here.',
      interpretation: 'Low use can mean spare capacity, or opening hours on the RIS that are wider than the device really runs.'
    },
    'ai-util-chart': {
      title: 'Device Utilization + 30-Day Forecast',
      purpose: 'The chosen device’s daily utilization in the dates picked, and the next 30 days.',
      valueMeaning: 'Blue = utilization; purple dashed = what it would be if every exam took its scheduled time; red dots = unusual days. Orange dashed = forecast, shaded = its likely range. The forecast is of the device’s minutes, divided by its opening minutes that weekday.',
      interpretation: 'A device with no opening hours on the RIS has no utilization, so its forecast is given in minutes in the line above the chart. A forecast that sits above 85 % for weeks is a device to relieve.'
    },
    'ai-phys-changes': {
      title: 'Biggest changes',
      purpose: 'Referring physicians whose studies in the last 13 weeks moved most against the same 13 weeks a year earlier, and those who stopped or are new.',
      valueMeaning: 'Each name shows the studies gained or lost and the change in %. Listed: stopped (10 % or less of last year), new (none last year, 10 or more now), and changes of 20 % or more with enough studies not to be chance. Biggest loss or gain in studies first. Hover a name for both counts.',
      interpretation: 'Start with the top of the red list: those are the most studies lost. Compare each % with the hospital’s own change given above the table: a referrer moving with the hospital is not a change of their own.'
    },
    'ai-phys-table': {
      title: 'Top 20 Referring Physicians',
      purpose: 'The 20 physicians with most studies in the dates picked, and each one’s last 13 weeks against the same 13 weeks a year earlier.',
      valueMeaning: 'Studies = in the dates picked. Last 13 weeks = to the end date. Same weeks last year = the same 13 weeks, 52 weeks earlier, so season and holidays line up. Change: steady within 10 %, then slight (10–20 %), clear (20–40 %), sharp (over 40 %); a ? means too few studies to rule out chance.',
      interpretation: 'The end date sets the 13 weeks: move it to look at another quarter. A physician down while the hospital is up is losing ground even if the % looks small.'
    },
    'ai-phys-chart': {
      title: 'Monthly Referrals',
      purpose: 'The chosen physician’s studies per month in the dates picked, with the same month a year earlier.',
      valueMeaning: 'Bars = studies per month, red when down or stopped, green when up or new (last 13 weeks against a year earlier). Dashed line = the same month last year.',
      interpretation: 'Bars below the dashed line month after month are a real decline. The current month is not finished, so its bar is short.'
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
