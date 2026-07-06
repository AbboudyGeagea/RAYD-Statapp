# LAUMC — Open Questions & Needed Inputs

Everything I still need to know, in plain language. For each one I explain **why I'm asking**
and **what changes depending on your answer**, so you can see why it matters.

Just write your answer on the "Answer:" line. Where I've written a **Default**, that means:
if you say nothing, I'll go with that and move on — so you only need to reply if you disagree.

---

## A. About the RIS worklist and how an order moves through its life

**A1. The full list of order statuses.**
From your export I could see some of the status codes and their meanings (10 = Requested
Signed, 40 = Scheduled, 160 = Approved, and a few others). But the codes between "Scheduled"
and "Signed" are missing from my sample — and those are exactly the ones I need: **patient
arrived, exam started, exam finished.** Those three moments are what power the live floor map
and the true waiting-time numbers.
*What I need:* the complete list of status codes and what each one means.
> **Answer (2026-07-05):** Not readily available — user will pull the status codes manually.
> Not blocking anything except the final floor-map labels.

**A2. How do you link several exams into one report?**
When a patient gets, say, a CT abdomen and a CT pelvis together, your system seems to tie them
into a single report. I saw evidence of this in the export (sibling exams sharing one report
number). I need to know exactly which field tells me "these belong together," so RAYD counts
them as **two exams but one report** — otherwise the report statistics double-count.
*What I need:* confirmation of which column is the link (is it `LINKED_ID`, `REPORT_KEY`, or
something else?).
> **Answer (2026-07-05):** Coming in ~2 hours for RIS and PACS. ORU (report message) link
> to be verified separately — but per B5 that's fine either way (duplicate reports just get
> deduped).

**A3. Two ID columns I couldn't identify.**
In the export there were two ID columns I couldn't match to a name (my sample had no headers).
The table schema you're sending will probably answer this on its own — just flagging it so we
don't forget to check.
> Answer:

**A4. When does the order get stamped with its site (Rafic Hariri vs SJH)?**
The site marker (`SAP_PROD` / `SAP_SJH`) — does it get written the moment the order is
**created**, or only later when it gets **scheduled**? This matters for one specific case: an
order that's created and then cancelled before it's ever scheduled. If the site is stamped at
creation, I can still tell you which hospital that cancelled order belonged to. If not, I
can't.
**Default:** cancelled-before-scheduling orders that have no site go into an "unassigned"
bucket that only managers who see both sites can view.
> Answer:

**A5. Is there a complete history of every status change?**
The schema showed a table that looks like it logs every status change with a timestamp
(WORKLIST_STATUS_HISTORY). If that table holds the **full history going back years**, then I
can calculate waiting times and exam durations for **all your historical data**, not just from
go-live onward — a much richer starting point. I just need to confirm it's complete and how
far back it goes.
> **Answer (2026-07-05):** That history table is NOT in use — we build the logic ourselves.
> BUT the timestamp of each status IS stored somewhere; user will pull that.
>
> **⭐ SHARPENED FOLLOW-UP (most important thing to check in the schema tomorrow):**
> Where those status-timestamps live decides our whole live-data method. Two possibilities:
> - **(Best case)** Each status has its OWN column that stays on the row — e.g. an
>   arrived-time column, a started-time column, a done-time column, all accumulating.
>   → Then RAYD just reads the worklist every 30-60s and captures everything, even if a
>   patient moves through all three states between two reads. Nothing lost. Simplest possible.
> - **(Harder case)** There's only ONE "current status + one timestamp" that gets overwritten
>   each change. → Then reading periodically could miss fast in-between transitions, and we'd
>   need a different approach.
>
> Which one is it? (Strong bet: separate accumulating columns.)
>
> **PARTIALLY ANSWERED (2026-07-06) from the column list:** the worklist HAS dedicated columns
> for scheduled / performed / approved — but NOT for arrived or started. So arrived & started
> only exist as status events, not columns. GOOD NEWS: the RIS already emits those as outbound
> HL7 messages (see B1), so RAYD subscribes to that feed for arrived/started and polls the DB
> for the rest. Still need: the STATUS_KEY numeric value for each state.

**A6. When linked exams are done, does PACS make one study or several?**
This is the workflow question you flagged yourself. When those linked exams (CT abdomen +
pelvis) are actually performed, does the PACS end up with **one study** or **one study per
exam**? I need this to match the RIS orders to the PACS studies correctly. You mentioned you'd
validate this with the hospital's actual workflow.
> Answer:

**A7. Which other RIS tables should I pull besides the worklist?**
The worklist is the main table, but I'll need the small "lookup" tables it points to — the
ones that translate codes into names. My wish list: the procedure/exam catalog, the status-code
list, the people list (radiologists, technicians, referring doctors), the department list, and
the status-history table. **Is anything missing from this list, or anything I should skip?**
> Answer:

---

## B. About the live message feeds (HL7)

**B1. Where does RAYD plug in to receive live messages — and do we even need to?**
Your messages flow through a central hub (SAP) out to several systems. Two questions in one:
first, does RAYD tap into that hub, or into a feed coming directly out of the RIS? Second — and
this might make life simpler — instead of receiving live "arrived/started/done" messages at
all, **RAYD could just check the RIS status-history table every 30–60 seconds** for the live
board and floor map. One less connection to set up and maintain, and the database already has
the truth. Is that acceptable to you?
> **Answer (2026-07-05):** User asked which is safer — an Oracle TRIGGER on the RIS that pushes
> data, or RAYD SELECTing from the RIS like usual.
>
> **RECOMMENDATION: RAYD polls (SELECT), read-only, NO trigger.** Reasons:
> - RAYD never writes to the production clinical RIS — nothing to blame if the RIS misbehaves.
> - No schema change to a live clinical system (a trigger is a permanent support liability,
>   even for you as the vendor).
> - A trigger fires inside the radiologist's transaction — adds risk/latency to clinical work.
>   If RAYD is down, a push trigger errors into the void; a poll just resumes quietly.
> - Same read-only watermark pattern RAYD already uses everywhere else. 30-60s is plenty fast
>   for a floor board.
> - Depends on A5 being the "best case" (separate accumulating timestamp columns) — if so,
>   polling is lossless and this is the whole solution. **Confirm A5 first.**
> - This also means "which live feed does RAYD tap" mostly disappears — for the live board and
>   floor map, RAYD reads the RIS DB directly.
>
> **UPDATED (2026-07-06):** Your status samples revealed the RIS ALREADY emits outbound HL7
> status messages (arrived/started/completed) to Carestream/HIS/SAP. So the cleanest design is
> a HYBRID, still no trigger: RAYD subscribes to that existing RIS-outbound feed for the live
> moments (especially arrived/started, which have no DB column), and polls the DB for the rest
> and for history. **One thing to confirm: can RAYD be added as a recipient of that RIS-outbound
> feed?** (Standard HL7 subscription — not a DB change.)

**B2. The order-status codes inside the HL7 messages.**
Separate from the database status codes (A1) — the live messages carry their own status codes
(I saw "E0001" for a new order). I need the full list of these codes too. This usually lives in
the integration document you mentioned you might still have.
> Answer:

**B3. The report messages don't say which site — is that always true?**
The radiology report messages (from Carestream) didn't carry a clear site marker. My plan is to
figure out the site by matching the report's accession number back to its order. Just
confirming that's the right approach and there's no site field I overlooked.
**Default:** match report → order by accession number to determine the site.
> Answer:

**B4. Preliminary vs final vs corrected reports.**
The report message I saw was marked "final/approved" (code FAP). What are the codes for a
**preliminary** report and a **corrected/amended** report? And when a radiologist amends a
report, does the system send a fresh message, or update the old one? This affects how RAYD
tracks report turnaround time.
> Answer:

**B5. For linked exams, does each exam get its own report message?**
Following on from A2 — when several exams share one report, does RAYD receive **one report
message per exam** (with the same text repeated), or just one? I want to avoid counting the
same report several times.
**Default:** if the same report arrives more than once, keep only one.
> Answer:

---

## C. About the PACS

**C1. Is LAUMC's main PACS the same type RAYD already knows?**
The studies export you sent looks like the same PACS database structure RAYD already reads at
other sites, with Carestream sitting on top as the reporting layer. If that's right, RAYD's
existing PACS data pull works almost as-is — I just add the site marker. Confirming this saves
a lot of work.
> Answer:

**C2. Where does PACS store its own "linked studies" marker?**
You mentioned PACS also has a linking field (IS_LINKED / LINK_ID). I need the exact table and
column name on the PACS side.
> Answer:

**C3. Do the report timestamps land in PACS the normal way?**
Since reporting stays on the PACS side, do the preliminary/final report times and the signing
radiologist's name get recorded in the PACS database the same way they do at your other sites?
If yes, the turnaround-time reports work unchanged.
> Answer:

**C4. The SJH mammography site-labeling bug — is it being fixed, or do we work around it?**
You flagged that SJH mammograms currently show up as Rafic Hariri in the PACS. My plan is to
trust the RIS for the correct site and flag the mismatches in a monitoring report. Is the PACS
bug going to be fixed at some point, or should RAYD's workaround be considered the permanent
solution?
**Default:** RAYD's workaround is permanent; the monitor report tracks how often it happens.
> Answer:

---

## D. Data extracts still to pull

These are things to export and send when you can (SQL for each is in `LAUMC_DATA_REQUEST.md`):

- **D1.** The suspect device-name (AE title) investigation on the old SJH PACS — you planned
  this for Monday.
- **D2.** The studies count by site + a few weeks of studies covering **both** sites, from the
  main PACS.
- **D3.** The device inventory on the main PACS — this decides whether LAUMC gets
  **per-device** statistics (each machine tracked separately) or just per-modality.
- **D4.** The worklist table's column list / schema — you're already sending this. My decoded
  version of the headerless export needs this to be confirmed correct.
- **D5.** Connection details for the old SJH PACS (so I can run the device investigation).
- **D6.** The image-storage summary extract — this one can wait until closer to go-live.

---

## E. Deployment setup decisions

**E1. Do the two hospitals run the same working hours?**
Do Rafic Hariri and SJH share the same shift times (morning/afternoon/night), or does each site
need its own hours configured? Affects the shift and utilization reports.
> Answer:

**E2. Go-live date, and how far back should the statistics go?**
When do you want this live, and how much history should RAYD load and show?
> Answer:

**E3. Who sees what?**
The list of users and which site(s) each one can see (you've confirmed the department head sees
both). This can come later, but I need it before setting up accounts.
> Answer:

**E4. Which features should be turned off at LAUMC?**
The patient portal is removed — confirmed. Is there anything else to hide at this site (the
financial dashboard? scheduling? CD printing)?
> Answer:

**E5. Floor-map materials (only when we build that part).**
A floor plan drawing for each site, and a list of which room holds which machine. Nothing else
depends on this, so no rush.
> Answer:

---

*Note: the radiation-dose feature and the mammography BI-RADS reporting are both set aside on
purpose — not part of this work, no questions here.*
