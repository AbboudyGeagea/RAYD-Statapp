# LAUMC — Open Questions & Needed Inputs

One place for everything unresolved. Answer inline (one line each is enough).
Where a **Default** is stated, silence = I proceed with the default.

---

## A. RIS — site_worklist & lifecycle

**A1. Status ladder** — full `STATUS_KEY` lookup list (code → label). Known: 10 Requested
Signed, 30 Cancelled by OP, 40 Scheduled, 50 Cancelled, 130 Signed 1, 140 Signed 2,
160 Approved. Missing: the 40→130 gap (arrived / started / exam-done) and anything else.
> Answer:

**A2. LINKED_ID semantics** — sibling SPS rows in the export share one `1008…` value
(REPORT_KEY?). Is that the linking mechanism, and what exactly does `LINKED_ID` hold?
> Answer:

**A3. `1004…` and `1006…` ID prefixes** — which columns are these (PPS_KEY? DICTATION_KEY?
REPORT_KEY?)? The DDL will likely answer this; flagging in case it doesn't.
> Answer:

**A4. Issuer population timing** — is `ISSUER_OF_PLACER_ORDER_NUMBER` set at ORDER CREATION
or only at SCHEDULING? Pre-scheduling rows show `ORG_STRUCTURE_KEY = 1` (default), so site
attribution for never-scheduled orders depends on this.
**Default**: never-scheduled orders go to an "unassigned" bucket visible only to all-sites users.
> Answer:

**A5. WORKLIST_STATUS_HISTORY** — does it hold the complete transition log (every status,
every row)? How far back? (Decides whether wait-time/exam-duration KPIs are backfillable
for all history.)
> Answer:

**A6. Grouped orders → PACS studies** — the hospital-workflow trap you flagged: when linked
SPS (e.g. CT abdo+pelvis) are acquired, does PACS create ONE study or one study per
accession? (RIS side is now understood; this is about the PACS side of the join.)
> Answer:

**A7. Adapter table list** — besides `site_worklist`, which RIS tables should RAYD import?
My wanted list: procedure catalog (SPS_CODE/RP_CODE lookups), STATUS_KEY lookup,
person/resource lookup (radiologists, technicians, referrers), org-structure lookup,
WORKLIST_STATUS_HISTORY. Anything else / anything to drop?
> Answer:

---

## B. HL7 feeds

**B1. RAYD's feed identity** — does RAYD subscribe to the SAP hub (as CareStream/PAXERABROKER
do) or to a RIS outbound feed? Related: which stream carries the arrived/started/done events?
**Alternative to consider**: skip a status HL7 feed entirely and poll WORKLIST_STATUS_HISTORY
every 30–60 s for the live feed / floor map — simpler, one less integration, DB is already
authoritative. Acceptable?
> Answer:

**B2. ORC-5 E-code vocabulary** — full list (E0001 = new, E0003 ≈ confirmed observed).
Comes with the integration document if you still have it.
> Answer:

**B3. ORU: MSH-4 semantics** — Carestream sent `2`. Site-stable per site, or fixed broker ID?
**Default**: ignore MSH-4; site via accession lookup (already agreed).
> Answer:

**B4. ORU status codes** — `FAP` = final/approved observed. What are the codes for
preliminary and addendum/amended reports? Are amended ORUs re-sent?
> Answer:

**B5. ORU per accession or per link group** — for linked SPS with one report, does each
accession get its own ORU (duplicated text)?
**Default**: dedupe reports by shared report/LINK id regardless.
> Answer:

---

## C. PACS

**C1. Main PACS product/schema** — is LAUMC's main PACS the same DIDB/`medistore` Oracle
schema as existing RAYD sites (the didb_studies.csv suggests yes), with Carestream as the
reporting layer? Confirms the existing PACS ETL works as-is with SITE_ID added.
> Answer:

**C2. PACS IS_LINKED / LINK_ID columns** — exact table + column names on the PACS side.
> Answer:

**C3. Report timestamps at LAUMC** — do `rep_prelim_timestamp` / `rep_final_timestamp` /
signer columns populate in DIDB the same way as existing sites (reporting stays PACS-side)?
> Answer:

**C4. Mammo SITE_ID bug** — any planned PACS-side fix, or does RAYD's RIS-authoritative
site + mismatch monitor remain the permanent compensation?
**Default**: permanent compensation; monitor quantifies it.
> Answer:

---

## D. Data pulls still to run (from LAUMC_DATA_REQUEST.md)

**D1.** A3 suspect-AE investigation results (legacy SJH PACS) — planned Monday.
**D2.** B1 SITE_ID distribution + B2 two-site DIDB_STUDIES extract (main PACS).
**D3.** B4 main-PACS AE inventory → decides per-device vs per-modality capacity tables.
**D4.** SITE_WORKLIST DDL / column list (you're sending — the headerless export is decoded
but unverified).
**D5.** E4 legacy SJH PACS connection details + schema owner.
**D6.** B5 aggregated image backfill extract (can wait until near go-live).

---

## E. Deployment configuration facts

**E1. Shift hours** — do RH and SJH share shift times, or per-site settings needed?
> Answer:

**E2. Go-live date + required stats history depth** (sets go_live_config + backfill window).
> Answer:

**E3. User list with site assignments** — who sees RH / SJH / both (HOD = both confirmed).
Can arrive later, but needed before user creation.
> Answer:

**E4. Feature set at LAUMC** — portal is REMOVED (confirmed). Any other pages to disable
(financial? scheduling? CD print?)?
> Answer:

**E5. Floor map inputs** (when we get there): floor plan image/SVG per site + room↔AE list.
Not blocking anything else.
> Answer:

---

*RDMS and BI-RADS: parked by explicit decision — not in scope, no questions.*
