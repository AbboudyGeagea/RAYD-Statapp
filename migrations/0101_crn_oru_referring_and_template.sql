-- Migration 0101: CRN grounding — ORU-native referring-physician columns,
-- referring_contacts.middle_name, and an admin-editable message template.
--
-- Operator instruction (2026-08-01): referring_contacts is empty in production because
-- its only auto-fill source (hl7_orders.referring_physician_code, from PV1-8 on the R2I
-- ORM feed) is starved -- that feed isn't flowing (same root cause as the Report 25/35
-- empty-data issue earlier this session). Operator is adding a PV1 segment (same XCN
-- shape as ORM's PV1-8) to the ORU feed instead, which IS live today -- these columns
-- are where hl7_listener.py's parse_oru_r01() will store what it parses from that once
-- it starts arriving. Nullable everywhere: today's ORU messages have no PV1 at all, so
-- every existing row stays NULL until the RIS-side change ships.
--
-- middle_name on referring_contacts: used only to disambiguate two contacts that would
-- otherwise collide on first+last (physician_name stays the single free-text natural
-- key, unchanged -- splitting it would ripple through referring_intel.py,
-- referring_contacts_admin.py, crn_scan.py, and crn_ack.py's detail query for no benefit
-- proportional to the disruption).
--
-- crn_message_template: replaces the hardcoded placeholder string in
-- utils/crn_scan.py's _build_message() (that function's own docstring already flagged
-- it as "operator has not signed off on real message wording yet"). Merge fields:
-- {accession}, {ack_url} -- deliberately no patient-identifying fields, matching
-- crn_ack.py's "minimal PHI in the body" design (detail lives behind the token).

ALTER TABLE hl7_oru_reports
    ADD COLUMN IF NOT EXISTS referring_physician_code        TEXT,
    ADD COLUMN IF NOT EXISTS referring_physician_first_name  TEXT,
    ADD COLUMN IF NOT EXISTS referring_physician_last_name   TEXT,
    ADD COLUMN IF NOT EXISTS referring_physician_middle_name TEXT;

ALTER TABLE referring_contacts
    ADD COLUMN IF NOT EXISTS middle_name TEXT;

INSERT INTO settings (key, value) VALUES ('crn_message_template',
    'RAYD: A radiology report requires your acknowledgment (accession {accession}). {ack_url}')
ON CONFLICT (key) DO NOTHING;
