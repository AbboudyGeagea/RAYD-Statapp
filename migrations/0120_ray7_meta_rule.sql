-- Migration 0120: seed the RAY7 self-monitoring rule.
--
-- RAY7 runs inline, inside the sender's ACK window, under a time budget. When that
-- budget is exceeded it abandons the remaining rules and raises a finding about
-- itself rather than silently screening less than it claims to.
--
-- That finding needs a catalogue row like any other, otherwise the studio's queue
-- shows a bare code with no title and no way to tune it — and this is the one
-- finding an operator most needs to be able to read, because it means screening is
-- degrading under load and some messages are passing with only part of the rule set
-- applied.
--
-- Left at info: exceeding the budget does not make the message suspect, so it must
-- not quarantine anything. It is a signal about RAY7, not about the data. Raise it
-- to warning at a site where it starts appearing regularly.
--
-- threshold_minutes is unused here; the budget is in milliseconds and lives in
-- utils/ray7._TIME_BUDGET_MS, since a per-message latency ceiling is an engineering
-- property of the ingest path rather than a clinical judgement an operator should be
-- editing from a web form.

INSERT INTO ray7_rules (rule_code, category, title, description, default_severity)
VALUES
    ('RAY7_BUDGET_EXCEEDED', 'referential', 'Screening budget exceeded',
     'RAY7 ran out of its inline time budget and skipped the remaining rules for this message. The message was accepted and stored; it was simply screened less thoroughly. Persistent occurrences mean the ingest path is under load.',
     'info')
ON CONFLICT (rule_code, modality) DO NOTHING;
