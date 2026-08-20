-- Migration 0046: cache the rule-based NLP fallback used by /oru/data.
--
-- routes/oru_analytics.py's /oru/data endpoint runs a synchronous, pure-Python
-- negation-aware phrase scan (~130 phrases x up to 8000 chars, with a backward
-- text-window scan per match) for any report the nlp-worker hasn't analyzed yet
-- (hl7_oru_analysis.affirmed_labels IS NULL). Per CLAUDE.md's rule that
-- hl7_oru_analysis is written ONLY by nlp_worker/worker.py, this route can never
-- persist its fallback result there -- so if the nlp-worker has any backlog, the
-- exact same expensive scan reruns on EVERY page load for the same still-pending
-- reports, for as long as that backlog persists. Likely the dominant cost behind
-- the "ORU analytics page very slow" complaint (operator punch-list #4).
--
-- This is a SEPARATE, route-owned cache table -- not hl7_oru_analysis -- so it
-- never touches the nlp-worker-only rule. The moment a report's real analysis
-- lands in hl7_oru_analysis, oru_data() already prefers it over this cache (the
-- fallback path only runs when affirmed_labels IS NULL), so there's no
-- staleness risk: this purely avoids redoing the same computation on every
-- request for reports that are still pending.

CREATE TABLE IF NOT EXISTS hl7_oru_rule_cache (
    report_id       INTEGER PRIMARY KEY REFERENCES hl7_oru_reports(id) ON DELETE CASCADE,
    affirmed_labels TEXT[] NOT NULL,
    rule_version    TEXT NOT NULL,
    computed_at     TIMESTAMP NOT NULL DEFAULT NOW()
);
