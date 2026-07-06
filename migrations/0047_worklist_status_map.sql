-- Migration 0047: RIS worklist STATUS_KEY -> canonical lifecycle stage map (LAUMC)
--
-- The LAUMC RIS has ~45 status codes, but they collapse into a handful of canonical
-- lifecycle stages. Many are ALIASES that must roll up to a core stage (e.g. "Porter",
-- "Oral STR", "General" all mean the patient has ARRIVED). RAYD must map via this table,
-- NEVER by hardcoding raw keys — new custom statuses appear over time.
--
-- Canonical stages (ordered):
--   requested -> scheduled -> arrived -> in_progress -> exam_done
--     -> dictated -> prelim -> signed -> approved      (report chain)
--   cancelled / discontinued                            (terminal, off-pathway)
--
-- KPIs downstream: wait_time   = t(arrived)     -> t(in_progress)
--                  exam_length = t(in_progress) -> t(exam_done)
--                  report_tat  = t(exam_done)   -> t(approved)

CREATE TABLE IF NOT EXISTS worklist_status_map (
    status_key    INTEGER PRIMARY KEY,        -- RIS SITE_WORKLIST.STATUS_KEY
    status_name   VARCHAR(64) NOT NULL,       -- RIS label (as-is, for traceability)
    stage         VARCHAR(20) NOT NULL,       -- canonical stage (see CHECK below)
    is_terminal   BOOLEAN NOT NULL DEFAULT FALSE, -- cancelled/discontinued/approved end the flow
    is_cancel     BOOLEAN NOT NULL DEFAULT FALSE, -- any cancellation flavour
    sort_order    SMALLINT NOT NULL,          -- lifecycle ordering for charts
    CONSTRAINT chk_stage CHECK (stage IN (
        'requested','scheduled','arrived','in_progress','exam_done',
        'dictated','prelim','signed','approved','cancelled','discontinued'
    ))
);

CREATE INDEX IF NOT EXISTS idx_wsm_stage ON worklist_status_map (stage);

-- ---------------------------------------------------------------------------
-- Seed from the LAUMC RIS status lookup (2026-07-06). Deprecated junk codes
-- ("REQUESTED-Remove", "IN PROGRESS-deleteMe") are intentionally omitted.
-- sort_order groups aliases onto their canonical stage's ordinal.
-- ---------------------------------------------------------------------------
INSERT INTO worklist_status_map (status_key, status_name, stage, is_terminal, is_cancel, sort_order) VALUES
    -- requested
    (5,    'Requested Unsigned',  'requested',    FALSE, FALSE, 10),
    (10,   'Requested Signed',    'requested',    FALSE, FALSE, 10),
    -- scheduled (+ aliases)
    (40,   'Scheduled',           'scheduled',    FALSE, FALSE, 20),
    (1723, 'DNA',                 'scheduled',    FALSE, FALSE, 20),
    (1825, 'Call',                'scheduled',    FALSE, FALSE, 20),
    (1827, 'Changed',             'scheduled',    FALSE, FALSE, 20),
    (1829, 'Req n.a.',            'scheduled',    FALSE, FALSE, 20),
    -- arrived (+ aliases) — patient present, waiting
    (60,   'Arrived',             'arrived',      FALSE, FALSE, 30),
    (1762, 'Preparation',         'arrived',      FALSE, FALSE, 30),
    (1823, 'Porter',              'arrived',      FALSE, FALSE, 30),
    (1826, 'Oral STR',            'arrived',      FALSE, FALSE, 30),
    (1831, 'General',             'arrived',      FALSE, FALSE, 30),
    -- in_progress (+ aliases) — on the table
    (70,   'Started',             'in_progress',  FALSE, FALSE, 40),
    (1120, 'Order In Progress',   'in_progress',  FALSE, FALSE, 40),
    (1822, 'In Progress',         'in_progress',  FALSE, FALSE, 40),
    (1824, 'Contrast',            'in_progress',  FALSE, FALSE, 40),
    (2223, 'Pre Exam',            'in_progress',  FALSE, FALSE, 40),
    -- exam_done (+ aliases)
    (100,  'Exam Done',           'exam_done',    FALSE, FALSE, 50),
    (1724, 'Series',              'exam_done',    FALSE, FALSE, 50),
    (1726, 'NRR',                 'exam_done',    FALSE, FALSE, 50),
    (1802, 'Technical Recall',    'exam_done',    FALSE, FALSE, 50),
    (1828, 'To Report',           'exam_done',    FALSE, FALSE, 50),
    (2022, 'Pending',             'exam_done',    FALSE, FALSE, 50),
    (2222, 'Inj Done',            'exam_done',    FALSE, FALSE, 50),
    -- report chain
    (110,  'Dictated',            'dictated',     FALSE, FALSE, 60),
    (120,  'Prelim Typed',        'prelim',       FALSE, FALSE, 70),
    (2025, 'Wet Read',            'prelim',       FALSE, FALSE, 70),
    (130,  'Signed 1',            'signed',       FALSE, FALSE, 80),
    (140,  'Signed 2',            'signed',       FALSE, FALSE, 80),
    (150,  'Signed 3',            'signed',       FALSE, FALSE, 80),
    (160,  'Approved',            'approved',     TRUE,  FALSE, 90),
    (1725, 'Ext.Rep.',            'approved',     TRUE,  FALSE, 90),
    (2024, 'Reviewed',            'approved',     TRUE,  FALSE, 90),
    -- cancellations / discontinuations (terminal, off-pathway)
    (20,   'Cancelled by RIS',    'cancelled',    TRUE,  TRUE,  99),
    (30,   'Cancelled by OP',     'cancelled',    TRUE,  TRUE,  99),
    (50,   'Cancelled',           'cancelled',    TRUE,  TRUE,  99),
    (1300, 'Rejected',            'cancelled',    TRUE,  TRUE,  99),
    (1702, 'Cancelled by PP',     'cancelled',    TRUE,  TRUE,  99),
    (1742, 'Cancelled by Patient','cancelled',    TRUE,  TRUE,  99),
    (1830, 'Not app',             'cancelled',    TRUE,  TRUE,  99),
    (2422, 'Cancelled Duplicate', 'cancelled',    TRUE,  TRUE,  99),
    (90,   'Discontinued',        'discontinued', TRUE,  FALSE, 99)
ON CONFLICT (status_key) DO UPDATE
    SET status_name = EXCLUDED.status_name,
        stage       = EXCLUDED.stage,
        is_terminal = EXCLUDED.is_terminal,
        is_cancel   = EXCLUDED.is_cancel,
        sort_order  = EXCLUDED.sort_order;

-- NOTE: unmapped STATUS_KEY values (future custom codes) resolve to NULL stage in
-- joins; RAYD logic must treat NULL stage as "unknown" (surfaced, not silently dropped)
-- and this seed extended when the RIS adds statuses.
