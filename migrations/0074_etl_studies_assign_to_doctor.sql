-- Migration 0074: keep the PACS "assign to radiologist" value on each study.
--
-- Source: medistore.DIDB_STUDIES.ASSIGN_TO_DOCTOR (VARCHAR2 300), loaded by the studies
-- ETL. Values are PACS logins such as DBUSTROS@DN. The PACS writes the literal text
-- NOT ASSIGNED when nobody is assigned; the ETL stores that as NULL.
-- Used by Report 36's unread-backlog-by-assignee card.

ALTER TABLE etl_didb_studies ADD COLUMN IF NOT EXISTS assign_to_doctor TEXT;
