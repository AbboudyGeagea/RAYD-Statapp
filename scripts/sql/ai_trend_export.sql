-- scripts/sql/ai_trend_export.sql — the daily series behind Report AI, for studying trends
-- (weekday/weekend, Ramadan, Christmas, holidays) before choosing a forecasting model.
-- READ-ONLY: one READ ONLY transaction of SELECTs. Writes CSV files under /tmp/rayd_trends
-- INSIDE the rayd_db container; nothing in the database is created or changed.
--
-- No patient data: counts only. Referring physicians are an md5 key, never a name.
-- Reads through the rh views (search_path rh, public), exactly as the Report AI page does,
-- so another site's rows, test patients and the HIS's duplicate orders are already out.
--
-- Run on the RAYD server:
--   docker exec -i rayd_db psql -U etl_user -d etl_db -f - < scripts/sql/ai_trend_export.sql
--   docker exec rayd_db tar czf - -C /tmp rayd_trends > rayd_trends.tgz
-- Send back rayd_trends.tgz and the text psql printed (the summary at the end).
--
-- Files (all CSV with a header):
--   01_volume_daily.csv      study_date, modality (mapped, rule 3), patient_class, studies
--   02_device_daily.csv      study_date, device, modality, studies, est_mins, actual_mins, cap_mins
--   03_storage_daily.csv     study_date, device, modality, device_site, gb, studies
--   04_referrer_daily.csv    study_date, referrer_key (md5), modality, studies
--   05_bookings_daily.csv    scheduled_date, orders, booked >=1/3/7/14 days ahead, by status
--   06_etl_runs_daily.csv    run_date, job_name, runs, failed, partial, records
--   07_hourly_profile.csv    insert_date, hour, studies (Ramadan shifts hours, not only volume)

\set ON_ERROR_STOP on
\pset footer off
\! mkdir -p /tmp/rayd_trends

BEGIN READ ONLY;
SET LOCAL search_path TO rh, public;
SET LOCAL statement_timeout = '15min';

-- 01 — studies per day, by mapped modality and patient class ---------------------------
\o /tmp/rayd_trends/01_volume_daily.csv
COPY (
    SELECT s.study_date,
           UPPER(TRIM(COALESCE(m.modality, s.study_modality, 'UNMAPPED'))) AS modality,
           COALESCE(NULLIF(TRIM(s.patient_class), ''), '?')                AS patient_class,
           COUNT(*)                                                         AS studies
      FROM etl_didb_studies s
      LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
     WHERE COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'BMD')
       AND s.study_date IS NOT NULL
     GROUP BY 1, 2, 3
     ORDER BY 1, 2, 3
) TO STDOUT WITH (FORMAT csv, HEADER);
\o

-- 02 — per device per day: estimated minutes (procedure durations, 15 by default), measured
--      minutes (RIS performed steps), the day's capacity (same chain as the page) -----------
\o /tmp/rayd_trends/02_device_daily.csv
COPY (
    WITH est AS (
        SELECT UPPER(TRIM(s.storing_ae)) AS device,
               s.study_date,
               MAX(UPPER(TRIM(COALESCE(m.modality, s.study_modality)))) AS modality,
               COUNT(*)                                                 AS studies,
               SUM(COALESCE(pm.duration_minutes, 15))                   AS est_mins
          FROM etl_didb_studies s
          LEFT JOIN procedure_duration_map pm ON pm.procedure_code = s.procedure_code
          LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
         WHERE s.storing_ae IS NOT NULL
           AND COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'BMD', 'PACS')
           AND COALESCE(m.site_id = rayd_served_site_id(), TRUE)
         GROUP BY 1, 2
    ), act AS (
        SELECT UPPER(TRIM(p.performing_ae_title)) AS device,
               p.start_datetime::date             AS study_date,
               ROUND(SUM(EXTRACT(EPOCH FROM (p.end_datetime - p.start_datetime)) / 60)::numeric, 1) AS actual_mins
          FROM std_pps p
          JOIN etl_didb_studies s ON s.study_db_uid = p.study_db_uid
         WHERE p.end_datetime > p.start_datetime
           AND p.end_datetime - p.start_datetime < INTERVAL '12 hours'
           AND p.performing_ae_title IS NOT NULL
         GROUP BY 1, 2
    ), cap AS (
        SELECT UPPER(TRIM(m.aetitle)) AS device, d.dow,
               COALESCE(dwa.available_minutes, m.daily_capacity_minutes, ws.std_opening_minutes, 480) AS cap_mins
          FROM aetitle_modality_map m
         CROSS JOIN generate_series(0, 6) AS d(dow)
          LEFT JOIN std_device_weekly_availability dwa
                 ON UPPER(TRIM(dwa.aetitle)) = UPPER(TRIM(m.aetitle)) AND dwa.day_of_week = d.dow
          LEFT JOIN device_weekly_schedule ws
                 ON UPPER(TRIM(ws.aetitle)) = UPPER(TRIM(m.aetitle)) AND ws.day_of_week = d.dow
    )
    SELECT e.study_date, e.device, e.modality, e.studies, e.est_mins, a.actual_mins, c.cap_mins
      FROM est e
      LEFT JOIN act a ON a.device = e.device AND a.study_date = e.study_date
      LEFT JOIN cap c ON c.device = e.device
                     AND c.dow = (EXTRACT(ISODOW FROM e.study_date)::int - 1)   -- 0 = Monday
     ORDER BY 2, 1
) TO STDOUT WITH (FORMAT csv, HEADER);
\o

-- 03 — GB stored per day (summary_storage_daily has no rh view: the device's site is
--      exported so the analysis can keep this site's devices only) ----------------------
\o /tmp/rayd_trends/03_storage_daily.csv
COPY (
    SELECT g.study_date,
           UPPER(TRIM(g.storing_ae))                              AS device,
           UPPER(TRIM(COALESCE(m.modality, g.modality, '?')))     AS modality,
           COALESCE(st.code, '?')                                  AS device_site,
           ROUND(SUM(g.total_gb)::numeric, 3)                      AS gb,
           SUM(g.study_count)                                      AS studies
      FROM public.summary_storage_daily g
      LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(g.storing_ae))
      LEFT JOIN sites st ON st.id = m.site_id
     GROUP BY 1, 2, 3, 4
     ORDER BY 1, 2
) TO STDOUT WITH (FORMAT csv, HEADER);
\o

-- 04 — studies per referring physician per day (md5 key, no names) -----------------------
\o /tmp/rayd_trends/04_referrer_daily.csv
COPY (
    SELECT s.study_date,
           LEFT(MD5(UPPER(TRIM(CONCAT_WS(' ', s.referring_physician_first_name,
                                               s.referring_physician_last_name)))), 12) AS referrer_key,
           UPPER(TRIM(COALESCE(m.modality, s.study_modality, 'UNMAPPED')))             AS modality,
           COUNT(*)                                                                      AS studies
      FROM etl_didb_studies s
      LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
     WHERE COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'BMD')
       AND NULLIF(TRIM(CONCAT_WS(' ', s.referring_physician_first_name,
                                      s.referring_physician_last_name)), '') IS NOT NULL
     GROUP BY 1, 2, 3
     ORDER BY 1, 2, 3
) TO STDOUT WITH (FORMAT csv, HEADER);
\o

-- 05 — RIS bookings per scheduled day, and how far ahead they were booked (do known
--      appointments predict the next days' volume better than history alone?) ------------
\o /tmp/rayd_trends/05_bookings_daily.csv
COPY (
    SELECT o.scheduled_datetime::date                                             AS scheduled_date,
           UPPER(TRIM(COALESCE(o.modality, '?')))                                  AS modality,
           LOWER(COALESCE(o.order_status, '?'))                                    AS order_status,
           COUNT(*)                                                                AS orders,
           COUNT(*) FILTER (WHERE o.has_study)                                     AS with_study,
           COUNT(*) FILTER (WHERE o.order_created_at < o.scheduled_datetime::date - 0)  AS booked_1d,
           COUNT(*) FILTER (WHERE o.order_created_at < o.scheduled_datetime::date - 2)  AS booked_3d,
           COUNT(*) FILTER (WHERE o.order_created_at < o.scheduled_datetime::date - 6)  AS booked_7d,
           COUNT(*) FILTER (WHERE o.order_created_at < o.scheduled_datetime::date - 13) AS booked_14d
      FROM etl_orders o
     WHERE o.scheduled_datetime IS NOT NULL
     GROUP BY 1, 2, 3
     ORDER BY 1, 2, 3
) TO STDOUT WITH (FORMAT csv, HEADER);
\o

-- 06 — ETL runs per day (a day with few studies and a failed ETL is a gap, not a holiday) --
\o /tmp/rayd_trends/06_etl_runs_daily.csv
COPY (
    SELECT start_time::date                                   AS run_date,
           job_name,
           COUNT(*)                                           AS runs,
           COUNT(*) FILTER (WHERE UPPER(status) = 'FAILED')   AS failed,
           COUNT(*) FILTER (WHERE UPPER(status) = 'PARTIAL')  AS partial,
           SUM(COALESCE(records_processed, 0))                AS records
      FROM public.etl_job_log
     GROUP BY 1, 2
     ORDER BY 1, 2
) TO STDOUT WITH (FORMAT csv, HEADER);
\o

-- 07 — studies per hour of day (Ramadan moves working hours). The hour the study reached
--      the PACS (insert_time): study_time is never written, study_date has no time ----------
\o /tmp/rayd_trends/07_hourly_profile.csv
COPY (
    SELECT s.insert_time::date                   AS insert_date,
           EXTRACT(HOUR FROM s.insert_time)::int AS hour,
           COUNT(*)                              AS studies
      FROM etl_didb_studies s
      LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
     WHERE COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'BMD')
       AND s.insert_time IS NOT NULL
     GROUP BY 1, 2
     ORDER BY 1, 2
) TO STDOUT WITH (FORMAT csv, HEADER);
\o

-- Summary printed on screen: paste this back even if the files can't be sent ------------
\echo
\echo '== History: studies per month (is there anything before 2026-01-01?) =='
SELECT TO_CHAR(DATE_TRUNC('month', s.study_date), 'YYYY-MM') AS month,
       COUNT(*)                                             AS studies,
       COUNT(DISTINCT s.study_date)                         AS days_with_studies,
       ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT s.study_date), 0), 1) AS per_day
  FROM etl_didb_studies s
  LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
 WHERE COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'BMD')
 GROUP BY 1 ORDER BY 1;

\echo
\echo '== Weekday profile since 2026-01-01 (1 = Monday) =='
WITH d AS (
    SELECT s.study_date, COUNT(*) AS n
      FROM etl_didb_studies s
      LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
     WHERE COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'BMD')
       AND s.study_date >= DATE '2026-01-01'
     GROUP BY 1
)
SELECT EXTRACT(ISODOW FROM study_date)::int AS dow,
       TO_CHAR(MIN(study_date), 'Dy')         AS day,
       COUNT(*)                               AS days,
       ROUND(AVG(n), 1)                       AS avg_studies,
       MIN(n)                                 AS min_studies,
       MAX(n)                                 AS max_studies,
       ROUND(STDDEV(n), 1)                    AS stddev
  FROM d GROUP BY 1 ORDER BY 1;

\echo
\echo '== The 25 quietest days since 2026-01-01 (holidays, closures, or ETL gaps?) =='
SELECT s.study_date, TO_CHAR(s.study_date, 'Dy') AS day, COUNT(*) AS studies
  FROM etl_didb_studies s
  LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(m.aetitle)) = UPPER(TRIM(s.storing_ae))
 WHERE COALESCE(m.modality, s.study_modality, '') NOT IN ('SR', 'BMD')
   AND s.study_date >= DATE '2026-01-01'
   AND s.study_date < CURRENT_DATE
   AND EXTRACT(ISODOW FROM s.study_date) < 7
 GROUP BY 1 ORDER BY 3, 1 LIMIT 25;

COMMIT;

\echo
\echo 'Files written to /tmp/rayd_trends inside rayd_db. Next:'
\echo '  docker exec rayd_db tar czf - -C /tmp rayd_trends > rayd_trends.tgz'
