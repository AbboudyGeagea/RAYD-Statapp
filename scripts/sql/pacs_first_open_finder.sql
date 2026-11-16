-- scripts/sql/pacs_first_open_finder.sql — where does LAUMC's PACS write that a study was
-- opened? (to-do item 2, first open for Report 37 / JCI). PACS Oracle, READ-ONLY, run once
-- by a person in SQL Developer (F5, "Run Script"). Nothing here writes, and RAYD never runs it.
--
-- Step 1 and step 2 read Oracle's catalogue only (no load).
-- Step 3 needs a live open: in the PACS viewer, open an OLD study (e.g. from 2022, so nobody
-- else touches it), note its accession, your PACS login and the time; then run step 3 within
-- 15 minutes with both filled in. It looks only at rows written in the last 20 minutes, read
-- through each table's time index, in every table this login can read.

-- ── Step 1: who am I, and can I read the audit schemas? ──────────────────────
SELECT USER AS my_login,
       (SELECT COUNT(*) FROM all_tables WHERE owner IN ('AUDITING', 'AUDIT_OFFLINE')) AS audit_tables_readable,
       (SELECT COUNT(*) FROM session_privs WHERE privilege = 'SELECT ANY TABLE')      AS select_any_table,
       (SELECT COUNT(*) FROM session_privs WHERE privilege = 'SELECT ANY DICTIONARY') AS select_any_dictionary
FROM dual;

-- ── Step 2: tables whose name looks like a log, with their size ──────────────
SELECT owner, table_name, num_rows, last_analyzed
FROM all_tables
WHERE owner NOT IN ('SYS', 'SYSTEM', 'AUDSYS', 'XDB', 'MDSYS', 'CTXSYS', 'ORDSYS', 'ORDDATA', 'WMSYS',
                    'OLAPSYS', 'DBSNMP', 'OUTLN', 'APPQOSSYS', 'DVSYS', 'LBACSYS', 'GSMADMIN_INTERNAL',
                    'OJVMSYS', 'EXFSYS', 'SYSMAN', 'ORACLE_OCM', 'DBSFWUSER', 'FLOWS_FILES', 'DIP')
  AND REGEXP_LIKE(table_name, 'AUDIT|ATNA|LOG|ACCESS|EVENT|TRAIL|HIST|ACTIV|SESSION|TRACK|OPEN', 'i')
  AND table_name NOT LIKE 'BIN$%'
ORDER BY num_rows DESC NULLS LAST;

-- ── Step 3: the finder (fill in the two values first) ────────────────────────
SET SERVEROUTPUT ON SIZE 1000000
DECLARE
  v_acc    VARCHAR2(64) := 'PUT_ACCESSION_HERE';   -- the study you just opened
  v_login  VARCHAR2(64) := 'PUT_PACS_LOGIN_HERE';  -- the login you opened it with
  TYPE t_list IS TABLE OF VARCHAR2(200) INDEX BY PLS_INTEGER;
  labels   t_list;
  vals     t_list;
  v_uid    VARCHAR2(200);
  v_key    NUMBER;
  TYPE t_dates IS TABLE OF DATE INDEX BY PLS_INTEGER;
  centres  t_dates;          -- "now" on each clock the PACS might write in
  v_now    DATE := SYSDATE;
  v_utc    DATE;
  v_bey    DATE;
  v_any    VARCHAR2(32767);
  v_part   VARCHAR2(4000);
  v_tab    VARCHAR2(300);
  n        NUMBER;
  total    NUMBER;
  searched PLS_INTEGER := 0;
  hits     PLS_INTEGER := 0;
  errors   PLS_INTEGER := 0;

  FUNCTION lit(s VARCHAR2) RETURN VARCHAR2 IS
  BEGIN
    RETURN '''' || REPLACE(s, '''', '''''') || '''';
  END;

  FUNCTION d(x DATE) RETURN VARCHAR2 IS
  BEGIN
    RETURN 'TO_DATE(''' || TO_CHAR(x, 'YYYY-MM-DD HH24:MI:SS') || ''', ''YYYY-MM-DD HH24:MI:SS'')';
  END;

  -- the last 20 minutes on clock k: one plain range, so Oracle reads it through the index
  FUNCTION win(col VARCHAR2, k PLS_INTEGER) RETURN VARCHAR2 IS
  BEGIN
    RETURN '"' || col || '" BETWEEN ' || d(centres(k) - 20/1440) || ' AND ' || d(centres(k) + 2/1440);
  END;

  -- rows of the table, written in the last 20 minutes on any clock, that meet cond
  FUNCTION hits_for(cond VARCHAR2, col VARCHAR2) RETURN NUMBER IS
    m NUMBER; s NUMBER := 0;
  BEGIN
    FOR k IN 1 .. centres.COUNT LOOP
      EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_tab || ' WHERE ' || win(col, k)
                        || ' AND (' || cond || ')' INTO m;
      s := s + m;
    END LOOP;
    RETURN s;
  END;

  -- does this column hold value i?
  FUNCTION has(col VARCHAR2, typ VARCHAR2, i PLS_INTEGER) RETURN VARCHAR2 IS
    q VARCHAR2(200) := '"' || col || '"';
  BEGIN
    IF vals(i) IS NULL OR LENGTH(vals(i)) < 4 THEN
      RETURN NULL;
    ELSIF typ = 'NUMBER' THEN
      IF REGEXP_LIKE(vals(i), '^[0-9]{1,30}$') THEN RETURN q || ' = ' || vals(i); END IF;
      RETURN NULL;
    ELSIF typ = 'CLOB' THEN
      RETURN 'DBMS_LOB.INSTR(UPPER(' || q || '), ' || lit(UPPER(vals(i))) || ') > 0';
    END IF;
    RETURN 'INSTR(UPPER(' || q || '), ' || lit(UPPER(vals(i))) || ') > 0';
  END;
BEGIN
  SELECT CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE),
         CAST(FROM_TZ(CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP), 'UTC') AT TIME ZONE 'Asia/Beirut' AS DATE)
    INTO v_utc, v_bey FROM dual;
  centres(1) := v_now;
  IF ABS(v_utc - v_now) > 5/1440 THEN centres(centres.COUNT + 1) := v_utc; END IF;
  IF ABS(v_bey - v_now) > 5/1440 AND ABS(v_bey - v_utc) > 5/1440 THEN
    centres(centres.COUNT + 1) := v_bey;
  END IF;
  BEGIN
    SELECT MAX(study_instance_uid), MAX(study_db_uid) INTO v_uid, v_key
    FROM medistore.didb_studies WHERE accession_number = v_acc;
  EXCEPTION WHEN OTHERS THEN NULL;
  END;
  labels(1) := 'accession';  vals(1) := v_acc;
  labels(2) := 'login';      vals(2) := v_login;
  labels(3) := 'study UID';  vals(3) := v_uid;
  labels(4) := 'study key';  vals(4) := TO_CHAR(v_key);
  DBMS_OUTPUT.PUT_LINE('Study key ' || NVL(TO_CHAR(v_key), '(accession not found in MEDISTORE)')
                       || ', UID ' || v_uid);
  DBMS_OUTPUT.PUT_LINE('Clocks: server ' || TO_CHAR(v_now, 'HH24:MI') || ', UTC ' || TO_CHAR(v_utc, 'HH24:MI')
                       || ', Beirut ' || TO_CHAR(v_bey, 'HH24:MI'));

  FOR t IN (SELECT DISTINCT ic.table_owner o, ic.table_name tn, ic.column_name tc
            FROM all_ind_columns ic
            JOIN all_tab_columns c ON c.owner = ic.table_owner AND c.table_name = ic.table_name
                                  AND c.column_name = ic.column_name
            WHERE ic.column_position = 1
              AND (c.data_type = 'DATE' OR c.data_type LIKE 'TIMESTAMP%')
              AND ic.table_name NOT LIKE 'BIN$%'
              AND ic.table_owner NOT IN ('SYS', 'SYSTEM', 'AUDSYS', 'XDB', 'MDSYS', 'CTXSYS', 'ORDSYS',
                    'ORDDATA', 'WMSYS', 'OLAPSYS', 'DBSNMP', 'OUTLN', 'APPQOSSYS', 'DVSYS', 'LBACSYS',
                    'GSMADMIN_INTERNAL', 'OJVMSYS', 'EXFSYS', 'SYSMAN', 'ORACLE_OCM', 'DBSFWUSER',
                    'FLOWS_FILES', 'DIP')
            ORDER BY 1, 2, 3) LOOP
    BEGIN
      v_tab := '"' || t.o || '"."' || t.tn || '"';
      v_any := NULL;
      FOR c IN (SELECT column_name, data_type FROM all_tab_columns
                WHERE owner = t.o AND table_name = t.tn
                  AND data_type IN ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR', 'CLOB', 'NUMBER')) LOOP
        FOR i IN 1 .. 4 LOOP
          v_part := has(c.column_name, c.data_type, i);
          IF v_part IS NOT NULL THEN
            v_any := v_any || CASE WHEN v_any IS NOT NULL THEN ' OR ' END || v_part;
          END IF;
        END LOOP;
      END LOOP;
      IF v_any IS NOT NULL THEN
        searched := searched + 1;
        total := hits_for(v_any, t.tc);
        IF total > 0 THEN
          hits := hits + 1;
          DBMS_OUTPUT.PUT_LINE('*** ' || t.o || '.' || t.tn || ' (time column ' || t.tc || '): '
                               || total || ' matching row(s)');
          -- which column holds which value
          FOR c IN (SELECT column_name, data_type FROM all_tab_columns
                    WHERE owner = t.o AND table_name = t.tn
                      AND data_type IN ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR', 'CLOB', 'NUMBER')
                    ORDER BY column_id) LOOP
            FOR i IN 1 .. 4 LOOP
              v_part := has(c.column_name, c.data_type, i);
              IF v_part IS NOT NULL THEN
                n := hits_for(v_part, t.tc);
                IF n > 0 THEN
                  DBMS_OUTPUT.PUT_LINE('      ' || c.column_name || ' holds the ' || labels(i)
                                       || ' (' || n || ' row(s))');
                END IF;
              END IF;
            END LOOP;
          END LOOP;
        END IF;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      errors := errors + 1;
      IF errors <= 10 THEN
        DBMS_OUTPUT.PUT_LINE('(skipped ' || t.o || '.' || t.tn || ': ' || SUBSTR(SQLERRM, 1, 120) || ')');
      END IF;
    END;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Done: ' || searched || ' table/time-column pairs searched, ' || hits
                       || ' with a match, ' || errors || ' skipped.');
END;
/

-- ── Step 4: look at the rows step 3 found ────────────────────────────────────
-- For each table it names, with the column that holds your login:
--   SELECT * FROM <owner>.<table>
--   WHERE <time column> > SYSDATE - 3/24 AND INSTR(UPPER(<login column>), UPPER('<login>')) > 0
--   ORDER BY <time column>;
-- The open we want: one row per open, with the study (accession, UID or key), the login and
-- its own time. Then check it is kept: the same query on an older day, a few opens of one study.
