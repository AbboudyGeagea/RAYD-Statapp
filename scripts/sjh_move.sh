#!/bin/bash
# scripts/sjh_move.sh — SJH into its own database, unattended (LAUMC item 13b; the runbook in
# docs/LAUMC_NEXT_SESSION.md item 13, steps 0-5, as one script).
#
#   sudo bash /opt/rayd/scripts/sjh_move.sh check   # changes nothing: done already? ETL running?
#                                                   #   disk, counts per site
#   sudo bash /opt/rayd/scripts/sjh_move.sh copy    # steps 0-3: backup of RH's database, SJH's
#                                                   #   database switched on, dry run, copy + verify.
#                                                   #   Deletes nothing from RH.
#   sudo bash /opt/rayd/scripts/sjh_move.sh move    # steps 0-5: the same, then the move (SJH's rows
#                                                   #   deleted from RH once every table verified),
#                                                   #   VACUUM ANALYZE, counts before / after
#
# Scheduled by the operator for Sunday 22 Nov 2026, 01:00 (Saturday night), validated Sunday and
# Monday. History is copied Postgres to Postgres (no PACS read); from the night after the move the
# 05:00 ETL stores SJH's new rows in SJH's database itself (ETL_JOBS/etl_site_route.py).
#
# Stops at the first problem. Until the move has started deleting, a stop also switches SJH's
# database off again (.env restored), so nobody sees a half-filled SJH and RH is as it was. The
# app container is never restarted while the ETL runs (the ETL runs inside it). Log:
# /opt/rayd/logs/sjh_move_<date>_<time>.log
set -uo pipefail

MODE="${1:-check}"
RAYD="${RAYD_DIR:-/opt/rayd}"
STAMP="$(date +%F_%H%M)"
LOG_DIR="$RAYD/logs"
BACKUP_DIR="$RAYD/backups"
LOCK_ID=872951065            # ETL_JOBS/etl_runner.py _ETL_SYNC_LOCK_ID: held while the ETL runs
SJH_DB=etl_db_sjh

mkdir -p "$LOG_DIR" "$BACKUP_DIR"
LOG="$LOG_DIR/sjh_move_$STAMP.log"
exec > >(tee -a "$LOG") 2>&1

say()  { echo "$(date '+%F %T') $*"; }
stop() { say "STOPPED: $*"; exit 1; }
sql()  { docker exec rayd_db psql -U etl_user -d "${2:-etl_db}" -v ON_ERROR_STOP=1 -At -c "$1"; }
show() { docker exec rayd_db psql -U etl_user -d "${2:-etl_db}" -v ON_ERROR_STOP=1 -c "$1"; }
size() { numfmt --to=iec "$1" 2>/dev/null || echo "$1 bytes"; }

case "$MODE" in check|copy|move) ;; *) stop "mode must be check, copy or move (got '$MODE')" ;; esac
if [ "$MODE" != check ] && [ "$(id -u)" != 0 ]; then stop "run as root (sudo)"; fi
cd "$RAYD" || stop "$RAYD not found"
if docker compose version >/dev/null 2>&1; then COMPOSE="docker compose"; else COMPOSE="docker-compose"; fi

say "SJH move, mode $MODE, log $LOG"

# One move at a time, and update.sh refuses to deploy while this file names a live process
# (2026-11-20: two copies were cut short by deploys that recreated the app container).
PIDFILE="$RAYD/.sjh_move.pid"
if [ "$MODE" != check ]; then
    if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE" 2>/dev/null)" 2>/dev/null; then
        stop "another SJH move is running (pid $(cat "$PIDFILE")); nothing done"
    fi
    echo $$ > "$PIDFILE"
    trap 'rm -f "$PIDFILE"' EXIT
fi
sql "SELECT 1" >/dev/null || stop "RH's database does not answer (rayd_db)"

# ── done already? ────────────────────────────────────────────────────────────
MOVED="$(sql "SELECT value FROM settings WHERE key = 'site_route_moved_SJH'")"
if [ -n "$MOVED" ]; then
    say "SJH has already been moved ($MOVED): nothing to do"
    exit 0
fi

# ── the ETL: never restart the app container under it ────────────────────────
etl_running() { [ "$(sql "SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND objid = $LOCK_ID AND objsubid = 1")" != 0 ]; }
# Before every step and every restart: the move takes the ETL's lock only while it copies or
# deletes, so the 05:00 ETL can start between steps (it skips its run while the lock is held).
wait_etl() {
    for i in $(seq 1 24); do
        etl_running || return 0
        say "the ETL is running; waiting 5 minutes"
        sleep 300
    done
    return 1
}
if [ "$MODE" = check ]; then
    if etl_running; then say "the ETL is running now"; else say "the ETL is not running"; fi
else
    wait_etl || stop "the ETL has been running for 2 hours; nothing done"
fi

# ── disk: the backup (~1/3 of the database, compressed) and SJH's copy (no image history) ──
DB_BYTES="$(sql "SELECT pg_database_size('etl_db')")"
FREE_BACKUP="$(df --output=avail -B1 "$BACKUP_DIR" | tail -1 | tr -d ' ')"
FREE_PGDATA="$(docker exec rayd_db df --output=avail -B1 /var/lib/postgresql/data | tail -1 | tr -d ' ')"
SAME_DISK=0
DOCKER_ROOT="$(docker info --format '{{.DockerRootDir}}' 2>/dev/null)"
if [ -n "$DOCKER_ROOT" ] && [ -d "$DOCKER_ROOT" ] && [ "$(stat -c %d "$BACKUP_DIR")" = "$(stat -c %d "$DOCKER_ROOT")" ]; then
    SAME_DISK=1
fi
NEED_BACKUP=$(( DB_BYTES * 4 / 10 ))
NEED_COPY=$(( DB_BYTES * 3 / 10 ))
say "etl_db $(size "$DB_BYTES"); free for the backup $(size "$FREE_BACKUP") (needs $(size "$NEED_BACKUP")),"\
" free for the databases $(size "$FREE_PGDATA") (needs $(size "$NEED_COPY")); same disk: $SAME_DISK"
DISK_OK=1
[ "$FREE_BACKUP" -gt "$NEED_BACKUP" ] || DISK_OK=0
[ "$FREE_PGDATA" -gt "$NEED_COPY" ] || DISK_OK=0
if [ "$SAME_DISK" = 1 ] && [ "$FREE_BACKUP" -le $(( NEED_BACKUP + NEED_COPY )) ]; then DISK_OK=0; fi
if [ "$DISK_OK" = 1 ]; then say "disk: enough room"; else say "disk: NOT enough room"; fi

# ── counts before, per site (rows; studies with no site stay on RH) ──────────
TOTALS="SELECT COALESCE(s.code, 'no site') AS site, COUNT(*) AS studies FROM etl_didb_studies d LEFT JOIN sites s ON s.id = d.site_id GROUP BY 1 ORDER BY 1"
MONTHS="SELECT COALESCE(s.code, 'no site') AS site, to_char(d.study_date, 'YYYY-MM') AS month, COUNT(*) AS studies FROM etl_didb_studies d LEFT JOIN sites s ON s.id = d.site_id WHERE d.study_date >= date_trunc('month', CURRENT_DATE) - interval '2 months' GROUP BY 1, 2 ORDER BY 2, 1"
count_site() { sql "SELECT COUNT(*) FROM etl_didb_studies d JOIN sites s ON s.id = d.site_id WHERE s.code = '$1'" "${2:-etl_db}"; }
say "RH's database now:"
show "$TOTALS"
show "$MONTHS"
RH_BEFORE="$(count_site RH)"
SJH_BEFORE="$(count_site SJH)"
NOSITE_BEFORE="$(sql "SELECT COUNT(*) FROM etl_didb_studies WHERE site_id IS NULL")"

if [ "$MODE" = check ]; then
    say "check done: nothing changed. On move day: sudo bash $RAYD/scripts/sjh_move.sh copy (or move)"
    [ "$DISK_OK" = 1 ] || exit 1
    exit 0
fi
[ "$DISK_OK" = 1 ] || stop "not enough disk (figures above); nothing done"

# ── 0. backup of RH's database (one from the last 48 hours is reused) ────────
DUMP="$(find "$BACKUP_DIR" -maxdepth 1 -name 'etl_db_before_sjh_move_*.dump' -mmin -2880 -size +1M 2>/dev/null | sort | tail -1)"
if [ -n "$DUMP" ]; then
    say "backup from the last 48 hours reused: $DUMP"
else
    DUMP="$BACKUP_DIR/etl_db_before_sjh_move_$STAMP.dump"
    say "backup of RH's database to $DUMP"
    docker exec rayd_db pg_dump -U etl_user -Fc etl_db > "$DUMP" || { rm -f "$DUMP"; stop "the backup failed; nothing done"; }
    [ "$(head -c 5 "$DUMP")" = PGDMP ] || stop "the backup is not a Postgres archive; nothing done"
    ENTRIES="$(docker exec -i rayd_db pg_restore -l < "$DUMP" 2>/dev/null | grep -c ';')"
    [ "${ENTRIES:-0}" -gt 10 ] || stop "the backup cannot be read back; nothing done"
    say "backup done: $(du -h "$DUMP" | cut -f1), $ENTRIES entries"
    FREE_PGDATA="$(docker exec rayd_db df --output=avail -B1 /var/lib/postgresql/data | tail -1 | tr -d ' ')"
    [ "$FREE_PGDATA" -gt "$NEED_COPY" ] || stop "after the backup only $(size "$FREE_PGDATA") free for SJH's copy; nothing else done"
fi

# ── 1. SJH's database switched on ─────────────────────────────────────────────
ENV_BACKUP=".env.before_sjh_move_$STAMP"
cp -p .env "$ENV_BACKUP" || stop "could not back up .env; nothing done"
switch_off() {
    say "switching SJH's database off again (.env restored); nothing deleted from RH"
    cp -p "$ENV_BACKUP" .env
    if wait_etl; then
        $COMPOSE up -d rayd-app
    else
        say "the ETL is still running: .env is restored, restart the app once it has finished:"\
" cd $RAYD && sudo $COMPOSE up -d rayd-app"
    fi
}
if grep -q '^POSTGRES_DB_SJH=' .env; then
    sed -i "s/^POSTGRES_DB_SJH=.*/POSTGRES_DB_SJH=$SJH_DB/" .env
else
    printf '\nPOSTGRES_DB_SJH=%s\n' "$SJH_DB" >> .env
fi
wait_etl || { cp -p "$ENV_BACKUP" .env; stop "the ETL has been running for 2 hours; .env restored, nothing done"; }
say "restarting the app with POSTGRES_DB_SJH=$SJH_DB"
$COMPOSE up -d rayd-app || { switch_off; stop "the app did not restart"; }
for i in $(seq 1 60); do
    docker logs rayd_service 2>&1 | grep -q "site SJH: database $SJH_DB ready" && break
    [ "$i" = 60 ] && { switch_off; stop "SJH's database not ready after 15 minutes (sudo docker logs rayd_service)"; }
    sleep 15
done
say "SJH's database is ready"

# In a one-off container of the app's image (same .env, same volumes), not inside the app's own
# container: a restart or a deploy of the app no longer kills the copy under way (2026-11-20).
ROUTE="$COMPOSE run --rm --no-deps -T rayd-app python -m ETL_JOBS.etl_site_route --site SJH"

# ── 2. dry run ────────────────────────────────────────────────────────────────
say "step 2, dry run"
wait_etl || { switch_off; stop "the ETL has been running for 2 hours"; }
$ROUTE --dry-run || { switch_off; stop "the dry run failed"; }

# ── 3. copy and verify ────────────────────────────────────────────────────────
say "step 3, copy and verify"
wait_etl || { switch_off; stop "the ETL has been running for 2 hours"; }
$ROUTE --copy || { switch_off; stop "the copy did not verify (table above); RH untouched"; }
SJH_COPIED="$(sql "SELECT COUNT(*) FROM etl_didb_studies" "$SJH_DB")"
say "SJH's database holds $SJH_COPIED studies (SJH's on RH: $SJH_BEFORE)"

if [ "$MODE" = copy ]; then
    say "DONE (copy). SJH's database is filled and on; RH's database is untouched (SJH's rows still"\
" there, hidden as before). Check SJH's reports (an account holding both sites: Report 22, the Super"\
" Report side by side), then: sudo bash $RAYD/scripts/sjh_move.sh move"
    exit 0
fi

# ── 4. the move: delete SJH's rows from RH once every table verified ─────────
say "step 4, move (about 182,000 studies in batches of 2,000; 1-2 hours)"
wait_etl || stop "the ETL has been running for 2 hours; the copy is in SJH's database, nothing deleted from RH;"\
" run 'sudo bash $RAYD/scripts/sjh_move.sh move' later"
$ROUTE --move || stop "the move stopped (above). Every row it deleted from RH is in SJH's database"\
" (verified before any delete); run 'sudo bash $RAYD/scripts/sjh_move.sh move' again to finish."\
" SJH's database stays on."

# ── 5. VACUUM ANALYZE, counts after ───────────────────────────────────────────
say "step 5, VACUUM ANALYZE on RH's database"
sql "VACUUM ANALYZE" || say "VACUUM ANALYZE failed (not critical; run it by hand)"
RH_AFTER="$(count_site RH)"
SJH_LEFT="$(count_site SJH)"
NOSITE_AFTER="$(sql "SELECT COUNT(*) FROM etl_didb_studies WHERE site_id IS NULL")"
SJH_DB_AFTER="$(sql "SELECT COUNT(*) FROM etl_didb_studies" "$SJH_DB")"
say "RH's database after:"
show "$TOTALS"
say "SJH's database after:"
show "$TOTALS" "$SJH_DB"
show "$MONTHS" "$SJH_DB"
OK=1
[ "$RH_AFTER" = "$RH_BEFORE" ] || { OK=0; say "CHECK: RH studies $RH_AFTER, were $RH_BEFORE"; }
[ "$SJH_LEFT" = 0 ] || { OK=0; say "CHECK: $SJH_LEFT SJH studies still on RH"; }
[ "$NOSITE_AFTER" = "$NOSITE_BEFORE" ] || { OK=0; say "CHECK: no-site studies $NOSITE_AFTER, were $NOSITE_BEFORE"; }
[ "$SJH_DB_AFTER" -ge "$SJH_BEFORE" ] || { OK=0; say "CHECK: SJH's database $SJH_DB_AFTER studies, SJH had $SJH_BEFORE"; }
if [ "$OK" = 1 ]; then
    say "DONE (move). RH $RH_AFTER studies (unchanged), no site $NOSITE_AFTER (unchanged), SJH's database"\
" $SJH_DB_AFTER. From the next 05:00 ETL, SJH's new rows go to SJH's database; HIS orders naming SJH go"\
" there from now. Next: SJH's mail server (pick SJH, Administration > Email Server)."
else
    say "DONE (move) with the CHECK lines above to look at; backup: $DUMP"
fi
