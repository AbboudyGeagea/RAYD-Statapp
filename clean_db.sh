#!/usr/bin/env bash
# clean_db.sh — Wipe ETL-derived data and rebuild it from the PACS source
#
# ┌─────────────────────────────────────────────────────────────┐
# │  USE THIS when:                                             │
# │  • Mappings were wrong and the stored values are now wrong  │
# │  • Orphan/duplicate rows exist that an upsert never clears  │
# │  • You want a clean rebuild before applying new mappings    │
# │                                                             │
# │  DO NOT USE to update code/migrations → use update.sh       │
# │  DO NOT USE for fresh installations   → use install.sh      │
# └─────────────────────────────────────────────────────────────┘
#
# Usage:
#   sudo bash clean_db.sh                 # current branch, wipe only
#   sudo bash clean_db.sh CHN             # CHN branch, wipe only
#   sudo bash clean_db.sh CHN --sync      # wipe then run the ETL immediately
#   sudo bash clean_db.sh CHN --with-procedures
#   sudo bash clean_db.sh CHN --yes       # skip the typed confirmation
#
# The ETL is NOT run by default, deliberately: the usual reason for wiping is to
# apply new mappings, and those have to go in BETWEEN the wipe and the rebuild.
# Pass --sync only when the mappings are already correct.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── Arguments ─────────────────────────────────────────────────────────────────
# First non-flag argument is the branch, same shape as update.sh. Everything
# else is a flag, so `clean_db.sh CHN --sync` and `clean_db.sh --sync` both work.
BRANCH=""
AUTO_YES=0
RUN_SYNC=0
WITH_PROCEDURES=0
NO_BACKUP=0

for arg in "$@"; do
    case "$arg" in
        --yes|-y)           AUTO_YES=1 ;;
        --sync)             RUN_SYNC=1 ;;
        --with-procedures)  WITH_PROCEDURES=1 ;;
        --no-backup)        NO_BACKUP=1 ;;
        -*)                 error "Unknown option: $arg" ;;
        *)                  [ -z "$BRANCH" ] && BRANCH="$arg" || error "Unexpected argument: $arg" ;;
    esac
done
BRANCH="${BRANCH:-$(git rev-parse --abbrev-ref HEAD)}"

# ── Fixed credentials (same as install.sh / update.sh) ────────────────────────
PG_USER="etl_user"
PG_DB="etl_db"

pg_exec()  { docker exec rayd_db psql -U "$PG_USER" -d "$PG_DB" -c "$1" -q; }
pg_query() { docker exec rayd_db psql -U "$PG_USER" -d "$PG_DB" -tAc "$1"; }

if docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE="docker-compose"
else
    error "Docker Compose not found."
fi

docker exec rayd_db pg_isready -U "$PG_USER" -d "$PG_DB" -q 2>/dev/null \
    || error "Database not reachable. Is the stack up?  $COMPOSE ps"

# ── Branch guard ──────────────────────────────────────────────────────────────
# The ETL code differs per site branch, so rebuilding under the wrong one would
# repopulate with another site's logic. Refuse rather than guess; switching code
# is update.sh's job, not this script's.
CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [ "$CURRENT_BRANCH" != "$BRANCH" ]; then
    error "Checked out '$CURRENT_BRANCH' but asked to clean '$BRANCH'.
        Run:  sudo bash update.sh $BRANCH
        then re-run this script."
fi

# ── What gets wiped ───────────────────────────────────────────────────────────
#
# Derived data. Every one of these is rebuilt from the PACS by the ETL, so
# losing them costs a sync, nothing more.
DATA_TABLES=(
    etl_didb_studies
    etl_patient_view
    etl_didb_serieses
    etl_didb_raw_images
    etl_image_locations
    etl_orders
    summary_storage_daily
)

# NOT derived, despite living next to the tables that are.
#
# aetitle_modality_map is CONFIGURATION with an auto-seed. ETL phase 8 re-creates
# only (aetitle, modality, daily_capacity_minutes=480) via ON CONFLICT DO NOTHING,
# so everything tuned on top is gone: room_name, description, display_aetitle,
# exclude_from_stats, and any capacity that is not 480. Its CASCADE also takes
# device_weekly_schedule (rebuilt flat at 480 x 7 days, Sundays included) and
# device_exceptions, which NOTHING rebuilds — those exist only via the admin UI.
#
# analytics_snapshots is point-in-time. The nightly job writes the CURRENT
# periods, so historical briefing narratives do not come back at all.
CONFIG_TABLES=(
    aetitle_modality_map
    analytics_snapshots
)

# Survives by default because procedure mappings are usually curated separately
# from device mappings. Opt in when the procedure attribution is part of what
# went wrong.
[ "$WITH_PROCEDURES" -eq 1 ] && CONFIG_TABLES+=(procedure_duration_map)

# Filter to what this branch's schema actually has — the site branches carry
# different tables, and TRUNCATE on a missing one aborts the whole statement.
WIPE=()
for t in "${DATA_TABLES[@]}" "${CONFIG_TABLES[@]}"; do
    if [ "$(pg_query "SELECT to_regclass('public.$t') IS NOT NULL;")" = "t" ]; then
        WIPE+=("$t")
    else
        warn "Skipping '$t' — not present on this schema."
    fi
done
[ ${#WIPE[@]} -eq 0 ] && error "Nothing to wipe — no matching tables found."

echo ""
echo "=================================================="
echo "        RAYD — Clean ETL Database"
echo "=================================================="
echo "  Branch : $BRANCH"
echo "  Site   : $(hostname)"
echo "  Tables : ${#WIPE[@]}"
echo ""

# ──────────────────────────────────────────────────────
# STEP 1: Backup everything the ETL cannot rebuild
# ──────────────────────────────────────────────────────
STAMP="$(date +%Y%m%d-%H%M%S)"
BACKUP="$SCRIPT_DIR/clean_db_backup_${BRANCH}_${STAMP}.sql"

if [ "$NO_BACKUP" -eq 1 ]; then
    warn "Step 1/4 — SKIPPED (--no-backup). Config loss will be unrecoverable."
else
    info "Step 1/4 — Backing up configuration that the ETL will not rebuild..."
    BACKUP_ARGS=()
    for t in aetitle_modality_map device_weekly_schedule device_exceptions \
             procedure_duration_map go_live_config analytics_snapshots; do
        [ "$(pg_query "SELECT to_regclass('public.$t') IS NOT NULL;")" = "t" ] \
            && BACKUP_ARGS+=(-t "$t")
    done

    [ ${#BACKUP_ARGS[@]} -eq 0 ] && error "No config tables found to back up — check the schema."

    docker exec rayd_db pg_dump -U "$PG_USER" -d "$PG_DB" \
        --data-only --column-inserts "${BACKUP_ARGS[@]}" > "$BACKUP" \
        || error "pg_dump failed — refusing to wipe without a backup."

    # A dump that "succeeded" but wrote nothing is the failure mode that matters:
    # it looks fine, and it is discovered only after the data is already gone.
    #
    # `|| true`, not `|| echo 0`: grep -c PRINTS the count and THEN exits 1 when
    # the count is zero, so the echo would append a second line and the numeric
    # test below would fail on "0\n0" instead of catching the empty backup.
    ROWS=$(grep -c "^INSERT INTO" "$BACKUP" || true)
    if [ "${ROWS:-0}" -eq 0 ]; then
        error "Backup contains 0 rows ($BACKUP) — refusing to wipe.
        Re-run with --no-backup only if the config really is empty."
    fi
    ok "Backed up $ROWS config row(s) → $(basename "$BACKUP")"
fi

# ──────────────────────────────────────────────────────
# STEP 2: Record the before-state
# ──────────────────────────────────────────────────────
# Wrong mappings show up as a wrong modality distribution, so capturing it here
# is what lets you prove afterwards that the rebuild actually fixed something
# rather than just moving the numbers around.
info "Step 2/4 — Recording current state..."
BEFORE="$SCRIPT_DIR/clean_db_before_${BRANCH}_${STAMP}.txt"
{
    echo "RAYD clean_db — before state — $(date) — branch $BRANCH — $(hostname)"
    echo ""
    docker exec rayd_db psql -U "$PG_USER" -d "$PG_DB" -P pager=off -c "
        SELECT COALESCE(m.modality, s.study_modality, 'UNMAPPED') AS modality,
               count(*) AS studies,
               min(s.study_date) AS oldest,
               max(s.study_date) AS newest
          FROM etl_didb_studies s
          LEFT JOIN aetitle_modality_map m ON m.aetitle = s.original_storing_ae
         WHERE COALESCE(m.modality, s.study_modality, '') != 'SR'
         GROUP BY 1 ORDER BY 2 DESC;"
    docker exec rayd_db psql -U "$PG_USER" -d "$PG_DB" -P pager=off -c "
        SELECT (SELECT count(*) FROM etl_didb_studies) AS studies,
               (SELECT count(*) FROM etl_orders)       AS orders,
               (SELECT count(*) FROM etl_patient_view) AS patients,
               (SELECT go_live_date FROM go_live_config LIMIT 1) AS go_live_date;"
} > "$BEFORE" 2>&1 || warn "Could not capture full before-state (continuing)."
ok "Before-state → $(basename "$BEFORE")"

# ──────────────────────────────────────────────────────
# STEP 3: Confirm, then wipe
# ──────────────────────────────────────────────────────
echo ""
warn "About to TRUNCATE on '$(hostname)' (branch $BRANCH):"
printf '         %s\n' "${WIPE[@]}"
echo ""
warn "These do NOT come back from a re-sync:"
warn "  • device_exceptions          — admin UI only, no ETL writer"
warn "  • device_weekly_schedule     — rebuilt flat at 480 min x 7 days"
warn "  • daily_capacity_minutes     — reset to 480 for every device"
warn "  • room_name / display_aetitle / exclude_from_stats — reset to NULL/default"
warn "  • analytics_snapshots        — historical briefings are point-in-time"
[ "$NO_BACKUP" -eq 0 ] && info "  Restore them with:  docker exec -i rayd_db psql -U $PG_USER -d $PG_DB < $(basename "$BACKUP")"
echo ""

if [ "$AUTO_YES" -eq 0 ]; then
    read -rp "Type the branch name '$BRANCH' to continue: " answer
    [ "$answer" = "$BRANCH" ] || error "Aborted — got '$answer'."
fi

info "Step 3/4 — Truncating ${#WIPE[@]} table(s)..."
TABLE_LIST=$(IFS=,; echo "${WIPE[*]}")
pg_exec "TRUNCATE TABLE ${TABLE_LIST} RESTART IDENTITY CASCADE" \
    || error "TRUNCATE failed — nothing was committed."
ok "Tables cleared."

# ──────────────────────────────────────────────────────
# STEP 4: Rebuild (opt-in)
# ──────────────────────────────────────────────────────
if [ "$RUN_SYNC" -eq 1 ]; then
    info "Step 4/4 — Running full ETL sync (this can take a while)..."
    $COMPOSE exec -T rayd-app python app.py -m || error "ETL sync failed — see: $COMPOSE logs rayd-app --tail 60"
    ok "ETL sync complete."

    info "After-state:"
    docker exec rayd_db psql -U "$PG_USER" -d "$PG_DB" -P pager=off -c "
        SELECT COALESCE(m.modality, s.study_modality, 'UNMAPPED') AS modality,
               count(*) AS studies
          FROM etl_didb_studies s
          LEFT JOIN aetitle_modality_map m ON m.aetitle = s.original_storing_ae
         WHERE COALESCE(m.modality, s.study_modality, '') != 'SR'
         GROUP BY 1 ORDER BY 2 DESC;" || true
else
    info "Step 4/4 — Skipped (no --sync). Tables are EMPTY until you rebuild."
fi

# ──────────────────────────────────────────────────────
# DONE
# ──────────────────────────────────────────────────────
echo ""
echo "=================================================="
echo -e "${GREEN}  Clean complete — branch $BRANCH${NC}"
echo "=================================================="
echo ""
[ "$NO_BACKUP" -eq 0 ] && echo "  Backup : $(basename "$BACKUP")"
echo "  Before : $(basename "$BEFORE")"
if [ "$RUN_SYNC" -eq 0 ]; then
    echo ""
    echo "  NEXT — the site has no data until you do this:"
    echo "    1. Apply the new mappings (migration, or the mapping screen)"
    echo "    2. sudo $COMPOSE exec rayd-app python app.py -m"
    echo "    3. Compare against $(basename "$BEFORE")"
fi
echo ""
