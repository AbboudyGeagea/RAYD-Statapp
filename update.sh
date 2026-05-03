#!/usr/bin/env bash
# update.sh — Safe in-place update for existing RAYD installations
#
# ┌─────────────────────────────────────────────────────────────┐
# │  USE THIS when:                                             │
# │  • Pulling new code/fixes to an existing live site          │
# │  • New database migrations need to be applied               │
# │  • requirements.txt changed (new Python packages)           │
# │                                                             │
# │  DO NOT USE for fresh installations → use install.sh        │
# │  DO NOT USE to reset ETL data       → use install.sh        │
# └─────────────────────────────────────────────────────────────┘
#
# Usage:
#   sudo bash update.sh              # pulls from main
#   sudo bash update.sh CHN          # pulls from CHN branch
#   sudo bash update.sh Mazloum      # pulls from Mazloum branch

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

BRANCH="${1:-main}"

# ── Fixed credentials (same as install.sh) ────────────────────────────────────
PG_USER="etl_user"
PG_DB="etl_db"

pg_exec() {
    docker exec rayd_db psql -U "$PG_USER" -d "$PG_DB" -c "$1" -q
}

if docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE="docker-compose"
else
    error "Docker Compose not found."
fi

echo ""
echo "=================================================="
echo "        RAYD — Update Script"
echo "=================================================="
echo "  Branch : $BRANCH"
echo "  Site   : $(hostname)"
echo ""

# ──────────────────────────────────────────────────────
# STEP 1: Pull latest code
# ──────────────────────────────────────────────────────
info "Step 1/3 — Pulling latest code from '$BRANCH'..."
git pull origin "$BRANCH" || error "git pull failed. Check remote URL and credentials."
ok "Code up to date with '$BRANCH'."

# ──────────────────────────────────────────────────────
# STEP 2: Build new image (old containers keep running)
# ──────────────────────────────────────────────────────
# Build BEFORE stopping anything. If the build fails, set -e aborts the
# script here and the currently-running containers are untouched — site
# stays up and no manual recovery is needed.
info "Step 2/3 — Building new image (site stays up during build)..."

REQ_HASH=$(md5sum requirements.txt 2>/dev/null | awk '{print $1}')
LAST_HASH=$(cat .last_req_hash 2>/dev/null || echo "")
[ "$REQ_HASH" != "$LAST_HASH" ] && info "requirements.txt changed — will reinstall Python packages."

$COMPOSE build       # exits script on failure thanks to set -e; old containers untouched
echo "$REQ_HASH" > .last_req_hash
ok "Build complete."

# Hot-swap to new image. `up -d` recreates only containers whose image
# changed; brief per-container restart only (no full down).
info "Swapping to new image..."
$COMPOSE up -d --remove-orphans

# Wait for DB to be healthy (app health check comes AFTER migrations)
info "Waiting for database..."
WAIT=0
until docker exec rayd_db pg_isready -U "$PG_USER" -d "$PG_DB" -q 2>/dev/null || [ $WAIT -ge 90 ]; do
    sleep 3; WAIT=$((WAIT+3))
done
docker exec rayd_db pg_isready -U "$PG_USER" -d "$PG_DB" -q 2>/dev/null || error "Database not ready. Check: $COMPOSE logs db"
ok "Database ready."

# ──────────────────────────────────────────────────────
# STEP 3: Apply pending migrations
# ──────────────────────────────────────────────────────
info "Step 3/3 — Applying pending migrations..."

# Ensure migration tracking table exists
pg_exec "
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename TEXT PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT now()
);" 2>/dev/null || true

APPLIED=0
SKIPPED=0
FAILED=0

for f in $(ls "$SCRIPT_DIR/migrations"/[0-9]*.sql 2>/dev/null | sort); do
    name=$(basename "$f")

    already=$(docker exec rayd_db psql -U "$PG_USER" -d "$PG_DB" -tAc \
        "SELECT COUNT(*) FROM schema_migrations WHERE filename='${name}';" 2>/dev/null || echo "0")

    if [ "${already:-0}" -gt "0" ]; then
        SKIPPED=$((SKIPPED+1))
        continue
    fi

    info "  Applying: $name"
    docker cp "$f" rayd_db:/tmp/rayd_migration.sql
    if docker exec rayd_db psql -U "$PG_USER" -d "$PG_DB" -f /tmp/rayd_migration.sql -q 2>/dev/null; then
        pg_exec "INSERT INTO schema_migrations (filename) VALUES ('${name}');" 2>/dev/null || true
        ok "  Applied : $name"
        APPLIED=$((APPLIED+1))
    else
        warn "  FAILED  : $name — check manually"
        FAILED=$((FAILED+1))
    fi
done

if [ $FAILED -gt 0 ]; then
    warn "Migrations: $APPLIED applied, $SKIPPED already done, $FAILED FAILED — review above."
else
    ok "Migrations: $APPLIED applied, $SKIPPED already up to date."
fi

# Restart app after migrations so it picks up any schema changes
if [ $APPLIED -gt 0 ]; then
    info "Restarting app service to apply schema changes..."
    $COMPOSE restart rayd-app
fi

# Health check — curl is installed in the container via Dockerfile
info "Waiting for app to respond..."
WAIT=0
until docker exec rayd_service curl -sf http://localhost:8080/ -o /dev/null 2>/dev/null || [ $WAIT -ge 90 ]; do
    sleep 3; WAIT=$((WAIT+3))
done
if ! docker exec rayd_service curl -sf http://localhost:8080/ -o /dev/null 2>/dev/null; then
    warn "App did not respond within 90 s — check logs: $COMPOSE logs rayd-app --tail 40"
else
    ok "App is responding."
fi

# ──────────────────────────────────────────────────────
# FINAL: Reload nginx so it picks up the new container IP.
# Runs last — after migrations and app restart — so the app
# is fully ready before nginx starts routing traffic to it.
# ──────────────────────────────────────────────────────
info "Reloading nginx..."
$COMPOSE exec -T nginx nginx -s reload 2>/dev/null && ok "nginx reloaded." || warn "nginx reload skipped (not running?)"

# ──────────────────────────────────────────────────────
# DONE
# ──────────────────────────────────────────────────────
echo ""
echo "=================================================="
echo -e "${GREEN}  RAYD update complete!${NC}"
echo "=================================================="
echo ""
echo "  Branch : $BRANCH  →  $(git log --oneline -1)"
echo "  Logs   : $COMPOSE logs -f"
echo "  Restart: $COMPOSE restart"
echo ""
