#!/usr/bin/env bash
# install.sh — RAYD full deployment setup
# Run as: sudo bash install.sh   (from the StatsApp directory)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# Run SQL against the Postgres container
pg_exec() {
    docker exec rayd_db psql -U "$PG_USER" -d "$PG_DB" -c "$1" -q
}

echo ""
echo "=================================================="
echo "        RAYD — Production Install Script"
echo "=================================================="
echo ""

# ──────────────────────────────────────────────────────
# STEP 1: Check prerequisites
# ──────────────────────────────────────────────────────
info "Step 1/7 — Checking prerequisites..."

command -v docker   >/dev/null 2>&1 || error "Docker is not installed. Install it from https://docs.docker.com/engine/install/"
command -v openssl  >/dev/null 2>&1 || error "'openssl' is required. Run: apt-get install -y openssl"

if docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE="docker-compose"
else
    error "Docker Compose is not installed. Run: apt-get install -y docker-compose-plugin"
fi

ok "Docker found: $(docker --version)"
ok "Docker Compose found: $($COMPOSE version)"

# ──────────────────────────────────────────────────────
# STEP 2: Oracle Instant Client 21.13
# ──────────────────────────────────────────────────────
info "Step 2/7 — Checking Oracle Instant Client..."

ORACLE_DIR="/opt/oracle/instantclient_21_13"
ORACLE_ZIP="instantclient-basiclite-linux.x64-21.13.0.0.0dbru.zip"
ORACLE_URL="https://download.oracle.com/otn_software/linux/instantclient/2113000/${ORACLE_ZIP}"

if [ -d "$ORACLE_DIR" ]; then
    ok "Oracle Instant Client 21.13 already installed at $ORACLE_DIR"
else
    warn "Oracle Instant Client not found. Installing..."
    apt-get update -qq && apt-get install -y -qq libaio1t64 libaio-dev unzip wget

    mkdir -p /opt/oracle
    cd /opt/oracle

    if [ ! -f "$ORACLE_ZIP" ]; then
        info "Downloading Oracle Instant Client..."
        wget -q "$ORACLE_URL" -O "$ORACLE_ZIP" || error "Download failed. Download manually from https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html and place at /opt/oracle/${ORACLE_ZIP}"
    fi

    unzip -q "$ORACLE_ZIP" -d /opt/oracle
    cd "$SCRIPT_DIR"

    echo "$ORACLE_DIR" | tee /etc/ld.so.conf.d/oracle-instantclient.conf
    ldconfig
    ok "Oracle Instant Client 21.13 installed."
fi

# ──────────────────────────────────────────────────────
# STEP 3: Environment file
# ──────────────────────────────────────────────────────
info "Step 3/7 — Setting up .env..."

if [ -f ".env" ]; then
    ok ".env already exists — skipping. Edit it manually if needed."
else
    cp .env.example .env

    echo ""
    echo "  Please enter the required configuration values."
    echo "  Press ENTER to keep the default shown in brackets."
    echo ""

    prompt_val() {
        local key="$1" prompt="$2" default="$3" val
        read -r -p "  ${prompt} [${default}]: " val
        val="${val:-$default}"
        sed -i "s|^${key}=.*|${key}=${val}|" .env
    }

    RANDOM_SECRET=$(openssl rand -hex 32)
    prompt_val "SECRET_KEY"         "Flask SECRET_KEY"                  "$RANDOM_SECRET"
    prompt_val "POSTGRES_PASSWORD"  "PostgreSQL password"               "$(openssl rand -hex 16)"
    prompt_val "MASTER_ADMIN_KEY"   "Master admin registration key"     "$(openssl rand -hex 12)"
    prompt_val "TZ"                 "Timezone (e.g. Asia/Beirut)"       "Asia/Beirut"
    prompt_val "BITNET_ENABLED"     "Enable BitNet AI? (true/false)"    "true"

    ok ".env created."
fi

# Load .env so we can use the values in later steps
set -a; source .env; set +a
PG_USER="${POSTGRES_USER:-etl_user}"
PG_DB="${POSTGRES_DB:-etl_db}"

# ──────────────────────────────────────────────────────
# STEP 4: SSL certificates (self-signed)
# ──────────────────────────────────────────────────────
info "Step 4/7 — Checking SSL certificates..."

CERT_DIR="./nginx/certs"
mkdir -p "$CERT_DIR"

if [ -f "${CERT_DIR}/fullchain.pem" ] && [ -f "${CERT_DIR}/privkey.pem" ]; then
    ok "SSL certificates already exist — skipping."
else
    info "Generating self-signed SSL certificate (valid 10 years)..."

    read -r -p "  Server hostname or IP for the certificate [$(hostname)]: " CERT_CN
    CERT_CN="${CERT_CN:-$(hostname)}"

    openssl req -x509 -nodes -newkey rsa:2048 \
        -keyout "${CERT_DIR}/privkey.pem" \
        -out    "${CERT_DIR}/fullchain.pem" \
        -days   3650 \
        -subj   "/CN=${CERT_CN}/O=RAYD/OU=IT" \
        -addext "subjectAltName=DNS:${CERT_CN},IP:127.0.0.1" \
        2>/dev/null

    chmod 600 "${CERT_DIR}/privkey.pem"
    ok "Self-signed certificate generated for '${CERT_CN}'."
fi

# ──────────────────────────────────────────────────────
# STEP 5: Build and start containers
# ──────────────────────────────────────────────────────
info "Step 5/7 — Building and starting Docker containers..."

$COMPOSE down --remove-orphans 2>/dev/null || true
$COMPOSE build --no-cache
$COMPOSE up -d

# Wait for Postgres to be healthy (up to 90s)
info "Waiting for database to be ready..."
WAIT=0
until docker exec rayd_db pg_isready -U "$PG_USER" -d "$PG_DB" -q 2>/dev/null || [ $WAIT -ge 90 ]; do
    sleep 3; WAIT=$((WAIT+3))
done
docker exec rayd_db pg_isready -U "$PG_USER" -d "$PG_DB" -q 2>/dev/null || error "Database did not become ready in time. Check: $COMPOSE logs db"

# Wait for rayd-app to be up
WAIT=0
until $COMPOSE ps | grep rayd_service | grep -q "Up" || [ $WAIT -ge 60 ]; do
    sleep 3; WAIT=$((WAIT+3))
done
$COMPOSE ps | grep rayd_service | grep -q "Up" || error "rayd_service failed to start. Check logs: $COMPOSE logs rayd-app"

ok "All containers are running."

# ──────────────────────────────────────────────────────
# STEP 6: Database configuration
# ──────────────────────────────────────────────────────
info "Step 6/7 — Configuring database..."

# ── 6a. Truncate all ETL tables ───────────────────────
echo ""
info "Truncating all ETL tables (RESTART IDENTITY CASCADE)..."

pg_exec "
DO \$\$
DECLARE
    t text;
BEGIN
    FOR t IN
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public' AND tablename LIKE 'etl_%'
        ORDER BY tablename
    LOOP
        EXECUTE 'TRUNCATE TABLE public.' || quote_ident(t) || ' RESTART IDENTITY CASCADE';
        RAISE NOTICE 'Truncated: %', t;
    END LOOP;
END \$\$;
"
ok "ETL tables truncated."

# ── 6b. PostgreSQL credentials ────────────────────────
echo ""
echo "  ── PostgreSQL Credentials ──────────────────────"
echo "  (Stored in .env — used by the app and the db container)"
echo "  Press ENTER to keep current values."
echo ""

# Read current values from .env as defaults
CUR_PG_USER="${POSTGRES_USER:-etl_user}"
CUR_PG_DB="${POSTGRES_DB:-etl_db}"
CUR_PG_PASSWORD="${POSTGRES_PASSWORD:-}"

read -r -p "  PostgreSQL username [${CUR_PG_USER}]: " NEW_PG_USER
NEW_PG_USER="${NEW_PG_USER:-$CUR_PG_USER}"

read -r -p "  PostgreSQL database name [${CUR_PG_DB}]: " NEW_PG_DB
NEW_PG_DB="${NEW_PG_DB:-$CUR_PG_DB}"

read -r -s -p "  PostgreSQL password [keep current]: " NEW_PG_PASSWORD; echo ""
NEW_PG_PASSWORD="${NEW_PG_PASSWORD:-$CUR_PG_PASSWORD}"
while [ -z "$NEW_PG_PASSWORD" ]; do
    read -r -s -p "  PostgreSQL password (required): " NEW_PG_PASSWORD; echo ""
done

# Update .env with new PG values
sed -i "s|^POSTGRES_USER=.*|POSTGRES_USER=${NEW_PG_USER}|"     .env
sed -i "s|^POSTGRES_DB=.*|POSTGRES_DB=${NEW_PG_DB}|"           .env
sed -i "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=${NEW_PG_PASSWORD}|" .env

# Reload for pg_exec to pick up updated values
PG_USER="$NEW_PG_USER"
PG_DB="$NEW_PG_DB"

# If credentials changed, restart containers to apply them
if [[ "$NEW_PG_USER" != "$CUR_PG_USER" || "$NEW_PG_DB" != "$CUR_PG_DB" || "$NEW_PG_PASSWORD" != "$CUR_PG_PASSWORD" ]]; then
    info "Restarting containers to apply new PostgreSQL credentials..."
    $COMPOSE down --remove-orphans
    $COMPOSE up -d
    WAIT=0
    until docker exec rayd_db pg_isready -U "$PG_USER" -d "$PG_DB" -q 2>/dev/null || [ $WAIT -ge 90 ]; do
        sleep 3; WAIT=$((WAIT+3))
    done
    docker exec rayd_db pg_isready -U "$PG_USER" -d "$PG_DB" -q 2>/dev/null || error "Database did not become ready after restart."
fi

ok "PostgreSQL credentials saved (${NEW_PG_USER}@${NEW_PG_DB})."

# ── 6c. PACS Oracle connection ────────────────────────
echo ""
echo "  ── PACS Oracle Connection ──────────────────────"
echo "  (Stored in db_params table as 'oracle_ris')"
echo ""

read -r -p "  Oracle PACS host IP or hostname: " ORACLE_HOST
while [ -z "$ORACLE_HOST" ]; do
    read -r -p "  Oracle PACS host IP or hostname (required): " ORACLE_HOST
done

read -r -p "  Oracle port [1521]: " ORACLE_PORT
ORACLE_PORT="${ORACLE_PORT:-1521}"

read -r -p "  Oracle SID: " ORACLE_SID
while [ -z "$ORACLE_SID" ]; do
    read -r -p "  Oracle SID (required): " ORACLE_SID
done

read -r -p "  Oracle username: " ORACLE_USER
while [ -z "$ORACLE_USER" ]; do
    read -r -p "  Oracle username (required): " ORACLE_USER
done

read -r -s -p "  Oracle password: " ORACLE_PASS; echo ""
while [ -z "$ORACLE_PASS" ]; do
    read -r -s -p "  Oracle password (required): " ORACLE_PASS; echo ""
done

pg_exec "
INSERT INTO db_params (name, db_role, db_type, host, port, sid, username, password, mode)
VALUES (
    'oracle_ris',
    'source',
    'oracle',
    '${ORACLE_HOST}',
    ${ORACLE_PORT},
    '${ORACLE_SID}',
    '${ORACLE_USER}',
    '${ORACLE_PASS}',
    ''
)
ON CONFLICT (name) DO UPDATE SET
    host     = EXCLUDED.host,
    port     = EXCLUDED.port,
    sid      = EXCLUDED.sid,
    username = EXCLUDED.username,
    password = EXCLUDED.password;
"
ok "Oracle PACS connection saved (${ORACLE_USER}@${ORACLE_HOST}:${ORACLE_PORT}/${ORACLE_SID})."

# ── 6d. Demo mode / Go-live date ──────────────────────
echo ""
echo "  ── Deployment Type ─────────────────────────────"
read -r -p "  Is this a demo installation? (y/N): " IS_DEMO
IS_DEMO="${IS_DEMO,,}"  # lowercase

if [[ "$IS_DEMO" == "y" || "$IS_DEMO" == "yes" ]]; then

    read -r -p "  Demo start date (YYYY-MM-DD): " DEMO_START
    while ! [[ "$DEMO_START" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; do
        read -r -p "  Invalid format. Demo start date (YYYY-MM-DD): " DEMO_START
    done

    read -r -p "  Demo end date (YYYY-MM-DD): " DEMO_END
    while ! [[ "$DEMO_END" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; do
        read -r -p "  Invalid format. Demo end date (YYYY-MM-DD): " DEMO_END
    done

    read -r -p "  Demo username (the account that gets full access): " DEMO_USER
    while [ -z "$DEMO_USER" ]; do
        read -r -p "  Demo username (required): " DEMO_USER
    done

    pg_exec "
INSERT INTO settings (key, value) VALUES
    ('demo_mode',  'true'),
    ('demo_start', '${DEMO_START}'),
    ('demo_end',   '${DEMO_END}'),
    ('demo_user',  '${DEMO_USER}')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
"
    ok "Demo mode activated (${DEMO_START} → ${DEMO_END}, user: ${DEMO_USER})."

else
    pg_exec "
INSERT INTO settings (key, value) VALUES ('demo_mode', 'false')
ON CONFLICT (key) DO UPDATE SET value = 'false';
"
    ok "Demo mode is OFF."

    # Ask for go-live date — ETL pulls data starting from this date
    echo ""
    read -r -p "  Go-live date for ETL (YYYY-MM-DD) — ETL will pull data from this date onwards: " GO_LIVE
    while ! [[ "$GO_LIVE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; do
        read -r -p "  Invalid format. Go-live date (YYYY-MM-DD): " GO_LIVE
    done

    pg_exec "
TRUNCATE go_live_config RESTART IDENTITY;
INSERT INTO go_live_config (go_live_date) VALUES ('${GO_LIVE}');
"
    ok "Go-live date set to ${GO_LIVE}."
fi

# ──────────────────────────────────────────────────────
# STEP 7: llama-server systemd service (optional)
# ──────────────────────────────────────────────────────
info "Step 7/7 — BitNet AI / llama-server setup..."

BITNET_BIN="/home/stats/BitNet/build/bin/llama-server"
BITNET_MODEL="/home/stats/BitNet/models/Meta-Llama-3.1-8B-Instruct-Q4_K_M.gguf"

# Stop any existing instance cleanly before reinstalling
if systemctl is-active --quiet llama-server 2>/dev/null; then
    info "Stopping existing llama-server service..."
    systemctl stop llama-server
fi

if [ ! -f "$BITNET_BIN" ]; then
    warn "llama-server binary not found at $BITNET_BIN"
    warn "Run the BitNet build first (setup_bitnet.sh), then re-run this script."
elif [ ! -f "$BITNET_MODEL" ]; then
    warn "Model file not found at $BITNET_MODEL"
    warn "Download the model first, then re-run this script."
else
    info "Installing llama-server systemd service..."
    cp "${SCRIPT_DIR}/llama-server.service" /etc/systemd/system/llama-server.service
    systemctl daemon-reload
    systemctl enable llama-server
    systemctl start llama-server

    # Give it a moment and verify it came up
    sleep 3
    if systemctl is-active --quiet llama-server; then
        ok "llama-server is running on port 8081."
    else
        warn "llama-server failed to start. Check: journalctl -u llama-server -n 50"
    fi
fi

# ──────────────────────────────────────────────────────
# DONE
# ──────────────────────────────────────────────────────
echo ""
echo "=================================================="
echo -e "${GREEN}  RAYD installation complete!${NC}"
echo "=================================================="
echo ""
echo "  App:      https://$(hostname)"
echo "  Logs:     $COMPOSE logs -f"
echo "  Restart:  $COMPOSE restart"
echo "  Stop:     $COMPOSE down"
echo ""
echo "  To run a manual ETL sync:"
echo "    $COMPOSE exec rayd-app python app.py -m"
echo ""
