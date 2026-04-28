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
    apt-get update -qq
    apt-get install -y -qq libaio-dev unzip wget
    apt-get install -y -qq libaio1t64 2>/dev/null || apt-get install -y -qq libaio1 2>/dev/null || warn "libaio not found — Oracle client may not work. Install libaio1 or libaio1t64 manually."

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
        if grep -q "^${key}=" .env; then
            sed -i "s|^${key}=.*|${key}=${val}|" .env
        else
            echo "${key}=${val}" >> .env
        fi
    }

    RANDOM_SECRET=$(openssl rand -hex 32)
    prompt_val "SECRET_KEY"         "Flask SECRET_KEY"                  "$RANDOM_SECRET"
    prompt_val "POSTGRES_PASSWORD"  "PostgreSQL password"               "$(openssl rand -hex 16)"
    prompt_val "MASTER_ADMIN_KEY"   "Master admin registration key"     "$(openssl rand -hex 12)"
    prompt_val "TZ"                 "Timezone (e.g. Asia/Beirut)"       "Asia/Beirut"
    prompt_val "BITNET_ENABLED"     "Enable Qwen AI assistant? (true/false)"    "true"

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

# Encrypt Oracle password using the app's SECRET_KEY (runs inside container where Flask is installed)
set -a; source .env; set +a
ENCRYPTED_ORACLE_PASS=$(docker exec rayd_service python3 -c "
import sys, os
os.environ['SECRET_KEY'] = '${SECRET_KEY}'
sys.path.insert(0, '/app')
from utils.crypto import encrypt
print(encrypt('${ORACLE_PASS}'))
")

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
    '${ENCRYPTED_ORACLE_PASS}',
    ''
)
ON CONFLICT (name) DO UPDATE SET
    host     = EXCLUDED.host,
    port     = EXCLUDED.port,
    sid      = EXCLUDED.sid,
    username = EXCLUDED.username,
    password = EXCLUDED.password;
"
ok "Oracle PACS connection saved (${ORACLE_USER}@${ORACLE_HOST}:${ORACLE_PORT}/${ORACLE_SID}) — password encrypted."

# ── 6d. License Tier ─────────────────────────────────
echo ""
echo "  ── License Tier ──────────────────────────────────"
echo "  1) Basic        — Report 22 only, 5 users, 2 sessions, 5k row cap"
echo "  2) Professional — All reports, unlimited users, no AI/portal"
echo "  3) Enterprise   — Everything enabled, no limits"
echo "  4) Custom       — Start from a tier and edit the JSON"
echo ""
read -r -p "  Select license tier [1-4] (default: 3): " TIER_CHOICE
TIER_CHOICE="${TIER_CHOICE:-3}"

case "$TIER_CHOICE" in
    1) TIER_KEY="basic" ;;
    2) TIER_KEY="professional" ;;
    4) TIER_KEY="custom" ;;
    *) TIER_KEY="enterprise" ;;
esac

# Build the license JSON — inlined to avoid requiring Flask on the host
ALL_REPORTS='[22,23,25,27,29]'
case "$TIER_KEY" in
    basic)
        LICENSE_JSON='{"tier":"basic","reports":[22],"ai_report":false,"capacity_ladder":false,"er_dashboard":false,"patient_portal":false,"live_feed":true,"hl7_orders":true,"oru_analytics":false,"saved_reports":false,"bitnet_ai":false,"export":false,"adapter_mapper":false,"super_report":false,"referring_intel":false,"max_users":5,"max_sessions":2,"expires":"","max_studies_per_report":5000}'
        ;;
    professional)
        LICENSE_JSON="{\"tier\":\"professional\",\"reports\":${ALL_REPORTS},\"ai_report\":false,\"capacity_ladder\":true,\"er_dashboard\":true,\"patient_portal\":false,\"live_feed\":true,\"hl7_orders\":true,\"oru_analytics\":true,\"saved_reports\":true,\"bitnet_ai\":false,\"export\":true,\"adapter_mapper\":false,\"super_report\":true,\"referring_intel\":false,\"max_users\":0,\"max_sessions\":0,\"expires\":\"\",\"max_studies_per_report\":0}"
        ;;
    *)  # enterprise and custom both start from enterprise
        LICENSE_JSON="{\"tier\":\"enterprise\",\"reports\":${ALL_REPORTS},\"ai_report\":true,\"capacity_ladder\":true,\"er_dashboard\":true,\"patient_portal\":true,\"live_feed\":true,\"hl7_orders\":true,\"oru_analytics\":true,\"saved_reports\":true,\"bitnet_ai\":true,\"export\":true,\"adapter_mapper\":true,\"super_report\":true,\"referring_intel\":true,\"max_users\":0,\"max_sessions\":0,\"expires\":\"\",\"max_studies_per_report\":0}"
        ;;
esac

if [[ "$TIER_KEY" == "custom" ]]; then
    echo ""
    echo "  Starting from enterprise tier. Edit the JSON below."
    echo "  Current license JSON:"
    echo "  $LICENSE_JSON" | docker exec -i rayd_service python3 -m json.tool 2>/dev/null || echo "  $LICENSE_JSON"
    echo ""

    # Helper: update a JSON field via the container's python3
    json_set() { echo "$1" | docker exec -i rayd_service python3 -c "import sys,json; d=json.load(sys.stdin); d['$2']=$3; print(json.dumps(d))"; }

    read -r -p "  Licensed report IDs (comma-separated, e.g. 22,23,25,27,29): " CUSTOM_REPORTS
    if [ -n "$CUSTOM_REPORTS" ]; then
        REPORTS_LIST="[$(echo "$CUSTOM_REPORTS" | tr ',' '\n' | grep -E '^[0-9]+$' | tr '\n' ',' | sed 's/,$//')]"
        LICENSE_JSON=$(echo "$LICENSE_JSON" | docker exec -i rayd_service python3 -c "
import sys, json
d = json.load(sys.stdin)
d['reports'] = [int(x.strip()) for x in '${CUSTOM_REPORTS}'.split(',') if x.strip().isdigit()]
d['tier'] = 'custom'
print(json.dumps(d))
")
    fi

    read -r -p "  Max users (0 = unlimited): " CUSTOM_MAX_USERS
    [[ -n "$CUSTOM_MAX_USERS" ]] && LICENSE_JSON=$(json_set "$LICENSE_JSON" max_users "${CUSTOM_MAX_USERS:-0}")

    read -r -p "  Max concurrent sessions (0 = unlimited): " CUSTOM_MAX_SESS
    [[ -n "$CUSTOM_MAX_SESS" ]] && LICENSE_JSON=$(json_set "$LICENSE_JSON" max_sessions "${CUSTOM_MAX_SESS:-0}")

    read -r -p "  Expiry date (YYYY-MM-DD, blank = never): " CUSTOM_EXPIRY
    [[ -n "$CUSTOM_EXPIRY" ]] && LICENSE_JSON=$(json_set "$LICENSE_JSON" expires "\"${CUSTOM_EXPIRY}\"")

    read -r -p "  Max studies per report (0 = unlimited): " CUSTOM_STUDY_CAP
    [[ -n "$CUSTOM_STUDY_CAP" ]] && LICENSE_JSON=$(json_set "$LICENSE_JSON" max_studies_per_report "${CUSTOM_STUDY_CAP:-0}")

    # Toggle individual features
    for feat in ai_report capacity_ladder er_dashboard patient_portal live_feed hl7_orders oru_analytics saved_reports bitnet_ai export adapter_mapper super_report referring_intel; do
        CURRENT=$(echo "$LICENSE_JSON" | docker exec -i rayd_service python3 -c "import sys,json; print(json.load(sys.stdin).get('$feat', False))")
        read -r -p "  Enable $feat? (current: $CURRENT) [y/n/Enter=keep]: " TOGGLE
        if [[ "${TOGGLE,,}" == "y" ]]; then
            LICENSE_JSON=$(json_set "$LICENSE_JSON" "$feat" True)
        elif [[ "${TOGGLE,,}" == "n" ]]; then
            LICENSE_JSON=$(json_set "$LICENSE_JSON" "$feat" False)
        fi
    done
fi

# Write license to settings table
pg_exec "
INSERT INTO settings (key, value) VALUES ('license', '${LICENSE_JSON}')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
"

echo ""
echo "  Final license:"
echo "  $LICENSE_JSON" | python3 -m json.tool 2>/dev/null || echo "  $LICENSE_JSON"
ok "License tier '${TIER_KEY}' saved."

# ── 6e. Demo mode / Go-live date ──────────────────────
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
# STEP 7: Qwen2.5-7B AI assistant setup (optional)
# ──────────────────────────────────────────────────────
info "Step 7/7 — Qwen2.5-7B AI assistant setup..."

# Clean up legacy BitNet / llama-server installation
for old_svc in llama-server bitnet; do
    if systemctl is-active --quiet "$old_svc" 2>/dev/null; then
        info "Stopping legacy service: $old_svc"
        systemctl stop "$old_svc" 2>/dev/null || true
    fi
    if systemctl is-enabled --quiet "$old_svc" 2>/dev/null; then
        systemctl disable "$old_svc" 2>/dev/null || true
    fi
    if [ -f "/etc/systemd/system/${old_svc}.service" ]; then
        rm -f "/etc/systemd/system/${old_svc}.service"
        info "Removed legacy unit file: ${old_svc}.service"
    fi
done
systemctl daemon-reload 2>/dev/null || true

# Remove old Llama model to free disk space (~4.4 GB)
OLD_MODEL="/home/stats/BitNet/models/Meta-Llama-3.1-8B-Instruct-Q4_K_M.gguf"
if [ -f "$OLD_MODEL" ]; then
    info "Removing old Llama 3.1 model file..."
    rm -f "$OLD_MODEL"
    ok "Freed space from old model."
fi

# Remove old BitNet installation directory
if [ -d "/opt/bitnet" ]; then
    info "Removing /opt/bitnet..."
    rm -rf "/opt/bitnet"
    ok "Removed /opt/bitnet."
fi

SETUP_SCRIPT="${SCRIPT_DIR}/scripts/setup_qwen_prod.sh"

if [ "${BITNET_ENABLED:-true}" != "true" ]; then
    warn "Qwen AI assistant is disabled. Skipping model setup."
    warn "To enable later, set BITNET_ENABLED=true in .env and re-run this script."
elif [ ! -f "$SETUP_SCRIPT" ]; then
    warn "Setup script not found at $SETUP_SCRIPT — skipping Qwen setup."
else
    info "Running Qwen2.5-7B production setup (downloads ~4.4 GB model on first run)..."
    bash "$SETUP_SCRIPT"
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
