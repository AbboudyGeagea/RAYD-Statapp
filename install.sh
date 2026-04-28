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

# ── Fixed PostgreSQL credentials (never changes across deployments) ────────────
PG_USER="etl_user"
PG_DB="etl_db"
PG_PASSWORD="SecureCrynBabe"

# ── Fixed app secret key (must match the key used to encrypt Oracle password) ──
# WARNING: changing this will invalidate all encrypted DB passwords stored in db_params
FIXED_SECRET_KEY="c0f1a2b3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1"

# ── Fixed Oracle PACS credentials (only host changes per site) ────────────────
ORACLE_PORT=1521
ORACLE_SID="mst1"
ORACLE_USER="sys"
# Pre-encrypted with FIXED_SECRET_KEY above — update both together if password changes
ORACLE_PASS_ENCRYPTED="gAAAAABp7K-egdXV1gJMKghGHJ3Iji-v81_fe1hL3EZ5krpgHT4YThnzsXVYUQkG_LJxfY8utazmYqc5EWPww7gh8D7LaI61VQ=="

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

if [ -f ".env" ] && grep -q "^SECRET_KEY=.\+" .env; then
    ok ".env already exists with SECRET_KEY — skipping. Edit it manually if needed."
else
    [ -f ".env" ] && warn ".env exists but is missing SECRET_KEY — regenerating..."
    echo ""
    read -r -p "  Master admin registration key [auto-generate]: " MASTER_ADMIN_KEY
    MASTER_ADMIN_KEY="${MASTER_ADMIN_KEY:-$(openssl rand -hex 12)}"

    cat > .env <<EOF
SECRET_KEY=${FIXED_SECRET_KEY}
MASTER_ADMIN_KEY=${MASTER_ADMIN_KEY}

POSTGRES_USER=${PG_USER}
POSTGRES_PASSWORD=${PG_PASSWORD}
POSTGRES_HOST=db
POSTGRES_PORT=5432
POSTGRES_DB=${PG_DB}

TZ=Asia/Beirut
LIVE_FEED_ENABLED=true
BITNET_ENABLED=true
BITNET_SERVER=http://172.17.0.1:8081
BITNET_TOKENS=200
BITNET_TIMEOUT=120
EOF

    ok ".env created."
fi

# Load .env so later steps can read it
set -a; source .env; set +a

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
$COMPOSE build
$COMPOSE up -d

info "Waiting for database to be ready..."
WAIT=0
until docker exec rayd_db pg_isready -U "$PG_USER" -d "$PG_DB" -q 2>/dev/null || [ $WAIT -ge 90 ]; do
    sleep 3; WAIT=$((WAIT+3))
done
docker exec rayd_db pg_isready -U "$PG_USER" -d "$PG_DB" -q 2>/dev/null || error "Database did not become ready in time. Check: $COMPOSE logs db"

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

# ── 6a-2. Schema migrations ───────────────────────────
pg_exec "ALTER TABLE public.settings ALTER COLUMN key TYPE TEXT, ALTER COLUMN value TYPE TEXT;"
ok "settings table widened to TEXT."

# ── 6b. PACS Oracle connection ────────────────────────
echo ""
echo "  ── PACS Oracle Connection ──────────────────────"
echo "  (All credentials are fixed — only the host IP changes per site)"
echo ""

read -r -p "  Oracle PACS host IP or hostname: " ORACLE_HOST
while [ -z "$ORACLE_HOST" ]; do
    read -r -p "  Oracle PACS host IP or hostname (required): " ORACLE_HOST
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
    '${ORACLE_PASS_ENCRYPTED}',
    ''
)
ON CONFLICT (name) DO UPDATE SET
    host     = EXCLUDED.host,
    port     = EXCLUDED.port,
    sid      = EXCLUDED.sid,
    username = EXCLUDED.username,
    password = EXCLUDED.password;
"
ok "Oracle PACS connection saved (${ORACLE_USER}@${ORACLE_HOST}:${ORACLE_PORT}/${ORACLE_SID}) — password pre-encrypted."

# ── 6c. License Tier ─────────────────────────────────
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

# Tier presets inlined — no Flask/Python import needed on the host
_JSON_BASIC='{"tier":"basic","reports":[22],"ai_report":false,"capacity_ladder":false,"er_dashboard":false,"patient_portal":false,"live_feed":true,"hl7_orders":true,"oru_analytics":false,"saved_reports":false,"bitnet_ai":false,"export":false,"adapter_mapper":false,"max_users":5,"max_sessions":2,"expires":"","max_studies_per_report":5000}'
_JSON_PRO='{"tier":"professional","reports":[22,23,25,27,29],"ai_report":false,"capacity_ladder":true,"er_dashboard":true,"patient_portal":false,"live_feed":true,"hl7_orders":true,"oru_analytics":true,"saved_reports":true,"bitnet_ai":false,"export":true,"adapter_mapper":false,"max_users":0,"max_sessions":0,"expires":"","max_studies_per_report":0}'
_JSON_ENT='{"tier":"enterprise","reports":[22,23,25,27,29],"ai_report":true,"capacity_ladder":true,"er_dashboard":true,"patient_portal":true,"live_feed":true,"hl7_orders":true,"oru_analytics":true,"saved_reports":true,"bitnet_ai":true,"export":true,"adapter_mapper":true,"super_report":true,"referring_intel":true,"max_users":0,"max_sessions":0,"expires":"","max_studies_per_report":0}'

case "$TIER_KEY" in
    basic)        LICENSE_JSON="$_JSON_BASIC" ;;
    professional) LICENSE_JSON="$_JSON_PRO"   ;;
    *)            LICENSE_JSON="$_JSON_ENT"   ;;
esac

if [[ "$TIER_KEY" == "custom" ]]; then
    echo ""
    echo "  Starting from enterprise tier. Edit the JSON below."
    echo "  Current license JSON:"
    echo "  $LICENSE_JSON" | python3 -m json.tool 2>/dev/null || echo "  $LICENSE_JSON"
    echo ""

    read -r -p "  Licensed report IDs (comma-separated, e.g. 22,23,25,27,29): " CUSTOM_REPORTS
    if [ -n "$CUSTOM_REPORTS" ]; then
        LICENSE_JSON=$(echo "$LICENSE_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
d['reports'] = [int(x.strip()) for x in '${CUSTOM_REPORTS}'.split(',') if x.strip().isdigit()]
d['tier'] = 'custom'
print(json.dumps(d))
")
    fi

    read -r -p "  Max users (0 = unlimited): " CUSTOM_MAX_USERS
    if [ -n "$CUSTOM_MAX_USERS" ]; then
        LICENSE_JSON=$(echo "$LICENSE_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
d['max_users'] = int('${CUSTOM_MAX_USERS}') if '${CUSTOM_MAX_USERS}'.isdigit() else 0
print(json.dumps(d))
")
    fi

    read -r -p "  Max concurrent sessions (0 = unlimited): " CUSTOM_MAX_SESS
    if [ -n "$CUSTOM_MAX_SESS" ]; then
        LICENSE_JSON=$(echo "$LICENSE_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
d['max_sessions'] = int('${CUSTOM_MAX_SESS}') if '${CUSTOM_MAX_SESS}'.isdigit() else 0
print(json.dumps(d))
")
    fi

    read -r -p "  Expiry date (YYYY-MM-DD, blank = never): " CUSTOM_EXPIRY
    if [ -n "$CUSTOM_EXPIRY" ]; then
        LICENSE_JSON=$(echo "$LICENSE_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
d['expires'] = '${CUSTOM_EXPIRY}'
print(json.dumps(d))
")
    fi

    read -r -p "  Max studies per report (0 = unlimited): " CUSTOM_STUDY_CAP
    if [ -n "$CUSTOM_STUDY_CAP" ]; then
        LICENSE_JSON=$(echo "$LICENSE_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
d['max_studies_per_report'] = int('${CUSTOM_STUDY_CAP}') if '${CUSTOM_STUDY_CAP}'.isdigit() else 0
print(json.dumps(d))
")
    fi

    for feat in ai_report capacity_ladder er_dashboard patient_portal live_feed liveview hl7_orders oru_analytics saved_reports bitnet_ai export adapter_mapper; do
        CURRENT=$(echo "$LICENSE_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('$feat', False))")
        read -r -p "  Enable $feat? (current: $CURRENT) [y/n/Enter=keep]: " TOGGLE
        if [[ "${TOGGLE,,}" == "y" ]]; then
            LICENSE_JSON=$(echo "$LICENSE_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
d['$feat'] = True
print(json.dumps(d))
")
        elif [[ "${TOGGLE,,}" == "n" ]]; then
            LICENSE_JSON=$(echo "$LICENSE_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
d['$feat'] = False
print(json.dumps(d))
")
        fi
    done
fi

pg_exec "
INSERT INTO settings (key, value) VALUES ('license', '${LICENSE_JSON}')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
"

echo ""
echo "  Final license:"
echo "  $LICENSE_JSON" | python3 -m json.tool 2>/dev/null || echo "  $LICENSE_JSON"
ok "License tier '${TIER_KEY}' saved."

# ── 6d. Demo mode / Go-live date ──────────────────────
echo ""
echo "  ── Deployment Type ─────────────────────────────"
read -r -p "  Is this a demo installation? (y/N): " IS_DEMO
IS_DEMO="${IS_DEMO,,}"

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
