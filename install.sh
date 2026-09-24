#!/usr/bin/env bash
# install.sh — RAYD HL7 Deployment Setup
# Run as: sudo bash install.sh   (from the StatsApp directory)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── Fixed PostgreSQL credentials (internal use only) ──────────────────────────
PG_USER="etl_user"
PG_DB="etl_db"
PG_PASSWORD="SecureCrynBabe"

# ── Fixed app secret key ───────────────────────────────────────────────────────
# Required: utils/crypto.py uses it, and it signs the Flask session.
FIXED_SECRET_KEY="c0f1a2b3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1"

# Run SQL against the Postgres container
pg_exec() {
    docker exec rayd_db psql -U "$PG_USER" -d "$PG_DB" -c "$1" -q
}

echo ""
echo "=================================================="
echo "     RAYD HL7 — Installation & Deployment"
echo "=================================================="
echo ""

# ──────────────────────────────────────────────────────
# STEP 1: Check prerequisites
# ──────────────────────────────────────────────────────
info "Step 1/4 — Checking prerequisites..."

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

# ── BuildKit cache garbage collection ─────────────────────────────────────────
GC_KEEP="${RAYD_BUILD_CACHE_KEEP:-20GB}"
DAEMON_JSON="/etc/docker/daemon.json"

if [ "$(id -u)" -ne 0 ]; then
    warn "Not root — skipping BuildKit cache GC setup. Re-run as root, or see docs."
elif ! command -v python3 >/dev/null 2>&1; then
    warn "python3 not found — skipping BuildKit cache GC setup (JSON edit needs it)."
elif [ -f "$DAEMON_JSON" ] && grep -q '"builder"' "$DAEMON_JSON" 2>/dev/null; then
    ok "BuildKit cache GC already configured in $DAEMON_JSON — left as is."
else
    info "Configuring BuildKit cache GC (keep ${GC_KEEP})..."
    [ -f "$DAEMON_JSON" ] && cp -a "$DAEMON_JSON" "${DAEMON_JSON}.rayd.bak"
    if python3 - "$DAEMON_JSON" "$GC_KEEP" <<'PYGC'
import json, os, sys
path, keep = sys.argv[1], sys.argv[2]
try:
    with open(path) as fh:
        cfg = json.load(fh)
    if not isinstance(cfg, dict):
        raise ValueError("daemon.json is not a JSON object")
except FileNotFoundError:
    cfg = {}
except Exception as exc:
    sys.stderr.write(f"{exc}\n")
    sys.exit(1)
cfg.setdefault("builder", {})["gc"] = {"enabled": True, "defaultKeepStorage": keep}
os.makedirs(os.path.dirname(path), exist_ok=True)
with open(path, "w") as fh:
    json.dump(cfg, fh, indent=2)
    fh.write("\n")
PYGC
    then
        if systemctl restart docker 2>/dev/null; then
            ok "BuildKit cache GC enabled (keep ${GC_KEEP}); docker restarted."
        else
            warn "Wrote $DAEMON_JSON but could not restart docker. Run: systemctl restart docker"
        fi
    else
        warn "Could not update $DAEMON_JSON (left unchanged) — configure builder GC manually."
        [ -f "${DAEMON_JSON}.rayd.bak" ] && mv "${DAEMON_JSON}.rayd.bak" "$DAEMON_JSON"
    fi
fi

# ──────────────────────────────────────────────────────
# STEP 2: Environment file
# ──────────────────────────────────────────────────────
info "Step 2/4 — Setting up .env..."

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
EOF

    ok ".env created."
fi

# Load .env so later steps can read it
set -a; source .env; set +a

# ──────────────────────────────────────────────────────
# STEP 3: SSL certificates (private CA → trusted HTTPS)
# ──────────────────────────────────────────────────────
info "Step 3/4 — Setting up SSL certificates..."

CERT_DIR="./nginx/certs"
mkdir -p "$CERT_DIR"

read -r -p "  Server hostname users will type in the browser [$(hostname)]: " CERT_CN
CERT_CN="${CERT_CN:-$(hostname)}"

# Generate private CA (once per organization)
if [ ! -f "${CERT_DIR}/rayd-ca.key" ] || [ ! -f "${CERT_DIR}/rayd-ca.crt" ]; then
    info "Generating RAYD private Certificate Authority..."
    openssl genrsa -out "${CERT_DIR}/rayd-ca.key" 4096 2>/dev/null
    openssl req -x509 -new -nodes \
        -key  "${CERT_DIR}/rayd-ca.key" \
        -sha256 -days 3650 \
        -out  "${CERT_DIR}/rayd-ca.crt" \
        -subj "/CN=RAYD Internal CA/O=Intermedic/OU=IT" \
        -addext "basicConstraints=critical,CA:true" \
        -addext "keyUsage=critical,keyCertSign,cRLSign" \
        2>/dev/null
    chmod 600 "${CERT_DIR}/rayd-ca.key"
    ok "Private CA certificate created."
else
    ok "Private CA already exists — reusing it."
fi

# Generate server cert signed by our CA
REGEN_CERT=false
if [ ! -f "${CERT_DIR}/fullchain.pem" ] || [ ! -f "${CERT_DIR}/privkey.pem" ]; then
    REGEN_CERT=true
else
    EXISTING_CN=$(openssl x509 -noout -subject -in "${CERT_DIR}/fullchain.pem" 2>/dev/null | sed 's/.*CN\s*=\s*//' | cut -d',' -f1 | tr -d ' ')
    if [ "$EXISTING_CN" != "$CERT_CN" ]; then
        warn "Hostname changed ($EXISTING_CN → $CERT_CN) — regenerating server certificate."
        REGEN_CERT=true
    else
        ok "Server certificate already exists for '${CERT_CN}' — skipping."
    fi
fi

if [ "$REGEN_CERT" = true ]; then
    info "Generating server certificate for '${CERT_CN}'..."

    openssl genrsa -out "${CERT_DIR}/privkey.pem" 2048 2>/dev/null
    chmod 600 "${CERT_DIR}/privkey.pem"

    openssl req -new \
        -key  "${CERT_DIR}/privkey.pem" \
        -out  "${CERT_DIR}/server.csr" \
        -subj "/CN=${CERT_CN}/O=Intermedic/OU=IT" \
        2>/dev/null

    cat > "${CERT_DIR}/server-ext.cnf" <<SRVEXT
[ext]
subjectAltName     = DNS:${CERT_CN},IP:127.0.0.1
basicConstraints   = CA:false
keyUsage           = critical,digitalSignature,keyEncipherment
extendedKeyUsage   = serverAuth
SRVEXT

    openssl x509 -req \
        -in      "${CERT_DIR}/server.csr" \
        -CA      "${CERT_DIR}/rayd-ca.crt" \
        -CAkey   "${CERT_DIR}/rayd-ca.key" \
        -CAcreateserial \
        -out     "${CERT_DIR}/fullchain.pem" \
        -days    825 \
        -sha256  \
        -extfile "${CERT_DIR}/server-ext.cnf" \
        -extensions ext \
        2>/dev/null

    rm -f "${CERT_DIR}/server.csr" "${CERT_DIR}/server-ext.cnf" "${CERT_DIR}/rayd-ca.srl"
    ok "Server certificate signed by RAYD CA for '${CERT_CN}'."
fi

echo ""
echo "  ┌─────────────────────────────────────────────────────────┐"
echo "  │  CLIENT SETUP — install CA cert once on each PC         │"
echo "  │                                                          │"
echo "  │  CA file:  ${CERT_DIR}/rayd-ca.crt                      │"
echo "  │                                                          │"
echo "  │  Windows (run as Administrator):                         │"
echo "  │    certutil -addstore Root nginx/certs/rayd-ca.crt       │"
echo "  │  Or: double-click → Install → Trusted Root CAs           │"
echo "  │                                                          │"
echo "  │  Linux/Mac:                                              │"
echo "  │    See nginx/certs/README.md                             │"
echo "  └─────────────────────────────────────────────────────────┘"
echo ""

# ──────────────────────────────────────────────────────
# STEP 4: Build, start containers, and create users
# ──────────────────────────────────────────────────────
info "Step 4/4 — Building and starting Docker containers..."

$COMPOSE down --remove-orphans 2>/dev/null || true

# Only reinstall Python packages if requirements.txt changed since last build
REQ_HASH=$(md5sum requirements.txt 2>/dev/null | awk '{print $1}')
LAST_HASH=$(cat .last_req_hash 2>/dev/null || echo "")

if [ "$REQ_HASH" != "$LAST_HASH" ]; then
    info "requirements.txt changed — installing packages..."
    $COMPOSE build
    echo "$REQ_HASH" > .last_req_hash
else
    ok "Python packages unchanged — skipping pip install."
    $COMPOSE build  # rebuilds app code only; pip layer served from Docker cache
fi

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

# ── License Tier Configuration ────────────────────────────────────────────────
echo ""
echo "  ── License Tier ───────────────────────────────────────────────────────────"
echo "  1) Essential    — Viewer dashboard, all reports, user mgmt,"
echo "                    activity log, modality/procedure config, export"
echo "                    Unlimited users/sessions"
echo ""
echo "  2) Professional — Everything in Essential, plus:"
echo "                    HL7 orders, report intelligence, custom reports,"
echo "                    ER dashboard, capacity ladder, saved reports"
echo "                    Unlimited users/sessions"
echo ""
echo "  3) Enterprise   — Everything in Professional, plus:"
echo "                    Revenue intelligence, AI reports, live department view"
echo "                    Unlimited users/sessions"
echo ""
echo "  4) Custom       — Start from Enterprise and toggle features manually"
echo "  ─────────────────────────────────────────────────────────────────────────"
echo ""
read -r -p "  Select license tier [1-4] (default: 3): " TIER_CHOICE
TIER_CHOICE="${TIER_CHOICE:-3}"

case "$TIER_CHOICE" in
    1) TIER_KEY="essential" ;;
    2) TIER_KEY="professional" ;;
    4) TIER_KEY="custom" ;;
    *) TIER_KEY="enterprise" ;;
esac

# Tier presets inlined — no Flask/Python import needed on the host
_JSON_ESS='{"tier":"essential","reports":[22,23,25,27,29,30],"export":true,"adapter_mapper":true,"hl7_orders":false,"oru_analytics":false,"custom_reports":false,"er_dashboard":false,"capacity_ladder":false,"saved_reports":false,"super_report":false,"referring_intel":false,"financial":false,"live_feed":false,"ai_report":false,"max_users":0,"max_sessions":0,"expires":"","max_studies_per_report":0}'
_JSON_PRO='{"tier":"professional","reports":[22,23,25,27,29,30],"export":true,"adapter_mapper":true,"hl7_orders":true,"oru_analytics":true,"custom_reports":true,"er_dashboard":true,"capacity_ladder":true,"saved_reports":true,"super_report":true,"referring_intel":true,"financial":false,"live_feed":false,"ai_report":false,"max_users":0,"max_sessions":0,"expires":"","max_studies_per_report":0}'
_JSON_ENT='{"tier":"enterprise","reports":[22,23,25,27,29,30],"export":true,"adapter_mapper":true,"hl7_orders":true,"oru_analytics":true,"custom_reports":true,"er_dashboard":true,"capacity_ladder":true,"saved_reports":true,"super_report":true,"referring_intel":true,"financial":true,"live_feed":true,"ai_report":true,"max_users":0,"max_sessions":0,"expires":"","max_studies_per_report":0}'

case "$TIER_KEY" in
    essential)    LICENSE_JSON="$_JSON_ESS" ;;
    professional) LICENSE_JSON="$_JSON_PRO" ;;
    *)            LICENSE_JSON="$_JSON_ENT" ;;
esac

if [[ "$TIER_KEY" == "custom" ]]; then
    echo ""
    echo "  Starting from Enterprise tier. Edit the JSON below."
    echo "  Current license JSON:"
    echo "  $LICENSE_JSON" | python3 -m json.tool 2>/dev/null || echo "  $LICENSE_JSON"
    echo ""

    read -r -p "  Licensed report IDs (comma-separated, e.g. 22,23,25,27,29,30): " CUSTOM_REPORTS
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

    echo ""
    echo "  Professional features:"
    for feat in hl7_orders oru_analytics custom_reports er_dashboard capacity_ladder saved_reports referring_intel super_report; do
        CURRENT=$(echo "$LICENSE_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('$feat', False))")
        read -r -p "    Enable $feat? (current: $CURRENT) [y/n/Enter=keep]: " TOGGLE
        if [[ "${TOGGLE,,}" == "y" ]]; then
            LICENSE_JSON=$(echo "$LICENSE_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); d['$feat']=True; print(json.dumps(d))")
        elif [[ "${TOGGLE,,}" == "n" ]]; then
            LICENSE_JSON=$(echo "$LICENSE_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); d['$feat']=False; print(json.dumps(d))")
        fi
    done

    echo ""
    echo "  Enterprise features:"
    for feat in financial live_feed ai_report; do
        CURRENT=$(echo "$LICENSE_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('$feat', False))")
        read -r -p "    Enable $feat? (current: $CURRENT) [y/n/Enter=keep]: " TOGGLE
        if [[ "${TOGGLE,,}" == "y" ]]; then
            LICENSE_JSON=$(echo "$LICENSE_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); d['$feat']=True; print(json.dumps(d))")
        elif [[ "${TOGGLE,,}" == "n" ]]; then
            LICENSE_JSON=$(echo "$LICENSE_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); d['$feat']=False; print(json.dumps(d))")
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

# ── Initial user setup ────────────────────────────────────────────────────────
echo ""
echo "  ── Initial User Setup ──────────────────────────────"
echo ""

info "Creating Super User (SU) account..."

AXADMIN_HASH=$(python3 << 'PYHASH'
from werkzeug.security import generate_password_hash
print(generate_password_hash("axilum2000", method="pbkdf2:sha256"))
PYHASH
)

pg_exec "
INSERT INTO users (username, password_hash, role, email, status, must_change_password, created_at)
VALUES ('axadmin', '${AXADMIN_HASH}', 'su', 'admin@intermedic.com', 'active', true, NOW())
ON CONFLICT (username) DO NOTHING;
"
ok "Super User 'axadmin' created (must change password on first login)."

echo ""
echo "  ── Implementation Team Account ──────────────────────"
echo "  The Implementation team will use this account to configure:"
echo "    • HL7→DB mappings"
echo "    • Modalities and procedures"
echo "    • CSV imports"
echo "    • Backend system configuration"
echo ""

read -r -p "  Create Implementation user now? (Y/n): " CREATE_IMPL
CREATE_IMPL="${CREATE_IMPL,,}"

if [[ "$CREATE_IMPL" != "n" && "$CREATE_IMPL" != "no" ]]; then
    read -r -p "  Implementation username: " IMPL_USERNAME
    while [ -z "$IMPL_USERNAME" ]; do
        read -r -p "  Username cannot be empty. Try again: " IMPL_USERNAME
    done

    read -r -sp "  Implementation password: " IMPL_PASSWORD
    echo ""
    while [ -z "$IMPL_PASSWORD" ]; do
        read -r -sp "  Password cannot be empty. Try again: " IMPL_PASSWORD
        echo ""
    done

    read -r -sp "  Confirm password: " IMPL_PASSWORD_CONFIRM
    echo ""
    while [ "$IMPL_PASSWORD" != "$IMPL_PASSWORD_CONFIRM" ]; do
        warn "Passwords do not match. Try again."
        read -r -sp "  Password: " IMPL_PASSWORD
        echo ""
        read -r -sp "  Confirm: " IMPL_PASSWORD_CONFIRM
        echo ""
    done

    IMPL_HASH=$(python3 << PYHASH2
from werkzeug.security import generate_password_hash
print(generate_password_hash("${IMPL_PASSWORD}", method="pbkdf2:sha256"))
PYHASH2
)

    pg_exec "
INSERT INTO users (username, password_hash, role, status, created_at)
VALUES ('${IMPL_USERNAME}', '${IMPL_HASH}', 'implementation', 'active', NOW())
ON CONFLICT (username) DO NOTHING;
"
    ok "Implementation user '${IMPL_USERNAME}' created."
else
    echo "  Skipping Implementation user creation. You can add one manually later via /admin/users"
fi

# ──────────────────────────────────────────────────────
# COMPLETE
# ──────────────────────────────────────────────────────
echo ""
echo "=================================================="
echo -e "${GREEN}  RAYD HL7 installation complete!${NC}"
echo "=================================================="
echo ""
echo "  Web App:  https://$(hostname)"
echo "  Logs:     $COMPOSE logs -f"
echo "  Restart:  $COMPOSE restart"
echo "  Stop:     $COMPOSE down"
echo ""
echo "  HL7 MLLP Listener"
echo "  ─────────────────────────────────────────────────"
echo "    Listening on: 0.0.0.0:6661/tcp"
echo "    Expects:      ADT, ORM, ORU messages from HIS/RIS/PACS"
echo ""
echo "    Verify:       $COMPOSE logs rayd-app | grep MLLP"
echo "    Watch:        $COMPOSE logs -f rayd-app | grep HL7"
echo ""
echo "  Default Credentials"
echo "  ─────────────────────────────────────────────────"
echo "    Super User:        axadmin / axilum2000 (change on first login)"
echo "    Implementation:    (as configured above)"
echo ""
echo "  This install has NO external database connectivity."
echo "  It starts empty and fills as HL7 messages arrive."
echo ""
