#!/usr/bin/env bash
# install.sh — RAYD Patient Portal (PortalSA) installation
# Run as: sudo bash install.sh   (from the repository directory)
#
# Installs RAYD licensed as the patient portal alone: no PACS/RIS database, no
# ETL. Orders arrive as HL7 ORM on port 6661; patients log in on /portal.
# Every secret (app key, database password, first admin password) is generated
# here, per install — nothing is shared between sites.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

PG_USER="etl_user"
PG_DB="etl_db"
# The app key every main-branch install shipped with; never accept it here.
SHARED_SECRET_KEY="c0f1a2b3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1"

pg_exec() {
    docker exec rayd_db psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -q -c "$1"
}

echo ""
echo "=================================================="
echo "     RAYD Patient Portal — Install Script"
echo "=================================================="
echo ""

# ──────────────────────────────────────────────────────
# STEP 1: Prerequisites
# ──────────────────────────────────────────────────────
info "Step 1/5 — Checking prerequisites..."

[ "$(id -u)" -eq 0 ] || error "Run as root: sudo bash install.sh"
command -v docker  >/dev/null 2>&1 || error "Docker is not installed. Install it from https://docs.docker.com/engine/install/"
command -v openssl >/dev/null 2>&1 || error "'openssl' is required. Run: apt-get install -y openssl"

if docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE="docker-compose"
else
    error "Docker Compose is not installed. Run: apt-get install -y docker-compose-plugin"
fi
ok "Docker found: $(docker --version)"

# Container names are fixed (rayd_*): another RAYD stack on this host would clash.
OTHER=$(docker ps -a --filter "name=^rayd_" --format '{{.Names}} {{.Label "com.docker.compose.project.working_dir"}}' \
        | awk -v d="$SCRIPT_DIR" '$2 != d {print $1 " (" $2 ")"}')
if [ -n "$OTHER" ]; then
    echo "$OTHER"
    error "Another RAYD installation owns the containers above. Stop it first from its own folder: sudo docker compose down"
fi

# ── BuildKit cache garbage collection (keeps rebuilds from filling the disk) ──
GC_KEEP="${RAYD_BUILD_CACHE_KEEP:-20GB}"
DAEMON_JSON="/etc/docker/daemon.json"
if ! command -v python3 >/dev/null 2>&1; then
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
# STEP 2: Environment file (secrets generated per install)
# ──────────────────────────────────────────────────────
info "Step 2/5 — Setting up .env..."

if [ -f ".env" ] && grep -q "^SECRET_KEY=.\+" .env; then
    ok ".env already exists — keeping its secrets."
    if grep -q "^SECRET_KEY=${SHARED_SECRET_KEY}$" .env; then
        warn "SECRET_KEY is the key shared by every old install — replacing it."
        sed -i "s/^SECRET_KEY=.*/SECRET_KEY=$(openssl rand -hex 32)/" .env
        warn "Re-enter the viewer signing secret and Twilio token in Portal Configuration (they were encrypted with the old key)."
    fi
else
    echo ""
    echo "  HL7 senders: the RIS/HIS addresses allowed to send orders to port 6661."
    echo "  Anyone else who can reach that port could create patient accounts."
    read -r -p "  Allowed sender IPs/CIDRs, comma-separated (blank = any, NOT recommended): " HL7_SENDERS
    [ -z "$HL7_SENDERS" ] && warn "No HL7 sender list — port 6661 accepts any host. Restrict it with a firewall."

    umask 077
    cat > .env <<EOF
SECRET_KEY=$(openssl rand -hex 32)

POSTGRES_USER=${PG_USER}
POSTGRES_PASSWORD=$(openssl rand -hex 24)
POSTGRES_HOST=db
POSTGRES_PORT=5432
POSTGRES_DB=${PG_DB}

TZ=Asia/Beirut
PATIENT_PORTAL_ENABLED=true
LIVE_FEED_ENABLED=false
HL7_ALLOWED_SENDERS=${HL7_SENDERS}
EOF
    umask 022
    ok ".env created (secrets generated for this install, file readable by root only)."
fi

set -a; source .env; set +a

# ──────────────────────────────────────────────────────
# STEP 3: TLS certificate (private CA → trusted HTTPS on the LAN)
# ──────────────────────────────────────────────────────
info "Step 3/5 — Setting up TLS certificate..."

CERT_DIR="./nginx/certs"
mkdir -p "$CERT_DIR"

read -r -p "  Hostname or IP users will type in the browser [$(hostname)]: " CERT_CN
CERT_CN="${CERT_CN:-$(hostname)}"
if [[ "$CERT_CN" =~ ^[0-9]+(\.[0-9]+){3}$ ]]; then
    SAN="IP:${CERT_CN},IP:127.0.0.1"
else
    SAN="DNS:${CERT_CN},IP:127.0.0.1"
fi

if [ ! -f "${CERT_DIR}/rayd-ca.key" ] || [ ! -f "${CERT_DIR}/rayd-ca.crt" ]; then
    info "Generating RAYD private Certificate Authority..."
    openssl genrsa -out "${CERT_DIR}/rayd-ca.key" 4096 2>/dev/null
    openssl req -x509 -new -nodes \
        -key  "${CERT_DIR}/rayd-ca.key" \
        -sha256 -days 3650 \
        -out  "${CERT_DIR}/rayd-ca.crt" \
        -subj "/CN=RAYD Internal CA/O=RAYD/OU=IT" \
        -addext "basicConstraints=critical,CA:true" \
        -addext "keyUsage=critical,keyCertSign,cRLSign" \
        2>/dev/null
    chmod 600 "${CERT_DIR}/rayd-ca.key"
    ok "Private CA certificate created."
else
    ok "Private CA already exists — reusing it."
fi

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
    openssl genrsa -out "${CERT_DIR}/privkey.pem" 2048 2>/dev/null
    chmod 600 "${CERT_DIR}/privkey.pem"
    openssl req -new -key "${CERT_DIR}/privkey.pem" -out "${CERT_DIR}/server.csr" \
        -subj "/CN=${CERT_CN}/O=RAYD/OU=IT" 2>/dev/null
    cat > "${CERT_DIR}/server-ext.cnf" <<SRVEXT
[ext]
subjectAltName     = ${SAN}
basicConstraints   = CA:false
keyUsage           = critical,digitalSignature,keyEncipherment
extendedKeyUsage   = serverAuth
SRVEXT
    openssl x509 -req -in "${CERT_DIR}/server.csr" \
        -CA "${CERT_DIR}/rayd-ca.crt" -CAkey "${CERT_DIR}/rayd-ca.key" -CAcreateserial \
        -out "${CERT_DIR}/fullchain.pem" -days 825 -sha256 \
        -extfile "${CERT_DIR}/server-ext.cnf" -extensions ext 2>/dev/null
    rm -f "${CERT_DIR}/server.csr" "${CERT_DIR}/server-ext.cnf" "${CERT_DIR}/rayd-ca.srl"
    ok "Server certificate signed by RAYD CA for '${CERT_CN}'."
fi
echo "  Staff PCs: install ${CERT_DIR}/rayd-ca.crt once as a trusted root"
echo "  (Windows, as Administrator: certutil -addstore Root rayd-ca.crt)."
echo "  Patients on the internet need a public certificate for the portal's public name."

# Staff networks (see nginx/conf.d/default.conf); untracked, survives updates.
STAFF_NETS="nginx/conf.d/staff_networks.local"
if [ ! -f "$STAFF_NETS" ]; then
    cat > "$STAFF_NETS" <<'NETS'
# Extra staff networks for nginx (private ranges are already allowed).
# One entry per line: "CIDR 1;" allows staff pages, "CIDR 0;" blocks them.
# Example — the firewall's NAT address for internet traffic: 10.0.0.1 0;
NETS
    ok "Created $STAFF_NETS (staff pages: private networks only)."
fi

# ──────────────────────────────────────────────────────
# STEP 4: Build and start containers
# ──────────────────────────────────────────────────────
info "Step 4/5 — Building, licensing and starting containers..."

$COMPOSE down --remove-orphans 2>/dev/null || true
$COMPOSE build
md5sum requirements.txt 2>/dev/null | awk '{print $1}' > .last_req_hash

# Database first: the app reads its license once, at startup, so the license
# row must exist before the app's first start.
$COMPOSE up -d db
info "Waiting for database to be ready..."
WAIT=0
until docker exec rayd_db pg_isready -U "$PG_USER" -d "$PG_DB" -q 2>/dev/null || [ $WAIT -ge 90 ]; do
    sleep 3; WAIT=$((WAIT+3))
done
docker exec rayd_db pg_isready -U "$PG_USER" -d "$PG_DB" -q 2>/dev/null || error "Database did not become ready in time. Check: $COMPOSE logs db"

pg_exec "ALTER TABLE public.settings ALTER COLUMN key TYPE TEXT, ALTER COLUMN value TYPE TEXT;"
LICENSE_JSON='{"tier":"portal","reports":[],"export":false,"adapter_mapper":false,"hl7_orders":false,"oru_analytics":false,"custom_reports":false,"cd_print":false,"er_dashboard":false,"capacity_ladder":false,"saved_reports":false,"super_report":false,"referring_intel":false,"financial":false,"scheduling":false,"live_feed":false,"patient_portal":true,"ai_report":false,"max_users":0,"max_sessions":0,"expires":"","max_studies_per_report":0}'
pg_exec "INSERT INTO settings (key, value) VALUES ('license', '${LICENSE_JSON}') ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;"
pg_exec "INSERT INTO settings (key, value) VALUES ('demo_mode', 'false') ON CONFLICT (key) DO UPDATE SET value = 'false';"
ok "License: patient portal only."

$COMPOSE up -d

wait_for_app() {
    local w=0
    until docker exec rayd_service curl -sf http://localhost:8080/health -o /dev/null 2>/dev/null || [ $w -ge 120 ]; do
        sleep 3; w=$((w+3))
    done
    docker exec rayd_service curl -sf http://localhost:8080/health -o /dev/null 2>/dev/null \
        || error "rayd_service did not come up. Check logs: $COMPOSE logs rayd-app --tail 60"
}
wait_for_app
$COMPOSE exec -T nginx nginx -s reload 2>/dev/null || true
ok "All containers are running."

if docker logs rayd_service 2>&1 | grep -q "License tier: portal"; then
    ok "App running in portal-only mode."
else
    warn "Could not confirm portal-only mode — check: $COMPOSE logs rayd-app | grep 'License tier'"
fi

# ──────────────────────────────────────────────────────
# STEP 5: First admin
# ──────────────────────────────────────────────────────
info "Step 5/5 — Creating the first admin..."

USER_COUNT=$(docker exec rayd_db psql -U "$PG_USER" -d "$PG_DB" -tAc "SELECT count(*) FROM users;")
ADMIN_LINE=""
if [ "${USER_COUNT:-0}" -eq 0 ]; then
    read -r -p "  First admin username [admin]: " ADMIN_USER
    ADMIN_USER="${ADMIN_USER:-admin}"
    [[ "$ADMIN_USER" =~ ^[A-Za-z0-9._-]{3,50}$ ]] || error "Username: 3-50 letters, digits, dot, dash or underscore."
    # Random password, shown once; the admin must change it at first login.
    ADMIN_PASS=$(docker exec -e RAYD_NEW_ADMIN="$ADMIN_USER" rayd_service python3 -c '
import os, secrets, string, psycopg2
from werkzeug.security import generate_password_hash
alpha = "".join(c for c in string.ascii_letters + string.digits if c not in "O0oIl1")
pw = "".join(secrets.choice(alpha) for _ in range(16))
conn = psycopg2.connect(host=os.environ["POSTGRES_HOST"], port=os.environ["POSTGRES_PORT"],
                        dbname=os.environ["POSTGRES_DB"], user=os.environ["POSTGRES_USER"],
                        password=os.environ["POSTGRES_PASSWORD"])
with conn, conn.cursor() as cur:
    cur.execute("""INSERT INTO users (username, password_hash, role, full_name, status, must_change_password)
                   VALUES (%s, %s, %s, %s, %s, TRUE)""",
                (os.environ["RAYD_NEW_ADMIN"], generate_password_hash(pw), "admin", "Administrator", "active"))
print(pw)
') || error "Could not create the first admin."
    ADMIN_LINE="  Admin:    ${ADMIN_USER} / ${ADMIN_PASS}   (shown once — change it at first login)"
    ok "First admin '${ADMIN_USER}' created."
else
    ok "Users already exist (${USER_COUNT}) — no admin created."
fi

# ──────────────────────────────────────────────────────
# DONE
# ──────────────────────────────────────────────────────
echo ""
echo "=================================================="
echo -e "${GREEN}  RAYD Patient Portal installed${NC}"
echo "=================================================="
echo ""
echo "  Patients: https://${CERT_CN}/portal"
echo "  Staff:    https://${CERT_CN}/login   (private networks only)"
[ -n "$ADMIN_LINE" ] && echo "$ADMIN_LINE"
echo ""
echo "  Next, in Patient Portal → Configuration:"
echo "    1. Viewer link template + signing secret (agreed with the PACS team)"
echo "    2. Twilio WhatsApp credentials and the portal's public address"
echo "  Firewall: 6661/tcp only from the RIS/HIS; 443/tcp from wherever patients connect."
echo ""
echo "  Logs:     $COMPOSE logs -f rayd-app"
echo "  Update:   sudo bash update.sh"
echo ""
