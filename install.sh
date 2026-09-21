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

# ── Fixed app secret key ───────────────────────────────────────────────────────
# Still required: utils/crypto.py uses it, and it signs the Flask session.
FIXED_SECRET_KEY="c0f1a2b3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1"

# HL7 BRANCH: the hardcoded Oracle PACS credentials that used to live here are gone
# — host, port, SID, the 'sys' username and the pre-encrypted password. This install
# never connects to a source database, so it ships with no credentials for one.

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
info "Step 1/5 — Checking prerequisites..."

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

# HL7 BRANCH: the Oracle Instant Client install step was removed. Nothing on
# this host connects to an Oracle database, so the client is not downloaded,
# not unpacked into /opt/oracle, and not registered with ldconfig.

# ── BuildKit cache garbage collection ─────────────────────────────────────────
# Every rebuild of rayd-app/rayd-nlp adds a new multi-GB cache layer (the Python
# deps, medspaCy and its models) that is never reused again. Nothing evicts it by
# default, so it grows unbounded: measured 56GB on the LAUMC host, 55GB of it
# reclaimable,
# on a host whose images totalled under 3GB. Operators chasing a full disk reach
# for `docker image prune` and reclaim nothing, because images were never the
# problem.
#
# This is GC, NOT `docker builder prune`. The difference matters and update.sh
# documents why: pruning was tried there twice and both times wiped the cache the
# NEXT build needed, turning every deploy into a full re-download of apt and pip.
# GC instead keeps the most recently used cache up to defaultKeepStorage and
# evicts only the oldest beyond it -- the layers you are actively rebuilding
# against stay, the six-week-old orphans go.
#
# Idempotent and conservative: skipped entirely if a builder config already
# exists, so a hand-tuned daemon.json is never overwritten.
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
    # Merge into whatever is already there rather than clobbering it -- this file
    # commonly holds unrelated daemon settings (log driver, registry mirrors,
    # storage driver) that must survive.
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
        # A restart is required for builder GC; on a fresh install nothing is
        # serving traffic yet, so this is the one safe moment to do it. update.sh
        # deliberately only warns instead, since restarting the daemon mid-update
        # would bounce a live site.
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
info "Step 2/5 — Setting up .env..."

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

# HL7 BRANCH: RAYD_ETL_LOOKUP_FROM_PACS dropped with ETL Phase 8 — there is no PACS
# database to mine modalities and procedures from. That master data arrives by MFN
# message or CSV import.
EOF

    ok ".env created."
fi

# Load .env so later steps can read it
set -a; source .env; set +a

# ──────────────────────────────────────────────────────
# STEP 3: SSL certificates (private CA → trusted HTTPS)
# ──────────────────────────────────────────────────────
info "Step 3/5 — Setting up SSL certificates..."

CERT_DIR="./nginx/certs"
mkdir -p "$CERT_DIR"

read -r -p "  Server hostname users will type in the browser [$(hostname)]: " CERT_CN
CERT_CN="${CERT_CN:-$(hostname)}"

# --- 4a. Generate private CA (once per organization) ---
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

# --- 4b. Generate server cert signed by our CA ---
REGEN_CERT=false
if [ ! -f "${CERT_DIR}/fullchain.pem" ] || [ ! -f "${CERT_DIR}/privkey.pem" ]; then
    REGEN_CERT=true
else
    # Regenerate if the hostname in the existing cert doesn't match
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

    # SAN extension file (modern browsers require this)
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
# STEP 4: Build and start containers
# ──────────────────────────────────────────────────────
info "Step 4/5 — Building and starting Docker containers..."

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

# ──────────────────────────────────────────────────────
# STEP 5: Database configuration
# ──────────────────────────────────────────────────────
info "Step 5/5 — Configuring database..."

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

# ── 5b. HL7 interface ─────────────────────────────────
#
# Replaces the old "PACS Oracle Connection" step, which prompted for a host and
# wrote a db_params row holding a pre-encrypted 'sys' password. Nothing is stored
# here: the install is a passive MLLP receiver, so the only thing that has to be
# true is that the sending system can reach port 6661 on this host.
echo ""
echo "  ── HL7 Interface ──────────────────────────────────────────────────────────"
echo "  This install receives all clinical data as HL7 v2 over MLLP."
echo "  No source-database credentials are stored."
echo ""
echo "    Listening on : 6661/tcp (MLLP)"
echo "    Expects      : ADT and ORM from the HIS, status events from the RIS,"
echo "                   ORU from the PACS"
echo ""
echo "  Ask the integration team to add this host as a destination on those feeds."
echo "  Restrict who may reach the port with: scripts/hl7_firewall.sh"
echo ""

# ── 6c. License Tier ─────────────────────────────
echo ""
echo "  ── License Tier ───────────────────────────────────────────────────────────"
echo "  1) Essential    — Viewer dashboard, all reports, ETL view, user mgmt,"
echo "                    activity log, modality/procedure config, export"
echo "                    Unlimited users/sessions"
echo ""
echo "  2) Professional — Everything in Essential, plus:"
echo "                    HL7 orders, report intelligence, custom reports,"
echo "                    patient CD log, ER dashboard, capacity ladder,"
echo "                    saved reports, referring intel"
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
_JSON_ESS='{"tier":"essential","reports":[22,23,25,27,29,30],"export":true,"adapter_mapper":true,"hl7_orders":false,"oru_analytics":false,"custom_reports":false,"cd_print":false,"er_dashboard":false,"capacity_ladder":false,"saved_reports":false,"super_report":false,"referring_intel":false,"financial":false,"scheduling":false,"live_feed":false,"patient_portal":false,"ai_report":false,"max_users":0,"max_sessions":0,"expires":"","max_studies_per_report":0}'
_JSON_PRO='{"tier":"professional","reports":[22,23,25,27,29,30],"export":true,"adapter_mapper":true,"hl7_orders":true,"oru_analytics":true,"custom_reports":true,"cd_print":true,"er_dashboard":true,"capacity_ladder":true,"saved_reports":true,"super_report":true,"referring_intel":true,"financial":false,"scheduling":false,"live_feed":false,"patient_portal":false,"ai_report":false,"max_users":0,"max_sessions":0,"expires":"","max_studies_per_report":0}'
_JSON_ENT='{"tier":"enterprise","reports":[22,23,25,27,29,30],"export":true,"adapter_mapper":true,"hl7_orders":true,"oru_analytics":true,"custom_reports":true,"cd_print":true,"er_dashboard":true,"capacity_ladder":true,"saved_reports":true,"super_report":true,"referring_intel":true,"financial":true,"scheduling":true,"live_feed":true,"patient_portal":true,"ai_report":true,"max_users":0,"max_sessions":0,"expires":"","max_studies_per_report":0}'

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
    for feat in hl7_orders oru_analytics custom_reports cd_print er_dashboard capacity_ladder saved_reports referring_intel super_report; do
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
    for feat in financial scheduling live_feed patient_portal ai_report; do
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

    # HL7 BRANCH: no go-live date is asked for.
    #
    # The Oracle ETL needed one because it faced a PACS database holding years of
    # history and had to be told how far back to pull. An HL7 feed has no history
    # to pull — data starts arriving the moment the sender is pointed here — so the
    # question has no answer worth storing, and asking it invites a wrong one.
    #
    # go_live_config is left empty. db.get_etl_cutoff_date() derives the reporting
    # floor from the earliest study actually held instead, so every report's default
    # date range still opens on real data without any report query changing.
fi

# HL7 BRANCH: there is no initial ETL step.
#
# The Oracle install ended by walking 18 phases against the PACS database —
# studies, series, 100M+ raw image rows, image locations, patients, orders,
# rollups — paced one phase at a time so it would not overwhelm the source.
# None of that happens here. The install finishes the moment the containers are
# up, and the database fills itself as HL7 messages arrive.
#
# The practical consequence for the operator: this install starts EMPTY and
# stays empty until the sending systems are pointed at port 6661. That is the
# expected state, not a failed install.

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
echo "  HL7 MLLP listener:  6661/tcp on this host"
echo "    Verify it is listening:  $COMPOSE logs rayd-app | grep MLLP"
echo "    Watch messages arrive:   $COMPOSE logs -f rayd-app | grep HL7"
echo ""
echo "  This install has no ETL and no source-database credentials."
echo "  It starts empty and fills as HL7 messages arrive. If nothing appears,"
echo "  the question is whether the senders have been pointed at port 6661 —"
echo "  check with the integration team before looking at the application."
echo ""
