#!/usr/bin/env bash
# scripts/regenerate_cert.sh — Reissue RAYD's HTTPS server certificate for a
# new/changed hostname, reusing the existing private CA. Only touches
# nginx/certs/{privkey.pem,fullchain.pem} and reloads nginx — no containers
# recreated, no data touched. Safe to run on a live production server, unlike
# install.sh (fresh-install / data-reset only — see docs).
#
# Usage:
#   sudo bash scripts/regenerate_cert.sh RAYD-Statapp.ad.umcrh.com
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

CERT_CN="${1:-}"
[ -n "$CERT_CN" ] || error "Usage: sudo bash scripts/regenerate_cert.sh <hostname>   e.g. RAYD-Statapp.ad.umcrh.com"

command -v openssl >/dev/null 2>&1 || error "'openssl' is required."

CERT_DIR="./nginx/certs"
[ -f "${CERT_DIR}/rayd-ca.key" ] && [ -f "${CERT_DIR}/rayd-ca.crt" ] || \
    error "No existing RAYD CA found at ${CERT_DIR} — this only reissues the server cert for an already-installed site. Run install.sh for a fresh install."

if docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE="docker-compose"
else
    error "Docker Compose not found."
fi

info "Reissuing server certificate for '${CERT_CN}' (reusing existing RAYD CA)..."

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

info "Reloading nginx..."
$COMPOSE exec -T nginx nginx -s reload && ok "nginx reloaded." || error "nginx reload failed — check: $COMPOSE logs nginx"

echo ""
ok "Done. Verify at: https://${CERT_CN}"
echo "     Note: this cert is issued for '${CERT_CN}' only — any other hostname will now fail TLS validation."
echo "     Clients that already trust the RAYD CA (rayd-ca.crt) need no further action."
