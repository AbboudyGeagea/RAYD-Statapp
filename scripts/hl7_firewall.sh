#!/usr/bin/env bash
#
# hl7_firewall.sh — restrict the MLLP/HL7 port (6661) to known sender IPs only.
#
# WHY THIS EXISTS
#   docker-compose publishes 6661 so the hospital's PACS/HIS/RIS can send HL7.
#   Published ports are reachable from the ENTIRE network by default, and the MLLP
#   listener has no authentication — anyone who can reach the port can inject HL7.
#
# WHY NOT JUST BIND 127.0.0.1
#   The senders (SAP/Mirth HIS, Carestream PACS, RIS) are REMOTE hosts. Binding the
#   port to localhost would silently kill all HL7 ingestion (no ORU -> no NLP/reports,
#   no ORM -> no live feed). A source-IP whitelist is the correct control here.
#
# ⚠️  THE DOCKER GOTCHA THIS SCRIPT EXISTS TO GET RIGHT
#   Traffic to a *published container port* is DNAT'd and traverses FORWARD — it does
#   NOT go through the INPUT chain. So the "obvious" rule:
#       iptables -A INPUT -p tcp --dport 6661 -j DROP
#   does ABSOLUTELY NOTHING for a Docker-published port, while looking like it works.
#   Docker guarantees the DOCKER-USER chain is traversed first and never clobbers it,
#   so that is the only correct place to filter container traffic.
#
# USAGE
#   Set HL7_ALLOWED_IPS in .env (space- or comma-separated; CIDR allowed), then:
#       sudo -E bash scripts/hl7_firewall.sh
#   Rules are NOT persistent across reboot — install the systemd unit for that:
#       sudo cp scripts/rayd-hl7-firewall.service /etc/systemd/system/
#       sudo systemctl enable --now rayd-hl7-firewall
#
# FAIL-SAFE
#   If HL7_ALLOWED_IPS is empty this script REFUSES to run rather than applying a
#   deny-all that would take HL7 offline. Misconfiguration must not cause an outage.

set -euo pipefail

PORT="${HL7_PORT:-6661}"
CHAIN="RAYD-HL7"

# Load .env if present (so the systemd unit and manual runs behave the same).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ENV_FILE:-${SCRIPT_DIR}/../.env}"
if [[ -f "$ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    set -a; source "$ENV_FILE"; set +a
fi

RAW_IPS="${HL7_ALLOWED_IPS:-}"
# Accept comma or space separated.
IPS=$(echo "$RAW_IPS" | tr ',' ' ' | xargs || true)

if [[ -z "$IPS" ]]; then
    cat >&2 <<'EOF'
ERROR: HL7_ALLOWED_IPS is not set.

Refusing to apply firewall rules — a deny-all would stop ALL HL7 ingestion
(no ORU -> NLP/report intelligence dies; no ORM -> live feed dies).

Set the sender IPs in .env, e.g.:
    HL7_ALLOWED_IPS=10.20.30.40 10.20.30.41   # PACS (Carestream), HIS/Mirth (SAP)

Find the real senders from the listener logs:
    docker compose logs rayd-app | grep -i mllp
EOF
    exit 1
fi

if ! command -v iptables >/dev/null 2>&1; then
    echo "ERROR: iptables not found on this host." >&2
    exit 1
fi

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    echo "ERROR: must run as root (sudo -E bash scripts/hl7_firewall.sh)." >&2
    exit 1
fi

echo "[hl7_firewall] port=${PORT} allowed='${IPS}'"

# Docker creates DOCKER-USER itself, but create it if the daemon has not yet.
iptables -N DOCKER-USER 2>/dev/null || true

# Our own chain: create, or flush so re-runs are idempotent (no rule pile-up).
iptables -N "$CHAIN" 2>/dev/null || iptables -F "$CHAIN"

# Established/related first — never interfere with in-flight or return traffic.
iptables -A "$CHAIN" -m conntrack --ctstate ESTABLISHED,RELATED -j RETURN

# Always permit loopback/host-local (health checks, local testing).
iptables -A "$CHAIN" -s 127.0.0.1/8 -j RETURN

# Whitelisted senders.
for ip in $IPS; do
    echo "[hl7_firewall]   allow ${ip}"
    iptables -A "$CHAIN" -s "$ip" -j RETURN
done

# Everything else destined for the HL7 port is dropped.
iptables -A "$CHAIN" -j DROP

# Hook into DOCKER-USER (idempotent: only insert if not already present).
if ! iptables -C DOCKER-USER -p tcp --dport "$PORT" -j "$CHAIN" 2>/dev/null; then
    iptables -I DOCKER-USER 1 -p tcp --dport "$PORT" -j "$CHAIN"
    echo "[hl7_firewall] hooked ${CHAIN} into DOCKER-USER"
else
    echo "[hl7_firewall] DOCKER-USER hook already present"
fi

echo "[hl7_firewall] applied. Current ${CHAIN} rules:"
iptables -L "$CHAIN" -n --line-numbers
