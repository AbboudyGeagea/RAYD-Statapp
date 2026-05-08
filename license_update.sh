#!/usr/bin/env bash
# license_update.sh — RAYD license tier manager
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
BACKUP_DIR="$SCRIPT_DIR/.license_backups"

# ── Load .env ─────────────────────────────────────────────────────────────────
[[ -f "$ENV_FILE" ]] || { echo "ERROR: .env not found at $ENV_FILE"; exit 1; }
set -a; source <(grep -v '^\s*#' "$ENV_FILE" | grep '='); set +a

# ── Password check ────────────────────────────────────────────────────────────
if [[ -z "${LICENSE_PASSWORD:-}" ]]; then
    echo "ERROR: LICENSE_PASSWORD is not set in .env"
    echo "       Add: LICENSE_PASSWORD=your_secret"
    exit 1
fi

read -rsp "License admin password: " INPUT_PW; echo
[[ "$INPUT_PW" == "$LICENSE_PASSWORD" ]] || { echo "Wrong password."; exit 1; }

# ── DB helpers ────────────────────────────────────────────────────────────────
PG_HOST="${POSTGRES_HOST:-localhost}"
PG_PORT="${POSTGRES_PORT:-5432}"
PG_USER="${POSTGRES_USER:-etl_user}"
PG_DB="${POSTGRES_DB:-etl_db}"
export PGPASSWORD="${POSTGRES_PASSWORD:-}"

psql_q() { psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -t -A "$@"; }

# ── Banner ────────────────────────────────────────────────────────────────────
echo
echo "╔══════════════════════════════════════════╗"
echo "║         RAYD  License  Manager           ║"
echo "╚══════════════════════════════════════════╝"
echo

# ── Show current license ──────────────────────────────────────────────────────
CURRENT_JSON=$(psql_q -c "SELECT value FROM settings WHERE key = 'license'" 2>/dev/null || true)

if [[ -z "$CURRENT_JSON" ]]; then
    echo "  Current license : NOT SET (runtime default = enterprise)"
else
    python3 - "$CURRENT_JSON" <<'PY'
import json, sys
d = json.loads(sys.argv[1])
tier = d.get("tier", "unknown").upper()
print(f"  Current tier    : {tier}\n")
FEATURES = [
    ("export",          "Export"),
    ("adapter_mapper",  "DB / Adapter Mapper"),
    ("hl7_orders",      "HL7 Orders"),
    ("oru_analytics",   "Report Intelligence (ORU)"),
    ("custom_reports",  "Custom Reports"),
    ("cd_print",        "Patient CD Log"),
    ("er_dashboard",    "ER Dashboard"),
    ("capacity_ladder", "Capacity Ladder"),
    ("saved_reports",   "Saved Reports"),
    ("super_report",    "Super Report"),
    ("referring_intel", "Referring Intelligence"),
    ("financial",       "Revenue Intelligence"),
    ("bitnet_ai",       "AI Assistant + Teaching"),
    ("scheduling",      "Scheduling"),
    ("live_feed",       "Live AE Status"),
    ("patient_portal",  "Patient Portal"),
    ("ai_report",       "AI Report Analysis"),
]
for key, label in FEATURES:
    mark = "✓" if d.get(key, False) else "✗"
    print(f"    {mark}  {label}")
PY
fi

echo

# ── Tier selection ────────────────────────────────────────────────────────────
echo "  1) Essential     — core reports, ETL, user management"
echo "  2) Professional  — + HL7, ER, capacity, saved reports, intelligence"
echo "  3) Enterprise    — full access (all features)"
echo "  4) Custom        — toggle each feature manually"
echo
read -rp "Select tier [1-4]: " CHOICE

case "$CHOICE" in
    1) TIER="essential"    ;;
    2) TIER="professional" ;;
    3) TIER="enterprise"   ;;
    4) TIER="custom"       ;;
    *) echo "Invalid choice."; exit 1 ;;
esac

# ── Build new license JSON ────────────────────────────────────────────────────
NEW_JSON=$(python3 - "$TIER" "${CURRENT_JSON:-{\}}" <<'PY'
import json, sys

TIER = sys.argv[1]
try:
    current = json.loads(sys.argv[2])
except Exception:
    current = {}

FEATURES = [
    ("export",          "Export"),
    ("adapter_mapper",  "DB / Adapter Mapper"),
    ("hl7_orders",      "HL7 Orders"),
    ("oru_analytics",   "Report Intelligence (ORU)"),
    ("custom_reports",  "Custom Reports"),
    ("cd_print",        "Patient CD Log"),
    ("er_dashboard",    "ER Dashboard"),
    ("capacity_ladder", "Capacity Ladder"),
    ("saved_reports",   "Saved Reports"),
    ("super_report",    "Super Report"),
    ("referring_intel", "Referring Intelligence"),
    ("financial",       "Revenue Intelligence"),
    ("bitnet_ai",       "AI Assistant + Teaching"),
    ("scheduling",      "Scheduling"),
    ("live_feed",       "Live AE Status"),
    ("patient_portal",  "Patient Portal"),
    ("ai_report",       "AI Report Analysis"),
]
KEYS = [k for k, _ in FEATURES]

PRESETS = {
    "essential": {k: k in ("export", "adapter_mapper") for k in KEYS},
    "professional": {k: k not in ("financial","bitnet_ai","scheduling","live_feed","patient_portal","ai_report") for k in KEYS},
    "enterprise": {k: True for k in KEYS},
}

if TIER != "custom":
    lic = PRESETS[TIER].copy()
    lic["tier"] = TIER
else:
    lic = {k: current.get(k, False) for k in KEYS}
    lic["tier"] = "custom"
    print("\n  Press Enter to keep current value, or type y/n:\n", file=__import__('sys').stderr)
    for key, label in FEATURES:
        curr = lic.get(key, False)
        curr_str = "y" if curr else "n"
        ans = input(f"    {label} [{curr_str}]: ").strip().lower()
        if ans in ("y", "yes", "1"):
            lic[key] = True
        elif ans in ("n", "no", "0"):
            lic[key] = False
        # blank → keep current
    # Ask for tier label
    custom_label = input("\n  Custom tier label (leave blank for 'custom'): ").strip()
    if custom_label:
        lic["tier"] = custom_label.lower()

# Always include reports list from current (preserve any report-level grants)
if "reports" in current:
    lic["reports"] = current["reports"]

print(json.dumps(lic))
PY
)

# ── Confirm ───────────────────────────────────────────────────────────────────
echo
echo "  New license:"
python3 - "$NEW_JSON" <<'PY'
import json, sys
d = json.loads(sys.argv[1])
tier = d.get("tier","?").upper()
print(f"    Tier: {tier}\n")
for k, v in d.items():
    if k in ("tier","reports"):
        continue
    mark = "✓" if v else "✗"
    print(f"    {mark}  {k}")
PY

echo
read -rp "Apply this license? [y/N]: " CONFIRM
[[ "${CONFIRM,,}" == "y" ]] || { echo "Cancelled."; exit 0; }

# ── Backup current ────────────────────────────────────────────────────────────
if [[ -n "$CURRENT_JSON" ]]; then
    mkdir -p "$BACKUP_DIR"
    BACKUP_FILE="$BACKUP_DIR/license_$(date +%Y%m%d_%H%M%S).json"
    echo "$CURRENT_JSON" > "$BACKUP_FILE"
    echo "  Backup saved → $BACKUP_FILE"
fi

# ── Write to DB ───────────────────────────────────────────────────────────────
psql_q -c "INSERT INTO settings (key, value) VALUES ('license', \$\$${NEW_JSON}\$\$)
           ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value" > /dev/null
echo "  License updated in database."

# ── Restart offer ─────────────────────────────────────────────────────────────
echo
read -rp "Restart rayd_service now to apply? [y/N]: " RESTART
if [[ "${RESTART,,}" == "y" ]]; then
    docker restart rayd_service && echo "  rayd_service restarted." || echo "  WARNING: restart failed — do it manually."
else
    echo "  Remember to restart rayd_service for the change to take effect."
fi

echo
echo "  Done."
