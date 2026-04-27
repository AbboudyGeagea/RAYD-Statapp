#!/usr/bin/env bash
# postgres_mcp.sh — launch the PostgreSQL MCP server for Claude Code
# Reads credentials from .env so nothing is hardcoded in .mcp.json
set -euo pipefail

PROJ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -f "$PROJ/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$PROJ/.env"
    set +a
fi

HOST="${POSTGRES_HOST:-localhost}"
PORT="${POSTGRES_PORT:-5432}"
USER="${POSTGRES_USER:?POSTGRES_USER not set}"
PASS="${POSTGRES_PASSWORD:?POSTGRES_PASSWORD not set}"
DB="${POSTGRES_DB:?POSTGRES_DB not set}"

# When running on the WSL2 host (outside Docker), the service name 'db'
# is not resolvable — map it back to localhost where port 5432 is exposed
# by docker-compose.dev.yml.
[[ "$HOST" == "db" ]] && HOST="localhost"

exec npx -y @modelcontextprotocol/server-postgres \
    "postgresql://${USER}:${PASS}@${HOST}:${PORT}/${DB}"
