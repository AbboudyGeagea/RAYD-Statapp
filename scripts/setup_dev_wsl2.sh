#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# setup_dev_wsl2.sh
# One-shot dev environment setup for Claude Code + Ruflo on WSL2 (Ubuntu).
#
# What this does:
#   1. Installs Node.js 20 (via nvm if not already present)
#   2. Installs Claude Code CLI globally
#   3. Makes postgres_mcp.sh executable
#   4. Adds the Ruflo (claude-flow) MCP server to Claude Code globally
#   5. Verifies the setup
#
# Run from the project root inside WSL2:
#   bash scripts/setup_dev_wsl2.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

echo ""
echo "════════════════════════════════════════════════"
echo "  RAYD Dev Environment — WSL2 Setup"
echo "════════════════════════════════════════════════"
echo ""

# ── 1. Node.js 20 ─────────────────────────────────────────────────────────────
info "Step 1/5 — Checking Node.js..."

NODE_VERSION_REQUIRED=20

if command -v node &>/dev/null; then
    NODE_MAJOR=$(node --version | sed 's/v//' | cut -d. -f1)
    if [ "$NODE_MAJOR" -ge "$NODE_VERSION_REQUIRED" ]; then
        ok "Node.js $(node --version) already installed."
    else
        warn "Node.js $(node --version) is too old (need ≥ v${NODE_VERSION_REQUIRED}). Upgrading via nvm..."
        _install_node=1
    fi
else
    warn "Node.js not found. Installing via nvm..."
    _install_node=1
fi

if [ "${_install_node:-0}" = "1" ]; then
    if ! command -v nvm &>/dev/null && [ ! -f "$HOME/.nvm/nvm.sh" ]; then
        info "Installing nvm..."
        curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
    fi
    # shellcheck disable=SC1091
    [ -f "$HOME/.nvm/nvm.sh" ] && source "$HOME/.nvm/nvm.sh"
    nvm install "$NODE_VERSION_REQUIRED"
    nvm use "$NODE_VERSION_REQUIRED"
    nvm alias default "$NODE_VERSION_REQUIRED"
    ok "Node.js $(node --version) installed via nvm."
fi

# ── 2. Claude Code CLI ────────────────────────────────────────────────────────
info "Step 2/5 — Checking Claude Code CLI..."

if command -v claude &>/dev/null; then
    ok "Claude Code already installed: $(claude --version 2>/dev/null || echo 'version unknown')"
else
    info "Installing Claude Code CLI..."
    npm install -g @anthropic-ai/claude-code
    ok "Claude Code installed: $(claude --version 2>/dev/null || echo 'ok')"
fi

# ── 3. postgres_mcp.sh permissions ───────────────────────────────────────────
info "Step 3/5 — Setting script permissions..."

chmod +x "${SCRIPT_DIR}/scripts/postgres_mcp.sh"
chmod +x "${SCRIPT_DIR}/scripts/start_qwen_server.sh" 2>/dev/null || true
ok "Scripts are executable."

# ── 4. Register MCP servers ───────────────────────────────────────────────────
info "Step 4/5 — Registering MCP servers in Claude Code..."

# Ruflo / claude-flow (global — available in all projects)
if claude mcp list 2>/dev/null | grep -q "claude-flow"; then
    ok "claude-flow MCP already registered."
else
    info "Adding claude-flow (Ruflo) MCP server..."
    claude mcp add --scope global claude-flow -- npx claude-flow@alpha mcp start
    ok "claude-flow registered globally."
fi

# PostgreSQL (project-scoped — picked up via .mcp.json in the project root)
ok "rayd-postgres MCP is project-scoped via .mcp.json — no global registration needed."

# ── 5. Verify ─────────────────────────────────────────────────────────────────
info "Step 5/5 — Verifying..."

echo ""
echo "  Registered MCP servers:"
claude mcp list 2>/dev/null | sed 's/^/    /' || warn "Could not list MCP servers (is Claude Code authenticated?)"

echo ""
echo "  Docker containers:"
docker compose ps 2>/dev/null | sed 's/^/    /' || warn "Docker not running or not in PATH."

echo ""
echo "════════════════════════════════════════════════"
echo -e "${GREEN}  Dev environment setup complete!${NC}"
echo "════════════════════════════════════════════════"
echo ""
echo "  Next steps:"
echo "  1. Start the dev stack:"
echo "       docker compose -f docker-compose.dev.yml up -d"
echo ""
echo "  2. Open this project in Claude Code:"
echo "       claude"
echo ""
echo "  3. Claude Code will auto-detect .mcp.json and connect to:"
echo "       • rayd-postgres  — live DB queries via SQL"
echo "       • claude-flow    — 60+ Ruflo specialist agents"
echo ""
echo "  4. Test the DB connection:"
echo "       psql postgresql://etl_user:etl_pass@localhost:5432/etl_db -c '\\dt'"
echo ""
