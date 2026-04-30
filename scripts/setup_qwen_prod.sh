#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# setup_qwen_prod.sh
# Qwen2.5-7B-Instruct — one-shot setup for the Ubuntu production server.
#
# What this does:
#   1. Installs llama.cpp (builds from source if binary not found)
#   2. Downloads Qwen2.5-7B-Instruct-Q4_K_M.gguf → /home/stats/Qwen/
#   3. Installs a systemd service (qwen-server) that starts on boot
#   4. Starts the service immediately
#
# Run as root (or with sudo):
#   sudo bash scripts/setup_qwen_prod.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

MODEL_DIR="/home/stats/Qwen"
MODEL_FILE="Qwen2.5-7B-Instruct-Q4_K_M.gguf"
MODEL_REPO="bartowski/Qwen2.5-7B-Instruct-GGUF"
PORT=8081
CTX=4096
THREADS=$(nproc)

echo "══════════════════════════════════════════════════"
echo " Qwen2.5-7B Production Setup"
echo "══════════════════════════════════════════════════"

# ── 0. Preflight checks ───────────────────────────────────────────────────────

# Must run as root
[ "$(id -u)" -eq 0 ] || error "Run as root: sudo bash $0"

# python3 must exist
command -v python3 &>/dev/null || {
    info "python3 not found — installing..."
    apt-get update -qq 2>&1 | grep -v "^W:" || true
    apt-get install -y -qq python3
}
PY=$(command -v python3)
ok "Python: $($PY --version)"

# Ensure pip is available (multiple fallbacks)
if ! $PY -m pip --version &>/dev/null; then
    info "pip not found — trying python3-pip..."
    apt-get update -qq 2>&1 | grep -v "^W:" || true
    if apt-get install -y -qq python3-pip 2>/dev/null; then
        ok "python3-pip installed."
    elif $PY -m ensurepip --upgrade 2>/dev/null; then
        ok "pip bootstrapped via ensurepip."
    else
        # Last resort: get-pip.py
        info "Trying get-pip.py fallback..."
        curl -sS https://bootstrap.pypa.io/get-pip.py | $PY
        ok "pip installed via get-pip.py."
    fi
fi
ok "pip: $($PY -m pip --version)"

# Disk space check — model is ~4.4 GB, need at least 6 GB free
AVAIL_KB=$(df -k "$MODEL_DIR" 2>/dev/null | awk 'NR==2{print $4}' || df -k / | awk 'NR==2{print $4}')
AVAIL_GB=$(echo "$AVAIL_KB / 1048576" | bc 2>/dev/null || echo "?")
if [ "$AVAIL_GB" != "?" ] && [ "$AVAIL_GB" -lt 6 ] 2>/dev/null; then
    error "Not enough disk space: ${AVAIL_GB}GB available, need at least 6GB."
fi
ok "Disk space: ${AVAIL_GB}GB available."

# ── 1. llama.cpp ──────────────────────────────────────────────────────────────
if command -v llama-server &>/dev/null; then
    LLAMA_BIN=$(command -v llama-server)
    ok "[1/4] llama-server found at $LLAMA_BIN — skipping build"
else
    info "[1/4] llama-server not found — building llama.cpp from source..."

    # Update apt ignoring broken third-party repo signatures (e.g. dbeaver)
    apt-get update -qq 2>&1 | grep -v "^W:" || true
    apt-get install -y -qq build-essential cmake git curl

    BUILD_DIR="/opt/llama.cpp"
    if [ ! -d "$BUILD_DIR" ]; then
        git clone --depth 1 https://github.com/ggerganov/llama.cpp "$BUILD_DIR"
    else
        git -C "$BUILD_DIR" pull --ff-only || warn "git pull skipped (local changes?)"
    fi

    cmake -S "$BUILD_DIR" -B "$BUILD_DIR/build" \
        -DLLAMA_CURL=OFF \
        -DCMAKE_BUILD_TYPE=Release \
        -DGGML_NATIVE=OFF
    cmake --build "$BUILD_DIR/build" --target llama-server -j"$THREADS"
    ln -sf "$BUILD_DIR/build/bin/llama-server" /usr/local/bin/llama-server
    LLAMA_BIN=/usr/local/bin/llama-server
    ok "[1/4] llama-server built → $LLAMA_BIN"
fi

# ── 2. Download model ─────────────────────────────────────────────────────────
mkdir -p "$MODEL_DIR"

if [ -f "$MODEL_DIR/$MODEL_FILE" ]; then
    ok "[2/4] Model already present at $MODEL_DIR/$MODEL_FILE — skipping download"
else
    info "[2/4] Downloading $MODEL_FILE (~4.4 GB) ..."

    # Install huggingface_hub if needed
    if ! $PY -c "import huggingface_hub" 2>/dev/null; then
        info "Installing huggingface_hub..."
        $PY -m pip install -q huggingface_hub || \
            $PY -m pip install -q --break-system-packages huggingface_hub || \
            error "Failed to install huggingface_hub. Try: $PY -m pip install huggingface_hub"
    fi

    $PY - <<PYEOF
from huggingface_hub import hf_hub_download
import sys
try:
    path = hf_hub_download(
        repo_id="$MODEL_REPO",
        filename="$MODEL_FILE",
        local_dir="$MODEL_DIR",
    )
    print(f"Saved to: {path}")
except Exception as e:
    print(f"Download failed: {e}", file=sys.stderr)
    sys.exit(1)
PYEOF

    [ -f "$MODEL_DIR/$MODEL_FILE" ] || error "Model file not found after download. Check the error above."
    ok "[2/4] Model downloaded → $MODEL_DIR/$MODEL_FILE"
fi

# ── 3. Systemd service ────────────────────────────────────────────────────────
if ! command -v systemctl &>/dev/null; then
    warn "[3/4] systemd not available — skipping service install."
    warn "      Start manually: $LLAMA_BIN --model $MODEL_DIR/$MODEL_FILE --port $PORT --host 0.0.0.0"
else
    info "[3/4] Installing systemd service: qwen-server"

    cat > /etc/systemd/system/qwen-server.service <<UNIT
[Unit]
Description=Qwen2.5-7B Inference Server (llama.cpp)
After=network.target
Wants=network.target

[Service]
Type=simple
User=root
ExecStart=$LLAMA_BIN \\
    --model $MODEL_DIR/$MODEL_FILE \\
    --ctx-size $CTX \\
    --port $PORT \\
    --host 0.0.0.0 \\
    --threads $THREADS \\
    --n-predict 512 \\
    --temp 0.1 \\
    --repeat-penalty 1.15 \\
    --no-mmap \\
    --log-disable
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=qwen-server

[Install]
WantedBy=multi-user.target
UNIT

    systemctl daemon-reload
    systemctl enable qwen-server
    ok "[3/4] Service installed and enabled."
fi

# ── 4. Start ──────────────────────────────────────────────────────────────────
if command -v systemctl &>/dev/null; then
    info "[4/4] Starting qwen-server..."

    # Check if port is already in use by something else
    if ss -tlnp 2>/dev/null | grep -q ":${PORT} " && ! systemctl is-active --quiet qwen-server 2>/dev/null; then
        warn "Port $PORT is already in use by another process. Check with: ss -tlnp | grep $PORT"
    fi

    systemctl restart qwen-server
    sleep 3
    systemctl status qwen-server --no-pager || warn "Service may not have started cleanly. Check: journalctl -u qwen-server -n 30"
else
    warn "[4/4] Start manually with: bash scripts/start_qwen_server.sh"
fi

echo ""
echo "══════════════════════════════════════════════════"
echo -e " ${GREEN}Done. Qwen2.5-7B serving on port $PORT${NC}"
echo " Test: curl http://localhost:${PORT}/health"
echo "══════════════════════════════════════════════════"
