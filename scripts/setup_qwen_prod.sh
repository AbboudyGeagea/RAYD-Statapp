#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# setup_qwen_prod.sh
# Qwen2.5-7B-Instruct — one-shot setup for the Ubuntu production server.
#
# Auto-detects:
#   • CPU capabilities (AVX512 / AVX2 / AVX) and builds llama.cpp with native
#     optimizations. Rebuilds automatically if a previous build lacked them.
#   • Available RAM and selects the highest-quality quantization that fits:
#       ≥ 12 GB → Q8_0  (near-lossless, ~8.5 GB)
#       ≥  9 GB → Q6_K  (high quality,  ~6.2 GB)
#       ≥  7 GB → Q5_K_M(good quality,  ~5.1 GB)
#       <  7 GB → Q4_K_M(baseline,      ~4.4 GB)
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
MODEL_REPO="bartowski/Qwen2.5-7B-Instruct-GGUF"
PORT=8081
CTX=4096
THREADS=$(nproc)
BUILD_DIR="/opt/llama.cpp"
BUILD_MARKER="$BUILD_DIR/.built_native"

echo "══════════════════════════════════════════════════"
echo " Qwen2.5-7B Production Setup"
echo "══════════════════════════════════════════════════"

# ── 0a. Root check ────────────────────────────────────────────────────────────
[ "$(id -u)" -eq 0 ] || error "Run as root: sudo bash $0"

# ── 0b. CPU capability detection ─────────────────────────────────────────────
CPU_FLAGS=$(grep -o 'avx[^ ]*' /proc/cpuinfo | sort -u | tr '\n' ' ' || true)
if echo "$CPU_FLAGS" | grep -q 'avx512f'; then
    CPU_LEVEL="AVX-512"
elif echo "$CPU_FLAGS" | grep -q 'avx2'; then
    CPU_LEVEL="AVX2"
elif echo "$CPU_FLAGS" | grep -q 'avx'; then
    CPU_LEVEL="AVX"
else
    CPU_LEVEL="baseline"
fi
ok "CPU: $CPU_LEVEL detected (GGML_NATIVE=ON will use all available features)"

# ── 0c. RAM-based model selection ────────────────────────────────────────────
# Use MB arithmetic to handle the 4.5 GB (4608 MB) reservation precisely
TOTAL_RAM_MB=$(free -m | awk '/^Mem:/{print $2}')
RESERVE_MB=4608   # keep 4.5 GB free for OS + Docker containers
USABLE_RAM_MB=$(( TOTAL_RAM_MB - RESERVE_MB ))

if [ "$USABLE_RAM_MB" -ge 9216 ]; then   # ≥ 9 GB usable
    MODEL_FILE="Qwen2.5-7B-Instruct-Q8_0.gguf"
    MODEL_SIZE="~8.5 GB"
    MODEL_LABEL="Q8_0 (near-lossless)"
elif [ "$USABLE_RAM_MB" -ge 6144 ]; then  # ≥ 6 GB usable
    MODEL_FILE="Qwen2.5-7B-Instruct-Q6_K.gguf"
    MODEL_SIZE="~6.2 GB"
    MODEL_LABEL="Q6_K (high quality)"
elif [ "$USABLE_RAM_MB" -ge 5120 ]; then  # ≥ 5 GB usable
    MODEL_FILE="Qwen2.5-7B-Instruct-Q5_K_M.gguf"
    MODEL_SIZE="~5.1 GB"
    MODEL_LABEL="Q5_K_M (good quality)"
else
    MODEL_FILE="Qwen2.5-7B-Instruct-Q4_K_M.gguf"
    MODEL_SIZE="~4.4 GB"
    MODEL_LABEL="Q4_K_M (baseline)"
fi
TOTAL_RAM_GB=$(( TOTAL_RAM_MB / 1024 ))
ok "RAM: ${TOTAL_RAM_GB}GB total, 4.5GB reserved → selecting $MODEL_LABEL ($MODEL_SIZE)"

# ── 0d. Python / pip ─────────────────────────────────────────────────────────
command -v python3 &>/dev/null || {
    info "python3 not found — installing..."
    apt-get update -qq 2>&1 | grep -v "^W:" || true
    apt-get install -y -qq python3
}
PY=$(command -v python3)
ok "Python: $($PY --version)"

if ! $PY -m pip --version &>/dev/null; then
    info "pip not found — trying python3-pip..."
    apt-get update -qq 2>&1 | grep -v "^W:" || true
    if apt-get install -y -qq python3-pip 2>/dev/null; then
        ok "python3-pip installed."
    elif $PY -m ensurepip --upgrade 2>/dev/null; then
        ok "pip bootstrapped via ensurepip."
    else
        info "Trying get-pip.py fallback..."
        curl -sS https://bootstrap.pypa.io/get-pip.py | $PY
        ok "pip installed via get-pip.py."
    fi
fi
ok "pip: $($PY -m pip --version)"

# ── 0e. Disk space check ─────────────────────────────────────────────────────
AVAIL_KB=$(df -k "$MODEL_DIR" 2>/dev/null | awk 'NR==2{print $4}' || df -k / | awk 'NR==2{print $4}')
AVAIL_GB=$(echo "$AVAIL_KB / 1048576" | bc 2>/dev/null || echo "?")
if [ "$AVAIL_GB" != "?" ] && [ "$AVAIL_GB" -lt 10 ] 2>/dev/null; then
    warn "Only ${AVAIL_GB}GB disk free — download may fail for larger quantizations."
fi
ok "Disk space: ${AVAIL_GB}GB available."

# ── 1. llama.cpp ──────────────────────────────────────────────────────────────
NEED_BUILD=false
if ! command -v llama-server &>/dev/null; then
    NEED_BUILD=true
    info "[1/4] llama-server not found — building from source..."
elif [ ! -f "$BUILD_MARKER" ]; then
    NEED_BUILD=true
    info "[1/4] Existing llama-server was not built with native CPU optimizations — rebuilding for $CPU_LEVEL..."
    rm -f /usr/local/bin/llama-server
else
    LLAMA_BIN=$(command -v llama-server)
    ok "[1/4] llama-server already built with native optimizations → $LLAMA_BIN"
fi

if [ "$NEED_BUILD" = true ]; then
    apt-get update -qq 2>&1 | grep -v "^W:" || true
    apt-get install -y -qq build-essential cmake git curl

    if [ ! -d "$BUILD_DIR" ]; then
        git clone --depth 1 https://github.com/ggerganov/llama.cpp "$BUILD_DIR"
    else
        git -C "$BUILD_DIR" pull --ff-only || warn "git pull skipped (local changes?)"
    fi

    # GGML_NATIVE=ON → compiler uses -march=native, enables all CPU features
    # (AVX512, AVX2, FMA, F16C, etc.) automatically
    cmake -S "$BUILD_DIR" -B "$BUILD_DIR/build" \
        -DLLAMA_CURL=OFF \
        -DCMAKE_BUILD_TYPE=Release \
        -DGGML_NATIVE=ON
    cmake --build "$BUILD_DIR/build" --target llama-server -j"$THREADS"
    ln -sf "$BUILD_DIR/build/bin/llama-server" /usr/local/bin/llama-server
    touch "$BUILD_MARKER"
    LLAMA_BIN=/usr/local/bin/llama-server
    ok "[1/4] llama-server built with $CPU_LEVEL optimizations → $LLAMA_BIN"
fi

# ── 2. Download model ─────────────────────────────────────────────────────────
mkdir -p "$MODEL_DIR"

# Check if the optimal model is already present; if a lower-quality one exists,
# offer to upgrade (but don't delete automatically — user may want to keep it).
if [ -f "$MODEL_DIR/$MODEL_FILE" ]; then
    ok "[2/4] Model already present: $MODEL_FILE — skipping download"
else
    # Check if a lower-quality model exists and warn
    for OLD in Q4_K_M Q5_K_M Q6_K Q8_0; do
        OLD_FILE="$MODEL_DIR/Qwen2.5-7B-Instruct-${OLD}.gguf"
        if [ -f "$OLD_FILE" ] && [ "$OLD_FILE" != "$MODEL_DIR/$MODEL_FILE" ]; then
            warn "Lower-quality model found: $OLD_FILE"
            warn "Downloading better model ($MODEL_LABEL). Old file kept — remove manually to free space."
        fi
    done

    info "[2/4] Downloading $MODEL_FILE ($MODEL_SIZE) ..."

    if ! $PY -c "import huggingface_hub" 2>/dev/null; then
        info "Installing huggingface_hub..."
        $PY -m pip install -q huggingface_hub || \
            $PY -m pip install -q --break-system-packages huggingface_hub || \
            error "Failed to install huggingface_hub."
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

    [ -f "$MODEL_DIR/$MODEL_FILE" ] || error "Model file not found after download."
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
Description=Qwen2.5-7B Inference Server (llama.cpp — $CPU_LEVEL / $MODEL_LABEL)
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

    if ss -tlnp 2>/dev/null | grep -q ":${PORT} " && ! systemctl is-active --quiet qwen-server 2>/dev/null; then
        warn "Port $PORT already in use by another process. Check: ss -tlnp | grep $PORT"
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
echo "  CPU optimizations : $CPU_LEVEL"
echo "  Model             : $MODEL_LABEL"
echo " Test: curl http://localhost:${PORT}/health"
echo "══════════════════════════════════════════════════"
