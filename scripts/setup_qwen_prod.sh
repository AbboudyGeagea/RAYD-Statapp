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

MODEL_DIR="/home/stats/Qwen"
MODEL_FILE="Qwen2.5-7B-Instruct-Q4_K_M.gguf"
MODEL_REPO="bartowski/Qwen2.5-7B-Instruct-GGUF"
PORT=8081
CTX=4096
THREADS=$(nproc)

echo "══════════════════════════════════════════════════"
echo " Qwen2.5-7B Production Setup"
echo "══════════════════════════════════════════════════"

# ── 1. llama.cpp ──────────────────────────────────────────────────────────────
if command -v llama-server &>/dev/null; then
    LLAMA_BIN=$(command -v llama-server)
    echo "[1/4] llama-server found at $LLAMA_BIN — skipping build"
else
    echo "[1/4] llama-server not found — building llama.cpp from source..."
    apt-get update -qq
    apt-get install -y -qq build-essential cmake git

    BUILD_DIR="/opt/llama.cpp"
    if [ ! -d "$BUILD_DIR" ]; then
        git clone --depth 1 https://github.com/ggerganov/llama.cpp "$BUILD_DIR"
    else
        git -C "$BUILD_DIR" pull --ff-only
    fi

    cmake -S "$BUILD_DIR" -B "$BUILD_DIR/build" -DLLAMA_CURL=OFF -DCMAKE_BUILD_TYPE=Release
    cmake --build "$BUILD_DIR/build" --target llama-server -j"$THREADS"
    ln -sf "$BUILD_DIR/build/bin/llama-server" /usr/local/bin/llama-server
    LLAMA_BIN=/usr/local/bin/llama-server
    echo "[1/4] llama-server built → $LLAMA_BIN"
fi

# ── 2. Download model ─────────────────────────────────────────────────────────
mkdir -p "$MODEL_DIR"

if [ -f "$MODEL_DIR/$MODEL_FILE" ]; then
    echo "[2/4] Model already present at $MODEL_DIR/$MODEL_FILE — skipping download"
else
    echo "[2/4] Downloading $MODEL_FILE (~4.4 GB) ..."

    # Install huggingface_hub if needed
    if ! python3 -c "import huggingface_hub" 2>/dev/null; then
        python3 -m pip install -q huggingface_hub
    fi

    python3 - <<PYEOF
from huggingface_hub import hf_hub_download
path = hf_hub_download(
    repo_id="$MODEL_REPO",
    filename="$MODEL_FILE",
    local_dir="$MODEL_DIR",
)
print(f"Saved to: {path}")
PYEOF
fi

# ── 3. Systemd service ────────────────────────────────────────────────────────
echo "[3/4] Installing systemd service: qwen-server"

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

# ── 4. Start ──────────────────────────────────────────────────────────────────
echo "[4/4] Starting qwen-server..."
systemctl restart qwen-server
sleep 3
systemctl status qwen-server --no-pager

echo ""
echo "══════════════════════════════════════════════════"
echo " Done. Qwen2.5-7B serving on port $PORT"
echo " Test: curl http://localhost:$PORT/health"
echo "══════════════════════════════════════════════════"
