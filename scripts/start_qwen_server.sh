#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# start_qwen_server.sh
# Start the Qwen2.5-7B llama-server manually (outside systemd).
# Useful for testing or when systemd is not available.
#
# Usage:  bash scripts/start_qwen_server.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

MODEL_DIR="${MODEL_DIR:-/home/stats/Qwen}"
MODEL_FILE="Qwen2.5-7B-Instruct-Q4_K_M.gguf"
PORT="${QWEN_PORT:-8081}"
THREADS="${THREADS:-$(nproc)}"

if [ ! -f "$MODEL_DIR/$MODEL_FILE" ]; then
    echo "ERROR: Model not found at $MODEL_DIR/$MODEL_FILE"
    echo "Run scripts/setup_qwen_prod.sh first."
    exit 1
fi

echo "Starting Qwen2.5-7B on port $PORT with $THREADS threads..."

exec llama-server \
    --model "$MODEL_DIR/$MODEL_FILE" \
    --ctx-size 4096 \
    --port "$PORT" \
    --host 0.0.0.0 \
    --threads "$THREADS" \
    --n-predict 512 \
    --temp 0.1 \
    --repeat-penalty 1.15 \
    --no-mmap \
    --log-disable
