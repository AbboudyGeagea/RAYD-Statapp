"""
routes/bitnet_service.py
────────────────────────────────────────────────────────────────
RAYD × BitNet — Local AI Service
Calls llama-server HTTP API (model kept warm in memory).

Start llama-server on the host before using:
  cd /home/stats/BitNet
  nohup ./build/bin/llama-server \
    -m models/BitNet-b1.58-2B-4T/ggml-model-i2_s.gguf \
    -t 4 --host 0.0.0.0 --port 8081 -c 2048 &

Register in registry.py:
    from routes.bitnet_service import bitnet_bp
    app.register_blueprint(bitnet_bp)
"""

import requests
import logging
import json
import os
from flask import Blueprint, request, jsonify, render_template
from flask_login import login_required
from sqlalchemy import text
from db import db

logger    = logging.getLogger("BITNET")
bitnet_bp = Blueprint("bitnet", __name__)

# ── Config ────────────────────────────────────────────────────
# llama-server runs on the host; container reaches it via host IP
BITNET_SERVER = os.environ.get("BITNET_SERVER", "http://172.17.0.1:8081")
MAX_TOKENS    = int(os.environ.get("BITNET_TOKENS", "512"))
TIMEOUT_SECS  = int(os.environ.get("BITNET_TIMEOUT", "120"))


def _run_inference(prompt: str, max_tokens: int = None) -> str:
    """
    Call llama-server /completion endpoint.
    Model is already loaded — no cold start, 3-5s per response.
    """
    url = f"{BITNET_SERVER}/completion"
    payload = {
        "prompt":           prompt,
        "n_predict":        max_tokens or MAX_TOKENS,
        "temperature":      0.6,
        "top_p":            0.9,
        "top_k":            40,
        "repeat_penalty":   1.15,
        "stop":             ["###", "[/INST]", "[INST]", "### Question", "### System", "User:"],
        "stream":           False,
    }
    try:
        resp = requests.post(url, json=payload, timeout=TIMEOUT_SECS)
        resp.raise_for_status()
        data = resp.json()
        return data.get("content", "").strip()
    except requests.exceptions.ConnectionError:
        return "ERROR: llama-server not running. Start it with: nohup ./build/bin/llama-server -m models/BitNet-b1.58-2B-4T/ggml-model-i2_s.gguf -t 4 --host 0.0.0.0 --port 8081 -c 2048 &"
    except requests.exceptions.Timeout:
        return "ERROR: Inference timeout — query too complex, try simplifying."
    except Exception as e:
        logger.error(f"[BitNet] Inference error: {e}")
        return f"ERROR: {str(e)}"


# ── Page ──────────────────────────────────────────────────────
@bitnet_bp.route("/ai/assistant")
@login_required
def assistant_page():
    return render_template("ai_assistant.html")


@bitnet_bp.route("/ai/context-debug")
@login_required
def context_debug():
    """Debug endpoint — shows what context would be sent for a given question."""
    q = request.args.get("q", "how many modalities")
    ctx = _build_db_context(q)
    return jsonify({"question": q, "context": ctx})


# ── Health check ──────────────────────────────────────────────
@bitnet_bp.route("/ai/health")
@login_required
def health():
    try:
        resp = requests.get(f"{BITNET_SERVER}/health", timeout=5)
        data = resp.json()
        return jsonify({
            "server":  BITNET_SERVER,
            "status":  data.get("status", "unknown"),
            "ready":   data.get("status") == "ok",
            "mode":    "llama-server (persistent)",
        })
    except Exception as e:
        return jsonify({
            "server": BITNET_SERVER,
            "ready":  False,
            "error":  str(e),
            "hint":   "Run: nohup ./build/bin/llama-server -m models/BitNet-b1.58-2B-4T/ggml-model-i2_s.gguf -t 4 --host 0.0.0.0 --port 8081 -c 2048 &"
        })


# ── Chat endpoint ─────────────────────────────────────────────
@bitnet_bp.route("/ai/chat", methods=["POST"])
@login_required
def chat():
    """
    Conversational Q&A grounded in live RAYD data.
    Fetches relevant DB context then passes to BitNet.
    """
    body    = request.get_json(force=True)
    message = (body.get("message") or "").strip()
    if not message:
        return jsonify({"error": "No message provided"}), 400

    # Fetch live context from PG to ground the answer
    context = _build_db_context(message)

    system = (
        "You are RAYD AI, a radiology department analytics assistant. "
        "You have access to live hospital data. "
        "Be concise, factual, and professional. "
        "Use the data in the context to give accurate answers. "
        "Answer in the same language the user writes in. "
        "Do not repeat the question. Do not make up numbers."
    )

    prompt = (
        f"### System:\n{system}\n\n"
        f"### Data:\n{context}\n\n"
        f"### Question:\n{message}\n\n"
        f"### Answer:\n"
    )

    raw = _run_inference(prompt, max_tokens=300)

    # Strip echoed prompt fragments and llama format artifacts
    # Cut at the first sign of repetition or prompt echo
    stop_markers = [
        "### Answer:", "### Question:", "### Context:", "### System:",
        "<</SYS>>", "[/INST]", "[INST]", "<<SYS>>",
        "### المهمة", "How many studies", "How many AEs",
    ]
    output = raw.strip()
    # Take only content before the first stop marker
    for marker in stop_markers:
        if marker in output:
            output = output.split(marker)[0].strip()

    # If output is empty after stripping, return a fallback
    if not output:
        output = "I found your department data but couldn't generate a clean response. Please try rephrasing."

    response = output
    return jsonify({"response": response, "context_used": bool(context)})


# ── Narrative endpoint ────────────────────────────────────────
@bitnet_bp.route("/ai/narrative", methods=["POST"])
@login_required
def narrative():
    """
    Generate an executive narrative from Super Report JSON stats.
    Called by super_report.py to replace the rule-based narrative.
    """
    body  = request.get_json(force=True)
    stats = body.get("stats", {})
    if not stats:
        return jsonify({"error": "No stats provided"}), 400

    stats_str = json.dumps(stats, indent=2)

    prompt = (
        f"### System:\nYou are a senior radiology department analyst. "
        f"Write a concise 3-paragraph executive summary. "
        f"Focus on key trends, anomalies, and actionable insights.\n\n"
        f"### Statistics:\n{stats_str}\n\n"
        f"### Executive Summary:\n"
    )

    narrative_text = _run_inference(prompt, max_tokens=400)
    # Strip echoed markers
    if "### Executive Summary:" in narrative_text:
        narrative_text = narrative_text.split("### Executive Summary:")[-1].strip()
    return jsonify({"narrative": narrative_text})


# ── WhatsApp message generator ────────────────────────────────
@bitnet_bp.route("/ai/whatsapp", methods=["POST"])
@login_required
def whatsapp_message():
    """
    Generate a personalized Arabic/English WhatsApp message
    for patient portal credential delivery.
    """
    body     = request.get_json(force=True)
    patient  = body.get("patient_name", "")
    hospital = body.get("hospital_name", "المستشفى")
    username = body.get("username", "")
    password = body.get("password", "")
    language = body.get("language", "ar")   # 'ar' or 'en'
    proc     = body.get("procedure", "")

    if language == "ar":
        prompt = (
            f"### المهمة:\n"
            f"اكتب رسالة واتساب قصيرة وودية باللغة العربية لإرسال بيانات دخول بوابة نتائج الأشعة.\n\n"
            f"### المعلومات:\n"
            f"اسم المريض: {patient}\n"
            f"المستشفى: {hospital}\n"
            f"الإجراء: {proc}\n"
            f"اسم المستخدم: {username}\n"
            f"كلمة المرور: {password}\n\n"
            f"### الرسالة:\n"
        )
    else:
        prompt = (
            f"### Task:\n"
            f"Write a short WhatsApp message to send radiology portal login credentials.\n\n"
            f"### Details:\n"
            f"Patient: {patient}\nHospital: {hospital}\n"
            f"Procedure: {proc}\nUsername: {username}\nPassword: {password}\n\n"
            f"### Message:\n"
        )

    message_text = _run_inference(prompt, max_tokens=200)
    for marker in ["### Message:", "### الرسالة:"]:
        if marker in message_text:
            message_text = message_text.split(marker)[-1].strip()
    return jsonify({"message": message_text})


# ── DB Context Builder ────────────────────────────────────────
def _build_db_context(question: str) -> str:
    """
    Fetch relevant DB stats to ground BitNet's answer.
    Uses engine.connect() for thread safety in Flask requests.
    """
    ctx_parts = []
    q = question.lower()

    try:
        with db.engine.connect() as conn:
            # Always: basic volume summary
            row = conn.execute(text("""
                SELECT COUNT(*) AS total_studies,
                       COUNT(DISTINCT storing_ae) AS total_aes,
                       MIN(study_date) AS earliest,
                       MAX(study_date) AS latest
                FROM etl_didb_studies
            """)).mappings().fetchone()
            if row and int(row['total_studies'] or 0) > 0:
                ctx_parts.append(
                    f"Total studies: {row['total_studies']}, "
                    f"{row['total_aes']} active AEs, "
                    f"from {row['earliest']} to {row['latest']}."
                )

            # Always: AE + modality list
            ae_rows = conn.execute(text("""
                SELECT modality, aetitle
                FROM aetitle_modality_map
                ORDER BY modality, aetitle
            """)).mappings().fetchall()
            if ae_rows:
                ae_lines = ", ".join([f"{r['aetitle']} ({r['modality']})" for r in ae_rows])
                ctx_parts.append(f"Department AE titles: {ae_lines}")

            # Storage
            if any(w in q for w in ['storage', 'gb', 'disk', 'space', 'تخزين', 'مساحة']):
                rows = conn.execute(text("""
                    SELECT study_date, SUM(total_gb) AS gb
                    FROM summary_storage_daily
                    GROUP BY study_date ORDER BY study_date DESC LIMIT 7
                """)).mappings().fetchall()
                if rows:
                    storage_lines = ", ".join([f"{r['study_date']}: {r['gb']}GB" for r in rows])
                    ctx_parts.append(f"Recent daily storage: {storage_lines}")

            # Modality breakdown
            if any(w in q for w in ['modality', 'ct', 'mr', 'mri', 'xray', 'us', 'ultrasound', 'أشعة', 'modalities']):
                rows = conn.execute(text("""
                    SELECT study_modality, COUNT(*) AS cnt
                    FROM etl_didb_studies
                    WHERE study_modality IS NOT NULL
                    GROUP BY study_modality ORDER BY cnt DESC LIMIT 8
                """)).mappings().fetchall()
                if rows:
                    mod_lines = ", ".join([f"{r['study_modality']}: {r['cnt']}" for r in rows])
                    ctx_parts.append(f"Studies by modality: {mod_lines}")

            # Physicians
            if any(w in q for w in ['physician', 'doctor', 'referring', 'طبيب', 'دكتور']):
                rows = conn.execute(text("""
                    SELECT TRIM(CONCAT_WS(' ',
                        referring_physician_first_name,
                        referring_physician_last_name)) AS physician,
                        COUNT(*) AS cnt
                    FROM etl_didb_studies
                    WHERE referring_physician_last_name IS NOT NULL
                    GROUP BY 1 ORDER BY cnt DESC LIMIT 5
                """)).mappings().fetchall()
                if rows:
                    doc_lines = ", ".join([f"{r['physician']}: {r['cnt']}" for r in rows])
                    ctx_parts.append(f"Top referring physicians: {doc_lines}")

            # Orders
            if any(w in q for w in ['order', 'schedule', 'pending', 'طلب', 'جدول']):
                row = conn.execute(text("""
                    SELECT COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE has_study = true)  AS fulfilled,
                           COUNT(*) FILTER (WHERE has_study = false) AS orphaned
                    FROM etl_orders
                """)).mappings().fetchone()
                if row:
                    ctx_parts.append(
                        f"Orders: {row['total']} total, "
                        f"{row['fulfilled']} fulfilled, {row['orphaned']} orphaned."
                    )

    except Exception as e:
        logger.error(f"[BitNet] Context build error: {e}", exc_info=True)
        ctx_parts.append(f"Note: Could not fetch live data ({str(e)[:100]})")

    return "\n".join(ctx_parts) if ctx_parts else "No specific context available."
