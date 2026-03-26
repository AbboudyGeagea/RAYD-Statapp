"""
routes/bitnet_service.py
────────────────────────────────────────────────────────────────
RAYD × BitNet — Local AI Service
Calls llama-server HTTP API (model kept warm in memory).

Model: Meta-Llama-3.1-8B-Instruct-Q4_K_M (~5GB RAM, CPU-only)

── PRODUCTION (/opt/bitnet) ───────────────────────────────────
1. Download model (run once):
     wget https://huggingface.co/bartowski/Meta-Llama-3.1-8B-Instruct-GGUF/resolve/main/Meta-Llama-3.1-8B-Instruct-Q4_K_M.gguf \
       -P /opt/bitnet/models/

2. Install systemd service (auto-start on boot):
     sudo cp llama-server.service /etc/systemd/system/
     sudo systemctl daemon-reload
     sudo systemctl enable llama-server
     sudo systemctl start llama-server

3. Check status:
     sudo systemctl status llama-server
     curl http://127.0.0.1:8081/health

4. docker-compose.yml env variable:
     BITNET_SERVER=http://172.18.0.1:8081

── TEST SERVER (/home/stats/BitNet) ──────────────────────────
1. Download model (run once):
     wget https://huggingface.co/bartowski/Meta-Llama-3.1-8B-Instruct-GGUF/resolve/main/Meta-Llama-3.1-8B-Instruct-Q4_K_M.gguf \
       -P /home/stats/BitNet/models/

2. Start manually:
     nohup /home/stats/BitNet/build/bin/llama-server \
       -m /home/stats/BitNet/models/Meta-Llama-3.1-8B-Instruct-Q4_K_M.gguf \
       -t 4 --host 0.0.0.0 --port 8081 -c 2048 > /tmp/llama-server.log 2>&1 &

3. Check it's running:
     curl http://127.0.0.1:8081/health

Register in registry.py:
    from routes.bitnet_service import bitnet_bp
    app.register_blueprint(bitnet_bp)
"""

import requests
import logging
import json
import os
import time
import hashlib
import psutil
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

# ── Caches ────────────────────────────────────────────────────
_context_cache   = {"data": None, "ts": 0}   # DB context, refreshed every 60s
_response_cache  = {}                         # identical questions, max 100 entries
CONTEXT_TTL      = 60    # seconds
RESPONSE_TTL     = 300   # seconds (5 min)


def _run_inference(system: str, user: str, max_tokens: int = None) -> str:
    """
    Call llama-server /v1/chat/completions endpoint.
    Uses OpenAI-compatible API — chat template is applied automatically by llama.cpp.
    """
    url = f"{BITNET_SERVER}/v1/chat/completions"
    payload = {
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ],
        "max_tokens":     max_tokens or MAX_TOKENS,
        "temperature":    0.4,
        "top_p":          0.9,
        "repeat_penalty": 1.1,
        "stream":         False,
    }
    try:
        resp = requests.post(url, json=payload, timeout=TIMEOUT_SECS)
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"].strip()
    except requests.exceptions.ConnectionError:
        return "ERROR: llama-server not running. Start it with: /home/stats/BitNet/build/bin/llama-server -m /home/stats/BitNet/models/Meta-Llama-3.1-8B-Instruct-Q4_K_M.gguf -t 4 --host 0.0.0.0 --port 8081 -c 4096 &"
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


# ── CPU usage per core ────────────────────────────────────────
@bitnet_bp.route("/ai/cpu")
@login_required
def cpu_usage():
    cores = psutil.cpu_percent(interval=0.2, percpu=True)
    return jsonify({"cores": cores, "total": psutil.cpu_percent()})


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
            "hint":   "Run: /home/stats/BitNet/build/bin/llama-server -m /home/stats/BitNet/models/Meta-Llama-3.1-8B-Instruct-Q4_K_M.gguf -t 4 --host 0.0.0.0 --port 8081 -c 4096 &"
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

    # Response cache — skip inference for identical questions
    cache_key = hashlib.md5(message.lower().strip().encode()).hexdigest()
    now = time.time()
    if cache_key in _response_cache:
        entry = _response_cache[cache_key]
        if (now - entry["ts"]) < RESPONSE_TTL:
            return jsonify({"response": entry["response"], "context_used": True, "cached": True})

    # Evict oldest entries if cache too large
    if len(_response_cache) > 100:
        oldest = min(_response_cache, key=lambda k: _response_cache[k]["ts"])
        del _response_cache[oldest]

    arabic_chars = sum(1 for c in message if '\u0600' <= c <= '\u06ff')
    lang_rule = "Reply in Arabic." if arabic_chars > 3 else "Reply in English."

    system = (
        "You are RAYD AI, a radiology department analytics assistant. "
        f"{lang_rule} "
        "You have access to live hospital data provided below. "
        "Give accurate, concise, professional answers based on that data. "
        "Do not invent numbers. Do not repeat the question."
    )
    user = f"Data:\n{context}\n\nQuestion: {message}"

    response = _run_inference(system, user, max_tokens=150)
    _response_cache[cache_key] = {"response": response, "ts": now}
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

    system = (
        "You are a senior radiology department analyst. "
        "Write a concise 3-paragraph executive summary in English. "
        "Focus on key trends, anomalies, and actionable insights."
    )
    user = f"Statistics:\n{stats_str}"

    narrative_text = _run_inference(system, user, max_tokens=400)
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
        system = "اكتب رسالة واتساب قصيرة وودية باللغة العربية فقط لإرسال بيانات دخول بوابة نتائج الأشعة."
        user = (
            f"اسم المريض: {patient}\nالمستشفى: {hospital}\n"
            f"الإجراء: {proc}\nاسم المستخدم: {username}\nكلمة المرور: {password}"
        )
    else:
        system = "Write a short, friendly WhatsApp message in English to deliver radiology portal login credentials to a patient."
        user = (
            f"Patient: {patient}\nHospital: {hospital}\n"
            f"Procedure: {proc}\nUsername: {username}\nPassword: {password}"
        )

    message_text = _run_inference(system, user, max_tokens=200)
    return jsonify({"message": message_text})


# ── Predefined Queries ────────────────────────────────────────
# Add your own queries here. Each entry:
#   "keywords" : list of trigger words (question is lowercased before matching)
#   "label"    : how the result is introduced to the model
#   "sql"      : the query to run (must return rows via .mappings().fetchall())
#   "always"   : if True, runs on every question regardless of keywords
#
PREDEFINED_QUERIES = [
    {
        "always": True,
        "label": "Department overview",
        "sql": """
            SELECT COUNT(*) AS total_studies,
                   COUNT(DISTINCT storing_ae) AS total_aes,
                   MIN(study_date) AS earliest,
                   MAX(study_date) AS latest
            FROM etl_didb_studies
        """,
        "format": lambda rows: (
            f"Total studies: {rows[0]['total_studies']}, "
            f"{rows[0]['total_aes']} active AEs, "
            f"data from {rows[0]['earliest']} to {rows[0]['latest']}."
        ) if rows and rows[0]['total_studies'] else None,
    },
    {
        "always": True,
        "label": "AE titles",
        "sql": "SELECT modality, aetitle FROM aetitle_modality_map ORDER BY modality, aetitle",
        "format": lambda rows: "AE titles: " + ", ".join([f"{r['aetitle']} ({r['modality']})" for r in rows]) if rows else None,
    },
    {
        "keywords": ["tat", "turnaround", "wait", "delay", "وقت", "انتظار", "تأخير"],
        "label": "Turnaround time (TAT) by modality — last 30 days",
        "sql": """
            SELECT
                COALESCE(UPPER(m.modality), 'N/A') AS modality,
                ROUND(AVG(EXTRACT(EPOCH FROM (s.rep_final_timestamp - s.study_date)) / 60)::numeric, 0) AS avg_tat_min,
                ROUND(AVG(EXTRACT(EPOCH FROM (s.rep_final_timestamp - s.study_date)) / 3600)::numeric, 1) AS avg_tat_hours,
                COUNT(*) AS studies
            FROM etl_didb_studies s
            LEFT JOIN aetitle_modality_map m ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
            WHERE s.study_date >= CURRENT_DATE - INTERVAL '30 days'
              AND s.rep_final_timestamp IS NOT NULL
              AND s.rep_final_signed_by IS NOT NULL
            GROUP BY m.modality
            ORDER BY avg_tat_min DESC
        """,
        "format": lambda rows: "TAT last 30 days: " + ", ".join([
            f"{r['modality']}: {r['avg_tat_min']} min avg ({r['studies']} studies)" for r in rows
        ]) if rows else None,
    },
    {
        "keywords": ["storage", "gb", "disk", "space", "تخزين", "مساحة"],
        "label": "Storage last 7 days",
        "sql": """
            SELECT study_date, ROUND(SUM(total_gb)::numeric, 2) AS gb
            FROM summary_storage_daily
            GROUP BY study_date ORDER BY study_date DESC LIMIT 7
        """,
        "format": lambda rows: "Daily storage (GB): " + ", ".join([f"{r['study_date']}: {r['gb']}GB" for r in rows]) if rows else None,
    },
    {
        "keywords": ["modality", "ct", "mr", "mri", "xray", "x-ray", "us", "ultrasound", "أشعة", "modalities"],
        "label": "Studies by modality",
        "sql": """
            SELECT study_modality, COUNT(*) AS cnt
            FROM etl_didb_studies
            WHERE study_modality IS NOT NULL
            GROUP BY study_modality ORDER BY cnt DESC LIMIT 10
        """,
        "format": lambda rows: "Studies by modality: " + ", ".join([f"{r['study_modality']}: {r['cnt']}" for r in rows]) if rows else None,
    },
    {
        "keywords": ["today", "اليوم"],
        "label": "Today's activity",
        "sql": """
            SELECT study_modality, COUNT(*) AS cnt
            FROM etl_didb_studies
            WHERE study_date = CURRENT_DATE
            GROUP BY study_modality ORDER BY cnt DESC
        """,
        "format": lambda rows: "Today's studies: " + ", ".join([f"{r['study_modality']}: {r['cnt']}" for r in rows]) if rows else "No studies recorded today yet.",
    },
    {
        "keywords": ["yesterday", "أمس"],
        "label": "Yesterday's activity",
        "sql": """
            SELECT study_modality, COUNT(*) AS cnt
            FROM etl_didb_studies
            WHERE study_date = CURRENT_DATE - INTERVAL '1 day'
            GROUP BY study_modality ORDER BY cnt DESC
        """,
        "format": lambda rows: "Yesterday's studies: " + ", ".join([f"{r['study_modality']}: {r['cnt']}" for r in rows]) if rows else None,
    },
    {
        "keywords": ["order", "schedule", "pending", "orphan", "طلب", "جدول"],
        "label": "Orders summary",
        "sql": """
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE has_study = true)  AS fulfilled,
                   COUNT(*) FILTER (WHERE has_study = false) AS orphaned
            FROM etl_orders
        """,
        "format": lambda rows: (
            f"Orders: {rows[0]['total']} total, {rows[0]['fulfilled']} fulfilled, {rows[0]['orphaned']} orphaned."
        ) if rows else None,
    },
    {
        "keywords": ["busy", "peak", "volume", "most", "highest", "أكثر", "ازدحام"],
        "label": "Busiest days this month",
        "sql": """
            SELECT study_date, COUNT(*) AS cnt
            FROM etl_didb_studies
            WHERE study_date >= DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY study_date ORDER BY cnt DESC LIMIT 5
        """,
        "format": lambda rows: "Busiest days this month: " + ", ".join([f"{r['study_date']}: {r['cnt']} studies" for r in rows]) if rows else None,
    },
    {
        "keywords": ["week", "weekly", "this week", "أسبوع"],
        "label": "This week by modality",
        "sql": """
            SELECT study_modality, COUNT(*) AS cnt
            FROM etl_didb_studies
            WHERE study_date >= DATE_TRUNC('week', CURRENT_DATE)
            GROUP BY study_modality ORDER BY cnt DESC
        """,
        "format": lambda rows: "This week's studies: " + ", ".join([f"{r['study_modality']}: {r['cnt']}" for r in rows]) if rows else None,
    },
    {
        "keywords": ["month", "monthly", "this month", "شهر"],
        "label": "This month by modality",
        "sql": """
            SELECT study_modality, COUNT(*) AS cnt
            FROM etl_didb_studies
            WHERE study_date >= DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY study_modality ORDER BY cnt DESC
        """,
        "format": lambda rows: "This month's studies: " + ", ".join([f"{r['study_modality']}: {r['cnt']}" for r in rows]) if rows else None,
    },
]


# ── Proactive Anomaly Alerts ──────────────────────────────────
@bitnet_bp.route("/ai/alerts")
@login_required
def alerts():
    """
    Data-driven anomaly detection — no AI inference, pure SQL comparisons.
    Checks TAT spikes, storage growth anomalies, and device utilization outliers.
    Intended for a dashboard badge or notification widget.
    """
    findings = []
    try:
        with db.engine.connect() as conn:

            # 1. TAT spike: this week vs prior 4-week rolling average
            tat_rows = conn.execute(text("""
                WITH weekly AS (
                    SELECT DATE_TRUNC('week', study_date) AS wk,
                           ROUND(AVG(EXTRACT(EPOCH FROM (rep_final_timestamp - study_date)) / 60)::numeric, 1) AS avg_tat
                    FROM etl_didb_studies
                    WHERE study_date >= CURRENT_DATE - INTERVAL '5 weeks'
                      AND rep_final_timestamp IS NOT NULL
                    GROUP BY 1 ORDER BY 1 DESC
                )
                SELECT wk, avg_tat FROM weekly LIMIT 5
            """)).fetchall()
            if len(tat_rows) >= 2:
                cur_tat = float(tat_rows[0][1] or 0)
                baseline = sum(float(r[1] or 0) for r in tat_rows[1:]) / len(tat_rows[1:])
                if baseline > 0:
                    pct = (cur_tat - baseline) / baseline * 100
                    if abs(pct) >= 20:
                        arrow = "↑" if pct > 0 else "↓"
                        findings.append({
                            "type": "tat",
                            "severity": "high" if abs(pct) >= 30 else "medium",
                            "msg": f"TAT {arrow} {abs(pct):.0f}% this week ({cur_tat:.0f}m) vs 4-week baseline ({baseline:.0f}m)"
                        })

            # 2. Storage growth anomaly: this week vs previous week
            stor_rows = {r[0]: float(r[1] or 0) for r in conn.execute(text("""
                SELECT
                    CASE WHEN study_date >= CURRENT_DATE - 7 THEN 'current' ELSE 'prior' END AS period,
                    ROUND(SUM(total_gb)::numeric, 2) AS gb
                FROM summary_storage_daily
                WHERE study_date >= CURRENT_DATE - 14
                GROUP BY 1
            """)).fetchall()}
            cur_gb  = stor_rows.get('current', 0)
            prev_gb = stor_rows.get('prior', 0)
            if prev_gb > 0:
                stor_pct = (cur_gb - prev_gb) / prev_gb * 100
                if stor_pct >= 25:
                    findings.append({
                        "type": "storage",
                        "severity": "high" if stor_pct >= 50 else "medium",
                        "msg": f"Storage ingestion ↑ {stor_pct:.0f}% this week ({cur_gb:.1f} GB) vs last week ({prev_gb:.1f} GB)"
                    })

            # 3. Device volume spike: AEs with this-week count > prior avg + 2σ
            for r in conn.execute(text("""
                WITH weekly_ae AS (
                    SELECT storing_ae,
                           DATE_TRUNC('week', study_date) AS wk,
                           COUNT(*) AS cnt
                    FROM etl_didb_studies
                    WHERE study_date >= CURRENT_DATE - INTERVAL '6 weeks'
                      AND storing_ae IS NOT NULL
                    GROUP BY 1, 2
                ),
                stats AS (
                    SELECT storing_ae,
                           AVG(cnt)    AS avg_cnt,
                           STDDEV(cnt) AS std_cnt
                    FROM weekly_ae
                    WHERE wk < DATE_TRUNC('week', CURRENT_DATE)
                    GROUP BY storing_ae
                ),
                this_week AS (
                    SELECT storing_ae, cnt
                    FROM weekly_ae
                    WHERE wk = DATE_TRUNC('week', CURRENT_DATE)
                )
                SELECT t.storing_ae, t.cnt, s.avg_cnt, s.std_cnt
                FROM this_week t
                JOIN stats s ON t.storing_ae = s.storing_ae
                WHERE s.std_cnt > 0 AND t.cnt > s.avg_cnt + 2 * s.std_cnt
                ORDER BY (t.cnt - s.avg_cnt) / s.std_cnt DESC
                LIMIT 3
            """)).fetchall():
                ae, cnt, avg, std = r
                z = (float(cnt) - float(avg)) / float(std)
                findings.append({
                    "type": "utilization",
                    "severity": "medium",
                    "msg": f"{ae}: {cnt} studies this week vs avg {avg:.0f} (z = {z:.1f}σ above normal)"
                })

    except Exception as e:
        logger.error(f"[BitNet] Alerts error: {e}")
        return jsonify({"alerts": [], "error": str(e)})

    return jsonify({"alerts": findings, "count": len(findings), "clean": len(findings) == 0})


# ── DB Context Builder ────────────────────────────────────────
def _build_db_context(question: str) -> str:
    """
    Runs matching predefined queries based on keywords in the question.
    Always-on queries are cached for CONTEXT_TTL seconds.
    Add new queries to PREDEFINED_QUERIES above.
    """
    ctx_parts = []
    q = question.lower()
    now = time.time()

    try:
        with db.engine.connect() as conn:
            for entry in PREDEFINED_QUERIES:
                always   = entry.get("always", False)
                keywords = entry.get("keywords", [])

                if not always and not any(w in q for w in keywords):
                    continue

                # Use cache for always-on queries
                if always:
                    cache_key = entry["label"]
                    cached = _context_cache.get(cache_key)
                    if cached and (now - cached["ts"]) < CONTEXT_TTL:
                        if cached["result"]:
                            ctx_parts.append(cached["result"])
                        continue
                    rows = conn.execute(text(entry["sql"])).mappings().fetchall()
                    result = entry["format"](list(rows))
                    _context_cache[cache_key] = {"result": result, "ts": now}
                else:
                    rows = conn.execute(text(entry["sql"])).mappings().fetchall()
                    result = entry["format"](list(rows))

                if result:
                    ctx_parts.append(result)

    except Exception as e:
        logger.error(f"[BitNet] Context build error: {e}", exc_info=True)
        ctx_parts.append(f"Note: Could not fetch live data ({str(e)[:100]})")

    return "\n".join(ctx_parts) if ctx_parts else "No specific context available."
