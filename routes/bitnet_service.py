"""
routes/bitnet_service.py
────────────────────────────────────────────────────────────────
RAYD × Llama 3.1 8B — Anti-Hallucination AI Service  (v3 — final)

Architecture:
  1. Python detects intent from question keywords
  2. Python fetches ALL facts from PostgreSQL (single connection, CTE where possible)
  3. If no data found → return fixed message, skip model entirely
  4. Model ONLY converts structured facts into natural language
  5. Output is scanned via compiled regex for SQL/code/table names → replaced with fallback
  6. Language auto-detected from question
  7. Response cache avoids duplicate inference for identical questions

The model NEVER invents numbers, names, dates, or schema.
"""

import re
import time
import hashlib
import requests
import logging
import json
import os
import psutil
from flask import Blueprint, request, jsonify, render_template, abort, Response, stream_with_context
from flask_login import login_required, current_user
from sqlalchemy import text
from db import db, AiFeedback, AiCorrection

logger    = logging.getLogger("BITNET")
bitnet_bp = Blueprint("bitnet", __name__)

# ── Ensure AI tables exist (runs once) ────────────────────────
_tables_checked = False

def _ensure_ai_tables():
    global _tables_checked
    if _tables_checked:
        return
    try:
        db.session.execute(text("""
            CREATE TABLE IF NOT EXISTS ai_feedback (
                id          SERIAL PRIMARY KEY,
                question    TEXT NOT NULL,
                response    TEXT NOT NULL,
                vote        VARCHAR(10) NOT NULL,
                user_id     INTEGER REFERENCES users(id),
                reviewed    BOOLEAN DEFAULT FALSE,
                created_at  TIMESTAMP DEFAULT NOW()
            )
        """))
        db.session.execute(text("""
            CREATE TABLE IF NOT EXISTS ai_corrections (
                id               SERIAL PRIMARY KEY,
                keywords         TEXT NOT NULL,
                correct_answer   TEXT NOT NULL,
                example_question TEXT,
                created_by       VARCHAR(100),
                is_active        BOOLEAN DEFAULT TRUE,
                created_at       TIMESTAMP DEFAULT NOW()
            )
        """))
        db.session.commit()
        _tables_checked = True
        logger.info("[BitNet] AI tables verified")
    except Exception as e:
        db.session.rollback()
        logger.error(f"[BitNet] Table creation error: {e}")
        _tables_checked = True  # don't retry every request

# ── Config ────────────────────────────────────────────────────
BITNET_SERVER = os.environ.get("BITNET_SERVER", "http://172.17.0.1:8081")
MAX_TOKENS    = int(os.environ.get("BITNET_TOKENS",  "512"))
TIMEOUT_SECS  = int(os.environ.get("BITNET_TIMEOUT", "180"))

# ── Response cache ────────────────────────────────────────────
_response_cache = {}
RESPONSE_TTL    = 300        # 5 min — same question gets cached answer
CACHE_MAX       = 200

# ── Base‑context cache (always-on queries) ────────────────────
_base_cache = {"facts": [], "ts": 0}
BASE_TTL    = 60             # refresh every 60s

# ── Compiled hallucination regex — built once at import ───────
_HALLUCINATION_TOKENS = [
    # Table names
    "etl_didb_studies", "etl_orders", "etl_didb_raw_images",
    "etl_image_locations", "etl_didb_serieses", "etl_patient_view",
    "summary_storage_daily", "aetitle_modality_map", "hl7_orders",
    "device_weekly_schedule", "device_exceptions", "procedure_duration_map",
    # Column names
    "study_db_uid", "storing_ae", "study_modality", "study_date",
    "patient_db_uid", "raw_image_db_uid", "series_db_uid",
    "image_size_kb", "total_gb", "study_count", "order_dbid",
    "scheduled_datetime", "has_study", "proc_id",
    # SQL keywords
    r"SELECT\s", r"FROM\s", r"WHERE\s", r"JOIN\s", "GROUP BY", "ORDER BY",
    r"INSERT\s", r"UPDATE\s", r"DELETE\s", r"CREATE\s", r"ALTER\s",
    r"LIMIT\s", "HAVING", r"UNION\s", r"COUNT\s*\(", r"SUM\s*\(",
]
_HALLUCINATION_RE = re.compile(
    "|".join(_HALLUCINATION_TOKENS) + r"|```|SELECT\n",
    re.IGNORECASE,
)

# Lighter scanner for narrative endpoint — only blocks schema leakage,
# NOT SQL keywords (which appear in normal English: "from", "where", "having", etc.)
_NARRATIVE_SCHEMA_TOKENS = [
    "etl_didb_studies", "etl_orders", "etl_didb_raw_images",
    "etl_image_locations", "etl_didb_serieses", "etl_patient_view",
    "summary_storage_daily", "aetitle_modality_map", "hl7_orders",
    "study_db_uid", "storing_ae", "raw_image_db_uid", "series_db_uid",
    "image_size_kb", "order_dbid", "scheduled_datetime", "has_study",
    r"```", r"SELECT\s+\w+\s+FROM",   # only block SELECT...FROM as a unit
]
_NARRATIVE_RE = re.compile(
    "|".join(_NARRATIVE_SCHEMA_TOKENS),
    re.IGNORECASE,
)

FALLBACK_EN = "I'm sorry, I generated an invalid response. Please rephrase your question."
FALLBACK_AR = "عذراً، لم أتمكن من توليد إجابة صحيحة. يرجى إعادة صياغة سؤالك."

NO_DATA_EN = "I don't have enough data to answer that question. Please check if the ETL has run and data is available."
NO_DATA_AR = "لا تتوفر بيانات كافية للإجابة على هذا السؤال. يرجى التحقق من تشغيل ETL وتوفر البيانات."


# ── Language detection ────────────────────────────────────────
def _is_arabic(txt: str) -> bool:
    arabic = sum(1 for c in txt if '\u0600' <= c <= '\u06FF')
    return arabic > len(txt) * 0.2


# ── Hallucination scanner (compiled regex — O(n) single pass) ─
def _contains_hallucination(response: str) -> bool:
    return bool(_HALLUCINATION_RE.search(response))


# ── Llama 3.1 inference ───────────────────────────────────────
def _run_inference(system: str, user_message: str, max_tokens: int = None) -> str:
    prompt = (
        "<|begin_of_text|>"
        f"<|start_header_id|>system<|end_header_id|>\n{system}<|eot_id|>"
        f"<|start_header_id|>user<|end_header_id|>\n{user_message}<|eot_id|>"
        "<|start_header_id|>assistant<|end_header_id|>\n"
    )

    payload = {
        "prompt":         prompt,
        "n_predict":      max_tokens or MAX_TOKENS,
        "temperature":    0.1,
        "repeat_penalty": 1.15,
        "stop":           [
            "<|eot_id|>", "<|end_of_text|>",
            "<|start_header_id|>", "User:", "Human:", "Question:",
        ],
        "stream": False,
        "seed":   42,
    }

    try:
        resp = requests.post(
            f"{BITNET_SERVER}/completion",
            json=payload,
            timeout=TIMEOUT_SECS,
        )
        resp.raise_for_status()
        return resp.json().get("content", "").strip()
    except requests.exceptions.ConnectionError:
        return "ERROR: llama-server not running on host port 8081."
    except requests.exceptions.Timeout:
        return "ERROR: Inference timeout — try a shorter question."
    except Exception as e:
        logger.error(f"[BitNet] Inference error: {e}")
        return f"ERROR: {e}"


# ── Pages ─────────────────────────────────────────────────────
@bitnet_bp.route("/ai/assistant")
@login_required
def assistant_page():
    return render_template("ai_assistant.html")


# ── CPU usage (polled every 1s by template) ───────────────────
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
            "server": BITNET_SERVER,
            "status": data.get("status", "unknown"),
            "ready":  data.get("status") == "ok",
            "mode":   "llama-server (persistent)",
        })
    except Exception as e:
        return jsonify({"server": BITNET_SERVER, "ready": False, "error": str(e)})


# ── Chat endpoint ─────────────────────────────────────────────
@bitnet_bp.route("/ai/chat", methods=["POST"])
@login_required
def chat():
    _ensure_ai_tables()
    body    = request.get_json(force=True)
    message = (body.get("message") or "").strip()
    if not message:
        return jsonify({"error": "No message provided"}), 400

    arabic       = _is_arabic(message)
    no_data_msg  = NO_DATA_AR  if arabic else NO_DATA_EN
    fallback_msg = FALLBACK_AR if arabic else FALLBACK_EN

    # ── Response cache check ──────────────────────────────────
    cache_key = hashlib.md5(message.lower().encode()).hexdigest()
    now = time.time()
    hit = _response_cache.get(cache_key)
    if hit and (now - hit["ts"]) < RESPONSE_TTL:
        return jsonify(hit["payload"])

    # ── Fetch DB facts ────────────────────────────────────────
    link, link_label, chart_type, chart_data, context_facts = _build_context(message)

    # ── No data → skip model ──────────────────────────────────
    if not context_facts:
        payload = {
            "response": no_data_msg, "context_used": False,
            "chart_type": None, "chart_data": None,
            "link": link, "link_label": link_label,
        }
        return jsonify(payload)

    # ── Fetch matching corrections (few-shot from admin teaching) ─
    corrections_block = _get_corrections(message)

    # ── System prompt ─────────────────────────────────────────
    system = (
        "You are RAYD AI, a radiology analytics assistant. "
        "Your ONLY job is to convert the provided facts into a clear, natural sentence or two. "
        "STRICT RULES:\n"
        "1. Answer in 1-3 sentences maximum.\n"
        "2. Use ONLY the numbers and names from the facts provided. Never invent any number, name, or date.\n"
        "3. NEVER write SQL, code, queries, or anything technical.\n"
        "4. NEVER mention database tables, column names, or technical terms.\n"
        "5. If the facts are empty or unclear, say you don't have enough information.\n"
        "6. Answer in the same language as the question — Arabic if asked in Arabic, English if asked in English.\n"
        "7. Be direct and concise. No greetings, no disclaimers, no filler.\n\n"
        "EXAMPLE:\n"
        "Facts: The department has 45,230 total studies across 12 imaging devices, from 2023-01-01 to 2025-04-07.\n"
        "Question: how many studies do we have?\n"
        "Answer: The department has performed 45,230 studies across 12 imaging devices since January 2023."
    )
    if corrections_block:
        system += "\n\n" + corrections_block

    user_prompt = f"Facts:\n{context_facts}\n\nQuestion: {message}"

    # ── Inference ─────────────────────────────────────────────
    raw = _run_inference(system, user_prompt, max_tokens=200)

    if raw.startswith("ERROR:"):
        response = raw
    elif _contains_hallucination(raw):
        logger.warning(f"[BitNet] Hallucination blocked: {raw[:200]}")
        response = fallback_msg
    else:
        response = raw.strip() or no_data_msg

    payload = {
        "response": response, "context_used": True,
        "chart_type": chart_type, "chart_data": chart_data,
        "link": link, "link_label": link_label,
    }

    # ── Cache successful responses ────────────────────────────
    if not response.startswith("ERROR:"):
        if len(_response_cache) >= CACHE_MAX:
            oldest = min(_response_cache, key=lambda k: _response_cache[k]["ts"])
            del _response_cache[oldest]
        _response_cache[cache_key] = {"payload": payload, "ts": now}

    return jsonify(payload)


# ── Narrative cache (keyed by stats hash, TTL 10 min) ─────────
_narrative_cache: dict = {}
NARRATIVE_TTL = 600

# ── Narrative endpoint ────────────────────────────────────────
@bitnet_bp.route("/ai/narrative", methods=["POST"])
@login_required
def narrative():
    body  = request.get_json(force=True)
    stats = body.get("stats", {})
    if not stats:
        return jsonify({"error": "No stats provided"}), 400

    # Cache check — same stats within 10 min returns instantly
    cache_key = hashlib.md5(json.dumps(stats, sort_keys=True).encode()).hexdigest()
    now = time.time()
    hit = _narrative_cache.get(cache_key)
    if hit and (now - hit["ts"]) < NARRATIVE_TTL:
        return jsonify(hit["payload"])

    facts_lines = [f"- {k.replace('_', ' ').title()}: {v}" for k, v in stats.items()]
    facts = "\n".join(facts_lines)

    system = (
        "You are a radiology department analyst. "
        "Write 3 concise sentences summarising the key findings from the statistics below. "
        "Be direct. Use only the numbers provided. No SQL, no code, no lists."
    )

    raw = _run_inference(system, f"Statistics:\n{facts}\n\nSummary:", max_tokens=150)

    if _NARRATIVE_RE.search(raw):
        logger.warning(f"[BitNet] Schema leak in narrative: {raw[:200]}")
        raw = "Unable to generate narrative — please review the statistics directly."

    payload = {"narrative": raw}
    _narrative_cache[cache_key] = {"payload": payload, "ts": now}
    return jsonify(payload)


# ── Narrative streaming endpoint ──────────────────────────────
@bitnet_bp.route("/ai/narrative/stream", methods=["POST"])
@login_required
def narrative_stream():
    """
    SSE endpoint — runs inference synchronously (proven path), then streams
    the result word-by-word for the typewriter effect.
    Yields:  data: {"token": "word "}\n\n
    Ends:    data: [DONE]\n\n
    Errors:  data: {"error": "message"}\n\n  then [DONE]
    Cache hit: replays stored text instantly (same typewriter feel).
    """
    body  = request.get_json(force=True)
    stats = body.get("stats", {})
    if not stats:
        def _no_stats():
            yield 'data: {"error": "No stats provided"}\n\n'
            yield 'data: [DONE]\n\n'
        return Response(stream_with_context(_no_stats()), mimetype="text/event-stream")

    cache_key = hashlib.md5(json.dumps(stats, sort_keys=True).encode()).hexdigest()
    now       = time.time()
    hit       = _narrative_cache.get(cache_key)

    # ── Cache hit or fresh inference ──────────────────────────
    if hit and (now - hit["ts"]) < NARRATIVE_TTL:
        narrative_text = hit["payload"]["narrative"]
        logger.debug(f"[BitNet/stream] cache hit, replaying {len(narrative_text)} chars")
    else:
        facts_lines = [f"- {k.replace('_', ' ').title()}: {v}" for k, v in stats.items()]
        facts       = "\n".join(facts_lines)
        system = (
            "You are a radiology department analyst. "
            "Write 3 concise sentences summarising the key findings. "
            "Be direct. Use only the numbers provided. No SQL, no code, no lists."
        )
        raw = _run_inference(system, f"Statistics:\n{facts}\n\nSummary:", max_tokens=120)
        logger.debug(f"[BitNet/stream] inference returned: {raw[:100]!r}")

        if raw.startswith("ERROR:"):
            def _err(msg=raw):
                yield f'data: {json.dumps({"error": msg})}\n\n'
                yield 'data: [DONE]\n\n'
            r = Response(stream_with_context(_err()), mimetype="text/event-stream")
            r.headers["X-Accel-Buffering"] = "no"
            r.headers["Cache-Control"]     = "no-cache"
            return r

        if _NARRATIVE_RE.search(raw):
            logger.warning(f"[BitNet/stream] schema leak filtered: {raw[:100]}")
            raw = "Unable to generate narrative — please review the statistics directly."

        narrative_text = raw.strip() or "No summary could be generated for this data."
        _narrative_cache[cache_key] = {"payload": {"narrative": narrative_text}, "ts": now}

    # ── Stream word-by-word (typewriter effect) ───────────────
    words = narrative_text.split(" ")

    def _replay(word_list=words):
        for word in word_list:
            yield f'data: {json.dumps({"token": word + " "})}\n\n'
        yield 'data: [DONE]\n\n'

    resp = Response(stream_with_context(_replay()), mimetype="text/event-stream")
    resp.headers["X-Accel-Buffering"] = "no"
    resp.headers["Cache-Control"]     = "no-cache"
    return resp


# ── WhatsApp generator ────────────────────────────────────────
@bitnet_bp.route("/ai/whatsapp", methods=["POST"])
@login_required
def whatsapp_message():
    body     = request.get_json(force=True)
    patient  = body.get("patient_name", "")
    hospital = body.get("hospital_name", "المستشفى")
    username = body.get("username", "")
    password = body.get("password", "")
    language = body.get("language", "ar")
    proc     = body.get("procedure", "")

    system = (
        "You write short, friendly WhatsApp messages for a hospital radiology department. "
        "Use only the information provided. Be warm and professional. "
        "Never add information not given to you."
    )

    if language == "ar":
        user_prompt = (
            f"اكتب رسالة واتساب قصيرة باللغة العربية لإرسال بيانات الدخول لبوابة نتائج الأشعة.\n"
            f"اسم المريض: {patient}\nالمستشفى: {hospital}\n"
            f"الإجراء: {proc}\nاسم المستخدم: {username}\nكلمة المرور: {password}"
        )
    else:
        user_prompt = (
            f"Write a short WhatsApp message to send radiology portal login credentials.\n"
            f"Patient: {patient}\nHospital: {hospital}\n"
            f"Procedure: {proc}\nUsername: {username}\nPassword: {password}"
        )

    msg = _run_inference(system, user_prompt, max_tokens=200)
    if _contains_hallucination(msg):
        msg = FALLBACK_AR if language == "ar" else FALLBACK_EN

    return jsonify({"message": msg})


# ── Correction lookup (few-shot injection) ────────────────────
def _get_corrections(question: str) -> str:
    """
    Find active corrections whose keywords match the question.
    Returns a formatted block to append to the system prompt,
    or empty string if no matches.
    """
    try:
        corrections = AiCorrection.query.filter_by(is_active=True).all()
        if not corrections:
            return ""

        q = question.lower()
        matches = []
        for c in corrections:
            kws = [k.strip().lower() for k in c.keywords.split(",") if k.strip()]
            if any(k in q for k in kws):
                matches.append(c)

        if not matches:
            return ""

        lines = ["LEARNED CORRECTIONS (use these as reference for similar questions):"]
        for c in matches[:3]:  # max 3 to keep prompt short
            if c.example_question:
                lines.append(f"Q: {c.example_question}")
            lines.append(f"Correct answer: {c.correct_answer}")
            lines.append("")

        return "\n".join(lines)
    except Exception as e:
        logger.error(f"[BitNet] Corrections lookup error: {e}")
        return ""


# ── Feedback endpoint (thumbs up/down) ────────────────────────
@bitnet_bp.route("/ai/feedback", methods=["POST"])
@login_required
def feedback():
    body     = request.get_json(force=True)
    question = (body.get("question") or "").strip()
    response = (body.get("response") or "").strip()
    vote     = body.get("vote", "").strip()

    if vote not in ("up", "down") or not question:
        return jsonify({"error": "Invalid feedback"}), 400

    try:
        entry = AiFeedback(
            question=question,
            response=response,
            vote=vote,
            user_id=current_user.id,
        )
        db.session.add(entry)
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# ── Admin: review feedback ────────────────────────────────────
@bitnet_bp.route("/ai/admin")
@login_required
def ai_admin():
    if current_user.role != "admin":
        abort(403)
    return render_template("ai_admin.html")


@bitnet_bp.route("/ai/admin/feedback")
@login_required
def admin_feedback_list():
    """JSON list of thumbs-down feedback for admin review."""
    if current_user.role != "admin":
        abort(403)
    show_all = request.args.get("all", "0") == "1"
    query = AiFeedback.query.filter_by(vote="down")
    if not show_all:
        query = query.filter_by(reviewed=False)
    rows = query.order_by(AiFeedback.created_at.desc()).limit(100).all()
    return jsonify([{
        "id": r.id,
        "question": r.question,
        "response": r.response,
        "reviewed": r.reviewed,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    } for r in rows])


@bitnet_bp.route("/ai/admin/feedback/<int:fid>/reviewed", methods=["POST"])
@login_required
def mark_reviewed(fid):
    if current_user.role != "admin":
        abort(403)
    entry = AiFeedback.query.get_or_404(fid)
    entry.reviewed = True
    db.session.commit()
    return jsonify({"ok": True})


# ── Admin: teach corrections ──────────────────────────────────
@bitnet_bp.route("/ai/teach", methods=["POST"])
@login_required
def teach():
    if current_user.role != "admin":
        abort(403)
    body     = request.get_json(force=True)
    keywords = (body.get("keywords") or "").strip()
    answer   = (body.get("correct_answer") or "").strip()
    example  = (body.get("example_question") or "").strip()

    if not keywords or not answer:
        return jsonify({"error": "keywords and correct_answer required"}), 400

    try:
        correction = AiCorrection(
            keywords=keywords,
            correct_answer=answer,
            example_question=example or None,
            created_by=current_user.username,
        )
        db.session.add(correction)
        db.session.commit()

        # Mark related feedback as reviewed
        feedback_id = body.get("feedback_id")
        if feedback_id:
            entry = AiFeedback.query.get(feedback_id)
            if entry:
                entry.reviewed = True
                db.session.commit()

        return jsonify({"ok": True, "id": correction.id})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@bitnet_bp.route("/ai/admin/corrections")
@login_required
def list_corrections():
    if current_user.role != "admin":
        abort(403)
    rows = AiCorrection.query.order_by(AiCorrection.created_at.desc()).all()
    return jsonify([{
        "id": r.id,
        "keywords": r.keywords,
        "correct_answer": r.correct_answer,
        "example_question": r.example_question,
        "is_active": r.is_active,
        "created_by": r.created_by,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    } for r in rows])


@bitnet_bp.route("/ai/admin/corrections/<int:cid>", methods=["DELETE"])
@login_required
def delete_correction(cid):
    if current_user.role != "admin":
        abort(403)
    c = AiCorrection.query.get_or_404(cid)
    c.is_active = False
    db.session.commit()
    return jsonify({"ok": True})


# ── Proactive Anomaly Alerts ──────────────────────────────────
@bitnet_bp.route("/ai/alerts")
@login_required
def alerts():
    """Pure SQL anomaly detection — no AI inference."""
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
                cur_tat  = float(tat_rows[0][1] or 0)
                baseline = sum(float(r[1] or 0) for r in tat_rows[1:]) / len(tat_rows[1:])
                if baseline > 0:
                    pct = (cur_tat - baseline) / baseline * 100
                    if abs(pct) >= 20:
                        findings.append({
                            "type": "tat",
                            "severity": "high" if abs(pct) >= 30 else "medium",
                            "msg": f"TAT {'↑' if pct > 0 else '↓'} {abs(pct):.0f}% this week "
                                   f"({cur_tat:.0f}m) vs 4-week baseline ({baseline:.0f}m)",
                        })

            # 2. Storage growth anomaly: this week vs previous week
            stor_rows = {r[0]: float(r[1] or 0) for r in conn.execute(text("""
                SELECT CASE WHEN study_date >= CURRENT_DATE - 7 THEN 'current' ELSE 'prior' END,
                       ROUND(SUM(total_gb)::numeric, 2)
                FROM summary_storage_daily
                WHERE study_date >= CURRENT_DATE - 14
                GROUP BY 1
            """)).fetchall()}
            cur_gb, prev_gb = stor_rows.get("current", 0), stor_rows.get("prior", 0)
            if prev_gb > 0:
                stor_pct = (cur_gb - prev_gb) / prev_gb * 100
                if stor_pct >= 25:
                    findings.append({
                        "type": "storage",
                        "severity": "high" if stor_pct >= 50 else "medium",
                        "msg": f"Storage ingestion ↑ {stor_pct:.0f}% this week "
                               f"({cur_gb:.1f} GB) vs last week ({prev_gb:.1f} GB)",
                    })

            # 3. Device volume outliers: this-week count > prior avg + 2σ
            for r in conn.execute(text("""
                WITH weekly_ae AS (
                    SELECT storing_ae, DATE_TRUNC('week', study_date) AS wk, COUNT(*) AS cnt
                    FROM etl_didb_studies
                    WHERE study_date >= CURRENT_DATE - INTERVAL '6 weeks'
                      AND storing_ae IS NOT NULL
                    GROUP BY 1, 2
                ),
                stats AS (
                    SELECT storing_ae, AVG(cnt) AS avg_cnt, STDDEV(cnt) AS std_cnt
                    FROM weekly_ae WHERE wk < DATE_TRUNC('week', CURRENT_DATE)
                    GROUP BY storing_ae
                ),
                this_week AS (
                    SELECT storing_ae, cnt FROM weekly_ae
                    WHERE wk = DATE_TRUNC('week', CURRENT_DATE)
                )
                SELECT t.storing_ae, t.cnt, s.avg_cnt, s.std_cnt
                FROM this_week t JOIN stats s ON t.storing_ae = s.storing_ae
                WHERE s.std_cnt > 0 AND t.cnt > s.avg_cnt + 2 * s.std_cnt
                ORDER BY (t.cnt - s.avg_cnt) / s.std_cnt DESC LIMIT 3
            """)).fetchall():
                ae, cnt, avg, std = r
                z = (float(cnt) - float(avg)) / float(std)
                findings.append({
                    "type": "utilization", "severity": "medium",
                    "msg": f"{ae}: {cnt} studies this week vs avg {avg:.0f} (z = {z:.1f}σ above normal)",
                })

    except Exception as e:
        logger.error(f"[BitNet] Alerts error: {e}")
        return jsonify({"alerts": [], "error": str(e)})

    return jsonify({"alerts": findings, "count": len(findings), "clean": len(findings) == 0})


# ── Report link map ───────────────────────────────────────────
REPORT_LINKS = {
    "storage":     ("/report/29",              "Storage Audit"),
    "modality":    ("/report/25",              "Modality & TAT"),
    "modalities":  ("/report/25",              "Modality & TAT"),
    "tat":         ("/report/25",              "Modality & TAT"),
    "turnaround":  ("/report/25",              "Modality & TAT"),
    "physician":   ("/report/22",              "Studies Fact"),
    "physicians":  ("/report/22",              "Studies Fact"),
    "doctor":      ("/report/22",              "Studies Fact"),
    "orders":      ("/report/27",              "Order Audit"),
    "order":       ("/report/27",              "Order Audit"),
    "capacity":    ("/viewer/capacity-ladder", "Capacity Ladder"),
    "schedule":    ("/viewer/capacity-ladder", "Capacity Ladder"),
    "live":        ("/viewer/live",            "Live AE Status"),
    "ai":          ("/report/ai",              "AI Intelligence"),
    "forecast":    ("/report/ai",              "AI Intelligence"),
    "productivity":("/report/22",              "Studies Fact"),
    "patient":     ("/report/22",              "Studies Fact"),
    # Arabic keywords
    "تخزين":       ("/report/29",              "Storage Audit"),
    "طبيب":        ("/report/22",              "Studies Fact"),
    "طلب":         ("/report/27",              "Order Audit"),
    "أشعة":        ("/report/25",              "Modality & TAT"),
    "سعة":         ("/viewer/capacity-ladder", "Capacity Ladder"),
}


# ── Intent keyword sets (defined once) ────────────────────────
_KW_MODALITY  = frozenset(['modality','modalities','ct','mr','mri','us','xray','x-ray',
                            'ultrasound','breakdown','split','أشعة','فحص','جهاز'])
_KW_STORAGE   = frozenset(['storage','gb','disk','space','full','تخزين','مساحة'])
_KW_PHYSICIAN = frozenset(['physician','doctor','referring','top','أطباء','طبيب','دكتور','محول'])
_KW_ORDERS    = frozenset(['order','orders','pending','orphan','طلب','طلبات'])
_KW_TODAY     = frozenset(['today','اليوم','الآن','now','current'])
_KW_YESTERDAY = frozenset(['yesterday','أمس','البارحة'])
_KW_WEEK      = frozenset(['week','weekly','this week','أسبوع','الأسبوع'])
_KW_MONTH     = frozenset(['month','monthly','this month','شهر','الشهر'])
_KW_PATIENT   = frozenset(['patient','class','inpatient','outpatient','emergency','مريض','طوارئ'])
_KW_DEVICE    = frozenset(['utilization','utilisation','busy','ae','device','جهاز','استخدام'])
_KW_TAT       = frozenset(['tat','turnaround','wait','delay','وقت','انتظار','تأخير','report time'])
_KW_TREND     = frozenset(['trend','compare','comparison','growth','decline','increase','decrease',
                            'مقارنة','اتجاه','نمو'])
_KW_BUSY      = frozenset(['busy','peak','volume','most','highest','أكثر','ازدحام','busiest'])


def _match(q: str, keywords: frozenset) -> bool:
    return any(w in q for w in keywords)


# ── Base context (always-on, cached 60s) ──────────────────────
def _fetch_base_context(conn) -> list:
    """Department overview + AE list — cached globally."""
    now = time.time()
    if _base_cache["facts"] and (now - _base_cache["ts"]) < BASE_TTL:
        return list(_base_cache["facts"])

    facts = []

    # Single CTE for overview + AE list
    rows = conn.execute(text("""
        WITH overview AS (
            SELECT COUNT(*)                 AS total,
                   COUNT(DISTINCT storing_ae) AS aes,
                   MIN(study_date)          AS earliest,
                   MAX(study_date)          AS latest
            FROM etl_didb_studies
        ),
        ae_list AS (
            SELECT aetitle, modality
            FROM aetitle_modality_map
            ORDER BY modality, aetitle
        )
        SELECT 'overview' AS src, total::text AS col1, aes::text AS col2,
               earliest::text AS col3, latest::text AS col4
        FROM overview
        UNION ALL
        SELECT 'ae', aetitle, modality, NULL, NULL
        FROM ae_list
    """)).fetchall()

    ae_parts = []
    for r in rows:
        if r[0] == "overview" and int(r[1] or 0) > 0:
            facts.append(
                f"The department has {int(r[1]):,} total studies across "
                f"{r[2]} imaging devices, from {r[3]} to {r[4]}."
            )
        elif r[0] == "ae":
            ae_parts.append(f"{r[1]} ({r[2]})")

    if ae_parts:
        facts.append(f"Imaging devices in this department: {', '.join(ae_parts)}.")

    _base_cache["facts"] = facts
    _base_cache["ts"]    = now
    return list(facts)


# ── Context builder ───────────────────────────────────────────
def _build_context(question: str):
    """
    Returns (link, link_label, chart_type, chart_data, context_facts_string).
    All facts are plain English/Arabic — no table names, no SQL.
    """
    q = question.lower()
    link = link_label = chart_type = chart_data = None
    facts = []

    # Detect report link
    for keyword, (url, label) in REPORT_LINKS.items():
        if keyword in q:
            link, link_label = url, label
            break

    try:
        with db.engine.connect() as conn:

            # ── Always: overview + AE list (cached) ───────────
            facts.extend(_fetch_base_context(conn))

            # ── Modality breakdown ────────────────────────────
            if _match(q, _KW_MODALITY):
                rows = conn.execute(text("""
                    SELECT study_modality AS mod, COUNT(*) AS cnt
                    FROM etl_didb_studies
                    WHERE study_modality IS NOT NULL
                    GROUP BY study_modality ORDER BY cnt DESC LIMIT 8
                """)).mappings().fetchall()
                if rows:
                    facts.append("Studies by modality: " +
                        ", ".join(f"{r['mod']}: {r['cnt']:,} studies" for r in rows) + ".")
                    chart_type = "pie"
                    chart_data = {
                        "labels": [r["mod"] for r in rows],
                        "values": [int(r["cnt"]) for r in rows],
                        "title":  "Studies by Modality",
                    }

            # ── Storage (14-day) ──────────────────────────────
            if _match(q, _KW_STORAGE):
                rows = conn.execute(text("""
                    SELECT study_date::text AS d, ROUND(SUM(total_gb)::numeric, 2) AS gb
                    FROM summary_storage_daily
                    GROUP BY study_date ORDER BY study_date DESC LIMIT 14
                """)).mappings().fetchall()
                if rows:
                    total_gb  = sum(float(r["gb"]) for r in rows)
                    latest_gb = float(rows[0]["gb"])
                    facts.append(
                        f"Storage in the last 14 days: {total_gb:.1f} GB total. "
                        f"Most recent day ({rows[0]['d']}): {latest_gb:.2f} GB."
                    )
                    rev = list(reversed(rows))
                    chart_type = "bar"
                    chart_data = {
                        "labels": [r["d"] for r in rev],
                        "values": [float(r["gb"]) for r in rev],
                        "title":  "Daily Storage (GB)", "color": "#60a5fa",
                    }

            # ── TAT by modality (last 30 days) ────────────────
            if _match(q, _KW_TAT):
                rows = conn.execute(text("""
                    SELECT COALESCE(UPPER(m.modality), 'N/A') AS modality,
                           ROUND(AVG(EXTRACT(EPOCH FROM (s.rep_final_timestamp - s.study_date)) / 60)::numeric, 0) AS avg_min,
                           ROUND(AVG(EXTRACT(EPOCH FROM (s.rep_final_timestamp - s.study_date)) / 3600)::numeric, 1) AS avg_hours,
                           COUNT(*) AS studies
                    FROM etl_didb_studies s
                    LEFT JOIN aetitle_modality_map m
                        ON UPPER(TRIM(s.storing_ae)) = UPPER(TRIM(m.aetitle))
                    WHERE s.study_date >= CURRENT_DATE - INTERVAL '30 days'
                      AND s.rep_final_timestamp IS NOT NULL
                      AND s.rep_final_signed_by IS NOT NULL
                    GROUP BY m.modality ORDER BY avg_min DESC
                """)).mappings().fetchall()
                if rows:
                    facts.append("Turnaround time (last 30 days): " +
                        ", ".join(f"{r['modality']}: {r['avg_hours']}h avg ({r['studies']} studies)" for r in rows) + ".")
                    chart_type = "bar"
                    chart_data = {
                        "labels": [r["modality"] for r in rows],
                        "values": [float(r["avg_hours"]) for r in rows],
                        "title":  "Average TAT by Modality (hours)", "color": "#f59e0b",
                    }

            # ── Physicians ────────────────────────────────────
            if _match(q, _KW_PHYSICIAN):
                rows = conn.execute(text("""
                    SELECT TRIM(CONCAT_WS(' ',
                        referring_physician_first_name,
                        referring_physician_last_name)) AS name,
                        COUNT(*) AS cnt
                    FROM etl_didb_studies
                    WHERE referring_physician_last_name IS NOT NULL
                    GROUP BY 1 ORDER BY cnt DESC LIMIT 10
                """)).mappings().fetchall()
                if rows:
                    facts.append("Top referring physicians by study count: " +
                        ", ".join(f"Dr. {r['name']} ({r['cnt']:,})" for r in rows) + ".")
                    chart_type = "bar"
                    chart_data = {
                        "labels": [r["name"] for r in rows],
                        "values": [int(r["cnt"]) for r in rows],
                        "title":  "Top Referring Physicians", "color": "#a855f7",
                    }

            # ── Orders ────────────────────────────────────────
            if _match(q, _KW_ORDERS):
                row = conn.execute(text("""
                    SELECT COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE has_study = true)  AS fulfilled,
                           COUNT(*) FILTER (WHERE has_study = false) AS orphaned
                    FROM etl_orders
                """)).mappings().fetchone()
                if row:
                    facts.append(
                        f"Orders in the system: {row['total']:,} total, "
                        f"{row['fulfilled']:,} fulfilled, "
                        f"{row['orphaned']:,} orphaned (no linked study)."
                    )

            # ── Today's activity ──────────────────────────────
            if _match(q, _KW_TODAY):
                rows = conn.execute(text("""
                    SELECT study_modality, COUNT(*) AS cnt
                    FROM etl_didb_studies
                    WHERE study_date = CURRENT_DATE
                    GROUP BY study_modality ORDER BY cnt DESC
                """)).mappings().fetchall()
                if rows:
                    total = sum(int(r["cnt"]) for r in rows)
                    facts.append(f"Studies today: {total:,} total — " +
                        ", ".join(f"{r['study_modality']}: {r['cnt']}" for r in rows) + ".")
                else:
                    facts.append("No studies recorded today yet.")

                row2 = conn.execute(text("""
                    SELECT COUNT(*) AS cnt FROM etl_orders
                    WHERE scheduled_datetime::date = CURRENT_DATE
                """)).mappings().fetchone()
                if row2:
                    facts.append(f"Orders scheduled today: {row2['cnt']:,}.")

            # ── Yesterday ─────────────────────────────────────
            if _match(q, _KW_YESTERDAY):
                rows = conn.execute(text("""
                    SELECT study_modality, COUNT(*) AS cnt
                    FROM etl_didb_studies
                    WHERE study_date = CURRENT_DATE - INTERVAL '1 day'
                    GROUP BY study_modality ORDER BY cnt DESC
                """)).mappings().fetchall()
                if rows:
                    total = sum(int(r["cnt"]) for r in rows)
                    facts.append(f"Yesterday's studies: {total:,} total — " +
                        ", ".join(f"{r['study_modality']}: {r['cnt']}" for r in rows) + ".")

            # ── This week ─────────────────────────────────────
            if _match(q, _KW_WEEK):
                rows = conn.execute(text("""
                    SELECT study_modality, COUNT(*) AS cnt
                    FROM etl_didb_studies
                    WHERE study_date >= DATE_TRUNC('week', CURRENT_DATE)
                    GROUP BY study_modality ORDER BY cnt DESC
                """)).mappings().fetchall()
                if rows:
                    total = sum(int(r["cnt"]) for r in rows)
                    facts.append(f"This week's studies: {total:,} total — " +
                        ", ".join(f"{r['study_modality']}: {r['cnt']}" for r in rows) + ".")

            # ── This month ────────────────────────────────────
            if _match(q, _KW_MONTH):
                rows = conn.execute(text("""
                    SELECT study_modality, COUNT(*) AS cnt
                    FROM etl_didb_studies
                    WHERE study_date >= DATE_TRUNC('month', CURRENT_DATE)
                    GROUP BY study_modality ORDER BY cnt DESC
                """)).mappings().fetchall()
                if rows:
                    total = sum(int(r["cnt"]) for r in rows)
                    facts.append(f"This month's studies: {total:,} total — " +
                        ", ".join(f"{r['study_modality']}: {r['cnt']}" for r in rows) + ".")

            # ── Patient class breakdown ───────────────────────
            if _match(q, _KW_PATIENT):
                rows = conn.execute(text("""
                    SELECT COALESCE(patient_class, 'Unknown') AS cls, COUNT(*) AS cnt
                    FROM etl_didb_studies
                    WHERE patient_class IS NOT NULL
                    GROUP BY 1 ORDER BY 2 DESC
                """)).mappings().fetchall()
                if rows:
                    facts.append("Studies by patient class: " +
                        ", ".join(f"{r['cls']}: {r['cnt']:,}" for r in rows) + ".")
                    chart_type = "pie"
                    chart_data = {
                        "labels": [r["cls"] for r in rows],
                        "values": [int(r["cnt"]) for r in rows],
                        "title":  "Studies by Patient Class",
                    }

            # ── AE / device utilization ───────────────────────
            if _match(q, _KW_DEVICE):
                rows = conn.execute(text("""
                    SELECT storing_ae AS ae, COUNT(*) AS cnt
                    FROM etl_didb_studies
                    WHERE storing_ae IS NOT NULL
                    GROUP BY 1 ORDER BY 2 DESC
                """)).mappings().fetchall()
                if rows:
                    facts.append("Studies per imaging device: " +
                        ", ".join(f"{r['ae']}: {r['cnt']:,} studies" for r in rows) + ".")
                    chart_type = "bar"
                    chart_data = {
                        "labels": [r["ae"] for r in rows],
                        "values": [int(r["cnt"]) for r in rows],
                        "title":  "Studies per AE", "color": "#2EC4A5",
                    }

            # ── Busiest days this month ───────────────────────
            if _match(q, _KW_BUSY):
                rows = conn.execute(text("""
                    SELECT study_date::text AS d, COUNT(*) AS cnt
                    FROM etl_didb_studies
                    WHERE study_date >= DATE_TRUNC('month', CURRENT_DATE)
                    GROUP BY study_date ORDER BY cnt DESC LIMIT 5
                """)).mappings().fetchall()
                if rows:
                    facts.append("Busiest days this month: " +
                        ", ".join(f"{r['d']}: {r['cnt']:,} studies" for r in rows) + ".")
                    chart_type = "bar"
                    chart_data = {
                        "labels": [r["d"] for r in rows],
                        "values": [int(r["cnt"]) for r in rows],
                        "title":  "Busiest Days", "color": "#ef4444",
                    }

            # ── Trend: this week vs last week ─────────────────
            if _match(q, _KW_TREND):
                rows = conn.execute(text("""
                    SELECT CASE
                             WHEN study_date >= DATE_TRUNC('week', CURRENT_DATE) THEN 'this_week'
                             ELSE 'last_week'
                           END AS period, COUNT(*) AS cnt
                    FROM etl_didb_studies
                    WHERE study_date >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '7 days'
                    GROUP BY 1
                """)).mappings().fetchall()
                periods = {r["period"]: int(r["cnt"]) for r in rows}
                tw = periods.get("this_week", 0)
                lw = periods.get("last_week", 0)
                if lw > 0:
                    pct = (tw - lw) / lw * 100
                    direction = "up" if pct > 0 else "down"
                    facts.append(
                        f"Week-over-week trend: {tw:,} studies this week vs {lw:,} last week "
                        f"({direction} {abs(pct):.1f}%)."
                    )

    except Exception as e:
        logger.error(f"[BitNet] Context error: {e}", exc_info=True)
        return link, link_label, None, None, ""

    context_string = "\n".join(facts) if facts else ""
    return link, link_label, chart_type, chart_data, context_string
