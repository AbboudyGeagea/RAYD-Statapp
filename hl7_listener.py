"""
hl7_listener.py
---------------
MLLP TCP listener on port 6661.
Accepts ORM^O01 messages, parses them, and stores into hl7_orders table.
Starts automatically as a daemon thread when the Flask app starts.
"""

import socket
import threading
import logging
import re
from datetime import datetime

logger = logging.getLogger("HL7_LISTENER")

# ── MLLP framing constants ────────────────────────────────────────────────────
MLLP_START  = b'\x0b'          # Vertical Tab  — start of block
MLLP_END    = b'\x1c\x0d'      # FS + CR       — end of block
ACK_AA      = "AA"             # Application Accept
ACK_AE      = "AE"             # Application Error

# ── SQL ───────────────────────────────────────────────────────────────────────
INSERT_SQL = """
    INSERT INTO hl7_orders (
        message_id, message_datetime, message_type,
        patient_id, patient_name, date_of_birth, gender,
        accession_number, placer_order_number,
        procedure_code, procedure_text,
        modality, scheduled_datetime,
        ordering_physician, order_status,
        raw_message, received_at
    ) VALUES (
        :message_id, :message_datetime, :message_type,
        :patient_id, :patient_name, :date_of_birth, :gender,
        :accession_number, :placer_order_number,
        :procedure_code, :procedure_text,
        :modality, :scheduled_datetime,
        :ordering_physician, :order_status,
        :raw_message, :received_at
    )
    ON CONFLICT (message_id) DO UPDATE SET
        message_datetime   = EXCLUDED.message_datetime,
        patient_id         = EXCLUDED.patient_id,
        patient_name       = EXCLUDED.patient_name,
        date_of_birth      = EXCLUDED.date_of_birth,
        gender             = EXCLUDED.gender,
        accession_number   = EXCLUDED.accession_number,
        placer_order_number= EXCLUDED.placer_order_number,
        procedure_code     = EXCLUDED.procedure_code,
        procedure_text     = EXCLUDED.procedure_text,
        modality           = EXCLUDED.modality,
        scheduled_datetime = EXCLUDED.scheduled_datetime,
        ordering_physician = EXCLUDED.ordering_physician,
        order_status       = EXCLUDED.order_status,
        raw_message        = EXCLUDED.raw_message,
        received_at        = EXCLUDED.received_at
"""


# ── HL7 helpers ───────────────────────────────────────────────────────────────

def _seg(segments, name):
    """Return first matching segment as a list of fields, or []."""
    for s in segments:
        if s.startswith(name + '|'):
            return s.split('|')
    return []

def _field(seg, index, default=None):
    """Safely get a field from a segment by index."""
    try:
        val = seg[index].strip()
        return val if val else default
    except IndexError:
        return default

def _parse_hl7_datetime(val):
    """Parse HL7 datetime string (YYYYMMDDHHMMSS or YYYYMMDD) to datetime."""
    if not val:
        return None
    val = val.strip().split('+')[0].split('-')[0]  # strip timezone
    try:
        if len(val) >= 14: return datetime.strptime(val[:14], '%Y%m%d%H%M%S')
        if len(val) >= 12: return datetime.strptime(val[:12], '%Y%m%d%H%M')
        if len(val) >= 8:  return datetime.strptime(val[:8],  '%Y%m%d')
    except Exception:
        pass
    return None

def _format_name(raw):
    """
    Convert HL7 XPN to readable name.
    Handles: Last^First^Mid  and  ID^^Full Name  formats.
    """
    if not raw:
        return None
    parts = [p.strip() for p in raw.split('^')]
    # Format: ID^^Full Name (e.g. ORC-12: ID2^^Jihad Falou)
    if len(parts) >= 3 and not parts[1] and parts[2]:
        return parts[2]
    # Format: Last^First^Mid (e.g. PID-5: KHALIL^ROUAIDA^FAYSAL)
    last  = parts[0] if len(parts) > 0 else ''
    first = parts[1] if len(parts) > 1 else ''
    mid   = parts[2] if len(parts) > 2 else ''
    name  = ' '.join(filter(None, [first, mid, last]))
    return name or None

def _component(field_val, index, default=None):
    """Get a sub-component from a field value (split by ^)."""
    if not field_val:
        return default
    parts = field_val.split('^')
    try:
        val = parts[index].strip()
        return val if val else default
    except IndexError:
        return default

ORU_INSERT_SQL = """
    INSERT INTO hl7_oru_reports
        (procedure_code, procedure_name, modality, physician_id,
         report_text, impression_text, result_datetime, received_at)
    VALUES
        (:procedure_code, :procedure_name, :modality, :physician_id,
         :report_text, :impression_text, :result_datetime, :received_at)
"""

# HL7 escape sequence cleaner
_HL7_ESC = re.compile(r'\\[A-Za-z.]+\\')

def _clean_obx_text(val):
    """Strip HL7 escape sequences and tidy whitespace."""
    val = _HL7_ESC.sub(' ', val)
    val = val.replace('\.br\\', ' ').replace('\\.br\\', ' ')
    return ' '.join(val.split())


def parse_oru_r01(raw_message):
    """
    Parse an ORU^R01 radiology result message.
    Stores procedure + report text only — no patient identifiers.
    Returns None if message is not ORU^R01.
    """
    text     = raw_message.replace('\r\n', '\r').replace('\n', '\r')
    segments = [s.strip() for s in text.split('\r') if s.strip()]

    msh = _seg(segments, 'MSH')
    obr = _seg(segments, 'OBR')

    msg_type = _field(msh, 8, '')
    if 'ORU' not in msg_type:
        return None

    # ── OBR: procedure & timing ───────────────────────────────────────────────
    proc_raw       = _field(obr, 4, '')
    procedure_code = _component(proc_raw, 0)
    procedure_name = _component(proc_raw, 1) or _component(proc_raw, 2)
    modality       = _field(obr, 24) or _field(obr, 19) or _field(obr, 17)
    result_dt      = _parse_hl7_datetime(_field(obr, 22) or _field(obr, 7))

    # Physician: OBR-32 (principal result interpreter) — ID component only
    phys_raw     = _field(obr, 32, '')
    physician_id = _component(phys_raw, 0) or _component(phys_raw, 2)

    # ── OBX: collect report text ──────────────────────────────────────────────
    report_parts     = []
    impression_parts = []

    for seg in segments:
        if not seg.startswith('OBX|'):
            continue
        f = seg.split('|')
        obs_type  = f[2]  if len(f) > 2  else ''   # TX / FT / ST
        obs_id    = f[3]  if len(f) > 3  else ''    # observation identifier
        obs_value = f[5]  if len(f) > 5  else ''    # the text
        obs_status= f[11] if len(f) > 11 else ''    # F = final

        # Only capture text-type observations
        if obs_type not in ('TX', 'FT', 'ST', ''):
            continue
        cleaned = _clean_obx_text(obs_value)
        if not cleaned:
            continue

        report_parts.append(cleaned)

        obs_upper = obs_id.upper()
        if any(k in obs_upper for k in ('IMP', 'IMPRESSION', 'CONCLUSION', 'CONCL')):
            impression_parts.append(cleaned)

    return {
        'procedure_code':  procedure_code,
        'procedure_name':  procedure_name,
        'modality':        modality,
        'physician_id':    physician_id,
        'report_text':     '\n'.join(report_parts) or None,
        'impression_text': '\n'.join(impression_parts) or None,
        'result_datetime': result_dt,
        'received_at':     datetime.now(),
    }


def parse_orm_o01(raw_message):
    """
    Parse an ORM^O01 HL7 message into a flat dict.
    Returns None if the message is not ORM^O01.

    Tested against:
      MSH|^~\\&|Experience 4 RIS|ORM_HIS|...
      PID|||00301796^^^HIS||KHALIL^ROUAIDA^FAYSAL|...
      ORC|SC||249389^HIS||IP||...
      OBR|1||249389^HIS|ECABDPEL^76700-76856-...|...||...|US|...
    """
    # Normalize line endings
    text     = raw_message.replace('\r\n', '\r').replace('\n', '\r')
    segments = [s.strip() for s in text.split('\r') if s.strip()]

    msh = _seg(segments, 'MSH')
    pid = _seg(segments, 'PID')
    orc = _seg(segments, 'ORC')
    obr = _seg(segments, 'OBR')

    # Validate message type
    msg_type = _field(msh, 8, '')
    if 'ORM' not in msg_type and 'O01' not in msg_type:
        logger.warning(f"Ignoring non-ORM message: {msg_type}")
        return None

    message_id       = _field(msh, 9)
    message_datetime = _parse_hl7_datetime(_field(msh, 6))
    msg_type         = _field(msh, 8)

    # ── PID — Patient info ───────────────────────────────────────────────────
    patient_id   = _component(_field(pid, 3, ''), 0)
    patient_name = _format_name(_field(pid, 5))
    dob          = _parse_hl7_datetime(_field(pid, 7))
    gender       = _field(pid, 8)

    # ── ORC — Order control ──────────────────────────────────────────────────
    orc_control  = _field(orc, 1)
    orc_status   = _field(orc, 5)
    order_status = orc_status or orc_control

    accession_number    = _component(_field(orc, 3, ''), 0)
    placer_order_number = _component(_field(orc, 2, ''), 0)
    ordering_physician  = _format_name(_field(orc, 12, ''))

    # ── OBR — Observation request ────────────────────────────────────────────
    if not accession_number:
        accession_number = _component(_field(obr, 3, ''), 0)
    if not placer_order_number:
        placer_order_number = _component(_field(obr, 3, ''), 0)

    proc_raw       = _field(obr, 4, '')
    procedure_code = _component(proc_raw, 0)
    procedure_text = _component(proc_raw, 1)

    modality = _field(obr, 24) or _field(obr, 19) or _field(obr, 17)

    scheduled_datetime = _parse_hl7_datetime(
        _field(obr, 7) or _field(obr, 36) or _field(obr, 6)
    )

    if not ordering_physician:
        ordering_physician = _format_name(_field(obr, 16, ''))

    return {
        "message_id":         message_id,
        "message_datetime":   message_datetime,
        "message_type":       msg_type,
        "patient_id":         patient_id or None,
        "patient_name":       patient_name,
        "date_of_birth":      dob,
        "gender":             gender,
        "accession_number":   accession_number or None,
        "placer_order_number":placer_order_number or None,
        "procedure_code":     procedure_code,
        "procedure_text":     procedure_text,
        "modality":           modality,
        "scheduled_datetime": scheduled_datetime,
        "ordering_physician": ordering_physician,
        "order_status":       order_status,
        "raw_message":        raw_message,
        "received_at":        datetime.now(),
    }


def _build_ack(msh, ack_code, error_msg=None):
    """Build a minimal HL7 ACK response."""
    now      = datetime.now().strftime('%Y%m%d%H%M%S')
    msg_id   = _field(msh, 10, 'UNKNOWN')
    send_app = _field(msh, 3, 'STATSAPP')
    recv_app = _field(msh, 5, 'SENDER')
    err_text = error_msg or ''
    ack = (
        f"MSH|^~\\&|{send_app}||{recv_app}||{now}||ACK^O01|ACK{now}|P|2.3\r"
        f"MSA|{ack_code}|{msg_id}|{err_text}\r"
    )
    return MLLP_START + ack.encode('utf-8') + MLLP_END


def _handle_client(conn, addr, app):
    """Handle a single MLLP client connection."""
    logger.info(f"HL7 connection from {addr[0]}:{addr[1]}")
    buffer = b''

    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            buffer += data

            # Process all complete MLLP messages in buffer
            while MLLP_START in buffer and MLLP_END in buffer:
                start = buffer.index(MLLP_START)
                end   = buffer.index(MLLP_END)

                if end < start:
                    buffer = buffer[start:]
                    break

                raw_bytes   = buffer[start + 1:end]
                buffer      = buffer[end + 2:]
                raw_message = raw_bytes.decode('utf-8', errors='replace')
                segments    = [s.strip() for s in raw_message.replace('\r\n','\r').replace('\n','\r').split('\r') if s.strip()]
                msh         = _seg(segments, 'MSH')

                try:
                    msg_type_raw = _field(msh, 8, '')

                    if 'ORU' in msg_type_raw:
                        # ── ORU^R01: radiology result ─────────────────────
                        parsed_oru = parse_oru_r01(raw_message)
                        if parsed_oru and parsed_oru.get('report_text'):
                            with app.app_context():
                                from sqlalchemy import text
                                from db import db
                                try:
                                    db.session.execute(text(ORU_INSERT_SQL), parsed_oru)
                                    db.session.commit()
                                except Exception:
                                    db.session.rollback()
                                    raise
                            logger.info(
                                f"✅ ORU stored | proc={parsed_oru['procedure_code']} "
                                f"| physician={parsed_oru['physician_id']}"
                            )

                    else:
                        # ── ORM^O01: radiology order ──────────────────────
                        parsed = parse_orm_o01(raw_message)

                        if parsed:
                            with app.app_context():
                                from sqlalchemy import text
                                from db import db
                                try:
                                    db.session.execute(text(INSERT_SQL), parsed)
                                    db.session.execute(
                                        text("SELECT pg_notify('hl7_new_order', :mid)"),
                                        {"mid": str(parsed.get("message_id") or "")}
                                    )
                                    db.session.commit()
                                except Exception:
                                    db.session.rollback()
                                    raise

                            logger.info(
                                f"✅ HL7 stored | msg_id={parsed['message_id']} "
                                f"| patient={parsed['patient_id']} "
                                f"| accession={parsed['accession_number']}"
                            )

                            try:
                                from routes.portal_bp import process_orm_for_portal
                                with app.app_context():
                                    process_orm_for_portal(
                                        raw_message,
                                        parsed.get('accession_number', '')
                                    )
                                logger.info(
                                    f"✅ Portal hook fired | "
                                    f"accession={parsed.get('accession_number')}"
                                )
                            except Exception as portal_err:
                                logger.warning(f"⚠ Portal hook error: {portal_err}")

                    ack = _build_ack(msh, ACK_AA)

                except Exception as e:
                    logger.error(f"HL7 processing error: {e}")
                    ack = _build_ack(msh, ACK_AE, str(e)[:80])

                conn.sendall(ack)

    except Exception as e:
        logger.error(f"HL7 connection error from {addr}: {e}")
    finally:
        conn.close()
        logger.info(f"HL7 connection closed: {addr[0]}:{addr[1]}")


def start_mllp_listener(app, host='0.0.0.0', port=6661):
    """
    Start the MLLP listener in a background daemon thread.
    Called once during Flask app startup.
    """
    def _server_loop():
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server.bind((host, port))
            server.listen(10)
            logger.info(f"🏥 HL7 MLLP listener started on {host}:{port}")
            print(f"🏥 HL7 MLLP listener running on port {port}")
            while True:
                try:
                    conn, addr = server.accept()
                    t = threading.Thread(
                        target=_handle_client,
                        args=(conn, addr, app),
                        daemon=True
                    )
                    t.start()
                except Exception as e:
                    logger.error(f"HL7 accept error: {e}")
        except Exception as e:
            logger.error(f"HL7 listener failed to start: {e}")
            print(f"❌ HL7 listener failed: {e}")
        finally:
            server.close()

    thread = threading.Thread(target=_server_loop, daemon=True, name="HL7-MLLP-Listener")
    thread.start()
    return thread
