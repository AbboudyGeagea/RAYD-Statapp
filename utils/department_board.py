"""
utils/department_board.py
────────────────────────────────────────────────────────────────
The Live Department board: for every RIS room of one hospital, how many exams are booked on
it today, who has arrived and how long they have waited, the exam running and when it should
end, and how busy the room has been today. Read from BRAYN's study ladder
(brayn_study_state), so it moves with the HL7 messages. LAUMC to-do item 38 (operator,
2026-11-19), which reworked the board ported from the HL7 branch (2026-11-02).

    RIS message ─▶ BRAYN: ris_room (ORC-16), appointment_at (OBR-36), booked_minutes (OBR-27),
                   scheduled / arrived / started / completed (migration 0173)
               ─▶ the room: aetitle_modality_map row of the RIS room whose room_code it is

One card per RIS room (aetitle_modality_map rows the RIS copy wrote: ris_modality_key, a
room_code), named by its room name, never by an AE title. The cardiology rooms and the
excluded devices stay out (etl_didb_studies.CARDIOLOGY_AES, EXCLUDED_AES), as everywhere.
A room shows when it has anything booked, waiting, running or done today, or when it is
open today (the RIS weekly availability, a device exception or a weekly schedule says so)
and the RIS booked something on it in the last 30 days: the PACS upload "room" and a demo
device, open on paper, stay off.

    scheduled     appointments today (OBR-36), cancelled ones left out; "to come" = not
                  arrived yet
    waiting       arrived, not started, last 12 h — wait = now − arrived; red from the
                  room's modality's limit (Live board limits, default 30 min)
    running       started, not done, last 12 h; expected end = start + the booked minutes
                  (OBR-27), else the procedure's duration (procedure_duration_map), else the
                  modality's median, else DEFAULT_MINUTES; orange when past it
    done early    the room's last exam done today took less than early_pct % of its expected
                  minutes (default 50 %): yellow until the room's next exam starts
    utilization   busy minutes ÷ open minutes so far, as before: both counted from the day
                  start (the morning shift's start, settings.shift_morning_start, 07:00 by
                  default; midnight for a room open 24 hours) up to now. Open minutes: the
                  device exception, else the RIS weekly availability, else the daily
                  capacity, else the weekly schedule. Overlapping exams count once.

A study whose room code no RIS room has is not placed: the board lists those codes, with
how many studies, so nothing is hidden. Patient names come from the patient record (ADT),
else from the patient's latest HIS order (hl7_* tables only).

BRAYN's tables and every HL7 message stay in the main database until BRAYN routes by site
(item 13d), so the board always reads the main database (utils.site_db.MAIN_BLUEPRINTS) and
picks the hospital itself: its own RH / SJH switch (operator's choice), one site at a time.

All times are the hospital's naive local time, as BRAYN stores them; "now" is the database's
LOCALTIMESTAMP on hospital time (brayn_shadow.clock) — LAUMC's Postgres runs on UTC.
"""
import json
from collections import defaultdict
from datetime import datetime, timedelta
from statistics import median

from sqlalchemy import text

from db import db
from utils.brayn_shadow import clock
from utils.ae_display import ae_display_sql

DEFAULT_MINUTES = 20          # expected duration when nothing else gives one
ACTIVE_HOURS = 12             # how far back an arrival / a start still counts as open
DEFAULT_DAY_START_HOUR = 7
DEFAULT_WAIT_LIMIT = 30       # minutes of waiting before a patient turns red
DEFAULT_EARLY_PCT = 50        # done in less than this share of its expected minutes: too early
USED_DAYS = 30                # an open room shows only if the RIS booked anything on it lately
LIMITS_KEY = 'department_board_limits'

# Lanes in the order a department reads them; anything else follows alphabetically.
MODALITY_ORDER = ['CT', 'MR', 'US', 'DX', 'CR', 'XR', 'RF', 'MG', 'XA', 'NM', 'PT', 'BMD', 'OT']
_NOT_LANES = ('SR', 'PR', 'KO', 'PACS')


def _left_out_aes():
    try:
        from ETL_JOBS.etl_didb_studies import CARDIOLOGY_AES, EXCLUDED_AES
        return [a.upper() for a in CARDIOLOGY_AES + EXCLUDED_AES]
    except Exception:
        return []


def _minutes(delta):
    return delta.total_seconds() / 60.0


def merged_busy_minutes(intervals, window_start, now):
    """Minutes covered by the union of [start, end) intervals, clipped to
    [window_start, now]. Overlapping studies count once."""
    clipped = sorted((max(s, window_start), min(e, now)) for s, e in intervals
                     if s is not None and e is not None)
    total, cur_s, cur_e = 0.0, None, None
    for s, e in clipped:
        if e <= s:
            continue
        if cur_e is None or s > cur_e:
            if cur_e is not None:
                total += _minutes(cur_e - cur_s)
            cur_s, cur_e = s, e
        else:
            cur_e = max(cur_e, e)
    if cur_e is not None:
        total += _minutes(cur_e - cur_s)
    return total


def window_start_for(day0, opening_minutes, day_start_hour):
    """Where today's opening starts: midnight for a 24-hour room, else the day start."""
    if opening_minutes is not None and opening_minutes >= 24 * 60:
        return day0
    return day0 + timedelta(hours=day_start_hour)


def open_minutes_so_far(window_start, now, opening_minutes):
    if not opening_minutes:
        return 0.0
    return max(0.0, min(_minutes(now - window_start), float(opening_minutes)))


# ── the limits (wait per modality, done-too-early share) ──────────────────────

def load_limits():
    """{'wait_min': {modality: minutes}, 'default_wait_min': n, 'early_pct': n} from
    settings.department_board_limits; the defaults where it says nothing."""
    raw = db.session.execute(text("SELECT value FROM settings WHERE key = :k"), {'k': LIMITS_KEY}).scalar()
    try:
        data = json.loads(raw) if raw else {}
    except ValueError:
        data = {}
    wait = {}
    for mod, value in (data.get('wait_min') or {}).items():
        try:
            if 1 <= int(value) <= 600:
                wait[str(mod).strip().upper()] = int(value)
        except (TypeError, ValueError):
            pass
    try:
        default_wait = int(data.get('default_wait_min', DEFAULT_WAIT_LIMIT))
        default_wait = default_wait if 1 <= default_wait <= 600 else DEFAULT_WAIT_LIMIT
    except (TypeError, ValueError):
        default_wait = DEFAULT_WAIT_LIMIT
    try:
        early = int(data.get('early_pct', DEFAULT_EARLY_PCT))
        early = early if 1 <= early <= 99 else DEFAULT_EARLY_PCT
    except (TypeError, ValueError):
        early = DEFAULT_EARLY_PCT
    return {'wait_min': wait, 'default_wait_min': default_wait, 'early_pct': early}


def save_limits(wait_min, default_wait_min, early_pct):
    """Validate and store the limits; returns them as load_limits() reads them. Raises
    ValueError with a message for the person when a value is out of range."""
    clean = {}
    for mod, value in (wait_min or {}).items():
        mod = str(mod).strip().upper()
        if not mod or value in (None, ''):
            continue
        try:
            n = int(value)
        except (TypeError, ValueError):
            raise ValueError(f'The wait limit for {mod} must be a whole number of minutes.')
        if not 1 <= n <= 600:
            raise ValueError(f'The wait limit for {mod} must be between 1 and 600 minutes.')
        clean[mod] = n
    try:
        default_wait = int(default_wait_min)
        early = int(early_pct)
    except (TypeError, ValueError):
        raise ValueError('The default wait limit and the early share must be whole numbers.')
    if not 1 <= default_wait <= 600:
        raise ValueError('The default wait limit must be between 1 and 600 minutes.')
    if not 1 <= early <= 99:
        raise ValueError('"Done too early" must be between 1 and 99 % of the expected time.')
    value = json.dumps({'wait_min': clean, 'default_wait_min': default_wait, 'early_pct': early})
    db.session.execute(text("INSERT INTO settings (key, value) VALUES (:k, :v) "
                            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"),
                       {'k': LIMITS_KEY, 'v': value})
    db.session.commit()
    return load_limits()


def wait_limit(limits, modality):
    return limits['wait_min'].get((modality or '').upper()) or limits['default_wait_min']


# ── reads ─────────────────────────────────────────────────────────────────────

_STUDIES_SQL = """
WITH cand AS (
    SELECT accession_number FROM brayn_study_state
     WHERE appointment_at >= :day0 AND appointment_at < :day1
    UNION
    SELECT accession_number FROM brayn_study_state
     WHERE started_at IS NULL AND cancelled_at IS NULL AND NOT is_closed
       AND arrived_at >= :active_since
    UNION
    SELECT accession_number FROM brayn_study_state
     WHERE completed_at IS NULL AND cancelled_at IS NULL AND NOT is_closed
       AND started_at >= :active_since
    UNION
    SELECT accession_number FROM brayn_study_state WHERE started_at >= :day0
    UNION
    SELECT accession_number FROM brayn_study_state WHERE completed_at >= :day0
)
SELECT s.accession_number, s.placer_order_number, s.patient_id,
       COALESCE(NULLIF(btrim(p.patient_name), ''), o.patient_name) AS patient_name,
       upper(btrim(s.ris_room)) AS room_key, btrim(s.ris_room) AS ris_room,
       s.procedure_code, s.procedure_text, s.patient_class, s.patient_location,
       s.appointment_at, s.booked_minutes, s.arrived_at, s.started_at,
       COALESCE(s.completed_at, s.pacs_completed_at) AS done_at, s.cancelled_at
  FROM cand
  JOIN brayn_study_state s USING (accession_number)
  LEFT JOIN hl7_patients p ON p.patient_id = s.patient_id
  -- No name in the patient record (a site without ADT; RIS status messages leave it
  -- alone): the name the HIS put on the patient's latest order.
  LEFT JOIN LATERAL (
      SELECT ho.patient_name FROM hl7_orders ho
       WHERE NULLIF(btrim(p.patient_name), '') IS NULL
         AND ho.patient_id = s.patient_id AND NULLIF(btrim(ho.patient_name), '') IS NOT NULL
       ORDER BY ho.received_at DESC NULLS LAST
       LIMIT 1
  ) o ON TRUE
 WHERE NULLIF(btrim(s.ris_room), '') IS NOT NULL
"""

# The RIS rooms: the rows the nightly RIS copy writes (ETL_JOBS/etl_ris_modality.py).
_ROOMS_SQL = """
SELECT m.aetitle, upper(btrim(m.room_code)) AS room_key, btrim(m.room_code) AS room_code,
       upper(btrim(COALESCE(m.modality, ''))) AS modality,
       """ + ae_display_sql('m', 'btrim(m.room_code)') + """ AS room,
       si.code AS site_code, si.name AS site_name, m.floor_x, m.floor_y,
       COALESCE(ex.actual_opening_minutes, dwa.available_minutes, m.daily_capacity_minutes,
                ws.std_opening_minutes) AS opening_minutes,
       COALESCE(ex.actual_opening_minutes, dwa.available_minutes, ws.std_opening_minutes, 0) > 0 AS open_today,
       (ex.actual_opening_minutes IS NOT NULL) AS has_exception, ex.reason AS exception_reason
  FROM aetitle_modality_map m
  LEFT JOIN sites si ON si.id = m.site_id
  LEFT JOIN device_exceptions ex
         ON ex.aetitle = m.aetitle AND ex.exception_date = CAST(:day0 AS date)
  LEFT JOIN std_device_weekly_availability dwa
         ON upper(btrim(dwa.aetitle)) = upper(btrim(m.aetitle)) AND dwa.day_of_week = :dow
  LEFT JOIN device_weekly_schedule ws
         ON upper(btrim(ws.aetitle)) = upper(btrim(m.aetitle)) AND ws.day_of_week = :dow
 WHERE m.ris_modality_key IS NOT NULL
   AND NULLIF(btrim(m.room_code), '') IS NOT NULL
   AND m.active IS NOT FALSE
   AND upper(btrim(COALESCE(m.modality, ''))) <> ALL(CAST(:not_lanes AS text[]))
   AND upper(btrim(m.aetitle)) <> ALL(CAST(:left_out AS text[]))
   AND (CAST(:site AS text) IS NULL OR si.code = CAST(:site AS text))
 ORDER BY m.modality, m.room_code, m.id
"""


def _durations():
    rows = db.session.execute(text("""
        SELECT upper(btrim(procedure_code)) AS code, upper(btrim(modality)) AS modality,
               duration_minutes
          FROM procedure_duration_map
         WHERE active IS NOT FALSE AND duration_minutes > 0
    """)).mappings().all()
    by_code = {r['code']: r['duration_minutes'] for r in rows}
    per_mod = defaultdict(list)
    for r in rows:
        if r['modality']:
            per_mod[r['modality']].append(r['duration_minutes'])
    return by_code, {m: round(median(v)) for m, v in per_mod.items()}


def _day_start_hour():
    """The morning shift's start (Settings > Shifts, an hour as text); 07:00 if unset."""
    value = db.session.execute(text(
        "SELECT value FROM settings WHERE key = 'shift_morning_start'")).scalar()
    try:
        hour = int(value)
    except (TypeError, ValueError):
        return DEFAULT_DAY_START_HOUR
    return hour if 0 <= hour <= 23 else DEFAULT_DAY_START_HOUR


def _expected(booked, code, modality, by_code, by_mod):
    if booked and booked > 0:
        return int(booked), 'booked'
    m = by_code.get((code or '').strip().upper())
    if m:
        return m, 'procedure'
    m = by_mod.get((modality or '').strip().upper())
    if m:
        return m, 'modality'
    return DEFAULT_MINUTES, 'default'


def _person(r):
    return {'accession': r['accession_number'], 'order': r['placer_order_number'],
            'patient_id': r['patient_id'],
            'patient': r['patient_name'] or r['patient_id'] or 'Unknown patient',
            'procedure': r['procedure_text'] or r['procedure_code'] or '—',
            'patient_class': r['patient_class'], 'location': r['patient_location'],
            'appointment_at': _iso(r['appointment_at'])}


def _iso(dt):
    return dt.isoformat(timespec='seconds') if dt else None


# ── the board ─────────────────────────────────────────────────────────────────

def build_board(site=None, now=None, sites=None):
    """The board of one hospital (site = its code; None = every site, for tests and the
    side-by-side view) as plain data; see routes/department_board.py for the JSON.
    sites = [{'code', 'name'}] the switch offers (the account's sites)."""
    if now is None:
        with clock():
            now = db.session.execute(text('SELECT LOCALTIMESTAMP(0)')).scalar()
    day0 = datetime.combine(now.date(), datetime.min.time())
    day1 = day0 + timedelta(days=1)
    day_start_hour = _day_start_hour()
    by_code, by_mod = _durations()
    limits = load_limits()

    # Rooms the RIS booked anything on in the last USED_DAYS days (BRAYN, the appointment
    # index): an "open" room nobody books (the PACS upload, a demo device) stays off the board.
    used = {k for (k,) in db.session.execute(text(
        "SELECT DISTINCT upper(btrim(ris_room)) FROM brayn_study_state "
        "WHERE appointment_at >= :since AND NULLIF(btrim(ris_room), '') IS NOT NULL"),
        {'since': day0 - timedelta(days=USED_DAYS)})}
    rooms = {}
    for d in db.session.execute(text(_ROOMS_SQL), {
            'day0': day0, 'dow': now.weekday(), 'site': site,
            'not_lanes': list(_NOT_LANES), 'left_out': _left_out_aes()}).mappings():
        if d['room_key'] in rooms:
            continue                       # one room code, one card (the first row wins)
        placed = d['floor_x'] is not None and d['floor_y'] is not None
        modality = d['modality'] or 'OT'
        rooms[d['room_key']] = {
            'key': d['room_key'], 'room_code': d['room_code'], 'aetitle': d['aetitle'],
            'room': d['room'], 'modality': modality,
            'site': d['site_name'], 'site_code': d['site_code'],
            'x': float(d['floor_x']) if placed else None,
            'y': float(d['floor_y']) if placed else None,
            'opening_minutes': d['opening_minutes'] or 0,
            'open_today': bool(d['open_today']) and d['room_key'] in used,
            'exception': d['exception_reason'] if d['has_exception'] else None,
            'wait_limit_min': wait_limit(limits, modality),
            'scheduled_today': 0, 'coming': [], 'waiting': [], 'running': [], 'done': [],
            'intervals': [],
        }

    rows = db.session.execute(text(_STUDIES_SQL), {
        'day0': day0, 'day1': day1, 'active_since': now - timedelta(hours=ACTIVE_HOURS),
    }).mappings().all()
    # Every RIS room code, any site, cardiology included: a study on one of those that is not
    # on this board belongs to the other hospital or to a room left out, not to "unknown".
    known = {k for (k,) in db.session.execute(text(
        "SELECT DISTINCT upper(btrim(room_code)) FROM aetitle_modality_map "
        "WHERE ris_modality_key IS NOT NULL AND NULLIF(btrim(room_code), '') IS NOT NULL"))}
    unknown = defaultdict(int)
    for r in rows:
        room = rooms.get(r['room_key'])
        if room is None:
            if r['room_key'] not in known:
                unknown[r['ris_room']] += 1
            continue
        if r['cancelled_at']:
            continue
        minutes, source = _expected(r['booked_minutes'], r['procedure_code'], room['modality'],
                                    by_code, by_mod)
        item = _person(r)
        item.update(expected_minutes=minutes, expected_source=source)
        appt = r['appointment_at']
        if appt is not None and day0 <= appt < day1:
            room['scheduled_today'] += 1

        if r['done_at']:
            if r['done_at'] >= day0:
                start = r['started_at'] or r['done_at'] - timedelta(minutes=minutes)
                room['intervals'].append((start, r['done_at']))
                room['done'].append((r['done_at'], r['started_at'], item))
            continue
        if r['started_at']:
            if r['started_at'] < now - timedelta(hours=ACTIVE_HOURS):
                continue
            end = r['started_at'] + timedelta(minutes=minutes)
            item.update(started_at=_iso(r['started_at']), expected_end=_iso(end),
                        elapsed_min=round(_minutes(now - r['started_at']), 1),
                        remaining_min=round(_minutes(end - now), 1), late=now > end)
            room['running'].append((r['started_at'], item))
            room['intervals'].append((r['started_at'], now))
            continue
        if r['arrived_at']:
            if r['arrived_at'] < now - timedelta(hours=ACTIVE_HOURS):
                continue
            wait = _minutes(now - r['arrived_at'])
            item.update(arrived_at=_iso(r['arrived_at']), wait_min=round(wait, 1),
                        over=wait >= room['wait_limit_min'])
            room['waiting'].append(item)
            continue
        if appt is not None and day0 <= appt < day1:
            room['coming'].append(item)

    cards = [_finish_room(room, now, day0, day_start_hour, limits) for room in rooms.values()]
    cards = [c for c in cards if c['show']]

    lanes = defaultdict(list)
    for c in cards:
        lanes[c['modality']].append(c)
    out = []
    for mod in sorted(lanes, key=lambda m: (MODALITY_ORDER.index(m) if m in MODALITY_ORDER
                                            else len(MODALITY_ORDER), m)):
        rs = sorted(lanes[mod], key=lambda c: (c['room'] or '').upper())
        out.append({'modality': mod, 'rooms': rs, 'summary': _summary(rs)})

    plans = db.session.execute(text("""
        SELECT si.code AS site_code, si.name AS site_name, fp.image_path,
               fp.image_width AS width, fp.image_height AS height
          FROM floor_plans fp JOIN sites si ON si.id = fp.site_id
         WHERE fp.image_path IS NOT NULL AND si.active IS NOT FALSE
           AND (CAST(:site AS text) IS NULL OR si.code = CAST(:site AS text))
         ORDER BY si.name
    """), {'site': site}).mappings().all()
    if sites is None:
        sites = [dict(s) for s in db.session.execute(text(
            "SELECT code, name FROM sites WHERE active IS NOT FALSE ORDER BY is_default DESC, id")).mappings()]
    modalities = sorted({c['modality'] for c in cards} | set(limits['wait_min']),
                        key=lambda m: (MODALITY_ORDER.index(m) if m in MODALITY_ORDER else len(MODALITY_ORDER), m))
    return {
        'now': _iso(now), 'day_start': _iso(day0 + timedelta(hours=day_start_hour)),
        'site': site, 'sites': sites, 'lanes': out,
        'totals': _summary(cards),
        'unknown_rooms': [{'code': k, 'studies': n} for k, n in sorted(unknown.items(), key=lambda x: -x[1])],
        'plans': [dict(p) for p in plans],
        'limits': {'wait_min': {m: wait_limit(limits, m) for m in modalities},
                   'default_wait_min': limits['default_wait_min'], 'early_pct': limits['early_pct']},
    }


def _finish_room(room, now, day0, day_start_hour, limits):
    """Status, the exam running, the last exam done early, queue and utilization of a room."""
    current, also = None, 0
    if room['running']:
        room['running'].sort(key=lambda x: x[0], reverse=True)
        current = room['running'][0][1]
        also = len(room['running']) - 1     # older starts with no completion: a missing CM
    room['waiting'].sort(key=lambda x: x['arrived_at'])
    room['coming'].sort(key=lambda x: x['appointment_at'] or '')

    early, last_done_at = None, None
    if room['done']:
        room['done'].sort(key=lambda x: x[0], reverse=True)
        done_at, started_at, item = room['done'][0]
        last_done_at = done_at
        if started_at is not None:
            took = _minutes(done_at - started_at)
            # yellow until the room's next exam starts
            later_start = current is not None and datetime.fromisoformat(current['started_at']) > done_at
            if took < item['expected_minutes'] * limits['early_pct'] / 100.0 and not later_start:
                early = dict(item, started_at=_iso(started_at), done_at=_iso(done_at),
                             took_min=round(took, 1))

    if current:
        status = 'late' if current['late'] else 'busy'
    elif room['opening_minutes'] <= 0 and room['exception'] is not None:
        status = 'closed'
    else:
        status = 'free'

    start = window_start_for(day0, room['opening_minutes'], day_start_hour)
    busy = merged_busy_minutes(room['intervals'], start, now)
    opened = open_minutes_so_far(start, now, room['opening_minutes'])
    activity = room['scheduled_today'] or room['waiting'] or current or room['done']
    return {
        'key': room['key'], 'room_code': room['room_code'], 'aetitle': room['aetitle'],
        'room': room['room'], 'modality': room['modality'],
        'site': room['site'], 'site_code': room['site_code'], 'x': room['x'], 'y': room['y'],
        'status': status, 'exception': room['exception'], 'show': bool(room['open_today'] or activity),
        'scheduled_today': room['scheduled_today'], 'to_come': len(room['coming']),
        'coming': room['coming'], 'waiting': room['waiting'],
        'waiting_over': sum(1 for w in room['waiting'] if w['over']),
        'wait_limit_min': room['wait_limit_min'],
        'current': current, 'also_in_progress': also, 'early': early,
        'done_today': len(room['done']), 'last_done_at': _iso(last_done_at),
        'opening_minutes': room['opening_minutes'], 'window_start': _iso(start),
        'busy_min': round(busy, 1), 'open_min': round(opened, 1),
        'utilization_pct': round(100 * busy / opened) if opened else None,
    }


def _summary(cards):
    waiting = [w for c in cards for w in c['waiting']]
    busy = sum(c['busy_min'] for c in cards)
    opened = sum(c['open_min'] for c in cards)
    return {
        'rooms': len(cards),
        'busy': sum(1 for c in cards if c['status'] in ('busy', 'late')),
        'late': sum(1 for c in cards if c['status'] == 'late'),
        'early': sum(1 for c in cards if c['early']),
        'scheduled_today': sum(c['scheduled_today'] for c in cards),
        'to_come': sum(c['to_come'] for c in cards),
        'waiting': len(waiting),
        'waiting_over': sum(1 for w in waiting if w['over']),
        'longest_wait_min': max((w['wait_min'] for w in waiting), default=None),
        'longest_wait_since': min((w['arrived_at'] for w in waiting), default=None),
        'done_today': sum(c['done_today'] for c in cards),
        'utilization_pct': round(100 * busy / opened) if opened else None,
    }
