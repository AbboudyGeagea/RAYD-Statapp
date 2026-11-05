"""
utils/department_board.py
────────────────────────────────────────────────────────────────
The Live Department board: for every device, what it is doing now, who is waiting
for it, what is coming to it, and how busy it has been today — read from BRAYN's
study ladder (brayn_study_state), so it moves with the HL7 messages.

ON LAUMC the ladder is the BRAYN shadow's (utils/brayn_shadow.py): it fills only
while settings.brayn_enabled = 'true', and arrivals / starts / RIS completions need
the RIS lifecycle feed (R2I). Until that feed reaches RAYD the board shows HIS
orders as incoming and PACS completions as done, and nobody waiting or in progress.
Ported from the HL7 branch with the same logic (operator, 2026-11-02).

    brayn_study_state ──▶ device (aetitle → aetitle_modality_map) ──▶ modality lane

    in progress   started, not completed (no CM, no images in PACS), last 12 h
    waiting       arrived, not started, last 12 h        — wait = now − arrived
    incoming      ordered or scheduled in the last 24 h, not arrived
    done today    completed (else images in PACS) since midnight

Each device carries its floor-plan position (aetitle_modality_map.floor_x / floor_y,
percent of the plan, set in Admin > Floor plan), and the board lists every site's
plan image (floor_plans). A site filter keeps that site's devices and the ones with
no site, which belong to no plan.

A study with no device yet (an order the RIS has not assigned) sits in its
modality's "Unassigned" card. A device that sends messages but is not in the AE
map gets a card of its own, flagged, so nothing it does is hidden. A device with no
room in the AE map shows the room its messages carry (PV1-3.2). Patient names come
from the patient record (ADT), else from the patient's latest HIS order.

Expected finish = start + the procedure's duration (procedure_duration_map), else
the median duration of the device's modality there, else DEFAULT_MINUTES. Past it,
the device is "overrun".

Utilization so far = busy minutes ÷ open minutes so far, both counted from the day
start (the morning shift's start, settings.shift_morning_start, 07:00 by default;
midnight for a device open 24 hours) up to now. Open minutes are Capacity Ladder's
for the day: the device exception, else the RIS weekly availability, else the daily
capacity, else the weekly schedule. Busy minutes merge overlapping studies, so two studies on one
device at once never count twice; over 100 % means the device worked past its
opening minutes.

All times are the hospital's naive local time, as BRAYN stores them; "now" is the
database's LOCALTIMESTAMP on hospital time (brayn_shadow.clock) — LAUMC's Postgres
runs on UTC, and its own clock would put every wait two or three hours off.
"""
from collections import defaultdict
from datetime import datetime, timedelta
from statistics import median

from sqlalchemy import text

from db import db
from utils.brayn_shadow import clock

DEFAULT_MINUTES = 20          # expected duration when neither procedure nor modality has one
ACTIVE_HOURS = 12             # how far back an arrival / a start still counts as open
INCOMING_HOURS = 24           # how far back an order / a scheduling still counts as coming
WAIT_WARN_MIN = 30            # waiting longer than this: amber
WAIT_ALERT_MIN = 60           # waiting longer than this: red
DEFAULT_DAY_START_HOUR = 7

# Lanes in the order a department reads them; anything else follows alphabetically.
MODALITY_ORDER = ['CT', 'MR', 'US', 'DX', 'CR', 'XR', 'RF', 'MG', 'XA', 'NM', 'PT', 'BMD', 'OT']
_NOT_LANES = {'SR', 'PR', 'KO', 'PACS'}


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
    """Where today's opening starts: midnight for a 24-hour device, else the day start."""
    if opening_minutes is not None and opening_minutes >= 24 * 60:
        return day0
    return day0 + timedelta(hours=day_start_hour)


def open_minutes_so_far(window_start, now, opening_minutes):
    if not opening_minutes:
        return 0.0
    return max(0.0, min(_minutes(now - window_start), float(opening_minutes)))


# ── reads ─────────────────────────────────────────────────────────────────────

_STUDIES_SQL = """
WITH cand AS (
    -- each branch reads one of brayn_study_state's partial indexes (0119, 0140)
    SELECT accession_number FROM brayn_study_state
     WHERE arrived_at IS NULL AND cancelled_at IS NULL AND NOT is_closed
       AND scheduled_at >= :incoming_since
    UNION
    SELECT accession_number FROM brayn_study_state
     WHERE scheduled_at IS NULL AND cancelled_at IS NULL AND NOT is_closed
       AND ordered_at >= :incoming_since
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
    UNION
    SELECT accession_number FROM brayn_study_state WHERE pacs_completed_at >= :day0
)
SELECT s.accession_number, s.placer_order_number, s.patient_id,
       COALESCE(NULLIF(btrim(p.patient_name), ''), o.patient_name) AS patient_name,
       s.modality, s.aetitle, s.room_name, s.procedure_code, s.procedure_text,
       s.patient_class, s.patient_location, s.ordered_at, s.scheduled_at,
       s.arrived_at, s.started_at, COALESCE(s.completed_at, s.pacs_completed_at) AS done_at,
       s.is_provisional, s.first_seen_at
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
 WHERE s.cancelled_at IS NULL
"""

_DEVICES_SQL = """
SELECT m.aetitle, upper(btrim(m.aetitle)) AS ae_key, m.modality,
       NULLIF(btrim(m.display_aetitle), '') AS display_name,
       NULLIF(btrim(m.room_name), '') AS room, si.code AS site_code, si.name AS site_name,
       m.floor_x, m.floor_y,
       COALESCE(ex.actual_opening_minutes, dwa.available_minutes, m.daily_capacity_minutes,
                ws.std_opening_minutes) AS opening_minutes,
       (ex.actual_opening_minutes IS NOT NULL) AS has_exception, ex.reason AS exception_reason
  FROM aetitle_modality_map m
  LEFT JOIN sites si ON si.id = m.site_id
  LEFT JOIN device_exceptions ex
         ON ex.aetitle = m.aetitle AND ex.exception_date = CAST(:day0 AS date)
  LEFT JOIN std_device_weekly_availability dwa
         ON upper(btrim(dwa.aetitle)) = upper(btrim(m.aetitle)) AND dwa.day_of_week = :dow
  LEFT JOIN device_weekly_schedule ws
         ON upper(btrim(ws.aetitle)) = upper(btrim(m.aetitle)) AND ws.day_of_week = :dow
 WHERE m.active IS NOT FALSE
   AND COALESCE(m.modality, '') NOT IN ('SR', 'PR', 'KO', 'PACS')
   AND COALESCE(m.site_id = (SELECT id FROM sites WHERE is_default), TRUE)
 ORDER BY m.modality, m.aetitle
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


# ── the board ─────────────────────────────────────────────────────────────────

def _expected(code, modality, by_code, by_mod):
    m = by_code.get((code or '').strip().upper())
    if m:
        return m, 'procedure'
    m = by_mod.get((modality or '').strip().upper())
    if m:
        return m, 'modality'
    return DEFAULT_MINUTES, 'default'


def _person(r):
    return {'accession': r['accession_number'] if not r['is_provisional'] else None,
            'order': r['placer_order_number'], 'patient_id': r['patient_id'],
            'patient': r['patient_name'] or r['patient_id'] or 'Unknown patient',
            'procedure': r['procedure_text'] or r['procedure_code'] or '—',
            'patient_class': r['patient_class'], 'location': r['patient_location']}


def _iso(dt):
    return dt.isoformat(timespec='seconds') if dt else None


def build_board(site=None, now=None):
    """The whole board as plain data (see routes/department_board.py for the JSON)."""
    if now is None:
        with clock():
            now = db.session.execute(text('SELECT LOCALTIMESTAMP(0)')).scalar()
    day0 = datetime.combine(now.date(), datetime.min.time())
    day_start_hour = _day_start_hour()
    by_code, by_mod = _durations()

    devices = {}
    for d in db.session.execute(text(_DEVICES_SQL), {'day0': day0, 'dow': now.weekday()}).mappings():
        if site and d['site_code'] and d['site_code'] != site:
            continue                       # another site's device; one with no site shows everywhere
        placed = d['floor_x'] is not None and d['floor_y'] is not None
        devices[d['ae_key']] = {
            'aetitle': d['aetitle'], 'name': d['display_name'] or d['aetitle'],
            'room': d['room'], 'modality': (d['modality'] or 'OT').upper(),
            'site': d['site_name'], 'site_code': d['site_code'], 'mapped': True,
            'x': float(d['floor_x']) if placed else None,
            'y': float(d['floor_y']) if placed else None,
            'opening_minutes': d['opening_minutes'] or 0,
            'exception': d['exception_reason'] if d['has_exception'] else None,
            'in_progress': [], 'waiting': [], 'incoming': [], 'intervals': [],
            'done_today': 0, 'last_done_at': None,
        }
    lanes_unassigned = defaultdict(lambda: {'waiting': [], 'incoming': [], 'in_progress': []})

    rows = db.session.execute(text(_STUDIES_SQL), {
        'day0': day0,
        'active_since': now - timedelta(hours=ACTIVE_HOURS),
        'incoming_since': now - timedelta(hours=INCOMING_HOURS),
    }).mappings().all()
    # An HIS order is first held as a provisional lifecycle; once the RIS schedules it
    # under an accession, the real one carries it. Never list the same order twice.
    real_placers = {r['placer_order_number'] for r in rows
                    if not r['is_provisional'] and r['placer_order_number']}

    for r in rows:
        if r['is_provisional'] and r['placer_order_number'] in real_placers:
            continue
        ae_key = (r['aetitle'] or '').strip().upper()
        dev = devices.get(ae_key) if ae_key else None
        if ae_key and dev is None:
            mod = (r['modality'] or 'OT').upper()
            if mod in _NOT_LANES:
                continue
            dev = devices[ae_key] = {
                'aetitle': r['aetitle'].strip(), 'name': r['aetitle'].strip(),
                'room': r['room_name'], 'modality': mod, 'site': None, 'site_code': None,
                'mapped': False, 'x': None, 'y': None, 'opening_minutes': 0, 'exception': None,
                'in_progress': [], 'waiting': [], 'incoming': [], 'intervals': [],
                'done_today': 0, 'last_done_at': None,
            }
        modality = dev['modality'] if dev else (r['modality'] or 'OT').upper()
        if modality in _NOT_LANES:
            continue
        if dev is not None and not dev['room'] and r['room_name']:
            dev['room'] = r['room_name']        # no room in the AE map: the one the RIS sends
        minutes, source = _expected(r['procedure_code'], modality, by_code, by_mod)
        item = _person(r)

        if r['done_at']:
            if dev is not None and r['done_at'] >= day0:
                dev['done_today'] += 1
                dev['last_done_at'] = max(filter(None, [dev['last_done_at'], r['done_at']]))
                start = r['started_at'] or r['done_at'] - timedelta(minutes=minutes)
                dev['intervals'].append((start, r['done_at']))
            continue
        if r['started_at']:
            if r['started_at'] < now - timedelta(hours=ACTIVE_HOURS):
                continue
            end = r['started_at'] + timedelta(minutes=minutes)
            item.update(started_at=_iso(r['started_at']), expected_minutes=minutes,
                        expected_source=source, expected_end=_iso(end),
                        elapsed_min=round(_minutes(now - r['started_at']), 1),
                        remaining_min=round(_minutes(end - now), 1))
            if dev is not None:
                dev['in_progress'].append((r['started_at'], item))
                dev['intervals'].append((r['started_at'], now))
            else:
                lanes_unassigned[modality]['in_progress'].append(item)
            continue
        if r['arrived_at']:
            if r['arrived_at'] < now - timedelta(hours=ACTIVE_HOURS):
                continue
            wait = _minutes(now - r['arrived_at'])
            item.update(arrived_at=_iso(r['arrived_at']), wait_min=round(wait, 1),
                        expected_minutes=minutes,
                        level='alert' if wait >= WAIT_ALERT_MIN else 'warn' if wait >= WAIT_WARN_MIN else 'ok')
            (dev['waiting'] if dev is not None else lanes_unassigned[modality]['waiting']).append(item)
            continue
        since = r['scheduled_at'] or r['ordered_at'] or r['first_seen_at']
        item.update(scheduled_at=_iso(r['scheduled_at']), ordered_at=_iso(r['ordered_at']),
                    since=_iso(since), expected_minutes=minutes,
                    state='Scheduled' if r['scheduled_at'] else 'Ordered')
        (dev['incoming'] if dev is not None else lanes_unassigned[modality]['incoming']).append(item)

    lanes = defaultdict(lambda: {'devices': []})
    for dev in devices.values():
        lanes[dev['modality']]['devices'].append(_finish_device(dev, now, day0, day_start_hour))
    for mod, extra in lanes_unassigned.items():
        if any(extra.values()):
            extra['waiting'].sort(key=lambda x: x['arrived_at'])
            extra['incoming'].sort(key=lambda x: x['since'] or '')
            lanes[mod]['unassigned'] = extra

    out = []
    for mod in sorted(lanes, key=lambda m: (MODALITY_ORDER.index(m) if m in MODALITY_ORDER
                                            else len(MODALITY_ORDER), m)):
        lane = lanes[mod]
        devs = sorted(lane['devices'], key=lambda d: (not d['mapped'], d['name']))
        un = lane.get('unassigned') or {'waiting': [], 'incoming': [], 'in_progress': []}
        waiting = [w for d in devs for w in d['waiting']] + un['waiting']
        busy = sum(d['busy_min'] for d in devs)
        opened = sum(d['open_min'] for d in devs)
        out.append({
            'modality': mod, 'devices': devs, 'unassigned': lane.get('unassigned'),
            'summary': {
                'devices': len(devs),
                'busy': sum(1 for d in devs if d['status'] in ('busy', 'overrun')),
                'free': sum(1 for d in devs if d['status'] == 'free'),
                'waiting': len(waiting),
                'longest_wait_min': max((w['wait_min'] for w in waiting), default=None),
                'longest_wait_since': min((w['arrived_at'] for w in waiting), default=None),
                'incoming': sum(len(d['incoming']) for d in devs) + len(un['incoming']),
                'done_today': sum(d['done_today'] for d in devs),
                'utilization_pct': round(100 * busy / opened) if opened else None,
            },
        })

    all_dev = [d for lane in out for d in lane['devices']]
    all_wait = [w for lane in out for d in lane['devices'] for w in d['waiting']] + \
               [w for lane in out if lane['unassigned'] for w in lane['unassigned']['waiting']]
    busy = sum(d['busy_min'] for d in all_dev)
    opened = sum(d['open_min'] for d in all_dev)
    sites = db.session.execute(text("""
        SELECT s.code, s.name FROM sites s
         WHERE s.active IS NOT FALSE
           AND COALESCE(s.id = (SELECT id FROM sites WHERE is_default), TRUE)
         ORDER BY s.name
    """)).mappings().all()
    # The floor plan of each site (Admin > Floor plan): the image, under static/.
    plans = db.session.execute(text("""
        SELECT si.code AS site_code, si.name AS site_name, fp.image_path,
               fp.image_width AS width, fp.image_height AS height
          FROM floor_plans fp JOIN sites si ON si.id = fp.site_id
         WHERE fp.image_path IS NOT NULL AND si.active IS NOT FALSE
           AND COALESCE(si.id = (SELECT id FROM sites WHERE is_default), TRUE)
         ORDER BY si.name
    """)).mappings().all()
    return {
        'now': _iso(now), 'day_start': _iso(day0 + timedelta(hours=day_start_hour)),
        'lanes': out,
        'totals': {
            'devices': len(all_dev),
            'busy': sum(1 for d in all_dev if d['status'] in ('busy', 'overrun')),
            'overrun': sum(1 for d in all_dev if d['status'] == 'overrun'),
            'waiting': len(all_wait),
            'longest_wait_min': max((w['wait_min'] for w in all_wait), default=None),
            'longest_wait_since': min((w['arrived_at'] for w in all_wait), default=None),
            'incoming': sum(lane['summary']['incoming'] for lane in out),
            'done_today': sum(d['done_today'] for d in all_dev),
            'utilization_pct': round(100 * busy / opened) if opened else None,
        },
        'sites': [dict(s) for s in sites],
        'plans': [dict(p) for p in plans],
        'site': site,
        'thresholds': {'wait_warn_min': WAIT_WARN_MIN, 'wait_alert_min': WAIT_ALERT_MIN},
    }


def _finish_device(dev, now, day0, day_start_hour):
    """Status, current study, queue estimate and utilization of one device."""
    current, also = None, 0
    if dev['in_progress']:
        dev['in_progress'].sort(key=lambda x: x[0], reverse=True)
        current = dev['in_progress'][0][1]
        also = len(dev['in_progress']) - 1     # older starts with no completion: a missing CM
    dev['waiting'].sort(key=lambda x: x['arrived_at'])
    dev['incoming'].sort(key=lambda x: x['since'] or '')

    if current:
        status = 'overrun' if current['remaining_min'] < 0 else 'busy'
    elif dev['opening_minutes'] <= 0 and dev['mapped'] and not dev['waiting']:
        status = 'closed'
    else:
        status = 'free'

    free_at = now
    if current:
        free_at = max(now, datetime.fromisoformat(current['expected_end']))
    queue_clear = free_at + timedelta(minutes=sum(w['expected_minutes'] for w in dev['waiting']))

    start = window_start_for(day0, dev['opening_minutes'], day_start_hour)
    busy = merged_busy_minutes(dev['intervals'], start, now)
    opened = open_minutes_so_far(start, now, dev['opening_minutes'])
    return {
        'aetitle': dev['aetitle'], 'name': dev['name'], 'room': dev['room'],
        'modality': dev['modality'], 'site': dev['site'], 'site_code': dev['site_code'],
        'x': dev['x'], 'y': dev['y'],
        'mapped': dev['mapped'], 'status': status, 'exception': dev['exception'],
        'current': current, 'also_in_progress': also,
        'waiting': dev['waiting'], 'incoming': dev['incoming'],
        'free_at': _iso(free_at) if current else None,
        'queue_clear_at': _iso(queue_clear) if dev['waiting'] else None,
        'done_today': dev['done_today'], 'last_done_at': _iso(dev['last_done_at']),
        'opening_minutes': dev['opening_minutes'], 'window_start': _iso(start),
        'busy_min': round(busy, 1), 'open_min': round(opened, 1),
        'utilization_pct': round(100 * busy / opened) if opened else None,
    }
