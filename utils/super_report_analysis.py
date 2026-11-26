"""
utils/super_report_analysis.py — the Super Report's written analysis, by rules (to-do item 42).

Operator, 24-26 Nov 2026: a weekly analysis like the one written by hand on the 16-22 Nov
export, done by RAYD itself (no AI, nothing leaves the server). It replaces the older narrative,
which measured against fixed targets and missed what mattered (its storage alert fired every
week; it named neither the growth nor who drove it, nor the residents' reports).

Rules (memory "show real changes"): every figure against the comparison period, real counts
first, then the change in %; a change of 10 % or more is named, up or down, and never called
"normal"; a base under 10 is a count difference ("+37", not "+3700 %"); each change says what
drives it. Data checks found on the export are written where they apply: reports whose last
author is a resident, no emergency patient class, a demo device, a device booked past its
opening hours, cancellations with no patient class, the CRN count covering both hospitals.

Output: the narrative's section format the page already draws ({icon, color, title, bullets}),
"In three lines" first and "To act on this week" last; Clinical Insights (signals) kept for
its own card. Only the blocks shown are written about (utils/super_report_blocks.py).
"""
from datetime import date

from utils import super_report_blocks as sr_blocks


# ── numbers ─────────────────────────────────────────────────────────────────────

def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def count(v):
    n = num(v)
    return f"{int(round(n)):,}" if n is not None else "—"


def pct_change(cur, prev):
    c, p = num(cur), num(prev)
    if c is None or not p:
        return None
    return (c - p) / p * 100


def _pct_text(p):
    if abs(p) < 10:
        return f"{p:+.1f} %"
    return f"{p:+.0f} %"


def change(cur, prev, unit=""):
    """'+207, +18 %' / '+37' (a base under 10) / 'new' / 'no change' / ''."""
    c, p = num(cur), num(prev)
    if c is None or p is None:
        return ""
    if p == 0:
        return "new" if c else "no change"
    d = c - p
    if d == 0:
        return "no change"
    if abs(p) < 10:
        return f"{d:+,.0f}{unit}"
    return f"{d:+,.0f}{unit}, {_pct_text(pct_change(c, p))}"


def notable(cur, prev):
    """A change worth naming: 10 % or more on a base of 10; 5 or more on a smaller base."""
    c, p = num(cur) or 0, num(prev) or 0
    if p >= 10:
        return abs(pct_change(c, p)) >= 10
    return abs(c - p) >= 5


def with_change(cur, prev, unit="", noun=""):
    """'1,358 studies (+207, +18 %)': the count, its noun, then the change."""
    ch = change(cur, prev, unit)
    return f"{count(cur)}{unit}" + (f" {noun}" if noun else "") + (f" ({ch})" if ch else "")


def dur(minutes):
    m = num(minutes)
    if not m:
        return "—"
    return f"{m / 60:.1f} h" if m >= 120 else f"{m:.0f} min"


def pts(cur, prev):
    c, p = num(cur), num(prev)
    if c is None:
        return "—"
    if p is None:
        return f"{c:g} %"
    return f"{c:g} % ({p:g} % before)"


def dmy(d):
    try:
        return date.fromisoformat(str(d)[:10]).strftime("%d/%m/%Y")
    except ValueError:
        return str(d)


def _by(rows, key, val="cnt"):
    out = {}
    for r in rows or []:
        k = r.get(key)
        if k is not None:
            out[k] = out.get(k, 0) + (num(r.get(val)) or 0)
    return out


def _changes(cur_map, prev_map, limit=5):
    """Notable changes of a {name: count} pair, largest first: (ups, downs, how many more)."""
    rows = []
    for k in set(cur_map) | set(prev_map):
        c, p = cur_map.get(k, 0), prev_map.get(k, 0)
        if notable(c, p):
            rows.append((k, c, p, c - p))
    rows.sort(key=lambda r: -abs(r[3]))
    ups = [r for r in rows if r[3] > 0]
    downs = [r for r in rows if r[3] < 0]
    more = max(len(ups) - limit, 0) + max(len(downs) - limit, 0)
    return ups[:limit], downs[:limit], more


def _list(rows, unit=""):
    return ", ".join(f"{k} {count(c)}{unit} ({change(c, p, unit)})" for k, c, p, _ in rows)


# ── the analysis ────────────────────────────────────────────────────────────────

SECTION_STYLE = {
    "In three lines": ("bi-stars", "#fbbf24"),
    "Volume": ("bi-graph-up", "#34d399"),
    "Referring physicians": ("bi-person-badge", "#a78bfa"),
    "Reporting": ("bi-clock-history", "#f59e0b"),
    "Radiologists and residents": ("bi-person-check", "#60a5fa"),
    "Devices": ("bi-speedometer2", "#22d3ee"),
    "Orders": ("bi-clipboard2-pulse", "#fb923c"),
    "Critical results": ("bi-exclamation-octagon", "#f87171"),
    "Scheduling": ("bi-calendar-check", "#38bdf8"),
    "Storage": ("bi-hdd-stack", "#2dd4bf"),
    "Patients": ("bi-people", "#f472b6"),
    "To act on this week": ("bi-check2-square", "#fbbf24"),
}


class _Analysis:
    def __init__(self, cur, prev, period, blocks):
        self.cur, self.prev, self.period = cur or {}, prev or {}, period or {}
        self.blocks = set(sr_blocks.clean(blocks))
        self.sections = []           # (title, bullets)
        self.actions = []            # (priority, text, why)
        self.headline = {}           # what the three lines may say

    def on(self, *keys):
        return any(k in self.blocks for k in keys)

    def add(self, title, bullets):
        bullets = [b for b in bullets if b]
        if bullets:
            self.sections.append((title, bullets))

    def act(self, priority, text, why=""):
        self.actions.append((priority, text, why))

    def c(self, *path, prev=False, default=None):
        node = self.prev if prev else self.cur
        for p in path:
            node = (node or {}).get(p) if isinstance(node, dict) else None
        return default if node is None else node

    # ── volume ──
    def volume(self):
        s, ps = self.c("kpis", "total_studies"), self.c("kpis", "total_studies", prev=True)
        pt, ppt = self.c("kpis", "total_patients"), self.c("kpis", "total_patients", prev=True)
        bullets = [f"{with_change(s, ps, noun='studies')} for {with_change(pt, ppt, noun='patients')}."]
        self.headline["studies"] = (s, ps, pt, ppt)

        days = self.c("daily_series", default=[])
        pdays = self.c("daily_series", prev=True, default=[])

        def split(rows):
            wk = we = 0
            for r in rows:
                try:
                    d = date.fromisoformat(str(r["date"])[:10])
                except (KeyError, ValueError):
                    continue
                if d.weekday() < 5:
                    wk += num(r.get("cnt")) or 0
                else:
                    we += num(r.get("cnt")) or 0
            return wk, we
        wk, we = split(days)
        pwk, pwe = split(pdays)
        if (wk or pwk) and (we or pwe):
            bullets.append(f"Monday to Friday {with_change(wk, pwk)}; Saturday and Sunday {with_change(we, pwe)}.")
        if days:
            top = max(days, key=lambda r: num(r.get("cnt")) or 0)
            bullets.append(f"Busiest day: {dmy(top['date'])}, {count(top['cnt'])} studies.")

        mods = _by(self.c("modality_series", default=[]), "modality")
        pmods = _by(self.c("modality_series", prev=True, default=[]), "modality")
        ups, downs, more = _changes(mods, pmods)
        if ups:
            bullets.append(f"Up: {_list(ups)}.")
        if downs:
            bullets.append(f"Down: {_list(downs)}.")
        total_d = (num(s) or 0) - (num(ps) or 0)
        movers = ups if total_d > 0 else downs
        if total_d and movers and abs(movers[0][3]) >= 0.4 * abs(total_d) and abs(total_d) >= 10:
            k, _, _, d = movers[0]
            word = "extra" if total_d > 0 else "missing"
            bullets.append(f"{k} accounts for {count(abs(d))} of the {count(abs(total_d))} {word} studies.")
            self.headline["driver"] = (k, abs(d), abs(total_d), word)
        self.add("Volume", bullets)

    # ── referring physicians ──
    def referrers(self):
        cur = _by(self.c("physicians", default=[]), "physician")
        prev = _by(self.c("physicians", prev=True, default=[]), "physician")
        if not cur:
            return
        ups, downs, more = _changes(cur, prev, limit=4)
        bullets = []
        top = sorted(cur.items(), key=lambda kv: -kv[1])[:3]
        bullets.append("Most studies: " + ", ".join(f"{k} {count(v)}" for k, v in top) + ".")
        if ups:
            bullets.append(f"Up: {_list(ups)}.")
        if downs:
            bullets.append(f"Down: {_list(downs)}.")
        s, ps = num(self.c("kpis", "total_studies")) or 0, num(self.c("kpis", "total_studies", prev=True)) or 0
        gain = sum(r[3] for r in ups[:3])
        if s > ps and gain >= 0.5 * (s - ps) and len(ups) >= 2:
            bullets.append(f"These {min(len(ups), 3)} physicians account for {count(gain)} of the {count(s - ps)} extra studies.")
        if more:
            bullets.append(f"{more} more referring physicians changed by 10 % or more.")
        self.add("Referring physicians", bullets)

    # ── reporting ──
    def reporting(self):
        t, pt_ = self.c("tat", default={}), self.c("tat", prev=True, default={})
        s, ps = num(self.c("kpis", "total_studies")) or 0, num(self.c("kpis", "total_studies", prev=True)) or 0
        rep, prep = num(t.get("reported_count")) or 0, num(pt_.get("reported_count")) or 0
        bullets = []
        if s:
            share = rep / s * 100
            pshare = (prep / ps * 100) if ps else None
            bullets.append(f"{count(rep)} of {count(s)} studies reported ({share:.1f} %"
                           + (f"; {pshare:.1f} % before)." if pshare is not None else ")."))
        med, pmed = num(t.get("median_tat_min")), num(pt_.get("median_tat_min"))
        if med:
            ch = pct_change(med, pmed)
            bullets.append(f"Median time from arrival on PACS to the report: {dur(med)}"
                           + (f" ({dur(pmed)} before, {_pct_text(ch)})." if ch is not None else "."))
            self.headline["tat"] = (med, pmed)
        res, pres = num(t.get("resident_last_author")) or 0, num(pt_.get("resident_last_author")) or 0
        if res and rep:
            share = res / rep * 100
            bullets.append(f"{count(res)} reports ({share:.0f} %) still have a resident as their last author "
                           f"({count(pres)} before): this period's times and per-radiologist counts will change "
                           "as radiologists approve them.")
            if share >= 10 and res > pres:
                self.headline["residents"] = res
                self.act(5, "Read this period's turnaround again next week.",
                         f"{count(res)} reports still await a radiologist's approval.")
        mods = {r["modality"]: r for r in t.get("by_modality") or []}
        pmods = {r["modality"]: r for r in pt_.get("by_modality") or []}
        slower, faster = [], []
        for m, r in mods.items():
            p = pmods.get(m)
            if not p or (num(r.get("cnt")) or 0) < 5 or (num(p.get("cnt")) or 0) < 5:
                continue
            ch = pct_change(r.get("median_tat_min"), p.get("median_tat_min"))
            if ch is None or abs(ch) < 10:
                continue
            (slower if ch > 0 else faster).append((m, r, p, ch))
        slower.sort(key=lambda x: -x[3])
        faster.sort(key=lambda x: x[3])
        if slower:
            bullets.append("Slower: " + ", ".join(f"{m} {dur(r['median_tat_min'])} (from {dur(p['median_tat_min'])})"
                                                 for m, r, p, _ in slower[:4]) + ".")
            for m, r, p, _ in slower[:2]:
                if (num(r.get("cnt")) or 0) >= 10:
                    self.act(1, f"{m} reporting takes {dur(r['median_tat_min'])}, from {dur(p['median_tat_min'])}.",
                             f"{count(r['cnt'])} studies.")
        if faster:
            bullets.append("Faster: " + ", ".join(f"{m} {dur(r['median_tat_min'])} (from {dur(p['median_tat_min'])})"
                                                 for m, r, p, _ in faster[:4]) + ".")
        slowest = sorted(mods.values(), key=lambda r: -(num(r.get("median_tat_min")) or 0))[:3]
        if slowest:
            bullets.append("Slowest now: " + ", ".join(f"{r['modality']} {dur(r['median_tat_min'])}" for r in slowest) + ".")
        if rep and not num(t.get("er_reported_count")):
            classes = [str(r.get("class")) for r in self.c("demographics", "pc_breakdown", default=[]) if r.get("class")]
            seen = f" (classes seen: {', '.join(classes[:6])})" if classes else ""
            bullets.append(f"No study carries an emergency patient class{seen}: the emergency turnaround is on Report 38.")
        self.add("Reporting", bullets)

    # ── radiologists and residents ──
    def people(self):
        bullets = []
        if self.on("radiologists"):
            cur = _by(self.c("rad_matrix", "by_modality", default=[]), "radiologist")
            prev = _by(self.c("rad_matrix", "by_modality", prev=True, default=[]), "radiologist")
            if cur:
                ups, downs, more = _changes(cur, prev, limit=4)
                top = sorted(cur.items(), key=lambda kv: -kv[1])[:3]
                bullets.append("Most reports: " + ", ".join(f"{k} {count(v)}" for k, v in top) + ".")
                if ups:
                    bullets.append(f"Up: {_list(ups)}.")
                if downs:
                    bullets.append(f"Down: {_list(downs)}.")
                    k, c, p, d = downs[0]
                    if p >= 50 and c <= 0.6 * p:
                        self.act(3, f"{k}'s reports fell to {count(c)}, from {count(p)}.",
                                 "Approvals still pending, or an absence.")
                if more:
                    bullets.append(f"{more} more changed by 10 % or more.")
        if self.on("wet_reads"):
            w, pw = self.c("resident_wetreads", default={}) or {}, self.c("resident_wetreads", prev=True, default={}) or {}
            if w.get("coverage") and w.get("total"):
                s = num(self.c("kpis", "total_studies")) or 0
                share = f" ({w['total'] / s * 100:.0f} % of the studies)" if s else ""
                top = ", ".join(f"{r['name']} {count(r['total'])}" for r in (w.get("residents") or [])[:3])
                bullets.append(f"Residents read first on {with_change(w['total'], pw.get('total') if pw.get('coverage') else None, noun='studies')}{share}"
                               + (f": {top}." if top else "."))
        self.add("Radiologists and residents", bullets)

    # ── devices ──
    def devices(self):
        bullets = []
        u, pu = self.c("utilization", default={}) or {}, self.c("utilization", prev=True, default={}) or {}
        if self.on("utilization") and u.get("available"):
            avg, pavg = num(u.get("avg_utilization")), num(pu.get("avg_utilization")) if pu.get("available") else None
            bullets.append(f"Booked time used {avg:g} % of opening hours on average"
                           + (f" ({pavg:g} % before)." if pavg is not None else "."))
            over = [b for b in u.get("busiest") or [] if (num(b.get("avg")) or 0) > 100]
            near = [b for b in u.get("busiest") or [] if 90 <= (num(b.get("avg")) or 0) <= 100]
            for b in over:
                bullets.append(f"{b['ae']} is booked above its opening hours ({num(b['avg']):g} %).")
                self.act(2, f"{b['ae']} is booked above its opening hours ({num(b['avg']):g} %).",
                         "It works outside its hours, or its hours in RAYD are too short.")
            if near:
                bullets.append("Near full: " + ", ".join(f"{b['ae']} {num(b['avg']):g} %" for b in near) + ".")
            low = u.get("idlest") or []
            if low:
                bullets.append(f"{count(u.get('low_util_count'))} devices under the low-use line; lowest: "
                               + ", ".join(f"{b['ae']} {num(b['avg']):g} %" for b in low) + ".")
        if self.on("kpis"):
            ae, pae = self.c("ae_ops", default={}) or {}, self.c("ae_ops", prev=True, default={}) or {}
            if ae.get("busiest_ae"):
                same = pae.get("busiest_ae") == ae["busiest_ae"]
                bullets.append(f"Busiest device: {ae['busiest_ae']}, {with_change(ae.get('busiest_cnt'), pae.get('busiest_cnt') if same else None, noun='studies')}.")
            idle = str(ae.get("idle_ae") or "")
            if idle and not num(ae.get("idle_cnt")) and any(w in idle.lower() for w in ("demo", "test")):
                bullets.append(f"The quietest device is {idle}, which looks like a demo or test entry with no studies.")
                self.act(6, f"Remove {idle} from the device list (Mapping).", "A demo or test entry with no studies.")
        self.add("Devices", bullets)

    # ── orders ──
    def orders(self):
        o, po = self.c("orders", default={}) or {}, self.c("orders", prev=True, default={}) or {}
        if not num(o.get("total")):
            return
        missing = (num(o.get("total")) or 0) - (num(o.get("fulfilled")) or 0)
        pmissing = (num(po.get("total")) or 0) - (num(po.get("fulfilled")) or 0)
        bullets = [f"{with_change(o.get('total'), po.get('total'), noun='RIS orders')}; "
                   f"{num(o.get('fulfillment_pct')) or 0:g} % became a study"
                   + (f" ({num(po.get('fulfillment_pct')):g} % before)." if num(po.get("fulfillment_pct")) is not None else "."),
                   f"{with_change(missing, pmissing, noun='orders')} without a study."]
        if missing >= 10 and pmissing and pct_change(missing, pmissing) >= 10:
            self.act(4, f"{count(missing)} RIS orders did not become a study ({change(missing, pmissing)}).",
                     "Check Report 27 for orders the PACS never received.")
        self.add("Orders", bullets)

    # ── critical results ──
    def crn(self):
        c, pc = self.c("crn", default={}) or {}, self.c("crn", prev=True, default={}) or {}
        if "critical_count" not in c:
            return
        bullets = [f"{with_change(c.get('critical_count'), pc.get('critical_count'), noun='reports')} flagged critical "
                   "by the text reader (both hospitals' reports)."]
        if not self.period.get("crn_enabled"):
            bullets.append("Notifications are off, so none were sent or acknowledged.")
        elif "total_notifications" in c:
            bullets.append(f"{count(c.get('total_notifications'))} notifications, {count(c.get('acknowledged'))} acknowledged"
                           + (f", median {dur(c.get('median_ack_min'))} to acknowledge." if c.get("median_ack_min") else "."))
            late = (num(c.get("overdue")) or 0) + (num(c.get("open")) or 0)
            if late:
                bullets.append(f"{count(c.get('overdue'))} went overdue, {count(c.get('open'))} still open.")
                self.act(1, f"{count(late)} critical result notifications overdue or open.",
                         "Admin > CRN > Report lists them.")
        self.add("Critical results", bullets)

    # ── scheduling ──
    def scheduling(self):
        lt, plt = self.c("lead_time", default={}) or {}, self.c("lead_time", prev=True, default={}) or {}
        k, pk = lt.get("kpi") or {}, plt.get("kpi") or {}
        if not k.get("exams"):
            return
        ahead = lt.get("ahead_days", 7)
        bullets = [f"{with_change(k.get('exams'), pk.get('exams'), noun='RIS appointments')}; booked the same day "
                   f"{pts(k.get('same_day_pct'), pk.get('same_day_pct'))}, {ahead}+ days ahead "
                   f"{pts(k.get('ahead_pct'), pk.get('ahead_pct'))}.",
                   f"Cancelled {pts(k.get('cancelled_pct'), pk.get('cancelled_pct'))}; scheduled more than once "
                   f"{pts(k.get('rescheduled_pct'), pk.get('rescheduled_pct'))}."]
        if k.get("median_days") is not None and pk.get("median_days") is not None and k["median_days"] != pk["median_days"]:
            bullets.append(f"Median lead time {num(k['median_days']):g} days, from {num(pk['median_days']):g}.")
        after, pafter = num(k.get("after")) or 0, num(pk.get("after"))
        if after:
            bullets.append(f"{with_change(after, pafter, noun='appointments')} were created on the RIS after the exam "
                           f"({after / num(k['exams']) * 100:.0f} %).")
            worst = sorted((r for r in lt.get("by_modality") or []
                            if (num(r.get("exams")) or 0) >= 20 and num(r.get("after"))),
                           key=lambda r: -(num(r["after"]) / num(r["exams"])))
            for r in worst[:1]:
                share = num(r["after"]) / num(r["exams"]) * 100
                if share >= 30:
                    bullets.append(f"{r['value']}: {count(r['after'])} of {count(r['exams'])} ({share:.0f} %) created after the exam.")
                    self.act(4, f"{share:.0f} % of {r['value']} appointments were created after the exam.",
                             f"How {r['value']} bookings reach the RIS.")
        cancels = [(r, (num(r.get("cancelled_pct")) or 0) * (num(r.get("exams")) or 0) / 100) for r in lt.get("by_class") or []]
        total_c = sum(n for _, n in cancels)
        unknown = sum(n for r, n in cancels if str(r.get("value")).lower() in ("not recorded", "unknown", "none", ""))
        if total_c >= 10 and unknown >= 0.8 * total_c:
            bullets.append(f"Nearly every cancelled appointment ({count(unknown)} of {count(total_c)}) has no patient class.")
        self.add("Scheduling", bullets)

    # ── storage ──
    def storage(self):
        st, pst = self.c("storage", default={}) or {}, self.c("storage", prev=True, default={}) or {}
        gb, pgb = num(st.get("total_gb")), num(pst.get("total_gb"))
        if gb is None:
            return
        ch = pct_change(gb, pgb)
        bullets = [f"{gb:,.1f} GB stored" + (f" ({_pct_text(ch)})" if ch is not None else "")
                   + f", {num(st.get('avg_gb_per_day')) or 0:,.1f} GB a day."]
        img, pimg = self.c("kpis", "total_images"), self.c("kpis", "total_images", prev=True)
        s_ch = pct_change(self.c("kpis", "total_studies"), self.c("kpis", "total_studies", prev=True))
        i_ch = pct_change(img, pimg)
        if s_ch is not None and i_ch is not None and (s_ch > 0) != (i_ch > 0) and max(abs(s_ch), abs(i_ch)) >= 5:
            bullets.append(f"Images {_pct_text(i_ch)} while studies {_pct_text(s_ch)}: the modality mix changed.")
        mods = {r["modality"]: num(r.get("gb")) or 0 for r in st.get("by_modality") or []}
        pmods = {r["modality"]: num(r.get("gb")) or 0 for r in pst.get("by_modality") or []}
        moved = [(m, v, pmods[m]) for m, v in mods.items() if pmods.get(m, 0) >= 1 and abs(pct_change(v, pmods[m])) >= 10]
        if moved:
            bullets.append("By modality: " + ", ".join(f"{m} {v:,.1f} GB ({_pct_text(pct_change(v, p))})" for m, v, p in moved) + ".")
        self.add("Storage", bullets)

    # ── patients ──
    def patients(self):
        d, pd = self.c("demographics", default={}) or {}, self.c("demographics", prev=True, default={}) or {}
        parts = [f"{label} {with_change(d.get(key), pd.get(key))}"
                 for label, key in (("Outpatients", "outpatient"), ("inpatients", "inpatient"),
                                    ("women", "female"), ("men", "male"))
                 if num(d.get(key))]
        if parts:
            self.add("Patients", ["; ".join(parts) + "."])

    # ── the three lines ──
    def three_lines(self):
        lines = []
        if "studies" in self.headline:
            s, ps, pt, ppt = self.headline["studies"]
            ch = pct_change(s, ps)
            word = ("A busier period" if ch is not None and ch >= 10 else
                    "A quieter period" if ch is not None and ch <= -10 else "Studies")
            text = f"{word}: {with_change(s, ps, noun='studies')} for {with_change(pt, ppt, noun='patients')}"
            if "driver" in self.headline:
                k, d, total, w = self.headline["driver"]
                text += f"; {k} accounts for {count(d)} of the {count(total)} {w} studies"
            lines.append(text + ".")
        if "tat" in self.headline:
            med, pmed = self.headline["tat"]
            ch = pct_change(med, pmed)
            text = f"Median turnaround {dur(med)}" + (f" ({dur(pmed)} before, {_pct_text(ch)})" if ch is not None else "")
            if "residents" in self.headline:
                text += f", but {count(self.headline['residents'])} reports are still residents' preliminary reads"
            lines.append(text + ".")
        for _, text, _why in sorted(self.actions, key=lambda a: a[0]):
            if len(lines) >= 3:
                break
            if not text.startswith("Read this period"):
                lines.append(text)
        return lines[:3]

    def run(self):
        if self.on("volume", "kpis", "summary"):
            self.volume()
        if self.on("referrers"):
            self.referrers()
        if self.on("tat"):
            self.reporting()
        if self.on("radiologists", "wet_reads"):
            self.people()
        if self.on("utilization", "kpis"):
            self.devices()
        if self.on("orders"):
            self.orders()
        if self.on("crn"):
            self.crn()
        if self.on("lead_time"):
            self.scheduling()
        if self.on("storage"):
            self.storage()
        if self.on("demographics"):
            self.patients()
        out = []
        three = self.three_lines()
        if three:
            out.append(("In three lines", three))
        out += self.sections
        acts = sorted(self.actions, key=lambda a: a[0])[:6]
        if acts:
            out.append(("To act on this week", [f"{t} {w}".strip() for _, t, w in acts]))
        return [{"icon": SECTION_STYLE.get(t, ("bi-dot", "#94a3b8"))[0],
                 "color": SECTION_STYLE.get(t, ("bi-dot", "#94a3b8"))[1],
                 "title": t, "bullets": b} for t, b in out]


def analyse(cur, prev, period=None, blocks=None):
    """The analysis as narrative sections. period: {'start', 'end', 'cmp_start', 'cmp_end',
    'crn_enabled'}; blocks: the blocks shown (None: all)."""
    return _Analysis(cur, prev, period, blocks).run()
