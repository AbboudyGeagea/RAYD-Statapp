"""
tests/test_super_report_analysis.py — the Super Report's analysis by rules (item 42,
utils/super_report_analysis.py), on the figures of the operator's export of 16-22 Nov 2026
against 9-15 Nov, the week analysed by hand on 24 Nov. No database.
"""
from copy import deepcopy

from utils.super_report_analysis import analyse, change, notable

DAYS = ["2026-11-16", "2026-11-17", "2026-11-18", "2026-11-19", "2026-11-20", "2026-11-21", "2026-11-22"]
PDAYS = ["2026-11-09", "2026-11-10", "2026-11-11", "2026-11-12", "2026-11-13", "2026-11-14", "2026-11-15"]

CUR = {
    "kpis": {"total_studies": 1358, "total_patients": 849, "total_images": 458127},
    "daily_series": [{"date": d, "cnt": c} for d, c in zip(DAYS, [252, 226, 223, 256, 250, 82, 69])],
    "modality_series": [{"modality": m, "cnt": c} for m, c in
                        [("DX", 623), ("US", 216), ("CT", 176), ("MR", 159), ("MG", 62), ("CR", 57), ("PT", 29)]],
    "tat": {"reported_count": 1304, "median_tat_min": 1674.5, "resident_last_author": 205, "er_reported_count": 0,
            "by_modality": [{"modality": "CR", "median_tat_min": 3024.1, "cnt": 57},
                            {"modality": "DX", "median_tat_min": 2294.3, "cnt": 600}]},
    "demographics": {"pc_breakdown": [{"class": "O"}, {"class": "I"}, {"class": "S"}],
                     "outpatient": 984, "inpatient": 356, "female": 790, "male": 563},
    "utilization": {"available": True, "avg_utilization": 34.5, "low_util_count": 12,
                    "busiest": [{"ae": "AMX2", "avg": 115.2}, {"ae": "MR750W", "avg": 92.0}],
                    "idlest": [{"ae": "LOGIQE9", "avg": 2.9}]},
    "ae_ops": {"busiest_ae": "XRAY-3 Emergency Department", "busiest_cnt": 247, "idle_ae": "USDemoPhilips", "idle_cnt": 0},
    "orders": {"total": 1588, "fulfilled": 1496, "fulfillment_pct": "94.2"},
    "crn": {"critical_count": 285},
    "lead_time": {"ahead_days": 7,
                  "kpi": {"exams": 1588, "same_day_pct": 92.2, "ahead_pct": 1.9, "cancelled_pct": 4.2,
                          "rescheduled_pct": 1.7, "after": 386, "median_days": 0},
                  "by_modality": [{"value": "MR", "exams": 199, "after": 75}, {"value": "DX", "exams": 690, "after": 127}],
                  "by_class": [{"value": "Not recorded", "exams": 92, "cancelled_pct": 71.7},
                               {"value": "O", "exams": 1096, "cancelled_pct": 0}]},
    "storage": {"total_gb": 112.1, "avg_gb_per_day": 16.0},
    "rad_matrix": {"by_modality": [{"radiologist": r, "cnt": c} for r, c in
                                   [("julie.ferzli", 250), ("tamina.rizk", 178), ("sabine.khoury@sj", 38)]]},
    "physicians": [{"physician": p, "cnt": c} for p, c in
                   [("CHAHINE CHAHINE ASSI", 123), ("ALAIN FADY TANIOS", 110), ("RAMZI NAKHLE", 88)]],
}
PREV = {
    "kpis": {"total_studies": 1151, "total_patients": 738, "total_images": 513358},
    "daily_series": [{"date": d, "cnt": c} for d, c in zip(PDAYS, [210, 179, 209, 189, 195, 98, 71])],
    "modality_series": [{"modality": m, "cnt": c} for m, c in
                        [("DX", 486), ("US", 194), ("CT", 183), ("MR", 148), ("MG", 47), ("CR", 32), ("PT", 33)]],
    "tat": {"reported_count": 1129, "median_tat_min": 1989.7, "resident_last_author": 1,
            "by_modality": [{"modality": "CR", "median_tat_min": 1700, "cnt": 32},
                            {"modality": "DX", "median_tat_min": 2844.8, "cnt": 482}]},
    "utilization": {"available": True, "avg_utilization": 30.6},
    "ae_ops": {"busiest_ae": "XRAY-3 Emergency Department", "busiest_cnt": 206},
    "orders": {"total": 1394, "fulfilled": 1280, "fulfillment_pct": "91.8"},
    "crn": {"critical_count": 316},
    "lead_time": {"kpi": {"exams": 1394, "same_day_pct": 88.5, "ahead_pct": 2.5, "cancelled_pct": 6.0,
                          "rescheduled_pct": 2.1, "after": 416, "median_days": 0}},
    "storage": {"total_gb": 122.4},
    "rad_matrix": {"by_modality": [{"radiologist": r, "cnt": c} for r, c in
                                   [("julie.ferzli", 192), ("tamina.rizk", 346), ("sabine.khoury@sj", 1)]]},
    "physicians": [{"physician": p, "cnt": c} for p, c in
                   [("CHAHINE CHAHINE ASSI", 43), ("ALAIN FADY TANIOS", 63), ("RAMZI NAKHLE", 40)]],
}


def sections(blocks=None, cur=CUR, prev=PREV, **period):
    return {s["title"]: s["bullets"] for s in analyse(cur, prev, period, blocks)}


def text(blocks=None, **kw):
    return " | ".join(b for bs in sections(blocks, **kw).values() for b in bs)


def test_changes_read_in_counts_then_percent_and_a_small_base_as_a_difference():
    assert change(1358, 1151) == "+207, +18 %"
    assert change(285, 316) == "-31, -9.8 %"          # under 10 %: one decimal
    assert change(38, 1) == "+37"                     # never "+3700 %"
    assert change(5, 0) == "new" and change(7, 7) == "no change" and change(None, 3) == ""
    assert notable(57, 32) and not notable(176, 183) and notable(38, 1) and not notable(4, 1)


def test_the_three_lines_name_the_growth_its_driver_and_the_turnaround_caveat():
    three = sections()["In three lines"]
    assert three[0] == ("A busier period: 1,358 studies (+207, +18 %) for 849 patients (+111, +15 %); "
                        "DX accounts for 137 of the 207 extra studies.")
    assert three[1].startswith("Median turnaround 27.9 h (33.2 h before, -16 %), but 205 reports")
    assert three[2] == "CR reporting takes 50.4 h, from 28.3 h."
    assert len(three) == 3


def test_volume_splits_weekdays_and_weekend_and_lists_the_moves():
    v = sections()["Volume"]
    assert "Monday to Friday 1,207 (+225, +23 %); Saturday and Sunday 151 (-18, -11 %)." in v
    assert "Busiest day: 19/11/2026, 256 studies." in v
    assert any(b.startswith("Up: DX 623 (+137, +28 %), CR 57 (+25, +78 %)") for b in v)
    assert "Down: PT 29 (-4, -12 %)." in v
    assert not any("CT 176" in b for b in v)          # -3.8 %: under 10 %, not listed


def test_referrers_who_drive_the_growth():
    r = sections()["Referring physicians"]
    assert "These 3 physicians account for 175 of the 207 extra studies." in r


def test_the_data_checks_of_the_export():
    t = text()
    assert "205 reports (16 %) still have a resident as their last author (1 before)" in t
    assert "No study carries an emergency patient class (classes seen: O, I, S): the emergency turnaround is on Report 38." in t
    assert "AMX2 is booked above its opening hours (115.2 %)." in t
    assert "The quietest device is USDemoPhilips, which looks like a demo or test entry with no studies." in t
    assert "MR: 75 of 199 (38 %) created after the exam." in t
    assert "Nearly every cancelled appointment (66 of 66) has no patient class." in t
    assert "Images -11 % while studies +18 %: the modality mix changed." in t
    assert "285 reports (-31, -9.8 %) flagged critical by the text reader (both hospitals' reports)." in t
    assert "Notifications are off, so none were sent or acknowledged." in t
    assert "sabine.khoury@sj 38 (+37)" in t
    assert "normal" not in t.lower()


def test_to_act_on_this_week_is_ordered_and_short():
    acts = sections()["To act on this week"]
    assert acts[0].startswith("CR reporting takes 50.4 h") and acts[1].startswith("AMX2 is booked")
    assert any(a.startswith("tamina.rizk's reports fell to 178, from 346.") for a in acts)
    assert any(a.startswith("Remove USDemoPhilips from the device list") for a in acts)
    assert len(acts) <= 6


def test_only_the_blocks_shown_are_written_about():
    s = sections(["summary", "volume"])
    assert set(s) <= {"In three lines", "Volume", "To act on this week"}
    assert "Reporting" not in s and "Devices" not in s
    assert "Scheduling" in sections(["lead_time"]) and "Scheduling" not in sections(["tat"])


def test_crn_with_notifications_on_flags_what_is_late():
    cur = deepcopy(CUR)
    cur["crn"] = {"critical_count": 20, "total_notifications": 18, "acknowledged": 15, "overdue": 2, "open": 1,
                  "median_ack_min": 14}
    s = sections(["crn"], cur=cur, crn_enabled=True)
    assert "2 went overdue, 1 still open." in s["Critical results"]
    assert s["To act on this week"][0].startswith("3 critical result notifications overdue or open.")


def test_missing_sections_never_break_it():
    assert analyse({}, {}, {}, None) == [] or isinstance(analyse({}, {}, {}, None), list)
    analyse({"kpis": {"total_studies": 5}}, {}, {}, None)
